// SPDX-FileCopyrightText: Copyright (c) 2026 The journal Authors
// SPDX-License-Identifier: Apache-2.0

package streams

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/llingr/journal/codec"
	"github.com/llingr/journal/internal/decode"
	"github.com/llingr/journal/internal/ports"
	"github.com/llingr/journal/persistence"
)

// Repository reads, creates and appends the streams of one aggregate, one stream per
// aggregate: Create, CreateAll, Append, AppendAll, Get and Head. S is the state its events
// fold to. journal.NewRepository builds one, and it is safe for concurrent use.
type Repository[S any] struct {
	store          persistence.Backend
	pipeline       *codec.Pipeline
	outbox         ports.Outbox
	newDomainEvent func(eventType string) (DomainEvent, error)
	apply          func(state S, event DomainEvent) (S, error)
	partitionKey   func(state Stream[S]) string
	stateCodec     *StateCodec[S]
}

// NewRepository is how journal.NewRepository builds a repository, and an application calls
// that instead. It wires a store, a codec pipeline, an outbox, nil when nothing is published, the
// lookup from a stored event type to an empty domain event, the fold, nil when the state never
// changes, a partition key, nil for the stream id, and how the state is stored, nil when it is
// not. Panics on a nil store, pipeline or lookup, or an incomplete state codec, as that is a
// wiring mistake.
func NewRepository[S any](store persistence.Backend, pipeline *codec.Pipeline,
	outbox ports.Outbox, newDomainEvent func(eventType string) (DomainEvent, error),
	apply func(state S, event DomainEvent) (S, error),
	partitionKey func(state Stream[S]) string, stateCodec *StateCodec[S]) *Repository[S] {
	if store == nil || pipeline == nil || newDomainEvent == nil {
		panic("streams.NewRepository needs a store, a pipeline and a domain event lookup")
	}
	if stateCodec != nil && !stateCodec.valid() {
		panic("streams.NewRepository needs a state codec with a version of 1 or above, a " +
			"known format, and both functions")
	}
	return &Repository[S]{
		store:          store,
		pipeline:       pipeline,
		outbox:         outbox,
		newDomainEvent: newDomainEvent,
		apply:          apply,
		partitionKey:   partitionKey,
		stateCodec:     stateCodec,
	}
}

// Get reads a stream at its last version. A repository that stores its state reads that, one
// row, and folds the events from version 1 only when there is none, when another version of
// the application stored it, or when it is behind the stream, as a writer that stores no
// state leaves it; it then stores what it folded. A stream with no events is the zero state
// at version 0.
func (s *Repository[S]) Get(ctx context.Context, streamID uuid.UUID) (Stream[S], error) {
	if s.stateCodec == nil {
		return s.fold(ctx, streamID)
	}

	stored, head, err := s.store.QueryState(ctx, streamID)
	if err != nil {
		return Stream[S]{}, err
	}
	if head == 0 {
		return Stream[S]{
			streamID: streamID,
			folded:   true,
		}, nil
	}
	if stored.Version == head && stored.StateVersion == s.stateCodec.Version {
		return s.fromState(ctx, stored)
	}

	stream, err := s.fold(ctx, streamID)
	if err != nil {
		return Stream[S]{}, err
	}
	state, err := s.persistedState(ctx, stream)
	if err != nil {
		return Stream[S]{}, err
	}
	// not stored when another writer stored a later state first, which is as good
	_, err = s.store.SaveState(ctx, state)
	if err != nil {
		return Stream[S]{}, err
	}
	return stream, nil
}

// Head reads only the stream's last version, for a caller whose state lives elsewhere and
// who appends without folding; the stream has the zero state and no events. A repository
// that folds or stores its state refuses an append to it with ErrStreamNotFolded, as the new
// events would land on the zero state rather than the stream's.
func (s *Repository[S]) Head(ctx context.Context, streamID uuid.UUID) (Stream[S], error) {
	version, err := s.store.QueryHead(ctx, streamID)
	if err != nil {
		return Stream[S]{}, err
	}
	return Stream[S]{
		streamID: streamID,
		version:  version,
	}, nil
}

// Create starts a stream with one event
func (s *Repository[S]) Create(ctx context.Context, streamID uuid.UUID, payload DomainEvent,
	options ...AppendOption) (Stream[S], error) {
	return s.CreateAll(ctx, streamID, []DomainEvent{payload}, options...)
}

// CreateAll starts a stream with its first events, from version 1; ErrStreamExists when
// the stream has events already.
func (s *Repository[S]) CreateAll(ctx context.Context, streamID uuid.UUID,
	payloads []DomainEvent, options ...AppendOption) (Stream[S], error) {
	created, err := s.write(ctx, Stream[S]{
		streamID: streamID,
		folded:   true,
	}, payloads, options)
	// not wrapped as a conflict too, as a caller retries a conflict and a Create never succeeds
	if errors.Is(err, ErrVersionConflict) {
		return Stream[S]{}, fmt.Errorf("create stream %s: %w: %v", streamID, ErrStreamExists, err)
	}
	return created, err
}

// Append adds one event to a stream
func (s *Repository[S]) Append(ctx context.Context, stream Stream[S], payload DomainEvent,
	options ...AppendOption) (Stream[S], error) {
	return s.AppendAll(ctx, stream, []DomainEvent{payload}, options...)
}

// AppendAll adds events after the stream's version; ErrVersionConflict when another writer
// got there first, and the caller reads the stream again.
func (s *Repository[S]) AppendAll(ctx context.Context, stream Stream[S],
	payloads []DomainEvent, options ...AppendOption) (Stream[S], error) {
	if stream.version == 0 {
		return stream, fmt.Errorf("append to stream %s: %w", stream.streamID, ErrStreamNotCreated)
	}
	if !stream.folded && (s.apply != nil || s.stateCodec != nil) {
		return stream, fmt.Errorf("append to stream %s: %w", stream.streamID, ErrStreamNotFolded)
	}
	return s.write(ctx, stream, payloads, options)
}

// write folds the payloads, so one the aggregate refuses is never written, writes them and
// the state they fold to as one unit, then publishes, then runs the after-commit options
func (s *Repository[S]) write(ctx context.Context, stream Stream[S], payloads []DomainEvent,
	options []AppendOption) (Stream[S], error) {
	if len(payloads) == 0 {
		return stream, fmt.Errorf("write to stream %s: %w", stream.streamID, ErrNoEvents)
	}
	var chosen appendOptions
	for _, option := range options {
		option(&chosen)
	}

	unwrapped := make([]DomainEvent, len(payloads))
	metadata := make([][]byte, len(payloads))
	for index, payload := range payloads {
		unwrapped[index], metadata[index] = withoutMetadata(payload)
	}

	written := stream
	var err error
	if s.apply != nil {
		for _, payload := range unwrapped {
			written.state, err = s.apply(written.state, payload)
			if err != nil {
				return stream, fmt.Errorf("apply %s: %w", payload.EventType(), err)
			}
		}
	}

	partitionKey, err := s.storedPartitionKey(written)
	if err != nil {
		return stream, err
	}

	appendTs := time.Now().UTC().Truncate(time.Microsecond)
	newEvents := make([]persistence.PersistedEvent, 0, len(payloads))
	decodedEvents := make([]Event, 0, len(payloads))
	for index, payload := range unwrapped {
		var plain []byte
		plain, err = payload.MarshalPayload()
		if err != nil {
			return stream, fmt.Errorf("marshal %s: %w", payload.EventType(), err)
		}
		var encoded codec.Encoded
		encoded, err = s.pipeline.Encode(ctx, payload.EventType(), payload.DataFormat(), plain)
		if err != nil {
			return stream, fmt.Errorf("encode %s: %w", payload.EventType(), err)
		}
		persisted := persistence.PersistedEvent{
			StreamID:          stream.streamID,
			Version:           stream.version + int32(index) + 1,
			EventType:         payload.EventType(),
			EventData:         encoded.Data,
			DataFormat:        encoded.Format,
			DataCompression:   encoded.Compression,
			DataEncryption:    encoded.Encryption,
			DataEncryptionKid: encoded.EncryptionKeyId,
			EventAppendTs:     appendTs,
			Metadata:          metadata[index],
			PartitionKey:      partitionKey,
		}
		newEvents = append(newEvents, persisted)
		decodedEvents = append(decodedEvents, decodedEvent(persisted, plain))
	}

	written.version = newEvents[len(newEvents)-1].Version
	written.lastEventType = newEvents[len(newEvents)-1].EventType
	written.lastAppendTs = appendTs
	var state *persistence.PersistedState
	if s.stateCodec != nil {
		var persisted persistence.PersistedState
		persisted, err = s.persistedState(ctx, written)
		if err != nil {
			return stream, err
		}
		state = &persisted
	}

	var earliestPending int32
	earliestPending, err = s.store.Append(ctx, persistence.AppendRequest{
		StreamID: stream.streamID,
		Events:   newEvents,
		AppendTs: appendTs,
		State:    state,
		Publish:  s.outbox != nil,
	})
	if err != nil {
		return stream, err
	}

	// committed: from here nothing fails the write, and a caller that goes away does not
	// stop what follows
	committedCtx := context.WithoutCancel(ctx)
	if s.outbox != nil {
		s.outbox.Publish(committedCtx, stream.streamID, earliestPending, newEvents)
	}
	for _, afterCommit := range chosen.afterCommit {
		afterCommit(committedCtx)
	}

	written.events = make([]Event, 0, len(stream.events)+len(decodedEvents))
	written.events = append(written.events, stream.events...)
	written.events = append(written.events, decodedEvents...)
	written.domainEvents = make([]DomainEvent, 0, len(stream.domainEvents)+len(unwrapped))
	written.domainEvents = append(written.domainEvents, stream.domainEvents...)
	written.domainEvents = append(written.domainEvents, unwrapped...)
	return written, nil
}

// decode turns a stored event back into the domain event type that wrote it, and into the Event
// a caller sees
func (s *Repository[S]) decode(ctx context.Context,
	persisted persistence.PersistedEvent) (DomainEvent, Event, error) {
	payload, err := s.newDomainEvent(persisted.EventType)
	if err != nil {
		return nil, Event{}, fmt.Errorf("version %d: %w", persisted.Version, err)
	}

	plain, err := decode.Payload(ctx, s.pipeline, persisted)
	if err != nil {
		return nil, Event{}, err
	}

	err = payload.UnmarshalPayload(plain)
	if err != nil {
		return nil, Event{}, fmt.Errorf("unmarshal %s version %d: %w",
			persisted.EventType, persisted.Version, err)
	}
	return payload, decodedEvent(persisted, plain), nil
}

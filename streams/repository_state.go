// SPDX-FileCopyrightText: Copyright (c) 2026 The journal Authors
// SPDX-License-Identifier: Apache-2.0

package streams

import (
	"context"
	"fmt"

	"github.com/google/uuid"

	"github.com/llingr/journal/codec"
	"github.com/llingr/journal/persistence"
)

// foldPageSize is how many events one query of a fold reads, so a long stream is read in
// bounded steps rather than in one result
const foldPageSize = 250

// fold reads a stream from version 1 a page at a time, folding each event as it comes. A
// stream with no events is the zero state at version 0.
func (s *Repository[S]) fold(ctx context.Context, streamID uuid.UUID) (Stream[S], error) {
	stream := Stream[S]{
		streamID: streamID,
		folded:   true,
	}
	for {
		page, err := s.store.QueryStream(ctx, streamID, stream.version+1, foldPageSize)
		if err != nil {
			return Stream[S]{}, err
		}
		for _, event := range page {
			var payload DomainEvent
			var decoded Event
			payload, decoded, err = s.decode(ctx, event)
			if err != nil {
				return Stream[S]{}, err
			}
			stream.events = append(stream.events, decoded)
			stream.domainEvents = append(stream.domainEvents, payload)
			stream.state, err = s.apply(stream.state, payload)
			if err != nil {
				return Stream[S]{}, fmt.Errorf("apply %s version %d: %w",
					event.EventType, event.Version, err)
			}
			stream.version = event.Version
			stream.lastEventType = event.EventType
			stream.lastAppendTs = event.EventAppendTs
		}
		if len(page) < foldPageSize {
			return stream, nil
		}
	}
}

// fromState is the stream at the version its stored state is folded to. A state that does not
// decode or unmarshal fails the read rather than being folded again, as that is a bug in the
// codec or a state version not increased, which a silent fold would hide.
func (s *Repository[S]) fromState(ctx context.Context,
	stored persistence.PersistedState) (Stream[S], error) {
	plain, err := s.pipeline.Decode(ctx, codec.Encoded{
		Data:            stored.StateData,
		Format:          stored.DataFormat,
		Compression:     stored.DataCompression,
		Encryption:      stored.DataEncryption,
		EncryptionKeyId: stored.DataEncryptionKid,
	})
	if err != nil {
		return Stream[S]{}, fmt.Errorf("decode the state of stream %s at version %d: %w",
			stored.StreamID, stored.Version, err)
	}
	state, err := s.stateCodec.Unmarshal(plain)
	if err != nil {
		return Stream[S]{}, fmt.Errorf("unmarshal the state of stream %s at version %d: %w",
			stored.StreamID, stored.Version, err)
	}
	return Stream[S]{
		streamID:      stored.StreamID,
		version:       stored.Version,
		state:         state,
		lastEventType: stored.LastEventType,
		lastAppendTs:  stored.LastAppendTs,
		folded:        true,
	}, nil
}

// persistedState is the stream's state as stored, encoded as its last event type is
func (s *Repository[S]) persistedState(ctx context.Context,
	stream Stream[S]) (persistence.PersistedState, error) {
	plain, err := s.stateCodec.Marshal(stream.state)
	if err != nil {
		return persistence.PersistedState{}, fmt.Errorf(
			"marshal the state of stream %s at version %d: %w", stream.streamID, stream.version,
			err)
	}
	encoded, err := s.pipeline.Encode(ctx, stream.lastEventType, s.stateCodec.Format, plain)
	if err != nil {
		return persistence.PersistedState{}, fmt.Errorf(
			"encode the state of stream %s at version %d: %w", stream.streamID, stream.version,
			err)
	}
	partitionKey, err := s.storedPartitionKey(stream)
	if err != nil {
		return persistence.PersistedState{}, err
	}
	return persistence.PersistedState{
		StreamID:          stream.streamID,
		Version:           stream.version,
		StateVersion:      s.stateCodec.Version,
		LastEventType:     stream.lastEventType,
		LastAppendTs:      stream.lastAppendTs,
		StateData:         encoded.Data,
		DataFormat:        encoded.Format,
		DataCompression:   encoded.Compression,
		DataEncryption:    encoded.Encryption,
		DataEncryptionKid: encoded.EncryptionKeyId,
		PartitionKey:      partitionKey,
	}, nil
}

// storedPartitionKey is the stream's key as stored: empty for the stream id, which costs a
// byte where the id would cost 37
func (s *Repository[S]) storedPartitionKey(stream Stream[S]) (string, error) {
	if s.partitionKey == nil {
		return "", nil
	}
	partitionKey := s.partitionKey(stream)
	if partitionKey == "" {
		return "", fmt.Errorf("write to stream %s: %w", stream.streamID, ErrNoPartitionKey)
	}
	if partitionKey == stream.streamID.String() {
		return "", nil
	}
	return partitionKey, nil
}

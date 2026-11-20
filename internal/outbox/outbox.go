// SPDX-FileCopyrightText: Copyright (c) 2026 The journal Authors
// SPDX-License-Identifier: Apache-2.0

package outbox

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/llingr/journal/codec"
	"github.com/llingr/journal/internal/decode"
	"github.com/llingr/journal/persistence"
	"github.com/llingr/journal/publish"
	"github.com/llingr/journal/streams"
)

const (
	DefaultSweepInterval = 3 * time.Hour
	MinimumSweepInterval = 15 * time.Second
	MaximumSweepInterval = 7 * 24 * time.Hour

	// short enough for an app's shutdown during a rolling deploy
	DefaultPublishTimeout = 10 * time.Second
	MinimumPublishTimeout = time.Second
	MaximumPublishTimeout = time.Minute

	DefaultPublishBatchSize = 25
	MinimumPublishBatchSize = 1
	MaximumPublishBatchSize = 1000

	// a sweep takes rows older than this, leaving a publish still in flight to finish; it is
	// measured against another host's clock, and skew only causes a repeat
	sweepAge = time.Minute
	// a timed sweep reaches back this many intervals, so consecutive sweeps overlap
	sweepWindows = 3
	// streams a sweep lists at a time
	sweepStreams = 100
	// versions read from the store at a time, so neither a backlog of failed publishes nor a
	// long republish is ever read whole
	readPageSize = 250
)

type outbox struct {
	store          Backend
	pipeline       *codec.Pipeline
	publisher      publish.Publisher
	hooks          publish.Hooks
	sweepInterval  time.Duration
	publishTimeout time.Duration
	batchSize      int

	lifecycle  sync.Mutex         // guards stop and isShutDown
	stop       context.CancelFunc // ends the sweeps, set by the one Start that runs them
	isShutDown bool               // so a Start after Shutdown runs nothing
	stopped    chan struct{}      // closed once the sweeps have ended
}

// Option configures an Outbox
type Option func(o *outbox)

// WithSweepInterval sets how often the sweep runs, DefaultSweepInterval without it; panics
// outside MinimumSweepInterval to MaximumSweepInterval, as that is a wiring mistake
func WithSweepInterval(interval time.Duration) Option {
	if interval < MinimumSweepInterval || interval > MaximumSweepInterval {
		panic(fmt.Sprintf("journal.WithSweepInterval(%s) is outside %s to %s",
			interval, MinimumSweepInterval, MaximumSweepInterval))
	}
	return func(o *outbox) {
		o.sweepInterval = interval
	}
}

// WithPublishTimeout bounds each call to the publisher, DefaultPublishTimeout without it, so
// an outage fails a publish rather than holding the writer; panics outside
// MinimumPublishTimeout to MaximumPublishTimeout, as that is a wiring mistake
func WithPublishTimeout(timeout time.Duration) Option {
	if timeout < MinimumPublishTimeout || timeout > MaximumPublishTimeout {
		panic(fmt.Sprintf("journal.WithPublishTimeout(%s) is outside %s to %s",
			timeout, MinimumPublishTimeout, MaximumPublishTimeout))
	}
	return func(o *outbox) {
		o.publishTimeout = timeout
	}
}

// WithPublishBatchSize sets the most events one call to the publisher sends,
// DefaultPublishBatchSize without it; panics outside MinimumPublishBatchSize to
// MaximumPublishBatchSize, as that is a wiring mistake
func WithPublishBatchSize(size int) Option {
	if size < MinimumPublishBatchSize || size > MaximumPublishBatchSize {
		panic(fmt.Sprintf("journal.WithPublishBatchSize(%d) is outside %d to %d",
			size, MinimumPublishBatchSize, MaximumPublishBatchSize))
	}
	return func(o *outbox) {
		o.batchSize = size
	}
}

// New returns an Outbox reading from the store, decoding through the pipeline and sending
// with the publisher. Panics on a nil collaborator, as that is a wiring mistake.
func New(store Backend, pipeline *codec.Pipeline, publisher publish.Publisher, hooks publish.Hooks,
	options ...Option) Outbox {
	if store == nil || pipeline == nil || publisher == nil {
		panic("outbox.New needs a store, a pipeline and a publisher")
	}
	built := &outbox{
		store:          store,
		pipeline:       pipeline,
		publisher:      publisher,
		hooks:          hooks,
		sweepInterval:  DefaultSweepInterval,
		publishTimeout: DefaultPublishTimeout,
		batchSize:      DefaultPublishBatchSize,
		stopped:        make(chan struct{}),
	}
	for _, option := range options {
		option(built)
	}
	return built
}

// Publish sends the committed events from memory, reading only earlier versions still
// pending, unbound from a caller that goes away after its commit so it does not hold its
// events back
func (o *outbox) Publish(ctx context.Context, streamID uuid.UUID, earliestPending int32,
	committed []persistence.PersistedEvent) {
	if len(committed) == 0 {
		return
	}
	ctx = context.WithoutCancel(ctx)

	var earlier []int32
	var err error
	if earliestPending > 0 && earliestPending < committed[0].Version {
		earlier, err = o.pendingThrough(ctx, streamID, committed[0].Version-1)
		if err != nil {
			o.failed(ctx, streamID, err)
			return
		}
	}
	_, err = o.sendPending(ctx, streamID, earlier, committed)
	if err != nil {
		o.failed(ctx, streamID, err)
	}
}

// pendingThrough is the stream's pending versions through throughVersion, in order
func (o *outbox) pendingThrough(ctx context.Context, streamID uuid.UUID,
	throughVersion int32) ([]int32, error) {
	versions, err := o.store.QueryPending(ctx, streamID)
	if err != nil {
		return nil, fmt.Errorf("publish stream %s: %w", streamID, err)
	}
	for len(versions) > 0 && versions[len(versions)-1] > throughVersion {
		versions = versions[:len(versions)-1]
	}
	return versions, nil
}

// sendPending sends the pending versions given and then the events after them, reading the
// versions a page of at most readPageSize at a time and sending each page before reading the
// next. The events after go with the last page, and nothing is sent past the first failure,
// so they never overtake a version still pending. It returns how many are durable.
func (o *outbox) sendPending(ctx context.Context, streamID uuid.UUID, pending []int32,
	after []persistence.PersistedEvent) (int, error) {
	sent := 0
	for {
		pageLength := 0
		for pageLength < len(pending) && pending[pageLength]-pending[0] < readPageSize {
			pageLength++
		}
		page, err := o.readVersions(ctx, streamID, pending[:pageLength])
		if err != nil {
			return sent, err
		}
		pending = pending[pageLength:]
		if len(pending) == 0 {
			page = append(page, after...)
		}
		var pageSent int
		pageSent, err = o.sendAll(ctx, streamID, page)
		sent += pageSent
		if err != nil || len(pending) == 0 {
			return sent, err
		}
	}
}

// readVersions reads the events of the versions given, in order; a version whose outbox row
// was deleted, such as one the destination refuses for good, is not among them
func (o *outbox) readVersions(ctx context.Context, streamID uuid.UUID,
	versions []int32) ([]persistence.PersistedEvent, error) {
	if len(versions) == 0 {
		return nil, nil
	}
	stored, err := o.store.QueryStreamRange(ctx, streamID, versions[0], versions[len(versions)-1])
	if err != nil {
		return nil, fmt.Errorf("publish stream %s: %w", streamID, err)
	}
	byVersion := make(map[int32]persistence.PersistedEvent, len(stored))
	for _, event := range stored {
		byVersion[event.Version] = event
	}

	read := make([]persistence.PersistedEvent, 0, len(versions))
	for _, version := range versions {
		event, found := byVersion[version]
		if !found {
			return nil, fmt.Errorf("publish stream %s version %d: %w",
				streamID, version, streams.ErrVersionMissing)
		}
		read = append(read, event)
	}
	return read, nil
}

// sendAll sends the events in their order, a batch at a time of one partition key and at
// most batchSize, stops at the first failure, then deletes the outbox rows through the last
// event published; it returns how many are durable
func (o *outbox) sendAll(ctx context.Context, streamID uuid.UUID,
	toSend []persistence.PersistedEvent) (int, error) {
	var lastSent int32
	var sent int
	var err error
	for start := 0; start < len(toSend); {
		end := start + 1
		for end < len(toSend) && end-start < o.batchSize &&
			toSend[end].PublishKey() == toSend[start].PublishKey() {
			end++
		}
		var durable int
		durable, err = o.send(ctx, toSend[start:end])
		if durable > 0 {
			lastSent = toSend[start+durable-1].Version
			sent += durable
		}
		if err != nil {
			break
		}
		start = end
	}
	if lastSent == 0 {
		return 0, err
	}
	if o.hooks.Published != nil {
		o.hooks.Published(streamID, toSend[0].Version, lastSent)
	}
	return sent, errors.Join(err, o.clear(ctx, streamID, lastSent))
}

// clear deletes the stream's outbox rows through the last version sent
func (o *outbox) clear(ctx context.Context, streamID uuid.UUID, lastSent int32) error {
	err := o.store.DeletePublished(ctx, streamID, lastSent)
	if err != nil {
		return fmt.Errorf("clear stream %s through version %d: %w", streamID, lastSent, err)
	}
	return nil
}

// send decodes one batch and hands it to the publisher within the publish timeout, a panic
// included as an error, and returns how many of it are durable
func (o *outbox) send(ctx context.Context,
	batch []persistence.PersistedEvent) (durable int, err error) {
	first := batch[0]
	published := make([]streams.Event, 0, len(batch))
	for _, persisted := range batch {
		var plain []byte
		plain, err = decode.Payload(ctx, o.pipeline, persisted)
		if err != nil {
			return 0, err
		}
		published = append(published, streams.Event{
			StreamID:   persisted.StreamID,
			Version:    persisted.Version,
			EventType:  persisted.EventType,
			DataFormat: persisted.DataFormat,
			AppendTs:   persisted.EventAppendTs,
			Payload:    plain,
			Metadata:   persisted.Metadata,
		})
	}

	defer func() {
		recovered := recover()
		if recovered != nil {
			durable = 0
			err = fmt.Errorf("publish stream %s from version %d: %w: %v",
				first.StreamID, first.Version, publish.ErrPublisherPanic, recovered)
		}
	}()
	ctx, cancel := context.WithTimeout(ctx, o.publishTimeout)
	defer cancel()
	durable, err = o.publisher.PublishAll(ctx, first.PublishKey(), published)
	if err == nil {
		return len(batch), nil
	}
	return min(max(durable, 0), len(batch)), fmt.Errorf("publish stream %s from version %d: %w",
		first.StreamID, first.Version, err)
}

// failed reports a publish's error, except the cancellation of a shutdown
func (o *outbox) failed(ctx context.Context, streamID uuid.UUID, err error) {
	if o.hooks.Failed != nil && ctx.Err() == nil {
		o.hooks.Failed(streamID, err)
	}
}

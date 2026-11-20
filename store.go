// SPDX-FileCopyrightText: Copyright (c) 2026 The journal Authors
// SPDX-License-Identifier: Apache-2.0

package journal

import (
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/klauspost/compress/zstd"

	"github.com/llingr/journal/codec"
	"github.com/llingr/journal/codec/compress"
	"github.com/llingr/journal/internal/outbox"
	"github.com/llingr/journal/persistence"
	"github.com/llingr/journal/publish"
)

// The smallest payload compressed, in bytes: below it compression's own framing costs about
// what it saves
const (
	defaultCompressionMinimumBytes = 128
	minimumCompressionMinimumBytes = 1
	maximumCompressionMinimumBytes = 2048
)

// Store is one database's events: its backend, how payloads are stored, and the publishing
// of every committed event. An application builds one per database and shares it between its
// repositories; it is safe for concurrent use.
type Store struct {
	backend  persistence.Backend
	pipeline *codec.Pipeline
	outbox   outbox.Outbox
}

// StoreOption configures a Store
type StoreOption func(settings *storeSettings)

type storeSettings struct {
	publisher            publish.Publisher
	compressor           compress.Compressor
	compressMinimumBytes int
	hooks                publish.Hooks
	outbox               []outbox.Option
}

// NewStore returns the Store over a backend. Payloads of 128 bytes or more are compressed with zstd
// unless WithDataCompression says otherwise, and payloads are never encrypted. Panics without a
// backend, as that is a wiring mistake.
func NewStore(backend persistence.Backend, options ...StoreOption) *Store {
	if backend == nil {
		panic("journal.NewStore needs a backend")
	}
	settings := storeSettings{
		compressor:           compressorFor(compress.Zstd),
		compressMinimumBytes: defaultCompressionMinimumBytes,
	}
	for _, option := range options {
		option(&settings)
	}

	// every compression is read, so a change of setting leaves older rows readable
	pipelineOptions := []codec.Option{
		codec.DecompressWith(compress.NewGzip(gzip.DefaultCompression), compress.NewLZ4(),
			compress.NewSnappy(), compress.NewZstd(zstd.SpeedDefault)),
	}
	if settings.compressor != nil {
		pipelineOptions = append(pipelineOptions, codec.CompressWith(settings.compressor),
			codec.WithSelector(func(_ string, plainSize int) (bool, bool) {
				return plainSize >= settings.compressMinimumBytes, true
			}))
	}
	pipeline, err := codec.NewPipeline(pipelineOptions...)
	if err != nil {
		panic(fmt.Errorf("journal.NewStore: %w", err))
	}

	eventStore := &Store{
		backend:  backend,
		pipeline: pipeline,
	}
	if settings.publisher != nil {
		eventStore.outbox = outbox.New(backend, pipeline, settings.publisher, settings.hooks,
			settings.outbox...)
	}
	return eventStore
}

// Start runs the sweep, at once and then every interval, which publishes events a crash or a
// failed publish left behind; without a publisher there is nothing to sweep. A Start after the
// first, or after Shutdown, does nothing.
func (s *Store) Start(ctx context.Context) {
	if s.outbox != nil {
		s.outbox.Start(ctx)
	}
}

// Shutdown stops the sweep for good, waiting for the one in hand until ctx ends; called again,
// as after its deadline, it waits again. Start and Shutdown are safe from any goroutine.
func (s *Store) Shutdown(ctx context.Context) error {
	if s.outbox == nil {
		return nil
	}
	return s.outbox.Shutdown(ctx)
}

// Sweep publishes by hand the events still unpublished that were appended in the last window
// and more than a minute ago, such as once a broker is back after an outage, and returns how
// many streams it published. A younger event is left to the publish in flight for it.
func (s *Store) Sweep(ctx context.Context, window time.Duration) (int, error) {
	if s.outbox == nil {
		return 0, nil
	}
	return s.outbox.Sweep(ctx, window)
}

// ErrNoPublisher is a republish on a store built without WithPublisher
var ErrNoPublisher = errors.New("the store has no publisher")

// RepublishEvents sends a stream's events from fromVersion on again, such as to a downstream
// that lost them: those still unpublished with publish.IfUnpublished, or every one to the
// stream's last with publish.Always. Each goes under the key it was appended with, and the
// count is how many events are durable. It refuses with publish.ErrEarlierUnpublished while a
// version below fromVersion is unpublished, as sending past it would break the stream's order.
func (s *Store) RepublishEvents(ctx context.Context, streamID uuid.UUID, fromVersion int32,
	condition publish.RepublishCondition) (int, error) {
	if s.outbox == nil {
		return 0, ErrNoPublisher
	}
	return s.outbox.RepublishEvents(ctx, streamID, fromVersion, condition)
}

// RepublishEvent sends one version again: with publish.IfUnpublished only while it is
// unpublished, publish.ErrAlreadyPublished otherwise, and with publish.Always whether or not. It
// refuses with publish.ErrEarlierUnpublished while an earlier version is unpublished.
func (s *Store) RepublishEvent(ctx context.Context, streamID uuid.UUID, version int32,
	condition publish.RepublishCondition) error {
	if s.outbox == nil {
		return ErrNoPublisher
	}
	return s.outbox.RepublishEvent(ctx, streamID, version, condition)
}

// WithPublisher sends every committed event to the publisher. Without it nothing is published
// and no outbox rows are written.
func WithPublisher(publisher publish.Publisher) StoreOption {
	return func(settings *storeSettings) {
		settings.publisher = publisher
	}
}

// WithDataCompression compresses payloads with compression, when that makes them smaller; zstd
// without it, and compress.None stores every payload as it is. Panics on any
// other value, as that is a wiring mistake.
func WithDataCompression(compression compress.DataCompression) StoreOption {
	compressor := compressorFor(compression)
	return func(settings *storeSettings) {
		settings.compressor = compressor
	}
}

func compressorFor(compression compress.DataCompression) compress.Compressor {
	switch compression {
	case compress.Zstd:
		return compress.NewZstd(zstd.SpeedDefault)
	case compress.Gzip:
		return compress.NewGzip(gzip.DefaultCompression)
	case compress.LZ4:
		return compress.NewLZ4()
	case compress.Snappy:
		return compress.NewSnappy()
	case compress.None:
		return nil
	}
	panic(fmt.Sprintf("journal.WithDataCompression: unknown compression %q", compression))
}

// WithSweepInterval sets how often the sweep runs, three hours without it; panics outside
// fifteen seconds to a week
func WithSweepInterval(interval time.Duration) StoreOption {
	outboxOption := outbox.WithSweepInterval(interval)
	return func(settings *storeSettings) {
		settings.outbox = append(settings.outbox, outboxOption)
	}
}

// WithPublishTimeout bounds each call to the publisher, ten seconds without it; panics outside
// one second to a minute
func WithPublishTimeout(timeout time.Duration) StoreOption {
	outboxOption := outbox.WithPublishTimeout(timeout)
	return func(settings *storeSettings) {
		settings.outbox = append(settings.outbox, outboxOption)
	}
}

// WithPublishBatchSize sets the most events one call to the publisher sends, 25 without it;
// panics outside 1 to 1000
func WithPublishBatchSize(size int) StoreOption {
	outboxOption := outbox.WithPublishBatchSize(size)
	return func(settings *storeSettings) {
		settings.outbox = append(settings.outbox, outboxOption)
	}
}

// WithHooks observes publishing: what was published, what failed, and sweeps that found work
func WithHooks(hooks publish.Hooks) StoreOption {
	return func(settings *storeSettings) {
		settings.hooks = hooks
	}
}

// WithCompressionMinimumBytes compresses payloads of at least minimum bytes, 128 without it;
// panics outside 1 to 2048
func WithCompressionMinimumBytes(minimum int) StoreOption {
	if minimum < minimumCompressionMinimumBytes || minimum > maximumCompressionMinimumBytes {
		panic(fmt.Sprintf("journal.WithCompressionMinimumBytes: %d is outside %d to %d", minimum,
			minimumCompressionMinimumBytes, maximumCompressionMinimumBytes))
	}
	return func(settings *storeSettings) {
		settings.compressMinimumBytes = minimum
	}
}

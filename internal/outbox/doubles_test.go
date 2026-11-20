// SPDX-FileCopyrightText: Copyright (c) 2026 The journal Authors
// SPDX-License-Identifier: Apache-2.0

package outbox_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/llingr/journal/codec"
	"github.com/llingr/journal/codec/compress"
	"github.com/llingr/journal/codec/encrypt"
	"github.com/llingr/journal/internal/outbox"
	"github.com/llingr/journal/persistence"
	"github.com/llingr/journal/publish"
	"github.com/llingr/journal/streams"
)

// pendingRow is a version waiting to be published and when it was appended
type pendingRow struct {
	version  int32
	appendTs time.Time
}

// memoryStore holds streams' events and pending rows in memory, as a store would
type memoryStore struct {
	mutex              sync.Mutex
	events             map[uuid.UUID][]persistence.PersistedEvent
	pending            map[uuid.UUID][]pendingRow
	pendingStreamCalls int
	pendingStreamsErr  error
	pendingErr         error    // fails QueryPending
	rangeErr           error    // fails QueryStreamRange
	headErr            error    // fails QueryHead
	deleteErr          error    // fails DeletePublished
	reads              []string // each QueryStreamRange range
}

// fail sets the error one call returns, nil to let it succeed again
func (m *memoryStore) fail(call *error, err error) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	*call = err
}

// deleteEvent removes one version's event and keeps its pending row, as an owner deleting the
// event alone leaves it
func (m *memoryStore) deleteEvent(streamID uuid.UUID, version int32) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.events[streamID] = slices.DeleteFunc(m.events[streamID],
		func(event persistence.PersistedEvent) bool {
			return event.Version == version
		})
}

func newMemoryStore() *memoryStore {
	return &memoryStore{
		events:  make(map[uuid.UUID][]persistence.PersistedEvent),
		pending: make(map[uuid.UUID][]pendingRow),
	}
}

// append stores versions of a stream at appendTs under the stream id as the key, leaves them
// pending and returns them as committed
func (m *memoryStore) append(streamID uuid.UUID, appendTs time.Time,
	versions ...int32) []persistence.PersistedEvent {
	return m.appendKeyed(streamID, appendTs, "", versions...)
}

// appendKeyed is append under partitionKey, empty for the stream id
func (m *memoryStore) appendKeyed(streamID uuid.UUID, appendTs time.Time, partitionKey string,
	versions ...int32) []persistence.PersistedEvent {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	var committed []persistence.PersistedEvent
	for _, version := range versions {
		event := persistence.PersistedEvent{
			StreamID:        streamID,
			Version:         version,
			EventType:       "Happened",
			EventData:       []byte(fmt.Sprintf("version %d", version)),
			DataFormat:      codec.Text,
			DataCompression: compress.None,
			DataEncryption:  encrypt.None,
			EventAppendTs:   appendTs,
			PartitionKey:    partitionKey,
		}
		committed = append(committed, event)
		m.events[streamID] = append(m.events[streamID], event)
		m.pending[streamID] = append(m.pending[streamID], pendingRow{
			version:  version,
			appendTs: appendTs,
		})
	}
	return committed
}

// readRanges is each range QueryStreamRange was asked for
func (m *memoryStore) readRanges() []string {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	return slices.Clone(m.reads)
}

// pendingVersions is the stream's versions still waiting
func (m *memoryStore) pendingVersions(streamID uuid.UUID) []int32 {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	var versions []int32
	for _, row := range m.pending[streamID] {
		versions = append(versions, row.version)
	}
	return versions
}

func (m *memoryStore) QueryPendingStreams(_ context.Context, appendedAfter time.Time,
	appendedBefore time.Time, limit int) ([]uuid.UUID, error) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	m.pendingStreamCalls++
	if m.pendingStreamsErr != nil {
		return nil, m.pendingStreamsErr
	}
	var streams []uuid.UUID
	for streamID, rows := range m.pending {
		for _, row := range rows {
			isInWindow := !row.appendTs.Before(appendedAfter) && row.appendTs.Before(appendedBefore)
			if isInWindow && len(streams) < limit {
				streams = append(streams, streamID)
				break
			}
		}
	}
	return streams, nil
}

func (m *memoryStore) QueryPending(_ context.Context, streamID uuid.UUID) ([]int32, error) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if m.pendingErr != nil {
		return nil, m.pendingErr
	}
	var pending []int32
	for _, row := range m.pending[streamID] {
		pending = append(pending, row.version)
	}
	return pending, nil
}

func (m *memoryStore) QueryHead(_ context.Context, streamID uuid.UUID) (int32, error) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if m.headErr != nil {
		return 0, m.headErr
	}
	stored := m.events[streamID]
	if len(stored) == 0 {
		return 0, nil
	}
	return stored[len(stored)-1].Version, nil
}

// deletePending removes one version's row, as an operator freeing a refused version does
func (m *memoryStore) deletePending(streamID uuid.UUID, version int32) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.pending[streamID] = slices.DeleteFunc(m.pending[streamID], func(row pendingRow) bool {
		return row.version == version
	})
}

func (m *memoryStore) QueryStreamRange(_ context.Context, streamID uuid.UUID, fromVersion int32,
	throughVersion int32) ([]persistence.PersistedEvent, error) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	m.reads = append(m.reads, fmt.Sprintf("%d to %d", fromVersion, throughVersion))
	if m.rangeErr != nil {
		return nil, m.rangeErr
	}
	var read []persistence.PersistedEvent
	for _, event := range m.events[streamID] {
		if event.Version >= fromVersion && event.Version <= throughVersion {
			read = append(read, event)
		}
	}
	return read, nil
}

func (m *memoryStore) DeletePublished(_ context.Context, streamID uuid.UUID,
	throughVersion int32) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if m.deleteErr != nil {
		return m.deleteErr
	}
	m.pending[streamID] = slices.DeleteFunc(m.pending[streamID], func(row pendingRow) bool {
		return row.version <= throughVersion
	})
	if len(m.pending[streamID]) == 0 {
		delete(m.pending, streamID)
	}
	return nil
}

// recordingPublisher records what reaches it; refuse, when set, decides each event's fate, and the
// events before the first it refuses are durable
type recordingPublisher struct {
	mutex     sync.Mutex
	published []publishedEvent
	batches   []string // each call's key and versions
	refuse    func(event streams.Event) error
}

// publishedEvent is an event as published and the key it went under
type publishedEvent struct {
	event        streams.Event
	partitionKey string
}

func (r *recordingPublisher) PublishAll(ctx context.Context, partitionKey string,
	toPublish []streams.Event) (int, error) {
	r.mutex.Lock()
	refuse := r.refuse
	r.batches = append(r.batches, fmt.Sprintf("%s %d to %d", partitionKey,
		toPublish[0].Version, toPublish[len(toPublish)-1].Version))
	r.mutex.Unlock()

	for index, event := range toPublish {
		if refuse != nil {
			err := refuse(event)
			if err != nil {
				return index, err
			}
		}
		if ctx.Err() != nil {
			return index, ctx.Err()
		}
		r.mutex.Lock()
		r.published = append(r.published, publishedEvent{event: event, partitionKey: partitionKey})
		r.mutex.Unlock()
	}
	return len(toPublish), nil
}

// versions is the versions published for a stream, in the order they arrived
func (r *recordingPublisher) versions(streamID uuid.UUID) []int32 {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	var versions []int32
	for _, published := range r.published {
		if published.event.StreamID == streamID {
			versions = append(versions, published.event.Version)
		}
	}
	return versions
}

// calls is each call's key and versions, in order
func (r *recordingPublisher) calls() []string {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	return slices.Clone(r.batches)
}

var errRefused = errors.New("record too large")

// failures records what the Failed hook reports
type failures struct {
	mutex   sync.Mutex
	errs    []error
	streams []uuid.UUID
}

func (f *failures) hook(streamID uuid.UUID, err error) {
	f.mutex.Lock()
	defer f.mutex.Unlock()
	f.streams = append(f.streams, streamID)
	f.errs = append(f.errs, err)
}

func (f *failures) reported() ([]uuid.UUID, []error) {
	f.mutex.Lock()
	defer f.mutex.Unlock()
	return slices.Clone(f.streams), slices.Clone(f.errs)
}

// newOutbox is an outbox over the test doubles with a pipeline that stores payloads as they are
func newOutbox(store *memoryStore, publisher *recordingPublisher, failed *failures,
	options ...outbox.Option) outbox.Outbox {
	pipeline, err := codec.NewPipeline()
	if err != nil {
		panic(err)
	}
	return outbox.New(store, pipeline, publisher, publish.Hooks{
		Failed: failed.hook,
	}, options...)
}

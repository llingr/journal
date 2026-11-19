// SPDX-FileCopyrightText: Copyright (c) 2026 The journal Authors
// SPDX-License-Identifier: Apache-2.0

// Package backendtest holds the contract every persistence.Backend implementation has
// to satisfy.
//
// The suite is a function taking a factory rather than a TestMain with package
// state. Each subtest asks the factory for a fresh store, so nothing is shared
// between them, a factory that cannot reach its backing store reports that
// against the subtest that needed it, and go test keeps its own semantics:
// -run filtering, subtest names and cleanup all behave normally.
package backendtest

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/llingr/journal/codec"
	"github.com/llingr/journal/codec/compress"
	"github.com/llingr/journal/codec/encrypt"
	"github.com/llingr/journal/persistence"
	"github.com/llingr/journal/streams"
)

// NewBackend returns a store with no events in it. It is given the subtest's own
// *testing.T so that it can skip or fail there rather than through a package
// variable, and so that it can register its teardown with t.Cleanup.
type NewBackend func(t *testing.T) persistence.Backend

// appendTs is the append timestamp wherever the value is not what is under
// test. Microsecond truncation matches the column.
var appendTs = time.Date(2026, time.September, 8, 12, 0, 0, 0, time.UTC)

// everyEvent is a QueryStream limit no test stream reaches
const everyEvent = 1_000_000

// RunContract runs every store test against one implementation
func RunContract(t *testing.T, newStore NewBackend) {
	t.Helper()

	t.Run("QueryStreamOfAnUnwrittenStreamIsEmpty", func(t *testing.T) {
		runQueryStreamOfAnUnwrittenStreamIsEmpty(t, newStore)
	})
	t.Run("AppendedEventsReadBackInVersionOrder", func(t *testing.T) {
		runAppendedEventsReadBackInVersionOrder(t, newStore)
	})
	t.Run("QueryStreamHonoursFromVersion", func(t *testing.T) {
		runQueryStreamHonoursFromVersion(t, newStore)
	})
	t.Run("QueryStreamReturnsOnlyTheStreamAsked", func(t *testing.T) {
		runQueryStreamReturnsOnlyTheStreamAsked(t, newStore)
	})
	t.Run("QueryHeadIsTheLastVersion", func(t *testing.T) {
		runQueryHeadIsTheLastVersion(t, newStore)
	})
	t.Run("PayloadBytesSurviveVerbatim", func(t *testing.T) {
		runPayloadBytesSurviveVerbatim(t, newStore)
	})
	t.Run("CodecColumnsSurviveVerbatim", func(t *testing.T) {
		runCodecColumnsSurviveVerbatim(t, newStore)
	})
	t.Run("MetadataIsOptional", func(t *testing.T) {
		runMetadataIsOptional(t, newStore)
	})
	t.Run("PartitionKeySurvivesVerbatim", func(t *testing.T) {
		runPartitionKeySurvivesVerbatim(t, newStore)
	})
	t.Run("AppendOfNoEventsIsAcceptedAndWritesNothing", func(t *testing.T) {
		runAppendOfNoEventsIsAcceptedAndWritesNothing(t, newStore)
	})
	t.Run("AppendRejectsVersionsThatAreNotConsecutive", func(t *testing.T) {
		runAppendRejectsVersionsThatAreNotConsecutive(t, newStore)
	})
	t.Run("AppendRejectsARepeatedVersion", func(t *testing.T) {
		runAppendRejectsARepeatedVersion(t, newStore)
	})
	t.Run("ARejectedAppendWritesNothing", func(t *testing.T) {
		runARejectedAppendWritesNothing(t, newStore)
	})
	t.Run("RacingAppendsOfOneVersionConflict", func(t *testing.T) {
		runRacingAppendsOfOneVersionConflict(t, newStore)
	})
	t.Run("AppendReturnsTheEarliestUnpublishedVersion", func(t *testing.T) {
		runAppendReturnsTheEarliestUnpublishedVersion(t, newStore)
	})
	t.Run("AnAppendNotPublishedLeavesNothingPending", func(t *testing.T) {
		runAnAppendNotPublishedLeavesNothingPending(t, newStore)
	})
	runStateContract(t, newStore)
	runOutboxContract(t, newStore)
}

func runQueryStreamOfAnUnwrittenStreamIsEmpty(t *testing.T, newStore NewBackend) {
	store, ctx := newStore(t), context.Background()

	streamEvents, err := store.QueryStream(ctx, uuid.New(), 1, everyEvent)
	if err != nil {
		t.Fatalf("read an unwritten stream: %v", err)
	}
	if len(streamEvents) != 0 {
		t.Fatalf("read an unwritten stream: got %d events, want 0", len(streamEvents))
	}
}

func runAppendedEventsReadBackInVersionOrder(t *testing.T, newStore NewBackend) {
	store := newStore(t)
	streamID := uuid.New()

	appended := []persistence.PersistedEvent{
		newEvent(streamID, 1), newEvent(streamID, 2), newEvent(streamID, 3),
	}
	if err := appendEvents(store, streamID, appended...); err != nil {
		t.Fatalf("append three events: %v", err)
	}

	read := requireQueryStream(t, store, streamID, 1)
	if len(read) != len(appended) {
		t.Fatalf("read back: got %d events, want %d", len(read), len(appended))
	}
	for index, event := range read {
		wantVersion := int32(index + 1)
		if event.Version != wantVersion {
			t.Errorf("event %d: version %d, want %d", index, event.Version, wantVersion)
		}
		if event.StreamID != streamID {
			t.Errorf("event %d: stream %s, want %s", index, event.StreamID, streamID)
		}
		if event.EventType != appended[index].EventType {
			t.Errorf("event %d: type %q, want %q",
				index, event.EventType, appended[index].EventType)
		}
		if !event.EventAppendTs.Equal(appendTs) {
			t.Errorf("event %d: append ts %s, want %s", index, event.EventAppendTs, appendTs)
		}
	}
}

func runQueryStreamHonoursFromVersion(t *testing.T, newStore NewBackend) {
	store := newStore(t)
	streamID := uuid.New()

	appendVersions(t, store, streamID, 1, 2, 3, 4)

	read := requireQueryStream(t, store, streamID, 3)
	if len(read) != 2 {
		t.Fatalf("read from version 3: got %d events, want 2", len(read))
	}
	if read[0].Version != 3 || read[1].Version != 4 {
		t.Fatalf("read from version 3: got versions %d and %d, want 3 and 4",
			read[0].Version, read[1].Version)
	}
}

func runQueryStreamReturnsOnlyTheStreamAsked(t *testing.T, newStore NewBackend) {
	store := newStore(t)
	wanted, other := uuid.New(), uuid.New()

	appendVersions(t, store, wanted, 1, 2)
	appendVersions(t, store, other, 1, 2, 3)

	read := requireQueryStream(t, store, wanted, 1)
	if len(read) != 2 {
		t.Fatalf("read one of two streams: got %d events, want 2", len(read))
	}
	for index, event := range read {
		if event.StreamID != wanted {
			t.Errorf("event %d belongs to stream %s, want %s", index, event.StreamID, wanted)
		}
	}
}

func runQueryHeadIsTheLastVersion(t *testing.T, newStore NewBackend) {
	store, ctx := newStore(t), context.Background()
	streamID := uuid.New()

	head, err := store.QueryHead(ctx, streamID)
	if err != nil || head != 0 {
		t.Fatalf("head of an unwritten stream: %d, %v, want 0", head, err)
	}
	appendVersions(t, store, streamID, 1, 2, 3)
	appendVersions(t, store, uuid.New(), 1, 2, 3, 4, 5)
	head, err = store.QueryHead(ctx, streamID)
	if err != nil || head != 3 {
		t.Fatalf("head after versions 1 to 3: %d, %v, want 3", head, err)
	}
}

func runPayloadBytesSurviveVerbatim(t *testing.T, newStore NewBackend) {
	store := newStore(t)
	streamID := uuid.New()

	// Bytes a text column would mangle: a NUL, a high byte, and nothing that
	// is valid UTF-8.
	payload := []byte{0x00, 0xFF, 0x01, 0x80, 0xC0, 0x0A, 0x0D}
	event := newEvent(streamID, 1)
	event.EventData = payload

	if err := appendEvents(store, streamID, event); err != nil {
		t.Fatalf("append a binary payload: %v", err)
	}

	read := requireQueryStream(t, store, streamID, 1)
	if len(read) != 1 {
		t.Fatalf("read back a binary payload: got %d events, want 1", len(read))
	}
	if !bytes.Equal(read[0].EventData, payload) {
		t.Fatalf("payload round trip: got %x, want %x", read[0].EventData, payload)
	}
}

func runCodecColumnsSurviveVerbatim(t *testing.T, newStore NewBackend) {
	store := newStore(t)
	streamID := uuid.New()

	event := newEvent(streamID, 1)
	event.DataFormat = codec.JSON
	event.DataCompression = compress.Gzip
	event.DataEncryption = encrypt.AESGCM
	event.DataEncryptionKid = "key-2026-09"

	if err := appendEvents(store, streamID, event); err != nil {
		t.Fatalf("append with codec columns set: %v", err)
	}

	read := requireQueryStream(t, store, streamID, 1)
	if len(read) != 1 {
		t.Fatalf("read back codec columns: got %d events, want 1", len(read))
	}
	got := read[0]
	if got.DataFormat != event.DataFormat {
		t.Errorf("data format: got %v, want %v", got.DataFormat, event.DataFormat)
	}
	if got.DataCompression != event.DataCompression {
		t.Errorf("data compression: got %v, want %v", got.DataCompression, event.DataCompression)
	}
	if got.DataEncryption != event.DataEncryption {
		t.Errorf("data encryption: got %v, want %v", got.DataEncryption, event.DataEncryption)
	}
	if got.DataEncryptionKid != event.DataEncryptionKid {
		t.Errorf("key id: got %q, want %q", got.DataEncryptionKid, event.DataEncryptionKid)
	}
}

func runMetadataIsOptional(t *testing.T, newStore NewBackend) {
	store := newStore(t)
	streamID := uuid.New()

	withMetadata := newEvent(streamID, 1)
	withMetadata.Metadata = json.RawMessage(`{"correlationId":"abc"}`)
	withoutMetadata := newEvent(streamID, 2)
	withoutMetadata.Metadata = nil

	err := appendEvents(store, streamID, withMetadata, withoutMetadata)
	if err != nil {
		t.Fatalf("append with and without metadata: %v", err)
	}

	read := requireQueryStream(t, store, streamID, 1)
	if len(read) != 2 {
		t.Fatalf("read back metadata: got %d events, want 2", len(read))
	}
	if len(read[0].Metadata) == 0 {
		t.Errorf("event 1 lost its metadata")
	}
	if len(read[1].Metadata) != 0 {
		t.Errorf("event 2 gained metadata %q, want none", read[1].Metadata)
	}
}

func runAppendOfNoEventsIsAcceptedAndWritesNothing(t *testing.T, newStore NewBackend) {
	store := newStore(t)
	streamID := uuid.New()

	if err := appendEvents(store, streamID); err != nil {
		t.Fatalf("append no events: %v", err)
	}

	read := requireQueryStream(t, store, streamID, 1)
	if len(read) != 0 {
		t.Fatalf("append no events wrote %d rows, want 0", len(read))
	}
}

// runAppendRejectsVersionsThatAreNotConsecutive: the store refuses a request whose versions
// skip, repeat, run backwards or start below 1, and writes none of it
func runAppendRejectsVersionsThatAreNotConsecutive(t *testing.T, newStore NewBackend) {
	store := newStore(t)

	for name, versions := range map[string][]int32{
		"a gap":      {1, 3},
		"a repeat":   {1, 1},
		"backwards":  {2, 1},
		"version 0":  {0, 1},
		"a negative": {-1},
	} {
		streamID := uuid.New()
		var request []persistence.PersistedEvent
		for _, version := range versions {
			request = append(request, newEvent(streamID, version))
		}
		err := appendEvents(store, streamID, request...)
		if !errors.Is(err, streams.ErrVersionsNotConsecutive) {
			t.Fatalf("append with %s %v: got %v, want ErrVersionsNotConsecutive", name,
				versions, err)
		}
		if read := requireQueryStream(t, store, streamID, 1); len(read) != 0 {
			t.Fatalf("append with %s wrote %d events, want none", name, len(read))
		}
	}
}

func runAppendRejectsARepeatedVersion(t *testing.T, newStore NewBackend) {
	store := newStore(t)
	streamID := uuid.New()

	appendVersions(t, store, streamID, 1)

	err := appendEvents(store, streamID, newEvent(streamID, 1))
	if !errors.Is(err, streams.ErrVersionConflict) {
		t.Fatalf("append of a repeated version 1: got %v, want ErrVersionConflict", err)
	}
}

func runARejectedAppendWritesNothing(t *testing.T, newStore NewBackend) {
	store := newStore(t)
	streamID := uuid.New()

	appendVersions(t, store, streamID, 1)

	// The first event of this batch is contiguous and the second is not, so
	// the batch has to fail whole.
	err := appendEvents(store, streamID, newEvent(streamID, 2), newEvent(streamID, 4))
	if err == nil {
		t.Fatal("append of a batch with a gap in it was accepted, want an error")
	}

	read := requireQueryStream(t, store, streamID, 1)
	if len(read) != 1 {
		t.Fatalf("after a rejected batch the stream holds %d events, want 1", len(read))
	}
	if read[0].Version != 1 {
		t.Fatalf("after a rejected batch the stream is at version %d, want 1", read[0].Version)
	}
}

// Two writers that read the same head both append the next version; one wins and the other
// is told to re-read, whichever of the database's two checks catches it.
func runRacingAppendsOfOneVersionConflict(t *testing.T, newStore NewBackend) {
	store := newStore(t)

	for round := range 20 {
		streamID := uuid.New()
		appendVersions(t, store, streamID, 1)

		results := make(chan error, 2)
		start := make(chan struct{})
		for range 2 {
			go func() {
				<-start
				results <- appendEvents(store, streamID, newEvent(streamID, 2))
			}()
		}
		close(start)

		first, second := <-results, <-results
		if (first == nil) == (second == nil) {
			t.Fatalf("round %d: two appends of version 2 returned %v and %v, want one winner",
				round, first, second)
		}
		lost := first
		if lost == nil {
			lost = second
		}
		if !errors.Is(lost, streams.ErrVersionConflict) {
			t.Fatalf("round %d: the losing append returned %v, want ErrVersionConflict",
				round, lost)
		}
	}
}

// An append learns, in its own write, the lowest version of its stream an earlier append
// left unpublished, so publishing after it starts there and keeps the stream's order.
func runAppendReturnsTheEarliestUnpublishedVersion(t *testing.T, newStore NewBackend) {
	store, ctx := newStore(t), context.Background()
	streamID := uuid.New()

	earliest, err := store.Append(ctx, persistence.AppendRequest{
		StreamID: streamID,
		Events:   []persistence.PersistedEvent{newEvent(streamID, 1), newEvent(streamID, 2)},
		AppendTs: appendTs,
		Publish:  true,
	})
	if err != nil || earliest != 0 {
		t.Fatalf("first append: earliest unpublished %d, %v, want 0", earliest, err)
	}
	earliest, err = store.Append(ctx, persistence.AppendRequest{
		StreamID: streamID,
		Events:   []persistence.PersistedEvent{newEvent(streamID, 3)},
		AppendTs: appendTs,
		Publish:  true,
	})
	if err != nil || earliest != 1 {
		t.Fatalf("second append: earliest unpublished %d, %v, want 1", earliest, err)
	}
	pending, err := store.QueryPending(ctx, streamID)
	if err != nil || len(pending) != 3 || pending[0] != 1 || pending[2] != 3 {
		t.Fatalf("pending after two published appends: %v, %v, want versions 1 to 3", pending,
			err)
	}
}

// runPartitionKeySurvivesVerbatim: the key an event was appended under reads back as written,
// and an empty one, meaning the stream id, stays empty
func runPartitionKeySurvivesVerbatim(t *testing.T, newStore NewBackend) {
	store := newStore(t)
	streamID := uuid.New()

	keyed := newEvent(streamID, 1)
	keyed.PartitionKey = "customer-7"
	unkeyed := newEvent(streamID, 2)
	if err := appendEvents(store, streamID, keyed, unkeyed); err != nil {
		t.Fatalf("append with and without a key: %v", err)
	}

	read := requireQueryStream(t, store, streamID, 1)
	if len(read) != 2 || read[0].PartitionKey != "customer-7" || read[1].PartitionKey != "" {
		t.Fatalf("read back keys %q, want customer-7 and empty", partitionKeys(read))
	}
}

// partitionKeys is each event's stored key
func partitionKeys(read []persistence.PersistedEvent) []string {
	keys := make([]string, 0, len(read))
	for _, event := range read {
		keys = append(keys, event.PartitionKey)
	}
	return keys
}

// An append that is not published leaves nothing pending and reports nothing pending, as no
// publishing would ever clear it.
func runAnAppendNotPublishedLeavesNothingPending(t *testing.T, newStore NewBackend) {
	store, ctx := newStore(t), context.Background()
	streamID := uuid.New()

	for _, version := range []int32{1, 2} {
		earliest, err := store.Append(ctx, persistence.AppendRequest{
			StreamID: streamID,
			Events:   []persistence.PersistedEvent{newEvent(streamID, version)},
			AppendTs: appendTs,
		})
		if err != nil || earliest != 0 {
			t.Fatalf("append of version %d: earliest unpublished %d, %v, want 0", version,
				earliest, err)
		}
	}
	pending, err := store.QueryPending(ctx, streamID)
	if err != nil || len(pending) != 0 {
		t.Fatalf("pending after unpublished appends: %v, %v, want none", pending, err)
	}
}

// appendEvents writes events, publishing none
func appendEvents(store persistence.Backend, streamID uuid.UUID,
	newEvents ...persistence.PersistedEvent) error {
	_, err := store.Append(context.Background(), persistence.AppendRequest{
		StreamID: streamID,
		Events:   newEvents,
		AppendTs: appendTs,
	})
	return err
}

// appendVersions writes one event per version in a single call.
func appendVersions(t *testing.T, store persistence.Backend, streamID uuid.UUID,
	versions ...int32) {
	t.Helper()

	newEvents := make([]persistence.PersistedEvent, 0, len(versions))
	for _, version := range versions {
		newEvents = append(newEvents, newEvent(streamID, version))
	}
	if err := appendEvents(store, streamID, newEvents...); err != nil {
		t.Fatalf("append versions %v: %v", versions, err)
	}
}

// requireQueryStream reads and fails the test rather than returning an error, so
// the assertions above stay about the thing under test.
func requireQueryStream(t *testing.T, store persistence.Backend, streamID uuid.UUID,
	fromVersion int32) []persistence.PersistedEvent {
	t.Helper()

	streamEvents, err := store.QueryStream(context.Background(), streamID, fromVersion,
		everyEvent)
	if err != nil {
		t.Fatalf("read stream %s from version %d: %v", streamID, fromVersion, err)
	}
	return streamEvents
}

// newEvent is one appendable event.
func newEvent(streamID uuid.UUID, version int32) persistence.PersistedEvent {
	return persistence.PersistedEvent{
		StreamID:        streamID,
		Version:         version,
		EventType:       "ContractEvent",
		EventData:       []byte(`{"contract":true}`),
		DataFormat:      codec.JSON,
		DataCompression: compress.None,
		DataEncryption:  encrypt.None,
		EventAppendTs:   appendTs,
	}
}

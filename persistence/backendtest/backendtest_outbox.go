// SPDX-FileCopyrightText: Copyright (c) 2026 The journal Authors
// SPDX-License-Identifier: Apache-2.0

package backendtest

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/llingr/journal/persistence"
	"github.com/llingr/journal/streams"
)

// runOutboxContract runs the tests of the outbox side and of what a backend keeps as written
func runOutboxContract(t *testing.T, newStore NewBackend) {
	t.Helper()

	t.Run("PendingStreamsAreThoseAppendedInTheWindow", func(t *testing.T) {
		runPendingStreamsAreThoseAppendedInTheWindow(t, newStore)
	})
	t.Run("PendingStreamsReadAtMostTheLimitInRows", func(t *testing.T) {
		runPendingStreamsReadAtMostTheLimitInRows(t, newStore)
	})
	t.Run("DeletePublishedClearsThroughTheVersion", func(t *testing.T) {
		runDeletePublishedClearsThroughTheVersion(t, newStore)
	})
	t.Run("QueryStreamRangeReadsTheRange", func(t *testing.T) {
		runQueryStreamRangeReadsTheRange(t, newStore)
	})
	t.Run("AppendRefusesAVersionPastTheHead", func(t *testing.T) {
		runAppendRefusesAVersionPastTheHead(t, newStore)
	})
	t.Run("AnEmptyPayloadSurvives", func(t *testing.T) {
		runAnEmptyPayloadSurvives(t, newStore)
	})
	t.Run("StoredBytesAreTheBackendsOwn", func(t *testing.T) {
		runStoredBytesAreTheBackendsOwn(t, newStore)
	})
	t.Run("EveryEventTakesTheRequestsAppendTime", func(t *testing.T) {
		runEveryEventTakesTheRequestsAppendTime(t, newStore)
	})
	t.Run("TimesReadBackInUTC", func(t *testing.T) {
		runTimesReadBackInUTC(t, newStore)
	})
	t.Run("AStatesPartitionKeyIsSetOnce", func(t *testing.T) {
		runAStatesPartitionKeyIsSetOnce(t, newStore)
	})
	t.Run("ACancelledCallWritesNothing", func(t *testing.T) {
		runACancelledCallWritesNothing(t, newStore)
	})
}

// runPendingStreamsAreThoseAppendedInTheWindow: a stream is listed for an outbox row appended
// from the window's start to before its end; one appended unpublished, outside the window or
// cleared is not
func runPendingStreamsAreThoseAppendedInTheWindow(t *testing.T, newStore NewBackend) {
	store, ctx := newStore(t), context.Background()
	windowStart, windowEnd := appendTs, appendTs.Add(time.Hour)

	atStart, inside, cleared := uuid.New(), uuid.New(), uuid.New()
	appendPublished(t, store, atStart, windowStart, 1)
	appendPublished(t, store, inside, windowStart.Add(30*time.Minute), 1, 2)
	appendPublished(t, store, cleared, windowStart.Add(30*time.Minute), 1)
	requireNoError(t, store.DeletePublished(ctx, cleared, 1), "clear a stream")
	appendPublished(t, store, uuid.New(), windowStart.Add(-time.Microsecond), 1)
	appendPublished(t, store, uuid.New(), windowEnd, 1)
	unpublished := uuid.New()
	_, err := store.Append(ctx, persistence.AppendRequest{
		StreamID: unpublished,
		Events:   []persistence.PersistedEvent{newEvent(unpublished, 1)},
		AppendTs: windowStart.Add(30 * time.Minute),
	})
	requireNoError(t, err, "append unpublished")

	listed, err := store.QueryPendingStreams(ctx, windowStart, windowEnd, everyEvent)
	requireNoError(t, err, "list pending streams")
	slices.SortFunc(listed, compareStreamIDs)
	want := []uuid.UUID{atStart, inside}
	slices.SortFunc(want, compareStreamIDs)
	if !slices.Equal(listed, want) {
		t.Fatalf("pending streams %v, want the one at the window's start and the one inside, "+
			"%v", listed, want)
	}
}

// runPendingStreamsReadAtMostTheLimitInRows: the limit bounds the rows read, so a list is no
// longer than it and names each stream once; with room for every row, every stream is listed
func runPendingStreamsReadAtMostTheLimitInRows(t *testing.T, newStore NewBackend) {
	store, ctx := newStore(t), context.Background()
	busy, quiet := uuid.New(), uuid.New()
	appendPublished(t, store, busy, appendTs, 1, 2, 3)
	appendPublished(t, store, quiet, appendTs, 1)
	windowEnd := appendTs.Add(time.Second)

	for limit := 1; limit <= 4; limit++ {
		listed, err := store.QueryPendingStreams(ctx, appendTs, windowEnd, limit)
		requireNoError(t, err, "list pending streams")
		if len(listed) == 0 || len(listed) > limit {
			t.Fatalf("limit %d listed %d streams, want 1 to %d", limit, len(listed), limit)
		}
		for index, streamID := range listed {
			if (streamID != busy && streamID != quiet) ||
				slices.Contains(listed[:index], streamID) {
				t.Fatalf("limit %d listed %v, want busy and quiet at most once each", limit,
					listed)
			}
		}
	}
	listed, err := store.QueryPendingStreams(ctx, appendTs, windowEnd, everyEvent)
	requireNoError(t, err, "list pending streams")
	if len(listed) != 2 {
		t.Fatalf("listed %v with room for every row, want both streams", listed)
	}
}

// runDeletePublishedClearsThroughTheVersion: only the stream's rows through the version go,
// and the events stay
func runDeletePublishedClearsThroughTheVersion(t *testing.T, newStore NewBackend) {
	store, ctx := newStore(t), context.Background()
	streamID, other := uuid.New(), uuid.New()
	appendPublished(t, store, streamID, appendTs, 1, 2, 3, 4)
	appendPublished(t, store, other, appendTs, 1, 2)

	requireNoError(t, store.DeletePublished(ctx, streamID, 2), "clear through version 2")
	requirePending(t, store, streamID, 3, 4)
	requirePending(t, store, other, 1, 2)
	requireNoError(t, store.DeletePublished(ctx, streamID, 10), "clear past the head")
	requirePending(t, store, streamID)
	requireNoError(t, store.DeletePublished(ctx, uuid.New(), 1), "clear an unwritten stream")
	if read := requireQueryStream(t, store, streamID, 1); len(read) != 4 {
		t.Fatalf("events after clearing the outbox: %d, want all 4", len(read))
	}
}

// runQueryStreamRangeReadsTheRange: both ends are included, a range past the head ends there,
// and a range that runs backwards or over an unwritten stream is empty
func runQueryStreamRangeReadsTheRange(t *testing.T, newStore NewBackend) {
	store, ctx := newStore(t), context.Background()
	streamID := uuid.New()
	appendVersions(t, store, streamID, 1, 2, 3, 4, 5)

	for _, testCase := range []struct {
		streamID uuid.UUID
		from     int32
		through  int32
		want     []int32
	}{
		{streamID, 2, 4, []int32{2, 3, 4}},
		{streamID, 4, 9, []int32{4, 5}},
		{streamID, 3, 3, []int32{3}},
		{streamID, 3, 2, nil},
		{streamID, 6, 9, nil},
		{uuid.New(), 1, 3, nil},
	} {
		read, err := store.QueryStreamRange(ctx, testCase.streamID, testCase.from,
			testCase.through)
		requireNoError(t, err, "read a range")
		if !equalVersions(versionsOf(read), testCase.want) {
			t.Fatalf("range %d to %d: got %v, want %v", testCase.from, testCase.through,
				versionsOf(read), testCase.want)
		}
	}
}

// runAppendRefusesAVersionPastTheHead: an append that would leave a gap, its first version
// beyond the head's next, is a conflict, as a caller answers it by reading the stream again,
// and writes nothing, its outbox rows and state included
func runAppendRefusesAVersionPastTheHead(t *testing.T, newStore NewBackend) {
	store, ctx := newStore(t), context.Background()

	for name, testCase := range map[string]struct {
		stored   []int32
		appended []int32
	}{
		"an unwritten stream from version 2": {nil, []int32{2}},
		"one past the head's next":           {[]int32{1, 2}, []int32{4}},
		"a batch past the head's next":       {[]int32{1, 2}, []int32{4, 5, 6}},
	} {
		streamID := uuid.New()
		if len(testCase.stored) > 0 {
			appendVersions(t, store, streamID, testCase.stored...)
		}
		appended := make([]persistence.PersistedEvent, 0, len(testCase.appended))
		for _, version := range testCase.appended {
			appended = append(appended, newEvent(streamID, version))
		}
		lastVersion := testCase.appended[len(testCase.appended)-1]
		state := newState(streamID, lastVersion, 1, []byte(`{}`))
		_, err := store.Append(ctx, persistence.AppendRequest{
			StreamID: streamID,
			Events:   appended,
			AppendTs: appendTs,
			State:    &state,
			Publish:  true,
		})
		if !errors.Is(err, streams.ErrVersionConflict) {
			t.Fatalf("append of %s: got %v, want ErrVersionConflict", name, err)
		}
		head, err := store.QueryHead(ctx, streamID)
		requireNoError(t, err, "read the head")
		if want := int32(len(testCase.stored)); head != want {
			t.Fatalf("after the refused append of %s the head is %d, want %d", name, head, want)
		}
		requirePending(t, store, streamID)
		stored, _, err := store.QueryState(ctx, streamID)
		requireNoError(t, err, "read the state")
		if stored.Version != 0 {
			t.Fatalf("the refused append of %s stored a state at version %d", name,
				stored.Version)
		}
	}
}

// runAnEmptyPayloadSurvives: an event whose payload marshals to nothing, nil or empty, is
// stored and reads back empty, as is a state with no bytes
func runAnEmptyPayloadSurvives(t *testing.T, newStore NewBackend) {
	store := newStore(t)
	streamID := uuid.New()

	nilPayload, emptyPayload := newEvent(streamID, 1), newEvent(streamID, 2)
	nilPayload.EventData, emptyPayload.EventData = nil, []byte{}
	requireNoError(t, appendEvents(store, streamID, nilPayload, emptyPayload),
		"append empty payloads")
	read := requireQueryStream(t, store, streamID, 1)
	if len(read) != 2 || len(read[0].EventData) != 0 || len(read[1].EventData) != 0 {
		t.Fatalf("empty payloads read back as %d events %q", len(read), payloadsOf(read))
	}

	state := newState(streamID, 3, 1, nil)
	requireNoError(t, appendWithState(store, streamID, &state, 3), "append with an empty state")
	state.StateData = []byte{}
	requireState(t, store, state, 3)
}

// runStoredBytesAreTheBackendsOwn: a caller changing the bytes it appended, or the bytes it
// read, changes nothing stored
func runStoredBytesAreTheBackendsOwn(t *testing.T, newStore NewBackend) {
	store := newStore(t)
	streamID := uuid.New()

	event := newEvent(streamID, 1)
	event.EventData = []byte(`{"kept":true}`)
	event.Metadata = []byte(`{"trace":1}`)
	state := newState(streamID, 1, 1, []byte(`{"state":1}`))
	requireNoError(t, appendWithEventsAndState(store, &state, event), "append")
	event.EventData[2], event.Metadata[2], state.StateData[2] = 'X', 'X', 'X'

	read := requireQueryStream(t, store, streamID, 1)
	if string(read[0].EventData) != `{"kept":true}` || string(read[0].Metadata) != `{"trace":1}` {
		t.Fatalf("after the caller changed its bytes, read %q and %q", read[0].EventData,
			read[0].Metadata)
	}
	read[0].EventData[2], read[0].Metadata[2] = 'Y', 'Y'
	read = requireQueryStream(t, store, streamID, 1)
	if string(read[0].EventData) != `{"kept":true}` || string(read[0].Metadata) != `{"trace":1}` {
		t.Fatalf("after a reader changed its bytes, read %q and %q", read[0].EventData,
			read[0].Metadata)
	}
	stored, _, err := store.QueryState(context.Background(), streamID)
	requireNoError(t, err, "read the state")
	if string(stored.StateData) != `{"state":1}` {
		t.Fatalf("after the caller changed its bytes, the state is %q", stored.StateData)
	}
}

// runEveryEventTakesTheRequestsAppendTime: the request's one append time is every event's,
// whatever each event carried
func runEveryEventTakesTheRequestsAppendTime(t *testing.T, newStore NewBackend) {
	store := newStore(t)
	streamID := uuid.New()
	requestTs := appendTs.Add(42 * time.Second)

	first, second := newEvent(streamID, 1), newEvent(streamID, 2)
	first.EventAppendTs, second.EventAppendTs = appendTs, time.Time{}
	_, err := store.Append(context.Background(), persistence.AppendRequest{
		StreamID: streamID,
		Events:   []persistence.PersistedEvent{first, second},
		AppendTs: requestTs,
	})
	requireNoError(t, err, "append")

	for _, event := range requireQueryStream(t, store, streamID, 1) {
		if !event.EventAppendTs.Equal(requestTs) {
			t.Fatalf("version %d appended at %s, want the request's %s", event.Version,
				event.EventAppendTs, requestTs)
		}
	}
}

// runTimesReadBackInUTC: every time a backend returns is in UTC, whatever zone it was written
// in and whatever the process's own, so a publisher sees one offset for events read back and
// for those published from memory
func runTimesReadBackInUTC(t *testing.T, newStore NewBackend) {
	store, ctx := newStore(t), context.Background()
	streamID := uuid.New()
	written := appendTs.In(time.FixedZone("New York", -5*60*60))

	state := newState(streamID, 2, 1, []byte(`{}`))
	state.LastAppendTs = written
	_, err := store.Append(ctx, persistence.AppendRequest{
		StreamID: streamID,
		Events:   []persistence.PersistedEvent{newEvent(streamID, 1), newEvent(streamID, 2)},
		AppendTs: written,
		State:    &state,
	})
	requireNoError(t, err, "append two events and their state")

	streamed := requireQueryStream(t, store, streamID, 1)
	ranged, err := store.QueryStreamRange(ctx, streamID, 1, 2)
	requireNoError(t, err, "read the range")
	stored, _, err := store.QueryState(ctx, streamID)
	requireNoError(t, err, "read the state")

	read := []time.Time{stored.LastAppendTs}
	for _, event := range append(streamed, ranged...) {
		read = append(read, event.EventAppendTs)
	}
	if len(read) != 5 {
		t.Fatalf("read %d times, want 5: the state's and two events each way", len(read))
	}
	for index, appended := range read {
		if appended.Location() != time.UTC || !appended.Equal(appendTs) {
			t.Errorf("time %d read back as %s in %q, want %s in UTC", index, appended,
				appended.Location(), appendTs)
		}
	}
}

// runAStatesPartitionKeyIsSetOnce: the key the first state was stored with stays, as a
// stream's key never changes
func runAStatesPartitionKeyIsSetOnce(t *testing.T, newStore NewBackend) {
	store, ctx := newStore(t), context.Background()
	streamID := uuid.New()

	first := newState(streamID, 1, 1, []byte(`{}`))
	first.PartitionKey = "first-key"
	requireNoError(t, appendWithState(store, streamID, &first, 1), "append with a keyed state")
	second := newState(streamID, 2, 1, []byte(`{}`))
	second.PartitionKey = "second-key"
	requireNoError(t, appendWithState(store, streamID, &second, 2), "append with another key")
	saved := newState(streamID, 2, 2, []byte(`{}`))
	saved.PartitionKey = "third-key"
	_, err := store.SaveState(ctx, saved)
	requireNoError(t, err, "save with another key")

	stored, _, err := store.QueryState(ctx, streamID)
	requireNoError(t, err, "read the state")
	if stored.PartitionKey != "first-key" || stored.StateVersion != 2 {
		t.Fatalf("state stored with key %q at state version %d, want first-key at 2",
			stored.PartitionKey, stored.StateVersion)
	}
}

// runACancelledCallWritesNothing: every call whose context has ended fails, rather than
// answer as if the stream were empty, and the writes among them leave the stream as it was
func runACancelledCallWritesNothing(t *testing.T, newStore NewBackend) {
	store := newStore(t)
	streamID := uuid.New()
	appendPublished(t, store, streamID, appendTs, 1, 2)
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()

	state := newState(streamID, 2, 1, []byte(`{}`))
	calls := map[string]func() error{
		"Append": func() error {
			_, err := store.Append(cancelled, persistence.AppendRequest{
				StreamID: streamID,
				Events:   []persistence.PersistedEvent{newEvent(streamID, 3)},
				AppendTs: appendTs,
				Publish:  true,
			})
			return err
		},
		"SaveState": func() error {
			_, err := store.SaveState(cancelled, state)
			return err
		},
		"DeletePublished": func() error {
			return store.DeletePublished(cancelled, streamID, 2)
		},
		"QueryStream": func() error {
			_, err := store.QueryStream(cancelled, streamID, 1, everyEvent)
			return err
		},
		"QueryStreamRange": func() error {
			_, err := store.QueryStreamRange(cancelled, streamID, 1, 2)
			return err
		},
		"QueryHead": func() error {
			_, err := store.QueryHead(cancelled, streamID)
			return err
		},
		"QueryState": func() error {
			_, _, err := store.QueryState(cancelled, streamID)
			return err
		},
		"QueryPending": func() error {
			_, err := store.QueryPending(cancelled, streamID)
			return err
		},
		"QueryPendingStreams": func() error {
			_, err := store.QueryPendingStreams(cancelled, appendTs, appendTs.Add(time.Hour),
				everyEvent)
			return err
		},
	}
	for name, call := range calls {
		if err := call(); err == nil {
			t.Fatalf("%s with a cancelled context succeeded, want it refused", name)
		}
	}

	if read := requireQueryStream(t, store, streamID, 1); len(read) != 2 {
		t.Fatalf("after cancelled calls the stream holds %d events, want 2", len(read))
	}
	requirePending(t, store, streamID, 1, 2)
	stored, _, err := store.QueryState(context.Background(), streamID)
	requireNoError(t, err, "read the state")
	if stored.Version != 0 {
		t.Fatalf("a cancelled save stored a state at version %d", stored.Version)
	}
}

// appendPublished writes one event per version at appendedAt, with their outbox rows
func appendPublished(t *testing.T, store persistence.Backend, streamID uuid.UUID,
	appendedAt time.Time, versions ...int32) {
	t.Helper()

	newEvents := make([]persistence.PersistedEvent, 0, len(versions))
	for _, version := range versions {
		newEvents = append(newEvents, newEvent(streamID, version))
	}
	_, err := store.Append(context.Background(), persistence.AppendRequest{
		StreamID: streamID,
		Events:   newEvents,
		AppendTs: appendedAt,
		Publish:  true,
	})
	requireNoError(t, err, "append published")
}

// appendWithEventsAndState writes the events as given and the state, publishing none
func appendWithEventsAndState(store persistence.Backend, state *persistence.PersistedState,
	newEvents ...persistence.PersistedEvent) error {
	_, err := store.Append(context.Background(), persistence.AppendRequest{
		StreamID: state.StreamID,
		Events:   newEvents,
		AppendTs: appendTs,
		State:    state,
	})
	return err
}

// requirePending fails unless the stream's unpublished versions are want
func requirePending(t *testing.T, store persistence.Backend, streamID uuid.UUID,
	want ...int32) {
	t.Helper()

	pending, err := store.QueryPending(context.Background(), streamID)
	requireNoError(t, err, "read pending versions")
	if !equalVersions(pending, want) {
		t.Fatalf("pending versions %v, want %v", pending, want)
	}
}

func requireNoError(t *testing.T, err error, action string) {
	t.Helper()

	if err != nil {
		t.Fatalf("%s: %v", action, err)
	}
}

func compareStreamIDs(first uuid.UUID, second uuid.UUID) int {
	return slices.Compare(first[:], second[:])
}

func payloadsOf(read []persistence.PersistedEvent) []string {
	payloads := make([]string, 0, len(read))
	for _, event := range read {
		payloads = append(payloads, string(event.EventData))
	}
	return payloads
}

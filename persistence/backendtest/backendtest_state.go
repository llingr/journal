// SPDX-FileCopyrightText: Copyright (c) 2026 The journal Authors
// SPDX-License-Identifier: Apache-2.0

package backendtest

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/llingr/journal/codec"
	"github.com/llingr/journal/codec/compress"
	"github.com/llingr/journal/codec/encrypt"
	"github.com/llingr/journal/persistence"
	"github.com/llingr/journal/streams"
)

// runStateContract runs the tests of paged reads and of the state stored beside the events
func runStateContract(t *testing.T, newStore NewBackend) {
	t.Helper()

	t.Run("QueryStreamStopsAtTheLimit", func(t *testing.T) {
		runQueryStreamStopsAtTheLimit(t, newStore)
	})
	t.Run("QueryStateOfAStreamWithoutOneIsEmpty", func(t *testing.T) {
		runQueryStateOfAStreamWithoutOneIsEmpty(t, newStore)
	})
	t.Run("AppendStoresTheStateWithItsEvents", func(t *testing.T) {
		runAppendStoresTheStateWithItsEvents(t, newStore)
	})
	t.Run("TheStateIsNeitherAnEventNorPublished", func(t *testing.T) {
		runTheStateIsNeitherAnEventNorPublished(t, newStore)
	})
	t.Run("ARejectedAppendLeavesTheState", func(t *testing.T) {
		runARejectedAppendLeavesTheState(t, newStore)
	})
	t.Run("AppendRefusesAStateNotFoldedToItsEvents", func(t *testing.T) {
		runAppendRefusesAStateNotFoldedToItsEvents(t, newStore)
	})
	t.Run("SaveStateNeverGoesBack", func(t *testing.T) {
		runSaveStateNeverGoesBack(t, newStore)
	})
	t.Run("RacingSavesNeverLeaveTheStateBehind", func(t *testing.T) {
		runRacingSavesNeverLeaveTheStateBehind(t, newStore)
	})
}

// runQueryStreamStopsAtTheLimit: a page holds at most the limit, from the version asked, and
// a short page is the stream's end
func runQueryStreamStopsAtTheLimit(t *testing.T, newStore NewBackend) {
	store, ctx := newStore(t), context.Background()
	streamID := uuid.New()
	appendVersions(t, store, streamID, 1, 2, 3, 4, 5)

	for _, page := range []struct {
		from  int32
		limit int
		want  []int32
	}{
		{1, 2, []int32{1, 2}},
		{3, 2, []int32{3, 4}},
		{5, 2, []int32{5}},
		{6, 2, nil},
		{2, 1, []int32{2}},
	} {
		read, err := store.QueryStream(ctx, streamID, page.from, page.limit)
		if err != nil {
			t.Fatalf("read %d from version %d: %v", page.limit, page.from, err)
		}
		if got := versionsOf(read); !equalVersions(got, page.want) {
			t.Fatalf("read %d from version %d: got %v, want %v", page.limit, page.from, got,
				page.want)
		}
	}
}

func runQueryStateOfAStreamWithoutOneIsEmpty(t *testing.T, newStore NewBackend) {
	store, ctx := newStore(t), context.Background()

	state, head, err := store.QueryState(ctx, uuid.New())
	if err != nil || head != 0 || state.Version != 0 {
		t.Fatalf("state of an unwritten stream: version %d, head %d, %v; want none at head 0",
			state.Version, head, err)
	}

	streamID := uuid.New()
	appendVersions(t, store, streamID, 1, 2, 3)
	state, head, err = store.QueryState(ctx, streamID)
	if err != nil || head != 3 || state.Version != 0 {
		t.Fatalf("state of a stream appended without one: version %d, head %d, %v; want "+
			"none at head 3", state.Version, head, err)
	}
}

// runAppendStoresTheStateWithItsEvents: every column of the state reads back as written, and
// the next append replaces it
func runAppendStoresTheStateWithItsEvents(t *testing.T, newStore NewBackend) {
	store := newStore(t)
	streamID := uuid.New()

	first := newState(streamID, 2, 1, []byte{0x00, 0xFF, 0x80})
	first.DataCompression = compress.Zstd
	first.DataEncryption = encrypt.AESGCM
	first.DataEncryptionKid = "key-2026-10"
	first.PartitionKey = "space-7"
	err := appendWithState(store, streamID, &first, 1, 2)
	if err != nil {
		t.Fatalf("append versions 1 and 2 with their state: %v", err)
	}
	requireState(t, store, first, 2)

	second := newState(streamID, 3, 1, []byte(`{"count":3}`))
	second.LastEventType = "Counted"
	second.LastAppendTs = appendTs.Add(time.Minute)
	second.PartitionKey = "space-7"
	err = appendWithState(store, streamID, &second, 3)
	if err != nil {
		t.Fatalf("append version 3 with its state: %v", err)
	}
	requireState(t, store, second, 3)
}

// runTheStateIsNeitherAnEventNorPublished: reads of events, the head and the outbox never see
// the state, and a read from below version 1 is refused rather than returning it
func runTheStateIsNeitherAnEventNorPublished(t *testing.T, newStore NewBackend) {
	store, ctx := newStore(t), context.Background()
	streamID := uuid.New()

	state := newState(streamID, 2, 1, []byte(`{}`))
	_, err := store.Append(ctx, persistence.AppendRequest{
		StreamID: streamID,
		Events:   []persistence.PersistedEvent{newEvent(streamID, 1), newEvent(streamID, 2)},
		AppendTs: appendTs,
		State:    &state,
		Publish:  true,
	})
	if err != nil {
		t.Fatalf("append with the state, published: %v", err)
	}

	if read := requireQueryStream(t, store, streamID, 1); !equalVersions(versionsOf(read),
		[]int32{1, 2}) {
		t.Fatalf("events read with a state stored: %v, want versions 1 and 2", versionsOf(read))
	}
	ranged, err := store.QueryStreamRange(ctx, streamID, 1, 2)
	if err != nil || !equalVersions(versionsOf(ranged), []int32{1, 2}) {
		t.Fatalf("range read with a state stored: %v, %v; want versions 1 and 2",
			versionsOf(ranged), err)
	}
	head, err := store.QueryHead(ctx, streamID)
	if err != nil || head != 2 {
		t.Fatalf("head with a state stored: %d, %v; want 2", head, err)
	}
	pending, err := store.QueryPending(ctx, streamID)
	if err != nil || !equalVersions(pending, []int32{1, 2}) {
		t.Fatalf("pending with a state stored: %v, %v; want versions 1 and 2", pending, err)
	}
	for _, fromVersion := range []int32{0, -1} {
		_, err = store.QueryStream(ctx, streamID, fromVersion, everyEvent)
		if err == nil {
			t.Fatalf("read from version %d was accepted, want it refused", fromVersion)
		}
		_, err = store.QueryStreamRange(ctx, streamID, fromVersion, 2)
		if err == nil {
			t.Fatalf("range read from version %d was accepted, want it refused", fromVersion)
		}
	}
}

// runARejectedAppendLeavesTheState: a state is only ever stored with the events it is folded
// from, so an append refused for a version another writer took stores neither
func runARejectedAppendLeavesTheState(t *testing.T, newStore NewBackend) {
	store := newStore(t)
	streamID := uuid.New()

	kept := newState(streamID, 1, 1, []byte(`{"kept":true}`))
	if err := appendWithState(store, streamID, &kept, 1); err != nil {
		t.Fatalf("append version 1 with its state: %v", err)
	}
	stale := newState(streamID, 1, 1, []byte(`{"stale":true}`))
	err := appendWithState(store, streamID, &stale, 1)
	if !errors.Is(err, streams.ErrVersionConflict) {
		t.Fatalf("append of a taken version with a state: got %v, want ErrVersionConflict", err)
	}
	requireState(t, store, kept, 1)
}

// runAppendRefusesAStateNotFoldedToItsEvents: a state folded to another version than the
// last appended, or for another stream, is refused, and nothing is written
func runAppendRefusesAStateNotFoldedToItsEvents(t *testing.T, newStore NewBackend) {
	store := newStore(t)

	for name, mismatched := range map[string]func(streamID uuid.UUID) persistence.PersistedState{
		"behind": func(streamID uuid.UUID) persistence.PersistedState {
			return newState(streamID, 1, 1, []byte(`{}`))
		},
		"ahead": func(streamID uuid.UUID) persistence.PersistedState {
			return newState(streamID, 3, 1, []byte(`{}`))
		},
		"another stream": func(uuid.UUID) persistence.PersistedState {
			return newState(uuid.New(), 2, 1, []byte(`{}`))
		},
		"negative state version": func(streamID uuid.UUID) persistence.PersistedState {
			return newState(streamID, 2, -1, []byte(`{}`))
		},
	} {
		streamID := uuid.New()
		state := mismatched(streamID)
		err := appendWithState(store, streamID, &state, 1, 2)
		if err == nil {
			t.Fatalf("append with a state %s: accepted, want it refused", name)
		}
		if read := requireQueryStream(t, store, streamID, 1); len(read) != 0 {
			t.Fatalf("append with a state %s wrote %d events, want none", name, len(read))
		}
	}
}

// runSaveStateNeverGoesBack: a state saved after the events is stored unless the stored one
// is folded further; at the same version it replaces it, as when the fold changed
func runSaveStateNeverGoesBack(t *testing.T, newStore NewBackend) {
	store, ctx := newStore(t), context.Background()

	unstated := uuid.New()
	appendVersions(t, store, unstated, 1, 2)
	folded := newState(unstated, 2, 1, []byte(`{"folded":2}`))
	saved, err := store.SaveState(ctx, folded)
	if err != nil || !saved {
		t.Fatalf("save the state of a stream with none: %t, %v; want it stored", saved, err)
	}
	requireState(t, store, folded, 2)

	streamID := uuid.New()
	current := newState(streamID, 3, 1, []byte(`{"version":3}`))
	if err = appendWithState(store, streamID, &current, 1, 2, 3); err != nil {
		t.Fatalf("append versions 1 to 3 with their state: %v", err)
	}
	behind := newState(streamID, 2, 2, []byte(`{"version":2}`))
	saved, err = store.SaveState(ctx, behind)
	if err != nil || saved {
		t.Fatalf("save a state behind the stored one: %t, %v; want it not stored", saved, err)
	}
	requireState(t, store, current, 3)

	refolded := newState(streamID, 3, 2, []byte(`{"version":3,"refolded":true}`))
	saved, err = store.SaveState(ctx, refolded)
	if err != nil || !saved {
		t.Fatalf("save a state refolded at the stored version: %t, %v; want it stored", saved,
			err)
	}
	requireState(t, store, refolded, 3)

	_, err = store.SaveState(ctx, newState(uuid.New(), 0, 1, []byte(`{}`)))
	if err == nil {
		t.Fatal("save a state folded to no event: accepted, want it refused")
	}
}

// runRacingSavesNeverLeaveTheStateBehind: a reader storing the state it folded races a writer
// appending the next event with its state; whichever commits first, the state the stream
// is left with is folded to its head
func runRacingSavesNeverLeaveTheStateBehind(t *testing.T, newStore NewBackend) {
	store, ctx := newStore(t), context.Background()

	for round := range 30 {
		streamID := uuid.New()
		appendVersions(t, store, streamID, 1)

		results := make(chan error, 2)
		start := make(chan struct{})
		go func() {
			<-start
			_, err := store.SaveState(ctx, newState(streamID, 1, 2, []byte(`{"reader":1}`)))
			results <- err
		}()
		go func() {
			<-start
			state := newState(streamID, 2, 2, []byte(`{"writer":2}`))
			results <- appendWithState(store, streamID, &state, 2)
		}()
		close(start)

		for range 2 {
			if err := <-results; err != nil {
				t.Fatalf("round %d: %v", round, err)
			}
		}
		state, head, err := store.QueryState(ctx, streamID)
		if err != nil || state.Version != head || head != 2 {
			t.Fatalf("round %d: state at version %d, head %d, %v; want both 2", round,
				state.Version, head, err)
		}
	}
}

// appendWithState writes one event per version and the state, publishing none
func appendWithState(store persistence.Backend, streamID uuid.UUID,
	state *persistence.PersistedState, versions ...int32) error {
	newEvents := make([]persistence.PersistedEvent, 0, len(versions))
	for _, version := range versions {
		newEvents = append(newEvents, newEvent(streamID, version))
	}
	_, err := store.Append(context.Background(), persistence.AppendRequest{
		StreamID: streamID,
		Events:   newEvents,
		AppendTs: appendTs,
		State:    state,
	})
	return err
}

// requireState fails unless the stored state is want, every column, at the head
func requireState(t *testing.T, store persistence.Backend, want persistence.PersistedState,
	wantHead int32) {
	t.Helper()

	got, head, err := store.QueryState(context.Background(), want.StreamID)
	if err != nil {
		t.Fatalf("read the state of stream %s: %v", want.StreamID, err)
	}
	if head != wantHead {
		t.Errorf("head: got %d, want %d", head, wantHead)
	}
	if !bytes.Equal(got.StateData, want.StateData) {
		t.Errorf("state data: got %q, want %q", got.StateData, want.StateData)
	}
	got.StateData, want.StateData = nil, nil
	if !got.LastAppendTs.Equal(want.LastAppendTs) {
		t.Errorf("last append ts: got %s, want %s", got.LastAppendTs, want.LastAppendTs)
	}
	got.LastAppendTs, want.LastAppendTs = time.Time{}, time.Time{}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("state: got %+v, want %+v", got, want)
	}
}

// newState is a state folded to version, its last event a ContractEvent appended at appendTs
func newState(streamID uuid.UUID, version int32, stateVersion int32,
	data []byte) persistence.PersistedState {
	return persistence.PersistedState{
		StreamID:        streamID,
		Version:         version,
		StateVersion:    stateVersion,
		LastEventType:   "ContractEvent",
		LastAppendTs:    appendTs,
		StateData:       data,
		DataFormat:      codec.JSON,
		DataCompression: compress.None,
		DataEncryption:  encrypt.None,
	}
}

// versionsOf is each event's version, nil for none
func versionsOf(read []persistence.PersistedEvent) []int32 {
	var versions []int32
	for _, event := range read {
		versions = append(versions, event.Version)
	}
	return versions
}

// equalVersions treats nil and empty alike
func equalVersions(got []int32, want []int32) bool {
	if len(got) != len(want) {
		return false
	}
	for index := range got {
		if got[index] != want[index] {
			return false
		}
	}
	return true
}

// SPDX-FileCopyrightText: Copyright (c) 2026 The journal Authors
// SPDX-License-Identifier: Apache-2.0

package streams_test

import (
	"context"
	"encoding/json"
	"slices"
	"testing"

	"github.com/google/uuid"

	"github.com/llingr/journal/codec"
	"github.com/llingr/journal/persistence"
	"github.com/llingr/journal/streams"
)

// counterState stores a counter's state as JSON at the given version
func counterState(version int32) *streams.StateCodec[int] {
	return &streams.StateCodec[int]{
		Version: version,
		Format:  codec.JSON,
		Marshal: func(state int) ([]byte, error) {
			return json.Marshal(state)
		},
		Unmarshal: func(encoded []byte) (int, error) {
			var state int
			err := json.Unmarshal(encoded, &state)
			return state, err
		},
	}
}

// TestGetReadsTheStoredState: a repository that stores its state reads one row, not the
// events, and the stream it returns is at the stored version with its last event's type
func TestGetReadsTheStoredState(t *testing.T) {
	backend := newReadLoggingBackend()
	counters := newCounters(t, backend, counterState(1))
	ctx := context.Background()
	streamID := uuid.New()

	counter, err := counters.CreateAll(ctx, streamID, []streams.DomainEvent{
		&added{Amount: 2},
		&added{Amount: 3},
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	written, err := counters.Append(ctx, counter, &added{Amount: 4})
	if err != nil {
		t.Fatalf("append: %v", err)
	}

	backend.reads = nil
	read, err := counters.Get(ctx, streamID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if !slices.Equal(backend.reads, []string{"state"}) {
		t.Fatalf("reads of a stream with its state stored: %v, want only the state",
			backend.reads)
	}
	if read.State() != 9 || read.Version() != 3 || len(read.Events()) != 0 {
		t.Fatalf("read from the state: %d at version %d with %d events, want 9 at version 3 "+
			"with none", read.State(), read.Version(), len(read.Events()))
	}
	if read.LastEventType() != "Added" || !read.LastAppendTs().Equal(written.LastAppendTs()) ||
		read.LastAppendTs().IsZero() {
		t.Fatalf("last event read from the state: %q at %s, want Added at %s",
			read.LastEventType(), read.LastAppendTs(), written.LastAppendTs())
	}
}

// TestAFoldIsReadInPages: a stream is read 250 events at a time, each page from the version
// after the last, and a short page ends the read
func TestAFoldIsReadInPages(t *testing.T) {
	for _, test := range []struct {
		events int
		reads  []string
	}{
		{1, []string{"events from 1"}},
		{249, []string{"events from 1"}},
		{250, []string{"events from 1", "events from 251"}},
		{251, []string{"events from 1", "events from 251"}},
		{500, []string{"events from 1", "events from 251", "events from 501"}},
		{501, []string{"events from 1", "events from 251", "events from 501"}},
	} {
		backend := newReadLoggingBackend()
		counters := newCounters(t, backend, nil)
		ctx := context.Background()
		streamID := uuid.New()

		payloads := make([]streams.DomainEvent, test.events)
		for index := range payloads {
			payloads[index] = &added{Amount: index + 1}
		}
		_, err := counters.CreateAll(ctx, streamID, payloads)
		if err != nil {
			t.Fatalf("create %d events: %v", test.events, err)
		}

		backend.reads = nil
		read, err := counters.Get(ctx, streamID)
		if err != nil {
			t.Fatalf("get %d events: %v", test.events, err)
		}
		if !slices.Equal(backend.reads, test.reads) {
			t.Fatalf("reads of %d events: %v, want %v", test.events, backend.reads, test.reads)
		}
		wantState := test.events * (test.events + 1) / 2
		if read.State() != wantState || read.Version() != int32(test.events) ||
			len(read.Events()) != test.events {
			t.Fatalf("%d events folded: %d at version %d with %d events, want %d at version "+
				"%d with all", test.events, read.State(), read.Version(), len(read.Events()),
				wantState, test.events)
		}
	}
}

// TestAStateFromAnotherVersionIsFoldedAgain: a state stored by another version of the
// application is ignored, the events folded, and the result stored, so the next read is the
// state alone
func TestAStateFromAnotherVersionIsFoldedAgain(t *testing.T) {
	backend := newReadLoggingBackend()
	ctx := context.Background()
	streamID := uuid.New()

	_, err := newCounters(t, backend, counterState(1)).CreateAll(ctx, streamID,
		[]streams.DomainEvent{
			&added{Amount: 2},
			&added{Amount: 3},
		})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	// the stored state is wrong, as an old fold would leave it
	replaceStateData(t, backend, streamID, []byte("100"))

	after := newCounters(t, backend, counterState(2))
	backend.reads = nil
	read, err := after.Get(ctx, streamID)
	if err != nil || read.State() != 5 || read.Version() != 2 {
		t.Fatalf("get with a new state version: %d at version %d, %v; want 5 at version 2",
			read.State(), read.Version(), err)
	}
	want := []string{"state", "events from 1", "save state at 2"}
	if !slices.Equal(backend.reads, want) {
		t.Fatalf("reads with a new state version: %v, want %v", backend.reads, want)
	}

	backend.reads = nil
	read, err = after.Get(ctx, streamID)
	if err != nil || read.State() != 5 || !slices.Equal(backend.reads, []string{"state"}) {
		t.Fatalf("the next get: %d, reads %v, %v; want 5 from the state alone", read.State(),
			backend.reads, err)
	}
}

// TestAStateThatDoesNotUnmarshalFailsTheRead: a state the codec cannot read is a bug to see,
// not one to hide by folding the events again
func TestAStateThatDoesNotUnmarshalFailsTheRead(t *testing.T) {
	backend := newReadLoggingBackend()
	ctx := context.Background()
	streamID := uuid.New()

	counters := newCounters(t, backend, counterState(1))
	_, err := counters.Create(ctx, streamID, &added{Amount: 2})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	replaceStateData(t, backend, streamID, []byte("not a number"))

	backend.reads = nil
	_, err = counters.Get(ctx, streamID)
	if err == nil {
		t.Fatal("get of a state that does not unmarshal: no error, want one")
	}
	if !slices.Equal(backend.reads, []string{"state"}) {
		t.Fatalf("reads of a state that does not unmarshal: %v, want only the state",
			backend.reads)
	}
}

// TestAGetOfAnUnwrittenStreamReadsOnlyTheState: a stream with no events is the zero state,
// found from the state's read alone, and nothing is stored for it
func TestAGetOfAnUnwrittenStreamReadsOnlyTheState(t *testing.T) {
	backend := newReadLoggingBackend()
	counters := newCounters(t, backend, counterState(1))

	read, err := counters.Get(context.Background(), uuid.New())
	if err != nil || read.Version() != 0 || read.State() != 0 {
		t.Fatalf("get of an unwritten stream: %d at version %d, %v; want 0 at version 0",
			read.State(), read.Version(), err)
	}
	if !slices.Equal(backend.reads, []string{"state"}) {
		t.Fatalf("get of an unwritten stream: reads %v, want only the state", backend.reads)
	}
}

// TestNewRepositoryRefusesAnIncompleteStateCodec: a codec that cannot store or read the state
// stops the program where the repository is built
func TestNewRepositoryRefusesAnIncompleteStateCodec(t *testing.T) {
	for name, broken := range map[string]func(stateCodec *streams.StateCodec[int]){
		"version 0": func(stateCodec *streams.StateCodec[int]) {
			stateCodec.Version = 0
		},
		"negative version": func(stateCodec *streams.StateCodec[int]) {
			stateCodec.Version = -1
		},
		"unknown format": func(stateCodec *streams.StateCodec[int]) {
			stateCodec.Format = "csv"
		},
		"no marshal": func(stateCodec *streams.StateCodec[int]) {
			stateCodec.Marshal = nil
		},
		"no unmarshal": func(stateCodec *streams.StateCodec[int]) {
			stateCodec.Unmarshal = nil
		},
	} {
		t.Run(name, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Fatalf("NewRepository with a state codec with %s did not panic", name)
				}
			}()
			stateCodec := counterState(1)
			broken(stateCodec)
			newCounters(t, newReadLoggingBackend(), stateCodec)
		})
	}
}

// replaceStateData stores other bytes as the stream's state, at the version and state version
// it was stored with
func replaceStateData(t *testing.T, backend persistence.Backend, streamID uuid.UUID,
	data []byte) {
	t.Helper()

	stored, _, err := backend.QueryState(context.Background(), streamID)
	if err != nil || stored.Version == 0 {
		t.Fatalf("read the stored state: version %d, %v", stored.Version, err)
	}
	stored.StateData = data
	saved, err := backend.SaveState(context.Background(), stored)
	if err != nil || !saved {
		t.Fatalf("replace the stored state: %t, %v", saved, err)
	}
}

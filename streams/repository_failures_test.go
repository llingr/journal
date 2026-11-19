// SPDX-FileCopyrightText: Copyright (c) 2026 The journal Authors
// SPDX-License-Identifier: Apache-2.0

package streams_test

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"

	"github.com/llingr/journal/codec"
	"github.com/llingr/journal/internal/memory"
	"github.com/llingr/journal/persistence"
	"github.com/llingr/journal/streams"
)

// errBackend is a backend call that failed
var errBackend = errors.New("backend unavailable")

// TestABackendFailureIsNeverSwallowed: whichever call the backend fails, the repository returns
// its error rather than a stream that looks empty or folded, and a write that fails publishes
// nothing and runs nothing after it
func TestABackendFailureIsNeverSwallowed(t *testing.T) {
	ctx := context.Background()

	for _, testCase := range []struct {
		failing    string
		stateCodec *streams.StateCodec[int]
		call       func(repository *streams.Repository[int], read streams.Stream[int],
			outbox *publishCounter) error
	}{
		{"QueryStream", nil, get},
		{"QueryStream", counterState(2), get},
		{"QueryState", counterState(1), get},
		{"SaveState", counterState(2), get},
		{"QueryHead", nil, func(repository *streams.Repository[int], read streams.Stream[int],
			_ *publishCounter) error {
			_, err := repository.Head(ctx, read.StreamID())
			return err
		}},
		{"Append", nil, appendOne},
		{"Append", counterState(1), appendOne},
	} {
		backend := &failingBackend{
			Backend: memory.NewBackend(),
		}
		outbox := &publishCounter{}
		streamID := uuid.New()
		creator := newCountersPublishing(t, backend, outbox, counterState(1))
		_, err := creator.CreateAll(ctx, streamID, []streams.DomainEvent{
			&added{Amount: 2},
		})
		if err != nil {
			t.Fatalf("create: %v", err)
		}
		// read through the creator, so a repository at another state version still has the
		// stream to fold again when it is called
		read, err := creator.Get(ctx, streamID)
		if err != nil {
			t.Fatalf("get before the failure: %v", err)
		}
		repository := newCountersPublishing(t, backend, outbox, testCase.stateCodec)
		outbox.published = 0

		backend.failing = testCase.failing
		err = testCase.call(repository, read, outbox)
		if !errors.Is(err, errBackend) {
			t.Fatalf("with %s failing: got %v, want the backend's error", testCase.failing, err)
		}
		if outbox.published != 0 || outbox.afterCommit != 0 {
			t.Fatalf("with %s failing: %d publishes and %d after-commit calls, want none",
				testCase.failing, outbox.published, outbox.afterCommit)
		}
	}
}

// TestAStateThatFailsToMarshalFailsTheRefold: a read that folds the stream again, for a new
// state version, and cannot marshal the result fails rather than return a state it could not
// store
func TestAStateThatFailsToMarshalFailsTheRefold(t *testing.T) {
	backend := memory.NewBackend()
	ctx := context.Background()
	streamID := uuid.New()
	_, err := newCounters(t, backend, counterState(1)).Create(ctx, streamID, &added{Amount: 2})
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	errMarshal := errors.New("cannot marshal")
	broken := counterState(2)
	broken.Marshal = func(int) ([]byte, error) {
		return nil, errMarshal
	}
	_, err = newCounters(t, backend, broken).Get(ctx, streamID)
	if !errors.Is(err, errMarshal) {
		t.Fatalf("refold through a codec that cannot marshal: got %v, want its error", err)
	}
}

// TestAFailedReadOfTheStoredStateIsNotFoldedOver: a stored state that does not decode fails
// the read, rather than being replaced by a fold that would hide the damage
func TestAFailedReadOfTheStoredStateIsNotFoldedOver(t *testing.T) {
	backend := newReadLoggingBackend()
	counters := newCounters(t, backend, counterState(1))
	ctx := context.Background()
	streamID := uuid.New()
	_, err := counters.Create(ctx, streamID, &added{Amount: 2})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	stored, _, err := backend.QueryState(ctx, streamID)
	if err != nil {
		t.Fatalf("read the stored state: %v", err)
	}
	stored.DataCompression = "lz4"
	stored.StateData = []byte("not an lz4 frame")
	if _, err = backend.SaveState(ctx, stored); err != nil {
		t.Fatalf("store a damaged state: %v", err)
	}

	backend.reads = nil
	_, err = counters.Get(ctx, streamID)
	if err == nil {
		t.Fatal("get of a damaged state succeeded, want an error")
	}
	for _, read := range backend.reads {
		if read != "state" {
			t.Fatalf("reads of a damaged state: %v, want only the state", backend.reads)
		}
	}
}

func get(repository *streams.Repository[int], read streams.Stream[int], _ *publishCounter) error {
	_, err := repository.Get(context.Background(), read.StreamID())
	return err
}

// appendOne appends one event to the stream read, counting any after-commit call
func appendOne(repository *streams.Repository[int], read streams.Stream[int],
	outbox *publishCounter) error {
	_, err := repository.Append(context.Background(), read, &added{Amount: 1},
		streams.AfterCommit(func(context.Context) {
			outbox.afterCommit++
		}))
	return err
}

// failingBackend is the memory backend with one call failing
type failingBackend struct {
	persistence.Backend
	failing string
}

func (f *failingBackend) QueryStream(ctx context.Context, streamID uuid.UUID, fromVersion int32,
	limit int) ([]persistence.PersistedEvent, error) {
	if f.failing == "QueryStream" {
		return nil, errBackend
	}
	return f.Backend.QueryStream(ctx, streamID, fromVersion, limit)
}

func (f *failingBackend) QueryState(ctx context.Context,
	streamID uuid.UUID) (persistence.PersistedState, int32, error) {
	if f.failing == "QueryState" {
		return persistence.PersistedState{}, 0, errBackend
	}
	return f.Backend.QueryState(ctx, streamID)
}

func (f *failingBackend) SaveState(ctx context.Context, state persistence.PersistedState) (bool,
	error) {
	if f.failing == "SaveState" {
		return false, errBackend
	}
	return f.Backend.SaveState(ctx, state)
}

func (f *failingBackend) QueryHead(ctx context.Context, streamID uuid.UUID) (int32, error) {
	if f.failing == "QueryHead" {
		return 0, errBackend
	}
	return f.Backend.QueryHead(ctx, streamID)
}

func (f *failingBackend) Append(ctx context.Context,
	request persistence.AppendRequest) (int32, error) {
	if f.failing == "Append" {
		return 0, errBackend
	}
	return f.Backend.Append(ctx, request)
}

// publishCounter counts the publishes the repository asks for
type publishCounter struct {
	published   int
	afterCommit int
}

func (p *publishCounter) Publish(context.Context, uuid.UUID, int32,
	[]persistence.PersistedEvent) {
	p.published++
}

// newCountersPublishing is newCounters handing its committed events to outbox
func newCountersPublishing(t *testing.T, backend persistence.Backend, outbox *publishCounter,
	stateCodec *streams.StateCodec[int]) *streams.Repository[int] {
	t.Helper()
	pipeline, err := codec.NewPipeline()
	if err != nil {
		t.Fatalf("build the pipeline: %v", err)
	}
	return streams.NewRepository(backend, pipeline, outbox, domainEvents(&added{}),
		func(state int, payload streams.DomainEvent) (int, error) {
			return state + payload.(*added).Amount, nil
		}, nil, stateCodec)
}

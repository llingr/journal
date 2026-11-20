// SPDX-FileCopyrightText: Copyright (c) 2026 The journal Authors
// SPDX-License-Identifier: Apache-2.0

package streams_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	"github.com/google/uuid"

	"github.com/llingr/journal/codec"
	"github.com/llingr/journal/internal/memory"
	"github.com/llingr/journal/persistence"
	"github.com/llingr/journal/streams"
)

// TestNewRepositoryRefusesAMissingCollaborator: a wiring mistake stops the program where the
// service is built, not at its first call.
func TestNewRepositoryRefusesAMissingCollaborator(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("NewRepository with no store did not panic")
		}
	}()
	pipeline, err := codec.NewPipeline()
	if err != nil {
		t.Fatalf("build the pipeline: %v", err)
	}
	streams.NewRepository[int](nil, pipeline, nil, domainEvents(&added{}), nil, nil, nil)
}

var errNegative = errors.New("a negative amount")

// TestARepositoryWithNoFoldKeepsTheZeroState: with no fold the state never changes, a read
// still returns the stream's events, and an append from Head is allowed, as there is no state
// for the new events to land on wrongly
func TestARepositoryWithNoFoldKeepsTheZeroState(t *testing.T) {
	ctx := context.Background()
	pipeline, err := codec.NewPipeline()
	if err != nil {
		t.Fatalf("build the pipeline: %v", err)
	}
	counters := streams.NewRepository[int](memory.NewBackend(), pipeline, nil,
		domainEvents(&added{}), nil, nil, nil)
	counterID := uuid.New()

	_, err = counters.Create(ctx, counterID, &added{Amount: 5})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	head, err := counters.Head(ctx, counterID)
	if err != nil {
		t.Fatalf("head: %v", err)
	}
	appended, err := counters.Append(ctx, head, &added{Amount: 7})
	if err != nil || appended.Version() != 2 || appended.State() != 0 {
		t.Fatalf("append from the head: version %d, state %d, %v; want version 2, state 0",
			appended.Version(), appended.State(), err)
	}
	read, err := counters.Get(ctx, counterID)
	if err != nil || read.State() != 0 || len(read.DomainEvents()) != 2 {
		t.Fatalf("get: state %d with %d events, %v; want state 0 with 2 events",
			read.State(), len(read.DomainEvents()), err)
	}
}

// added is an event that adds its amount to a counter
type added struct {
	Amount int `json:"amount"`
}

func (a *added) EventType() string {
	return "Added"
}

func (a *added) DataFormat() codec.DataFormat {
	return codec.JSON
}

func (a *added) MarshalPayload() ([]byte, error) {
	return json.Marshal(a)
}

func (a *added) UnmarshalPayload(encoded []byte) error {
	return json.Unmarshal(encoded, a)
}

// newCounters is a repository of counters over the backend, publishing nothing, storing its
// state as stateCodec says, nil for not
func newCounters(t *testing.T, backend persistence.Backend,
	stateCodec *streams.StateCodec[int]) *streams.Repository[int] {
	t.Helper()
	pipeline, err := codec.NewPipeline()
	if err != nil {
		t.Fatalf("build the pipeline: %v", err)
	}
	return streams.NewRepository(backend, pipeline, nil, domainEvents(&added{}),
		func(state int, payload streams.DomainEvent) (int, error) {
			amount := payload.(*added).Amount
			if amount < 0 {
				return state, errNegative
			}
			return state + amount, nil
		}, nil, stateCodec)
}

// readLoggingBackend is the memory backend, recording the reads and state saves asked of it
type readLoggingBackend struct {
	persistence.Backend
	reads []string
}

func newReadLoggingBackend() *readLoggingBackend {
	return &readLoggingBackend{
		Backend: memory.NewBackend(),
	}
}

func (r *readLoggingBackend) QueryStream(ctx context.Context, streamID uuid.UUID,
	fromVersion int32, limit int) ([]persistence.PersistedEvent, error) {
	r.reads = append(r.reads, fmt.Sprintf("events from %d", fromVersion))
	return r.Backend.QueryStream(ctx, streamID, fromVersion, limit)
}

func (r *readLoggingBackend) QueryState(ctx context.Context,
	streamID uuid.UUID) (persistence.PersistedState, int32, error) {
	r.reads = append(r.reads, "state")
	return r.Backend.QueryState(ctx, streamID)
}

func (r *readLoggingBackend) SaveState(ctx context.Context,
	state persistence.PersistedState) (bool, error) {
	r.reads = append(r.reads, fmt.Sprintf("save state at %d", state.Version))
	return r.Backend.SaveState(ctx, state)
}

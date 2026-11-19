// SPDX-FileCopyrightText: Copyright (c) 2026 The journal Authors
// SPDX-License-Identifier: Apache-2.0

package journaltest

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/google/uuid"

	"github.com/llingr/journal"
	"github.com/llingr/journal/persistence"
	"github.com/llingr/journal/streams"
)

var streamCases = []suiteCase{
	{"TransitionsFoldTheStream", runTransitionsFoldTheStream},
	{"CreateAllAndAppendAllNumberFromTheStream", runCreateAllAndAppendAllNumberFromTheStream},
	{"AnUnwrittenStreamIsTheZeroStateAtVersion0", runAnUnwrittenStreamIsTheZeroStateAtVersion0},
	{"ATransitionThatRefusesWritesNothing", runATransitionThatRefusesWritesNothing},
	{"AnEventWithNoTransitionIsRefused", runAnEventWithNoTransitionIsRefused},
	{"AHostWithNoTransitionsAppendsFromTheHead", runAHostWithNoTransitionsAppendsFromTheHead},
	{"EachCallRefusesTheWrongCase", runEachCallRefusesTheWrongCase},
	{"AStaleAppendIsAVersionConflict", runAStaleAppendIsAVersionConflict},
	{"RacingAppendsHaveOneWinner", runRacingAppendsHaveOneWinner},
	{"MetadataTravelsWithItsEvent", runMetadataTravelsWithItsEvent},
	{"AfterCommitRunsOnlyOnceCommitted", runAfterCommitRunsOnlyOnceCommitted},
	{"AnEventWithNoPayloadIsStored", runAnEventWithNoPayloadIsStored},
}

// runTransitionsFoldTheStream: each event moves the state through its own typed transition,
// on a write and again when the stream is read back, which returns the events as written
func runTransitionsFoldTheStream(t *testing.T, backend persistence.Backend) {
	ctx := context.Background()
	orders := newOrders(storeWith(backend, &recordingPublisher{}, nil))
	orderID := uuid.New()

	created, err := orders.Create(ctx, orderID, &placed{Amount: 5})
	requireNoError(t, err, "create")
	written, err := orders.Append(ctx, created, &shipped{})
	requireNoError(t, err, "append")
	reread, err := orders.Get(ctx, orderID)
	requireNoError(t, err, "get")

	want := order{
		Amount: 5,
		Status: "SHIPPED",
		Events: 2,
	}
	for name, stream := range map[string]streams.Stream[order]{
		"written": written,
		"reread":  reread,
	} {
		if stream.State() != want || stream.Version() != 2 || stream.StreamID() != orderID {
			t.Fatalf("%s: %+v at version %d of %s, want %+v at version 2 of %s", name,
				stream.State(), stream.Version(), stream.StreamID(), want, orderID)
		}
		if stream.LastEventType() != "SHIPPED" ||
			!stream.LastAppendTs().Equal(stream.Events()[1].AppendTs) {
			t.Fatalf("%s: last event %s at %s, want SHIPPED at the second event's time", name,
				stream.LastEventType(), stream.LastAppendTs())
		}
		domainEvents := stream.DomainEvents()
		_, isPlaced := domainEvents[0].(*placed)
		_, isShipped := domainEvents[1].(*shipped)
		if len(domainEvents) != 2 || !isPlaced || !isShipped {
			t.Fatalf("%s: domain events %#v, want *placed then *shipped", name, domainEvents)
		}
		for index, event := range stream.Events() {
			if event.Version != int32(index+1) || event.StreamID != orderID {
				t.Fatalf("%s: event %d is version %d of %s", name, index, event.Version,
					event.StreamID)
			}
		}
	}
}

// runCreateAllAndAppendAllNumberFromTheStream: the caller never supplies a version; the stream
// it holds says where the next one starts, and every event of a call is stored
func runCreateAllAndAppendAllNumberFromTheStream(t *testing.T, backend persistence.Backend) {
	ctx := context.Background()
	orders := newOrders(storeWith(backend, &recordingPublisher{}, nil))
	orderID := uuid.New()

	created, err := orders.CreateAll(ctx, orderID, []streams.DomainEvent{
		&placed{Amount: 2},
		&placed{Amount: 3},
	})
	requireNoError(t, err, "create two events")
	appended, err := orders.AppendAll(ctx, created, []streams.DomainEvent{
		&placed{Amount: 4},
		&shipped{},
	})
	requireNoError(t, err, "append two events")

	if created.Version() != 2 || appended.Version() != 4 || appended.State().Amount != 9 ||
		len(appended.Events()) != 4 {
		t.Fatalf("created at version %d, appended to %d with amount %d and %d events; want 2, "+
			"4, 9 and 4", created.Version(), appended.Version(), appended.State().Amount,
			len(appended.Events()))
	}
	var stored []int32
	for _, event := range storedEvents(t, backend, orderID) {
		stored = append(stored, event.Version)
	}
	requireVersions(t, "stored", stored, 1, 2, 3, 4)
}

// runAnUnwrittenStreamIsTheZeroStateAtVersion0: a stream with no events reads as the zero
// state with no events, by Get and by Head
func runAnUnwrittenStreamIsTheZeroStateAtVersion0(t *testing.T, backend persistence.Backend) {
	ctx := context.Background()
	orders := newOrders(storeWith(backend, &recordingPublisher{}, nil))
	orderID := uuid.New()

	read, err := orders.Get(ctx, orderID)
	requireNoError(t, err, "get")
	head, err := orders.Head(ctx, orderID)
	requireNoError(t, err, "head")
	if read.Version() != 0 || read.State() != (order{}) || len(read.Events()) != 0 ||
		head.Version() != 0 {
		t.Fatalf("unwritten stream: %+v at version %d with %d events, head %d; want the zero "+
			"state at 0", read.State(), read.Version(), len(read.Events()), head.Version())
	}
}

// runATransitionThatRefusesWritesNothing: an event the aggregate refuses never becomes a
// fact, nor do the events beside it, and the caller keeps the stream it had
func runATransitionThatRefusesWritesNothing(t *testing.T, backend persistence.Backend) {
	ctx := context.Background()
	publisher := &recordingPublisher{}
	orders := newOrders(storeWith(backend, publisher, nil))
	orderID := uuid.New()

	_, err := orders.CreateAll(ctx, orderID, []streams.DomainEvent{
		&placed{Amount: 1},
		&placed{Amount: -1},
	})
	requireError(t, err, errNegativeAmount, "create with a refused event")
	requireHead(t, backend, orderID, 0)

	created, err := orders.Create(ctx, orderID, &placed{Amount: 1})
	requireNoError(t, err, "create")
	kept, err := orders.Append(ctx, created, &placed{Amount: -1})
	requireError(t, err, errNegativeAmount, "append a refused event")
	if kept.Version() != 1 || kept.State().Amount != 1 {
		t.Fatalf("after a refused append the caller holds %+v at version %d, want the stream "+
			"it had", kept.State(), kept.Version())
	}
	requireHead(t, backend, orderID, 1)
	requireVersions(t, "published", publisher.versions(), 1)
}

// runAnEventWithNoTransitionIsRefused: an event type the repository has no transition for is
// refused before anything is written, and a stored one fails the read that meets it
func runAnEventWithNoTransitionIsRefused(t *testing.T, backend persistence.Backend) {
	ctx := context.Background()
	eventStore := storeWith(backend, &recordingPublisher{}, nil)
	placedOnly := journal.NewRepository(eventStore, journal.WithTransitions(
		journal.Transition(func(state order, event *placed) (order, error) {
			state.Amount += event.Amount
			return state, nil
		}),
	))
	orderID := uuid.New()

	_, err := placedOnly.Create(ctx, orderID, &shipped{})
	requireError(t, err, journal.ErrNoTransition, "create with no transition for the event")
	requireHead(t, backend, orderID, 0)

	created, err := newOrders(eventStore).Create(ctx, orderID, &placed{Amount: 1})
	requireNoError(t, err, "create")
	_, err = newOrders(eventStore).Append(ctx, created, &shipped{})
	requireNoError(t, err, "append")
	_, err = placedOnly.Get(ctx, orderID)
	requireError(t, err, journal.ErrNoTransition, "get a stream holding an event with no "+
		"transition")
}

// runAHostWithNoTransitionsAppendsFromTheHead: a host whose state lives elsewhere appends
// from Head without folding, and its streams cannot be folded
func runAHostWithNoTransitionsAppendsFromTheHead(t *testing.T, backend persistence.Backend) {
	ctx := context.Background()
	publisher := &recordingPublisher{}
	runs := journal.NewRepository[struct{}](storeWith(backend, publisher, nil))
	runID := uuid.New()

	_, err := runs.Create(ctx, runID, &placed{Amount: 1})
	requireNoError(t, err, "create")
	head, err := runs.Head(ctx, runID)
	requireNoError(t, err, "head")
	appended, err := runs.Append(ctx, head, &shipped{})
	if err != nil || appended.Version() != 2 {
		t.Fatalf("append from the head: version %d, %v, want 2", appended.Version(), err)
	}
	requireVersions(t, "published", publisher.versions(), 1, 2)
	_, err = runs.Get(ctx, runID)
	requireError(t, err, journal.ErrNoTransition, "get without transitions")
}

// runEachCallRefusesTheWrongCase: a stream is started once, by Create, continued only from a
// stream that exists, and never with no events
func runEachCallRefusesTheWrongCase(t *testing.T, backend persistence.Backend) {
	ctx := context.Background()
	orders := newOrders(storeWith(backend, &recordingPublisher{}, nil))
	orderID := uuid.New()

	unwritten, err := orders.Get(ctx, orderID)
	requireNoError(t, err, "get an unwritten stream")
	_, err = orders.Append(ctx, unwritten, &placed{Amount: 1})
	requireError(t, err, streams.ErrStreamNotCreated, "append to an unwritten stream")
	_, err = orders.CreateAll(ctx, orderID, nil)
	requireError(t, err, streams.ErrNoEvents, "create with no events")
	requireHead(t, backend, orderID, 0)

	created, err := orders.Create(ctx, orderID, &placed{Amount: 1})
	requireNoError(t, err, "create")
	_, err = orders.Create(ctx, orderID, &placed{Amount: 1})
	requireError(t, err, streams.ErrStreamExists, "create a stream that exists")
	if errors.Is(err, streams.ErrVersionConflict) {
		t.Fatalf("create of a stream that exists is also a conflict, which a caller retries: %v",
			err)
	}
	_, err = orders.AppendAll(ctx, created, nil)
	requireError(t, err, streams.ErrNoEvents, "append no events")
	requireHead(t, backend, orderID, 1)
}

// runAStaleAppendIsAVersionConflict: a writer holding a stream another writer has appended
// to is refused and writes nothing, and succeeds once it reads the stream again
func runAStaleAppendIsAVersionConflict(t *testing.T, backend persistence.Backend) {
	ctx := context.Background()
	publisher := &recordingPublisher{}
	orders := newOrders(storeWith(backend, publisher, nil))
	orderID := uuid.New()

	created, err := orders.Create(ctx, orderID, &placed{Amount: 1})
	requireNoError(t, err, "create")
	_, err = orders.Append(ctx, created, &placed{Amount: 2})
	requireNoError(t, err, "the first writer's append")
	_, err = orders.Append(ctx, created, &placed{Amount: 3})
	requireError(t, err, streams.ErrVersionConflict, "the stale writer's append")
	requireHead(t, backend, orderID, 2)

	reread, err := orders.Get(ctx, orderID)
	requireNoError(t, err, "read again")
	retried, err := orders.Append(ctx, reread, &placed{Amount: 3})
	if err != nil || retried.Version() != 3 || retried.State().Amount != 6 {
		t.Fatalf("retry: amount %d at version %d, %v; want 6 at version 3",
			retried.State().Amount, retried.Version(), err)
	}
	requireVersions(t, "published", publisher.versions(), 1, 2, 3)
}

// runRacingAppendsHaveOneWinner: writers appending to the same version at once store one
// event, and every other writer is told to read again
func runRacingAppendsHaveOneWinner(t *testing.T, backend persistence.Backend) {
	ctx := context.Background()
	orders := newOrders(storeWith(backend, &recordingPublisher{}, nil))
	orderID := uuid.New()
	created, err := orders.Create(ctx, orderID, &placed{Amount: 1})
	requireNoError(t, err, "create")

	const writers = 8
	results := make(chan error, writers)
	start := make(chan struct{})
	var racing sync.WaitGroup
	for range writers {
		racing.Go(func() {
			<-start
			_, appendErr := orders.Append(ctx, created, &placed{Amount: 1})
			results <- appendErr
		})
	}
	close(start)
	racing.Wait()
	close(results)

	won := 0
	for result := range results {
		if result == nil {
			won++
			continue
		}
		requireError(t, result, streams.ErrVersionConflict, "a losing append")
	}
	if won != 1 {
		t.Fatalf("%d of %d racing appends won, want 1", won, writers)
	}
	requireHead(t, backend, orderID, 2)
}

// runMetadataTravelsWithItsEvent: metadata given to one event is stored with that event alone
// and reaches the stream written, the stream read and the publisher unchanged
func runMetadataTravelsWithItsEvent(t *testing.T, backend persistence.Backend) {
	ctx := context.Background()
	publisher := &recordingPublisher{}
	orders := newOrders(storeWith(backend, publisher, nil))
	orderID := uuid.New()
	traceHeaders := []byte(`{"traceparent":"00-4bf92f3577b34da6a3ce929d0e0e4736-01"}`)

	created, err := orders.Create(ctx, orderID, streams.WithMetadata(&placed{Amount: 1},
		traceHeaders))
	requireNoError(t, err, "create with metadata")
	written, err := orders.Append(ctx, created, &shipped{})
	requireNoError(t, err, "append without metadata")
	reread, err := orders.Get(ctx, orderID)
	requireNoError(t, err, "get")

	for name, events := range map[string][]streams.Event{
		"written":   written.Events(),
		"reread":    reread.Events(),
		"published": publisher.published(),
	} {
		if len(events) != 2 || !bytes.Equal(events[0].Metadata, traceHeaders) ||
			len(events[1].Metadata) != 0 {
			t.Fatalf("%s events carry the wrong metadata, want the headers on version 1 only",
				name)
		}
	}
	stored := storedEvents(t, backend, orderID)
	if !bytes.Equal(stored[0].Metadata, traceHeaders) || len(stored[1].Metadata) != 0 {
		t.Fatalf("stored metadata %q and %q, want the headers on version 1 only",
			stored[0].Metadata, stored[1].Metadata)
	}
	if _, isPlaced := reread.DomainEvents()[0].(*placed); !isPlaced ||
		reread.State().Amount != 1 {
		t.Fatalf("the transition did not receive the event's own type through its metadata")
	}
}

// runAfterCommitRunsOnlyOnceCommitted: an after-commit function runs once the events are
// stored and published, with a context the caller's cancellation does not reach, and never
// for a write that failed
func runAfterCommitRunsOnlyOnceCommitted(t *testing.T, backend persistence.Backend) {
	publisher := &recordingPublisher{}
	orders := newOrders(storeWith(backend, publisher, nil))
	orderID := uuid.New()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var ran []string
	created, err := orders.Create(ctx, orderID, &placed{Amount: 1},
		streams.AfterCommit(func(committedCtx context.Context) {
			cancel()
			head, headErr := backend.QueryHead(context.Background(), orderID)
			ran = append(ran, "create")
			if headErr != nil || head != 1 || len(publisher.versions()) != 1 ||
				committedCtx.Err() != nil {
				t.Errorf("after commit: head %d, %v, published %v, context %v; want the event "+
					"stored and published and the context live", head, headErr,
					publisher.versions(), committedCtx.Err())
			}
		}))
	requireNoError(t, err, "create")
	_, err = orders.Create(context.Background(), orderID, &placed{Amount: 1},
		streams.AfterCommit(func(context.Context) {
			ran = append(ran, "failed create")
		}))
	requireError(t, err, streams.ErrStreamExists, "create again")
	_, err = orders.Append(context.Background(), created, &placed{Amount: -1},
		streams.AfterCommit(func(context.Context) {
			ran = append(ran, "refused append")
		}))
	requireError(t, err, errNegativeAmount, "a refused append")
	if len(ran) != 1 || ran[0] != "create" {
		t.Fatalf("after-commit functions ran for %v, want the create alone", ran)
	}
}

// runAnEventWithNoPayloadIsStored: an event whose payload marshals to nothing, as an empty
// protobuf message does, is stored, read back and published empty
func runAnEventWithNoPayloadIsStored(t *testing.T, backend persistence.Backend) {
	ctx := context.Background()
	publisher := &recordingPublisher{}
	orders := newOrders(storeWith(backend, publisher, nil))
	orderID := uuid.New()

	created, err := orders.Create(ctx, orderID, &marked{})
	requireNoError(t, err, "create with an empty payload")
	_, err = orders.Append(ctx, created, &marked{})
	requireNoError(t, err, "append with an empty payload")
	reread, err := orders.Get(ctx, orderID)
	requireNoError(t, err, "get")
	if reread.State().Events != 2 || len(reread.Events()[0].Payload) != 0 {
		t.Fatalf("read back %d events, the first with %d bytes; want 2 empty", reread.State().Events,
			len(reread.Events()[0].Payload))
	}
	published := publisher.published()
	if len(published) != 2 || len(published[0].Payload) != 0 || len(published[1].Payload) != 0 {
		t.Fatalf("published %d events, want 2 with empty payloads", len(published))
	}
}

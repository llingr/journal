// SPDX-FileCopyrightText: Copyright (c) 2026 The journal Authors
// SPDX-License-Identifier: Apache-2.0

package journaltest

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/llingr/journal"
	"github.com/llingr/journal/codec"
	"github.com/llingr/journal/persistence"
	"github.com/llingr/journal/publish"
	"github.com/llingr/journal/streams"
)

var publishCases = []suiteCase{
	{"EventsArePublishedInOrderOnceCommitted", runEventsArePublishedInOrderOnceCommitted},
	{"ThePublisherGetsTheDecodedEvent", runThePublisherGetsTheDecodedEvent},
	{"PublishedReportsTheVersionsSent", runPublishedReportsTheVersionsSent},
	{"AStoreWithoutAPublisherWritesNoOutbox", runAStoreWithoutAPublisherWritesNoOutbox},
	{"AFailedPublishGoesWithTheNextAppend", runAFailedPublishGoesWithTheNextAppend},
	{"ARefusedVersionHoldsBackItsStream", runARefusedVersionHoldsBackItsStream},
	{"APanickingPublisherIsAFailure", runAPanickingPublisherIsAFailure},
	{"APartlyDurableBatchClearsWhatIsDurable", runAPartlyDurableBatchClearsWhatIsDurable},
	{"ABatchHoldsAtMostTheBatchSize", runABatchHoldsAtMostTheBatchSize},
	{"APublishGivesUpAtItsTimeout", runAPublishGivesUpAtItsTimeout},
	{"TwoStoresDeliverEachVersionFirstInOrder", runTwoStoresDeliverEachVersionFirstInOrder},
}

// runEventsArePublishedInOrderOnceCommitted: the publisher is called only once the events are
// stored, a stream's events go in order, and nothing is left unpublished
func runEventsArePublishedInOrderOnceCommitted(t *testing.T, backend persistence.Backend) {
	ctx := context.Background()
	publisher := &recordingPublisher{
		respond: func(ctx context.Context, _ int, events []streams.Event) (int, error) {
			head, err := backend.QueryHead(ctx, events[0].StreamID)
			if err != nil || head < events[len(events)-1].Version {
				return 0, fmt.Errorf("published at head %d, %v, before the commit", head, err)
			}
			return len(events), nil
		},
	}
	failed := &failures{}
	orders := newOrders(storeWith(backend, publisher, failed))
	orderID := uuid.New()

	created, err := orders.CreateAll(ctx, orderID, []streams.DomainEvent{
		&placed{Amount: 1},
		&placed{Amount: 2},
	})
	requireNoError(t, err, "create")
	_, err = orders.Append(ctx, created, &shipped{})
	requireNoError(t, err, "append")

	if reported := failed.reported(); len(reported) != 0 {
		t.Fatalf("publishing failed: %v", reported)
	}
	requireVersions(t, "published", publisher.versions(), 1, 2, 3)
	requirePending(t, backend, orderID)
}

// runThePublisherGetsTheDecodedEvent: the publisher receives each event's stream, version,
// type, format, append time and metadata as stored, and its payload plain
func runThePublisherGetsTheDecodedEvent(t *testing.T, backend persistence.Backend) {
	publisher := &recordingPublisher{}
	orders := newOrders(storeWith(backend, publisher, nil))
	orderID := uuid.New()
	payload := &placed{
		Amount: 4,
		Note:   "plain",
	}
	plain, err := payload.MarshalPayload()
	requireNoError(t, err, "marshal")

	_, err = orders.Create(context.Background(), orderID,
		streams.WithMetadata(payload, []byte("headers")))
	requireNoError(t, err, "create")

	stored := storedEvents(t, backend, orderID)[0]
	published := publisher.published()
	want := streams.Event{
		StreamID:   orderID,
		Version:    1,
		EventType:  "PLACED",
		DataFormat: codec.JSON,
		AppendTs:   stored.EventAppendTs,
		Payload:    plain,
		Metadata:   []byte("headers"),
	}
	if len(published) != 1 || published[0].StreamID != want.StreamID ||
		published[0].Version != want.Version || published[0].EventType != want.EventType ||
		published[0].DataFormat != want.DataFormat || !published[0].AppendTs.Equal(want.AppendTs) ||
		string(published[0].Payload) != string(want.Payload) ||
		string(published[0].Metadata) != string(want.Metadata) {
		t.Fatalf("published %+v, want %+v", published, want)
	}
}

// runPublishedReportsTheVersionsSent: Hooks.Published names the stream and the first and last
// versions each publish sent
func runPublishedReportsTheVersionsSent(t *testing.T, backend persistence.Backend) {
	ctx := context.Background()
	var reported []string
	eventStore := journal.NewStore(backend,
		journal.WithPublisher(&recordingPublisher{}),
		journal.WithHooks(publish.Hooks{
			Published: func(streamID uuid.UUID, fromVersion int32, toVersion int32) {
				reported = append(reported, fmt.Sprintf("%s %d to %d", streamID, fromVersion,
					toVersion))
			},
		}))
	orderID := uuid.New()

	created, err := newOrders(eventStore).CreateAll(ctx, orderID, []streams.DomainEvent{
		&placed{Amount: 1},
		&placed{Amount: 2},
	})
	requireNoError(t, err, "create")
	_, err = newOrders(eventStore).Append(ctx, created, &shipped{})
	requireNoError(t, err, "append")

	want := []string{fmt.Sprintf("%s 1 to 2", orderID), fmt.Sprintf("%s 3 to 3", orderID)}
	if !slices.Equal(reported, want) {
		t.Fatalf("published reported %v, want %v", reported, want)
	}
}

// runAStoreWithoutAPublisherWritesNoOutbox: with nothing to publish to, nothing would ever
// clear an outbox row, so none is written, a sweep has nothing to do and a republish is
// refused
func runAStoreWithoutAPublisherWritesNoOutbox(t *testing.T, backend persistence.Backend) {
	ctx := context.Background()
	eventStore := journal.NewStore(backend)
	eventStore.Start(ctx)
	orders := newOrders(eventStore)
	orderID := uuid.New()

	created, err := orders.Create(ctx, orderID, &placed{Amount: 1})
	requireNoError(t, err, "create")
	_, err = orders.Append(ctx, created, &shipped{})
	requireNoError(t, err, "append")
	requireHead(t, backend, orderID, 2)
	requirePending(t, backend, orderID)

	swept, err := eventStore.Sweep(ctx, time.Hour)
	if err != nil || swept != 0 {
		t.Fatalf("sweep without a publisher: %d streams, %v; want 0", swept, err)
	}
	_, err = eventStore.RepublishEvents(ctx, orderID, 1, publish.Always)
	requireError(t, err, journal.ErrNoPublisher, "republish events without a publisher")
	err = eventStore.RepublishEvent(ctx, orderID, 1, publish.Always)
	requireError(t, err, journal.ErrNoPublisher, "republish an event without a publisher")
	requireNoError(t, eventStore.Shutdown(ctx), "shut down")
}

// runAFailedPublishGoesWithTheNextAppend: a failed publish does not fail the write; its events
// stay unpublished, Hooks.Failed hears of it, and the stream's next append sends them first
func runAFailedPublishGoesWithTheNextAppend(t *testing.T, backend persistence.Backend) {
	ctx := context.Background()
	publisher := &recordingPublisher{
		respond: failFirst,
	}
	failed := &failures{}
	orders := newOrders(storeWith(backend, publisher, failed))
	orderID := uuid.New()

	created, err := orders.Create(ctx, orderID, &placed{Amount: 1})
	requireNoError(t, err, "create, its publish failing")
	if reported := failed.reported(); len(reported) != 1 || !errors.Is(reported[0], errBrokerDown) {
		t.Fatalf("failures reported %v, want the broker's", reported)
	}
	requirePending(t, backend, orderID, 1)

	_, err = orders.Append(ctx, created, &shipped{})
	requireNoError(t, err, "append")
	requireVersions(t, "published", publisher.versions(), 1, 2)
	requirePending(t, backend, orderID)
}

// runARefusedVersionHoldsBackItsStream: a version the destination refuses for good holds back
// every later one, so none overtakes it; deleting its outbox row, the event staying stored,
// lets the next append publish the rest
func runARefusedVersionHoldsBackItsStream(t *testing.T, backend persistence.Backend) {
	ctx := context.Background()
	publisher := &recordingPublisher{
		respond: func(_ context.Context, _ int, events []streams.Event) (int, error) {
			for index, event := range events {
				if event.Version == 1 {
					return index, errors.New("record too large")
				}
			}
			return len(events), nil
		},
	}
	failed := &failures{}
	orders := newOrders(storeWith(backend, publisher, failed))
	orderID := uuid.New()

	created, err := orders.Create(ctx, orderID, &placed{Amount: 1})
	requireNoError(t, err, "create")
	appended, err := orders.Append(ctx, created, &placed{Amount: 2})
	requireNoError(t, err, "append version 2")
	if len(failed.reported()) != 2 || len(publisher.versions()) != 0 {
		t.Fatalf("with version 1 refused: %d failures, published %v; want 2 and none",
			len(failed.reported()), publisher.versions())
	}
	requirePending(t, backend, orderID, 1, 2)

	requireNoError(t, backend.DeletePublished(ctx, orderID, 1), "delete version 1's outbox row")
	_, err = orders.Append(ctx, appended, &shipped{})
	requireNoError(t, err, "append version 3")
	requireVersions(t, "published", publisher.versions(), 2, 3)
	requirePending(t, backend, orderID)
	requireHead(t, backend, orderID, 3)
}

// runAPanickingPublisherIsAFailure: a publisher that panics fails the publish rather than the
// write or the process, Hooks.Failed hears ErrPublisherPanic, and the events stay unpublished
func runAPanickingPublisherIsAFailure(t *testing.T, backend persistence.Backend) {
	publisher := &recordingPublisher{
		respond: func(context.Context, int, []streams.Event) (int, error) {
			panic("publisher bug")
		},
	}
	failed := &failures{}
	orderID := uuid.New()

	_, err := newOrders(storeWith(backend, publisher, failed)).Create(context.Background(),
		orderID, &placed{Amount: 1})
	requireNoError(t, err, "create, its publisher panicking")
	if reported := failed.reported(); len(reported) != 1 ||
		!errors.Is(reported[0], publish.ErrPublisherPanic) {
		t.Fatalf("failures reported %v, want ErrPublisherPanic", reported)
	}
	requirePending(t, backend, orderID, 1)
}

// runAPartlyDurableBatchClearsWhatIsDurable: a publish that fails part way clears the events
// before the failure and leaves the rest, so the next publish sends only those
func runAPartlyDurableBatchClearsWhatIsDurable(t *testing.T, backend persistence.Backend) {
	ctx := context.Background()
	publisher := &recordingPublisher{
		respond: func(_ context.Context, call int, events []streams.Event) (int, error) {
			if call == 0 {
				return 1, errBrokerDown
			}
			return len(events), nil
		},
	}
	orders := newOrders(storeWith(backend, publisher, &failures{}))
	orderID := uuid.New()

	created, err := orders.CreateAll(ctx, orderID, []streams.DomainEvent{
		&placed{Amount: 1},
		&placed{Amount: 2},
		&placed{Amount: 3},
	})
	requireNoError(t, err, "create")
	requirePending(t, backend, orderID, 2, 3)
	_, err = orders.Append(ctx, created, &shipped{})
	requireNoError(t, err, "append")
	requireVersions(t, "published", publisher.versions(), 1, 2, 3, 4)
	requirePending(t, backend, orderID)
}

// runABatchHoldsAtMostTheBatchSize: a long write goes out a batch at a time, 25 by default or
// as WithPublishBatchSize says
func runABatchHoldsAtMostTheBatchSize(t *testing.T, backend persistence.Backend) {
	for _, testCase := range []struct {
		name    string
		options []journal.StoreOption
		events  int
		want    []int
	}{
		{name: "default", events: 30, want: []int{25, 5}},
		{name: "two", options: []journal.StoreOption{journal.WithPublishBatchSize(2)}, events: 5,
			want: []int{2, 2, 1}},
	} {
		publisher := &recordingPublisher{}
		payloads := make([]streams.DomainEvent, testCase.events)
		for index := range payloads {
			payloads[index] = &placed{Amount: 1}
		}
		_, err := newOrders(storeWith(backend, publisher, nil, testCase.options...)).CreateAll(
			context.Background(), uuid.New(), payloads)
		requireNoError(t, err, "%s: create", testCase.name)

		if sizes := publisher.batchSizes(); !slices.Equal(sizes, testCase.want) {
			t.Fatalf("%s: batches of %v, want %v", testCase.name, sizes, testCase.want)
		}
	}
}

// runAPublishGivesUpAtItsTimeout: a destination that never answers fails the publish at the
// timeout, so the writer is not held for an outage, and the events stay unpublished
func runAPublishGivesUpAtItsTimeout(t *testing.T, backend persistence.Backend) {
	publisher := &recordingPublisher{
		respond: func(ctx context.Context, _ int, _ []streams.Event) (int, error) {
			<-ctx.Done()
			return 0, ctx.Err()
		},
	}
	failed := &failures{}
	orders := newOrders(storeWith(backend, publisher, failed,
		journal.WithPublishTimeout(time.Second)))
	orderID := uuid.New()

	started := time.Now()
	_, err := orders.Create(context.Background(), orderID, &placed{Amount: 1})
	requireNoError(t, err, "create, its publisher hanging")
	if took := time.Since(started); took > 5*time.Second {
		t.Fatalf("the write took %s, want about the second's timeout", took)
	}
	if reported := failed.reported(); len(reported) != 1 ||
		!errors.Is(reported[0], context.DeadlineExceeded) {
		t.Fatalf("failures reported %v, want the deadline", reported)
	}
	requirePending(t, backend, orderID, 1)
}

// runTwoStoresDeliverEachVersionFirstInOrder: two instances appending to one stream and
// publishing what they append, with no lock between them, never send a version for the first
// time before a lower one; a repeat of a lower version is all a consumer sees out of order
func runTwoStoresDeliverEachVersionFirstInOrder(t *testing.T, backend persistence.Backend) {
	ctx := context.Background()
	// up to 4ms a call as a broker takes, so the other instance commits while a batch is out
	publisher := &recordingPublisher{
		respond: func(_ context.Context, _ int, events []streams.Event) (int, error) {
			time.Sleep(time.Duration(rand.IntN(4000)) * time.Microsecond)
			return len(events), nil
		},
	}
	failed := &failures{}
	first := journal.NewRepository[struct{}](storeWith(backend, publisher, failed))
	second := journal.NewRepository[struct{}](storeWith(backend, publisher, failed))
	streamID := uuid.New()
	_, err := first.Create(ctx, streamID, &placed{Amount: 1})
	requireNoError(t, err, "create")

	const rounds = 25
	var instances sync.WaitGroup
	for _, instance := range []*streams.Repository[struct{}]{first, second} {
		instances.Go(func() {
			for range rounds {
				appendErr := appendFromHead(ctx, instance, streamID)
				if appendErr != nil {
					t.Errorf("append: %v", appendErr)
					return
				}
			}
		})
	}
	instances.Wait()

	if reported := failed.reported(); len(reported) != 0 {
		t.Fatalf("publishing failed: %v", reported)
	}
	delivered := publisher.versions()
	var firsts []int32
	for _, version := range delivered {
		if !slices.Contains(firsts, version) {
			firsts = append(firsts, version)
		}
	}
	for index, version := range firsts {
		if version != int32(index+1) {
			t.Fatalf("first deliveries %v are not versions 1 to %d in order (all: %v)", firsts,
				2*rounds+1, delivered)
		}
	}
	if len(firsts) != 2*rounds+1 {
		t.Fatalf("%d versions delivered, want %d", len(firsts), 2*rounds+1)
	}
	requirePending(t, backend, streamID)
}

// appendFromHead is one host write: read the head, append, and read again when another
// instance got there first
func appendFromHead(ctx context.Context, repository *streams.Repository[struct{}],
	streamID uuid.UUID) error {
	for {
		stream, err := repository.Head(ctx, streamID)
		if err != nil {
			return err
		}
		_, err = repository.Append(ctx, stream, &placed{Amount: 1})
		if !errors.Is(err, streams.ErrVersionConflict) {
			return err
		}
	}
}

// SPDX-FileCopyrightText: Copyright (c) 2026 The journal Authors
// SPDX-License-Identifier: Apache-2.0

package journaltest

import (
	"context"
	"slices"
	"testing"

	"github.com/google/uuid"

	"github.com/llingr/journal"
	"github.com/llingr/journal/persistence"
	"github.com/llingr/journal/streams"
)

var partitionKeyCases = []suiteCase{
	{"WithoutAKeyFunctionTheKeyIsTheStreamID", runWithoutAKeyFunctionTheKeyIsTheStreamID},
	{"TheKeyIsChosenFromTheNewState", runTheKeyIsChosenFromTheNewState},
	{"AKeyThatIsTheStreamIDIsStoredAsNone", runAKeyThatIsTheStreamIDIsStoredAsNone},
	{"AnEmptyKeyFailsTheWriteBeforeAnythingIsStored",
		runAnEmptyKeyFailsTheWriteBeforeAnythingIsStored},
}

// runWithoutAKeyFunctionTheKeyIsTheStreamID: a stream's events are published under its id,
// and the event rows carry no key, which costs a byte rather than the id again
func runWithoutAKeyFunctionTheKeyIsTheStreamID(t *testing.T, backend persistence.Backend) {
	publisher := &recordingPublisher{}
	orderID := uuid.New()

	_, err := newOrders(storeWith(backend, publisher, nil)).Create(context.Background(), orderID,
		&placed{Amount: 1})
	requireNoError(t, err, "create")
	if keys := publisher.keys(); !slices.Equal(keys, []string{orderID.String()}) {
		t.Fatalf("published under %v, want the stream id", keys)
	}
	if stored := storedEvents(t, backend, orderID)[0]; stored.PartitionKey != "" {
		t.Fatalf("stored key %q, want none", stored.PartitionKey)
	}
}

// runTheKeyIsChosenFromTheNewState: the key function sees the stream with the new events'
// state, and its key is stored on every event and published under
func runTheKeyIsChosenFromTheNewState(t *testing.T, backend persistence.Backend) {
	ctx := context.Background()
	publisher := &recordingPublisher{}
	var seen []int
	orders := newOrders(storeWith(backend, publisher, nil),
		journal.WithPartitionKey(func(stream streams.Stream[order]) string {
			seen = append(seen, stream.State().Amount)
			return "customer-7"
		}))
	orderID := uuid.New()

	created, err := orders.Create(ctx, orderID, &placed{Amount: 1})
	requireNoError(t, err, "create")
	_, err = orders.AppendAll(ctx, created, []streams.DomainEvent{
		&placed{Amount: 2},
		&shipped{},
	})
	requireNoError(t, err, "append")

	if !slices.Equal(seen, []int{1, 3}) {
		t.Fatalf("the key function saw amounts %v, want 1 then 3, each write's new state", seen)
	}
	if keys := publisher.keys(); !slices.Equal(keys, []string{"customer-7", "customer-7"}) {
		t.Fatalf("published under %v, want customer-7 for each write", keys)
	}
	for _, stored := range storedEvents(t, backend, orderID) {
		if stored.PartitionKey != "customer-7" {
			t.Fatalf("version %d stored with key %q, want customer-7", stored.Version,
				stored.PartitionKey)
		}
	}
}

// runAKeyThatIsTheStreamIDIsStoredAsNone: a key function returning the stream id stores no
// key, as having none would, and still publishes under the id
func runAKeyThatIsTheStreamIDIsStoredAsNone(t *testing.T, backend persistence.Backend) {
	publisher := &recordingPublisher{}
	orders := newOrders(storeWith(backend, publisher, nil),
		journal.WithPartitionKey(func(stream streams.Stream[order]) string {
			return stream.StreamID().String()
		}))
	orderID := uuid.New()

	_, err := orders.Create(context.Background(), orderID, &placed{Amount: 1})
	requireNoError(t, err, "create")
	if stored := storedEvents(t, backend, orderID)[0]; stored.PartitionKey != "" {
		t.Fatalf("stored key %q, want none", stored.PartitionKey)
	}
	if keys := publisher.keys(); !slices.Equal(keys, []string{orderID.String()}) {
		t.Fatalf("published under %v, want the stream id", keys)
	}
}

// runAnEmptyKeyFailsTheWriteBeforeAnythingIsStored: a key function returning nothing is a
// mistake that fails the write, so no event is ever published under no key
func runAnEmptyKeyFailsTheWriteBeforeAnythingIsStored(t *testing.T, backend persistence.Backend) {
	publisher := &recordingPublisher{}
	orders := newOrders(storeWith(backend, publisher, nil),
		journal.WithPartitionKey(func(streams.Stream[order]) string {
			return ""
		}))
	orderID := uuid.New()

	_, err := orders.Create(context.Background(), orderID, &placed{Amount: 1})
	requireError(t, err, streams.ErrNoPartitionKey, "create under an empty key")
	requireHead(t, backend, orderID, 0)
	if published := publisher.published(); len(published) != 0 {
		t.Fatalf("published %d events, want none", len(published))
	}
}

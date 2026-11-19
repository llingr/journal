// SPDX-FileCopyrightText: Copyright (c) 2026 The journal Authors
// SPDX-License-Identifier: Apache-2.0

package journaltest

import (
	"context"
	"errors"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/llingr/journal"
	"github.com/llingr/journal/persistence"
	"github.com/llingr/journal/publish"
)

var sweepCases = []suiteCase{
	{"ASweepPublishesWhatACrashLeft", runASweepPublishesWhatACrashLeft},
	{"ASweepLeavesWhatItsWindowDoesNotReach", runASweepLeavesWhatItsWindowDoesNotReach},
	{"ASweepPublishesEachKeyInItsOwnBatches", runASweepPublishesEachKeyInItsOwnBatches},
	{"ASweepStopsAtTheFirstFailure", runASweepStopsAtTheFirstFailure},
	{"StartSweepsAtOnceAndReportsWhatItSwept", runStartSweepsAtOnceAndReportsWhatItSwept},
	{"TheNextAppendPublishesWhatACrashLeft", runTheNextAppendPublishesWhatACrashLeft},
}

// runASweepPublishesWhatACrashLeft: events committed by a writer that crashed before
// publishing are published by a sweep, each stream in order, and cleared
func runASweepPublishesWhatACrashLeft(t *testing.T, backend persistence.Backend) {
	ctx := context.Background()
	first, second := uuid.New(), uuid.New()
	seed(t, backend, first, minutesAgo(5), true, "", 1, 2)
	seed(t, backend, second, minutesAgo(10), true, "", 1)
	publisher := &recordingPublisher{}
	eventStore := storeWith(backend, publisher, nil)

	swept, err := eventStore.Sweep(ctx, time.Hour)
	if err != nil || swept != 2 {
		t.Fatalf("sweep: %d streams, %v; want 2", swept, err)
	}
	byStream := make(map[uuid.UUID][]int32)
	for _, event := range publisher.published() {
		byStream[event.StreamID] = append(byStream[event.StreamID], event.Version)
	}
	requireVersions(t, "first stream published", byStream[first], 1, 2)
	requireVersions(t, "second stream published", byStream[second], 1)
	requirePending(t, backend, first)
	requirePending(t, backend, second)

	swept, err = eventStore.Sweep(ctx, time.Hour)
	if err != nil || swept != 0 {
		t.Fatalf("sweep with nothing left: %d streams, %v; want 0", swept, err)
	}
}

// runASweepLeavesWhatItsWindowDoesNotReach: a row less than a minute old is left to the
// publish still in flight for it, and one older than the window to a sweep reaching further
func runASweepLeavesWhatItsWindowDoesNotReach(t *testing.T, backend persistence.Backend) {
	ctx := context.Background()
	inFlight, old := uuid.New(), uuid.New()
	seed(t, backend, inFlight, time.Now().Add(-10*time.Second), true, "", 1)
	seed(t, backend, old, minutesAgo(90), true, "", 1)
	publisher := &recordingPublisher{}
	eventStore := storeWith(backend, publisher, nil)

	swept, err := eventStore.Sweep(ctx, time.Hour)
	if err != nil || swept != 0 {
		t.Fatalf("sweep of the last hour: %d streams, %v; want 0", swept, err)
	}
	requirePending(t, backend, inFlight, 1)
	requirePending(t, backend, old, 1)

	swept, err = eventStore.Sweep(ctx, 2*time.Hour)
	if err != nil || swept != 1 {
		t.Fatalf("sweep of the last two hours: %d streams, %v; want 1", swept, err)
	}
	requirePending(t, backend, old)
	requirePending(t, backend, inFlight, 1)
}

// runASweepPublishesEachKeyInItsOwnBatches: the sweep has no stream state, so it publishes
// under the key each event was stored with, and no call carries two keys
func runASweepPublishesEachKeyInItsOwnBatches(t *testing.T, backend persistence.Backend) {
	streamID := uuid.New()
	seed(t, backend, streamID, minutesAgo(5), true, "tenant-a", 1, 2)
	seed(t, backend, streamID, minutesAgo(5), true, "tenant-b", 3)
	seed(t, backend, streamID, minutesAgo(5), true, "", 4)
	publisher := &recordingPublisher{}

	_, err := storeWith(backend, publisher, nil).Sweep(context.Background(), time.Hour)
	requireNoError(t, err, "sweep")
	want := []string{"tenant-a", "tenant-b", streamID.String()}
	if keys := publisher.keys(); !slices.Equal(keys, want) {
		t.Fatalf("published under %v, want %v", keys, want)
	}
	requireVersions(t, "published", publisher.versions(), 1, 2, 3, 4)
}

// runASweepStopsAtTheFirstFailure: a destination that refuses one stream is likely down, so
// the sweep leaves the rest for a later one rather than hammer it, and returns the failure
func runASweepStopsAtTheFirstFailure(t *testing.T, backend persistence.Backend) {
	first, second := uuid.New(), uuid.New()
	seed(t, backend, first, minutesAgo(5), true, "", 1)
	seed(t, backend, second, minutesAgo(5), true, "", 1)
	publisher := &recordingPublisher{
		respond: failAlways,
	}

	swept, err := storeWith(backend, publisher, nil).Sweep(context.Background(), time.Hour)
	if !errors.Is(err, errBrokerDown) || swept != 0 {
		t.Fatalf("sweep with the broker down: %d streams, %v; want 0 and the broker's error",
			swept, err)
	}
	if attempts := publisher.attemptCount(); attempts != 1 {
		t.Fatalf("the sweep called the publisher %d times, want once", attempts)
	}
	requirePending(t, backend, first, 1)
	requirePending(t, backend, second, 1)
}

// runStartSweepsAtOnceAndReportsWhatItSwept: a restart publishes what the last run left
// without holding up the start, Hooks.Swept says how many streams it took, and Shutdown
// stops the sweeps
func runStartSweepsAtOnceAndReportsWhatItSwept(t *testing.T, backend persistence.Backend) {
	ctx := context.Background()
	first, second := uuid.New(), uuid.New()
	seed(t, backend, first, minutesAgo(5), true, "", 1)
	seed(t, backend, second, minutesAgo(5), true, "", 1, 2)
	publisher := &recordingPublisher{}
	var mutex sync.Mutex
	var sweptReports []int
	eventStore := journal.NewStore(backend,
		journal.WithPublisher(publisher),
		journal.WithHooks(publish.Hooks{
			Swept: func(streams int) {
				mutex.Lock()
				defer mutex.Unlock()
				sweptReports = append(sweptReports, streams)
			},
		}))

	eventStore.Start(ctx)
	eventually(t, "the sweep at start", func() bool {
		mutex.Lock()
		defer mutex.Unlock()
		return len(sweptReports) == 1
	})
	shutdownCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	requireNoError(t, eventStore.Shutdown(shutdownCtx), "shut down")

	if sweptReports[0] != 2 || len(publisher.versions()) != 3 {
		t.Fatalf("the sweep at start reported %d streams and published %v; want 2 and three "+
			"events", sweptReports[0], publisher.versions())
	}
	requirePending(t, backend, first)
	requirePending(t, backend, second)
}

// runTheNextAppendPublishesWhatACrashLeft: a stream's next append, by any instance, publishes
// what a crashed writer left before its own events
func runTheNextAppendPublishesWhatACrashLeft(t *testing.T, backend persistence.Backend) {
	ctx := context.Background()
	orderID := uuid.New()
	seed(t, backend, orderID, time.Now(), true, "", 1, 2)
	publisher := &recordingPublisher{}
	orders := newOrders(storeWith(backend, publisher, nil))

	read, err := orders.Get(ctx, orderID)
	requireNoError(t, err, "get")
	_, err = orders.Append(ctx, read, &shipped{})
	requireNoError(t, err, "append")
	requireVersions(t, "published", publisher.versions(), 1, 2, 3)
	requirePending(t, backend, orderID)
}

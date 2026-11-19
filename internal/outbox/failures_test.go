// SPDX-FileCopyrightText: Copyright (c) 2026 The journal Authors
// SPDX-License-Identifier: Apache-2.0

package outbox_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/google/uuid"

	"github.com/llingr/journal/internal/outbox"
	"github.com/llingr/journal/publish"
	"github.com/llingr/journal/streams"
)

// errStore is a store call that failed
var errStore = errors.New("store unavailable")

// TestAFailedReadOfEarlierVersionsPublishesNothing: when the versions an earlier publish left
// cannot be read, the committed events are not sent past them; the failure is reported and
// everything stays pending
func TestAFailedReadOfEarlierVersionsPublishesNothing(t *testing.T) {
	for name, failing := range map[string]func(store *memoryStore) *error{
		"pending versions": func(store *memoryStore) *error {
			return &store.pendingErr
		},
		"their events": func(store *memoryStore) *error {
			return &store.rangeErr
		},
	} {
		store, publisher, failed := newMemoryStore(), &recordingPublisher{}, &failures{}
		sender := newOutbox(store, publisher, failed)
		streamID := uuid.New()
		store.append(streamID, time.Now(), 1)
		committed := store.append(streamID, time.Now(), 2)
		store.fail(failing(store), errStore)

		sender.Publish(context.Background(), streamID, 1, committed)
		requireVersions(t, name+": published", publisher.versions(streamID))
		requireVersions(t, name+": still pending", store.pendingVersions(streamID), 1, 2)
		requireReported(t, failed, streamID, errStore)
	}
}

// TestAFailedClearIsReportedAndTheEventsGoAgain: events the destination has but whose outbox
// rows could not be cleared are reported, and the next publish sends them again rather than
// lose them, as delivery is at least once
func TestAFailedClearIsReportedAndTheEventsGoAgain(t *testing.T) {
	store, publisher, failed := newMemoryStore(), &recordingPublisher{}, &failures{}
	sender := newOutbox(store, publisher, failed)
	streamID := uuid.New()
	store.fail(&store.deleteErr, errStore)

	sender.Publish(context.Background(), streamID, 0, store.append(streamID, time.Now(), 1))
	requireVersions(t, "published", publisher.versions(streamID), 1)
	requireVersions(t, "still pending", store.pendingVersions(streamID), 1)
	requireReported(t, failed, streamID, errStore)

	store.fail(&store.deleteErr, nil)
	sender.Publish(context.Background(), streamID, 1, store.append(streamID, time.Now(), 2))
	requireVersions(t, "published after the store recovered", publisher.versions(streamID), 1,
		1, 2)
	requireVersions(t, "pending after the store recovered", store.pendingVersions(streamID))
}

// TestAPendingVersionWithoutItsEventHoldsBackTheStream: an outbox row whose event an owner
// deleted is a missing version, reported, and the events after it are not sent past it
func TestAPendingVersionWithoutItsEventHoldsBackTheStream(t *testing.T) {
	store, publisher, failed := newMemoryStore(), &recordingPublisher{}, &failures{}
	sender := newOutbox(store, publisher, failed)
	streamID := uuid.New()
	store.append(streamID, time.Now(), 1)
	store.deleteEvent(streamID, 1)

	sender.Publish(context.Background(), streamID, 1, store.append(streamID, time.Now(), 2))
	requireVersions(t, "published", publisher.versions(streamID))
	requireReported(t, failed, streamID, streams.ErrVersionMissing)
}

// TestARepublishReportsTheStoresFailure: a republish whose reads fail returns the store's
// error and sends nothing, rather than reporting a republish of nothing as done
func TestARepublishReportsTheStoresFailure(t *testing.T) {
	for name, testCase := range map[string]struct {
		failing   func(store *memoryStore) *error
		republish func(sender outbox.Outbox, streamID uuid.UUID) error
	}{
		"events, pending versions": {
			failing: func(store *memoryStore) *error {
				return &store.pendingErr
			},
			republish: republishEvents,
		},
		"events, the head": {
			failing: func(store *memoryStore) *error {
				return &store.headErr
			},
			republish: republishEvents,
		},
		"events, their range": {
			failing: func(store *memoryStore) *error {
				return &store.rangeErr
			},
			republish: republishEvents,
		},
		"one event, pending versions": {
			failing: func(store *memoryStore) *error {
				return &store.pendingErr
			},
			republish: republishEvent,
		},
		"one event, its range": {
			failing: func(store *memoryStore) *error {
				return &store.rangeErr
			},
			republish: republishEvent,
		},
	} {
		store, publisher, failed := newMemoryStore(), &recordingPublisher{}, &failures{}
		sender := newOutbox(store, publisher, failed)
		streamID := uuid.New()
		store.append(streamID, time.Now(), 1, 2)
		store.fail(testCase.failing(store), errStore)

		err := testCase.republish(sender, streamID)
		if !errors.Is(err, errStore) {
			t.Fatalf("%s: got %v, want the store's error", name, err)
		}
		requireVersions(t, name+": published", publisher.versions(streamID))
	}
}

// TestShutdownBeforeStartReturnsAtOnce: an outbox never started has no sweep to wait for
func TestShutdownBeforeStartReturnsAtOnce(t *testing.T) {
	sender := newOutbox(newMemoryStore(), &recordingPublisher{}, &failures{})
	if err := sender.Shutdown(context.Background()); err != nil {
		t.Fatalf("shutdown before start: %v", err)
	}
}

// TestShutdownGivesUpAtItsDeadline: a sweep held by a destination that never answers does not
// hold the service's shutdown past the deadline it was given, which is reported
func TestShutdownGivesUpAtItsDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store, failed := newMemoryStore(), &failures{}
		release := make(chan struct{})
		publisher := &recordingPublisher{
			refuse: func(streams.Event) error {
				<-release
				return nil
			},
		}
		sender := newOutbox(store, publisher, failed)
		store.append(uuid.New(), time.Now().Add(-time.Hour), 1)
		sender.Start(t.Context())
		synctest.Wait()

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		err := sender.Shutdown(ctx)
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("shutdown with the sweep held: got %v, want the deadline", err)
		}
		close(release)
		synctest.Wait()
	})
}

// TestASweepOfACancelledContextSaysSo: a sweep by hand that is cancelled reports the
// cancellation, rather than that it found nothing to publish
func TestASweepOfACancelledContextSaysSo(t *testing.T) {
	store, publisher, failed := newMemoryStore(), &recordingPublisher{}, &failures{}
	sender := newOutbox(store, publisher, failed)
	store.append(uuid.New(), time.Now().Add(-time.Hour), 1)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	swept, err := sender.Sweep(ctx, 2*time.Hour)
	if !errors.Is(err, context.Canceled) || swept != 0 {
		t.Fatalf("cancelled sweep: %d streams, %v; want 0 and the cancellation", swept, err)
	}
}

// TestASweepThatCannotReadAStreamStopsThere: a sweep that lists a stream but cannot read its
// pending versions returns the store's error, naming nothing as published
func TestASweepThatCannotReadAStreamStopsThere(t *testing.T) {
	store, publisher, failed := newMemoryStore(), &recordingPublisher{}, &failures{}
	sender := newOutbox(store, publisher, failed)
	streamID := uuid.New()
	store.append(streamID, time.Now().Add(-time.Hour), 1)
	store.fail(&store.pendingErr, errStore)

	swept, err := sender.Sweep(context.Background(), 2*time.Hour)
	if !errors.Is(err, errStore) || swept != 0 {
		t.Fatalf("sweep with pending versions unreadable: %d streams, %v; want 0 and the "+
			"store's error", swept, err)
	}
	requireVersions(t, "published", publisher.versions(streamID))
}

// TestARefusedRepublishNamesTheOneVersionToPublishFirst: with one earlier version unpublished
// the refusal names it alone, as a reader acts on it
func TestARefusedRepublishNamesTheOneVersionToPublishFirst(t *testing.T) {
	store, publisher, failed := newMemoryStore(), &recordingPublisher{}, &failures{}
	sender := newOutbox(store, publisher, failed)
	streamID := uuid.New()
	store.append(streamID, time.Now(), 1, 2)
	markPublished(store, streamID, 2)

	err := sender.RepublishEvent(context.Background(), streamID, 2, publish.Always)
	if !errors.Is(err, publish.ErrEarlierUnpublished) ||
		!strings.Contains(err.Error(), "publish version 1 first") {
		t.Fatalf("republish past version 1: got %v, want it named alone", err)
	}
}

// TestAnUnknownConditionForRepublishEventsPanics: RepublishEvents refuses a condition that is
// neither IfUnpublished nor Always where it is made, as RepublishEvent does
func TestAnUnknownConditionForRepublishEventsPanics(t *testing.T) {
	sender := newOutbox(newMemoryStore(), &recordingPublisher{}, &failures{})
	defer func() {
		if recover() == nil {
			t.Fatal("RepublishEvents with condition 0 did not panic")
		}
	}()
	_, _ = sender.RepublishEvents(context.Background(), uuid.New(), 1,
		publish.RepublishCondition(0))
}

func republishEvents(sender outbox.Outbox, streamID uuid.UUID) error {
	_, err := sender.RepublishEvents(context.Background(), streamID, 1, publish.Always)
	return err
}

func republishEvent(sender outbox.Outbox, streamID uuid.UUID) error {
	return sender.RepublishEvent(context.Background(), streamID, 1, publish.Always)
}

// requireReported fails unless the Failed hook heard of the stream with an error wrapping want
func requireReported(t *testing.T, failed *failures, streamID uuid.UUID, want error) {
	t.Helper()
	reportedStreams, errs := failed.reported()
	if len(errs) == 0 || reportedStreams[0] != streamID || !errors.Is(errs[0], want) {
		t.Fatalf("failures reported %v for %v, want %v for %s", errs, reportedStreams, want,
			streamID)
	}
}

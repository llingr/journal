// SPDX-FileCopyrightText: Copyright (c) 2026 The journal Authors
// SPDX-License-Identifier: Apache-2.0

package outbox_test

import (
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/llingr/journal/publish"
	"github.com/llingr/journal/streams"
)

// TestRepublishEventsAlwaysSendsToTheHead: every version from fromVersion to the stream's last
// goes again, in order and under the key each was appended with, published or not, and the
// outbox is clear afterwards.
func TestRepublishEventsAlwaysSendsToTheHead(t *testing.T) {
	store, publisher, failed := newMemoryStore(), &recordingPublisher{}, &failures{}
	sender := newOutbox(store, publisher, failed)
	streamID := uuid.New()
	store.appendKeyed(streamID, time.Now(), "customer-1", 1, 2, 3)
	markPublished(store, streamID, 1, 2, 3)
	store.appendKeyed(streamID, time.Now(), "customer-1", 4)

	sent, err := sender.RepublishEvents(t.Context(), streamID, 2, publish.Always)
	if err != nil || sent != 3 {
		t.Fatalf("republish from version 2: %d sent, %v, want 3 and no error", sent, err)
	}
	if calls := publisher.calls(); !slices.Equal(calls, []string{"customer-1 2 to 4"}) {
		t.Fatalf("publisher calls %v, want versions 2 to 4 under customer-1", calls)
	}
	requireVersions(t, "still pending", store.pendingVersions(streamID))
}

// TestRepublishEventsIfUnpublishedSendsOnlyThePending: versions already published are not sent
// again, and with nothing pending nothing is sent.
func TestRepublishEventsIfUnpublishedSendsOnlyThePending(t *testing.T) {
	store, publisher, failed := newMemoryStore(), &recordingPublisher{}, &failures{}
	sender := newOutbox(store, publisher, failed)
	streamID := uuid.New()
	store.append(streamID, time.Now(), 1, 2, 3, 4)
	markPublished(store, streamID, 1, 2)

	sent, err := sender.RepublishEvents(t.Context(), streamID, 1, publish.IfUnpublished)
	if err != nil || sent != 2 {
		t.Fatalf("republish of the pending: %d sent, %v, want 2 and no error", sent, err)
	}
	requireVersions(t, "published", publisher.versions(streamID), 3, 4)
	requireVersions(t, "still pending", store.pendingVersions(streamID))

	sent, err = sender.RepublishEvents(t.Context(), streamID, 1, publish.IfUnpublished)
	if err != nil || sent != 0 {
		t.Fatalf("republish with nothing pending: %d sent, %v, want 0 and no error", sent, err)
	}
}

// TestARepublishNeverOvertakesAnUnpublishedVersion: neither method sends a version while an
// earlier one is unpublished, under either condition, and the error names what to publish
// first; nothing is sent.
func TestARepublishNeverOvertakesAnUnpublishedVersion(t *testing.T) {
	store, publisher, failed := newMemoryStore(), &recordingPublisher{}, &failures{}
	sender := newOutbox(store, publisher, failed)
	streamID := uuid.New()
	store.append(streamID, time.Now(), 1, 2, 3, 4, 5, 6)
	markPublished(store, streamID, 1, 4)

	attempts := map[string]error{}
	_, attempts["events always"] = sender.RepublishEvents(t.Context(), streamID, 6,
		publish.Always)
	_, attempts["events if unpublished"] = sender.RepublishEvents(t.Context(), streamID, 6,
		publish.IfUnpublished)
	attempts["event always"] = sender.RepublishEvent(t.Context(), streamID, 6, publish.Always)
	attempts["event if unpublished"] = sender.RepublishEvent(t.Context(), streamID, 6,
		publish.IfUnpublished)
	for name, err := range attempts {
		if !errors.Is(err, publish.ErrEarlierUnpublished) ||
			!strings.Contains(err.Error(), "publish versions 2 to 3, 5 first") {
			t.Errorf("%s past versions 2, 3 and 5: got %v, want ErrEarlierUnpublished naming "+
				"versions 2 to 3, 5", name, err)
		}
	}
	if calls := publisher.calls(); len(calls) != 0 {
		t.Fatalf("publisher calls %v after refused republishes, want none", calls)
	}
	requireVersions(t, "still pending", store.pendingVersions(streamID), 2, 3, 5, 6)
}

// TestRepublishEventSendsOneVersion: Always sends a version already published under the key
// it was appended with; IfUnpublished sends one still pending and clears it, and refuses one
// that is not.
func TestRepublishEventSendsOneVersion(t *testing.T) {
	store, publisher, failed := newMemoryStore(), &recordingPublisher{}, &failures{}
	sender := newOutbox(store, publisher, failed)
	streamID := uuid.New()
	store.appendKeyed(streamID, time.Now(), "customer-1", 1, 2)
	markPublished(store, streamID, 1)

	err := sender.RepublishEvent(t.Context(), streamID, 1, publish.IfUnpublished)
	if !errors.Is(err, publish.ErrAlreadyPublished) {
		t.Fatalf("republish of a published version if unpublished: got %v, want ErrAlreadyPublished",
			err)
	}
	err = sender.RepublishEvent(t.Context(), streamID, 1, publish.Always)
	if err != nil {
		t.Fatalf("republish of a published version always: %v", err)
	}
	err = sender.RepublishEvent(t.Context(), streamID, 2, publish.IfUnpublished)
	if err != nil {
		t.Fatalf("republish of a pending version: %v", err)
	}
	want := []string{"customer-1 1 to 1", "customer-1 2 to 2"}
	if calls := publisher.calls(); !slices.Equal(calls, want) {
		t.Fatalf("publisher calls %v, want %v", calls, want)
	}
	requireVersions(t, "still pending", store.pendingVersions(streamID))
}

// TestARepublishPastTheHeadIsAMissingVersion: a version the stream does not hold is
// ErrVersionMissing, not a silent success.
func TestARepublishPastTheHeadIsAMissingVersion(t *testing.T) {
	store, publisher, failed := newMemoryStore(), &recordingPublisher{}, &failures{}
	sender := newOutbox(store, publisher, failed)
	streamID := uuid.New()
	store.append(streamID, time.Now(), 1)
	markPublished(store, streamID, 1)

	_, eventsErr := sender.RepublishEvents(t.Context(), streamID, 2, publish.Always)
	eventErr := sender.RepublishEvent(t.Context(), streamID, 2, publish.Always)
	for _, err := range []error{eventsErr, eventErr} {
		if !errors.Is(err, streams.ErrVersionMissing) {
			t.Errorf("republish of version 2 of a one-event stream: got %v, want "+
				"ErrVersionMissing", err)
		}
	}
}

// TestAnUnknownRepublishConditionPanics: a condition that is neither IfUnpublished nor Always
// is a wiring mistake and stops the program where it is made.
func TestAnUnknownRepublishConditionPanics(t *testing.T) {
	store, publisher, failed := newMemoryStore(), &recordingPublisher{}, &failures{}
	sender := newOutbox(store, publisher, failed)
	defer func() {
		if recover() == nil {
			t.Fatal("a republish with condition 0 did not panic")
		}
	}()
	_ = sender.RepublishEvent(t.Context(), uuid.New(), 1, publish.RepublishCondition(0))
}

// markPublished removes the versions' outbox rows, as a publish that succeeded would
func markPublished(store *memoryStore, streamID uuid.UUID, versions ...int32) {
	for _, version := range versions {
		store.deletePending(streamID, version)
	}
}

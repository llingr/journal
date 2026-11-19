// SPDX-FileCopyrightText: Copyright (c) 2026 The journal Authors
// SPDX-License-Identifier: Apache-2.0

package journaltest

import (
	"context"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/llingr/journal"
	"github.com/llingr/journal/persistence"
	"github.com/llingr/journal/publish"
	"github.com/llingr/journal/streams"
)

var republishCases = []suiteCase{
	{"AlwaysSendsEveryVersionToTheHead", runAlwaysSendsEveryVersionToTheHead},
	{"IfUnpublishedSendsOnlyThePending", runIfUnpublishedSendsOnlyThePending},
	{"ARepublishNeverOvertakesAnUnpublishedVersion",
		runARepublishNeverOvertakesAnUnpublishedVersion},
	{"RepublishEventSendsOneVersion", runRepublishEventSendsOneVersion},
	{"ARepublishPastTheHeadIsAMissingVersion", runARepublishPastTheHeadIsAMissingVersion},
}

// runAlwaysSendsEveryVersionToTheHead: published versions from fromVersion to the stream's
// last go again, in order, under the key the repository chose when it appended them
func runAlwaysSendsEveryVersionToTheHead(t *testing.T, backend persistence.Backend) {
	ctx := context.Background()
	publisher := &recordingPublisher{}
	eventStore := storeWith(backend, publisher, nil)
	orders := newOrders(eventStore, journal.WithPartitionKey(func(streams.Stream[order]) string {
		return "customer-9"
	}))
	orderID := uuid.New()
	_, err := orders.CreateAll(ctx, orderID, []streams.DomainEvent{
		&placed{Amount: 1},
		&placed{Amount: 2},
		&shipped{},
	})
	requireNoError(t, err, "create")

	sent, err := eventStore.RepublishEvents(ctx, orderID, 2, publish.Always)
	if err != nil || sent != 2 {
		t.Fatalf("republish from version 2: %d sent, %v; want 2", sent, err)
	}
	requireVersions(t, "published", publisher.versions(), 1, 2, 3, 2, 3)
	if keys := publisher.keys(); !slices.Equal(keys, []string{"customer-9", "customer-9"}) {
		t.Fatalf("published under %v, want customer-9 each time", keys)
	}
	requirePending(t, backend, orderID)
}

// runIfUnpublishedSendsOnlyThePending: versions already published are not sent again, and
// with nothing pending nothing is sent
func runIfUnpublishedSendsOnlyThePending(t *testing.T, backend persistence.Backend) {
	ctx := context.Background()
	orderID := uuid.New()
	seed(t, backend, orderID, time.Now(), false, "", 1, 2)
	seed(t, backend, orderID, time.Now(), true, "", 3, 4)
	publisher := &recordingPublisher{}
	eventStore := storeWith(backend, publisher, nil)

	sent, err := eventStore.RepublishEvents(ctx, orderID, 1, publish.IfUnpublished)
	if err != nil || sent != 2 {
		t.Fatalf("republish the unpublished: %d sent, %v; want 2", sent, err)
	}
	requireVersions(t, "published", publisher.versions(), 3, 4)
	requirePending(t, backend, orderID)

	sent, err = eventStore.RepublishEvents(ctx, orderID, 1, publish.IfUnpublished)
	if err != nil || sent != 0 {
		t.Fatalf("republish with nothing unpublished: %d sent, %v; want 0", sent, err)
	}
}

// runARepublishNeverOvertakesAnUnpublishedVersion: neither call sends a version while an
// earlier one is unpublished, whatever the condition, and the error names the versions to
// publish first; once those go, the republish does
func runARepublishNeverOvertakesAnUnpublishedVersion(t *testing.T, backend persistence.Backend) {
	ctx := context.Background()
	orderID := uuid.New()
	seed(t, backend, orderID, time.Now(), true, "", 1, 2)
	seed(t, backend, orderID, time.Now(), false, "", 3)
	publisher := &recordingPublisher{}
	eventStore := storeWith(backend, publisher, nil)

	for _, condition := range []publish.RepublishCondition{publish.Always, publish.IfUnpublished} {
		_, err := eventStore.RepublishEvents(ctx, orderID, 3, condition)
		requireError(t, err, publish.ErrEarlierUnpublished, "republish events from version 3")
		if !strings.Contains(err.Error(), "publish versions 1 to 2 first") {
			t.Fatalf("the refusal %q does not name versions 1 to 2", err)
		}
		err = eventStore.RepublishEvent(ctx, orderID, 3, condition)
		requireError(t, err, publish.ErrEarlierUnpublished, "republish version 3")
	}
	if published := publisher.versions(); len(published) != 0 {
		t.Fatalf("published %v while refusing, want nothing", published)
	}

	sent, err := eventStore.RepublishEvents(ctx, orderID, 1, publish.IfUnpublished)
	if err != nil || sent != 2 {
		t.Fatalf("republish the unpublished versions 1 and 2: %d sent, %v; want 2", sent, err)
	}
	sent, err = eventStore.RepublishEvents(ctx, orderID, 3, publish.Always)
	if err != nil || sent != 1 {
		t.Fatalf("republish from version 3: %d sent, %v; want 1", sent, err)
	}
	requireVersions(t, "published", publisher.versions(), 1, 2, 3)
}

// runRepublishEventSendsOneVersion: Always sends one published version again under the key it
// was appended with; IfUnpublished sends one still pending and clears it, and refuses one that
// is not
func runRepublishEventSendsOneVersion(t *testing.T, backend persistence.Backend) {
	ctx := context.Background()
	orderID := uuid.New()
	seed(t, backend, orderID, time.Now(), false, "tenant-3", 1)
	seed(t, backend, orderID, time.Now(), true, "tenant-3", 2, 3)
	publisher := &recordingPublisher{}
	eventStore := storeWith(backend, publisher, nil)

	err := eventStore.RepublishEvent(ctx, orderID, 1, publish.IfUnpublished)
	requireError(t, err, publish.ErrAlreadyPublished, "republish a published version if "+
		"unpublished")
	requireNoError(t, eventStore.RepublishEvent(ctx, orderID, 1, publish.Always),
		"republish version 1")
	requireNoError(t, eventStore.RepublishEvent(ctx, orderID, 2, publish.IfUnpublished),
		"republish the unpublished version 2")
	requireVersions(t, "published", publisher.versions(), 1, 2)
	if keys := publisher.keys(); !slices.Equal(keys, []string{"tenant-3", "tenant-3"}) {
		t.Fatalf("published under %v, want tenant-3 each time", keys)
	}
	requirePending(t, backend, orderID, 3)
}

// runARepublishPastTheHeadIsAMissingVersion: a version the stream does not hold is
// ErrVersionMissing, not a silent success
func runARepublishPastTheHeadIsAMissingVersion(t *testing.T, backend persistence.Backend) {
	ctx := context.Background()
	orderID := uuid.New()
	seed(t, backend, orderID, time.Now(), false, "", 1, 2)
	eventStore := storeWith(backend, &recordingPublisher{}, nil)

	_, err := eventStore.RepublishEvents(ctx, orderID, 3, publish.Always)
	requireError(t, err, streams.ErrVersionMissing, "republish events past the head")
	err = eventStore.RepublishEvent(ctx, orderID, 3, publish.Always)
	requireError(t, err, streams.ErrVersionMissing, "republish a version past the head")
	_, err = eventStore.RepublishEvents(ctx, uuid.New(), 1, publish.Always)
	requireError(t, err, streams.ErrVersionMissing, "republish an unwritten stream")
}

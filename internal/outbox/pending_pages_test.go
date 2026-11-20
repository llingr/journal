// SPDX-FileCopyrightText: Copyright (c) 2026 The journal Authors
// SPDX-License-Identifier: Apache-2.0

package outbox_test

import (
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/llingr/journal/internal/outbox"
	"github.com/llingr/journal/publish"
	"github.com/llingr/journal/streams"
)

// TestPendingVersionsAreReadInPages: what failed publishes left is read 250 versions at a time
// by every path that sends it, each page sent before the next is read, so however many
// publishes failed none of them reads it whole; a failure stops it there, and a write's own
// events go with the last page and never ahead of a version still pending
func TestPendingVersionsAreReadInPages(t *testing.T) {
	for _, testCase := range []struct {
		name          string
		refuse        int32
		send          func(t *testing.T, sender outbox.Outbox, store *memoryStore, id uuid.UUID)
		wantReads     []string
		wantPublished []int32
		wantPending   []int32
	}{
		{
			name: "a write's backlog",
			send: func(t *testing.T, sender outbox.Outbox, store *memoryStore, id uuid.UUID) {
				store.append(id, time.Now(), versionRange(1, 600)...)
				sender.Publish(t.Context(), id, 1, store.append(id, time.Now(), 601))
			},
			wantReads:     []string{"1 to 250", "251 to 500", "501 to 600"},
			wantPublished: versionRange(1, 601),
		},
		{
			name: "a sweep",
			send: func(t *testing.T, sender outbox.Outbox, store *memoryStore, id uuid.UUID) {
				store.append(id, time.Now().Add(-time.Hour), versionRange(1, 600)...)
				swept, err := sender.Sweep(t.Context(), 24*time.Hour)
				if swept != 1 || err != nil {
					t.Fatalf("sweep: %d streams, %v; want 1 and no error", swept, err)
				}
			},
			wantReads:     []string{"1 to 250", "251 to 500", "501 to 600"},
			wantPublished: versionRange(1, 600),
		},
		{
			name: "a republish of the unpublished",
			send: func(t *testing.T, sender outbox.Outbox, store *memoryStore, id uuid.UUID) {
				store.append(id, time.Now(), versionRange(1, 600)...)
				sent, err := sender.RepublishEvents(t.Context(), id, 1, publish.IfUnpublished)
				if sent != 600 || err != nil {
					t.Fatalf("republish: %d sent, %v; want 600 and no error", sent, err)
				}
			},
			wantReads:     []string{"1 to 250", "251 to 500", "501 to 600"},
			wantPublished: versionRange(1, 600),
		},
		{
			name: "a backlog of exactly one page",
			send: func(t *testing.T, sender outbox.Outbox, store *memoryStore, id uuid.UUID) {
				store.append(id, time.Now(), versionRange(1, 250)...)
				sender.Publish(t.Context(), id, 1, store.append(id, time.Now(), 251))
			},
			wantReads:     []string{"1 to 250"},
			wantPublished: versionRange(1, 251),
		},
		{
			name: "a backlog of one more than a page",
			send: func(t *testing.T, sender outbox.Outbox, store *memoryStore, id uuid.UUID) {
				store.append(id, time.Now(), versionRange(1, 251)...)
				sender.Publish(t.Context(), id, 1, store.append(id, time.Now(), 252))
			},
			wantReads:     []string{"1 to 250", "251 to 251"},
			wantPublished: versionRange(1, 252),
		},
		{
			// a page spans at most 250 versions, so the 597 published between them are not read
			name: "pending versions far apart",
			send: func(t *testing.T, sender outbox.Outbox, store *memoryStore, id uuid.UUID) {
				store.append(id, time.Now(), versionRange(1, 601)...)
				markPublished(store, id, versionRange(3, 599)...)
				sent, err := sender.RepublishEvents(t.Context(), id, 1, publish.IfUnpublished)
				if sent != 4 || err != nil {
					t.Fatalf("republish: %d sent, %v; want 4 and no error", sent, err)
				}
			},
			wantReads:     []string{"1 to 2", "600 to 601"},
			wantPublished: []int32{1, 2, 600, 601},
		},
		{
			name:   "a refusal in the backlog's second page",
			refuse: 300,
			send: func(t *testing.T, sender outbox.Outbox, store *memoryStore, id uuid.UUID) {
				store.append(id, time.Now(), versionRange(1, 600)...)
				sender.Publish(t.Context(), id, 1, store.append(id, time.Now(), 601))
			},
			wantReads:     []string{"1 to 250", "251 to 500"},
			wantPublished: versionRange(1, 299),
			wantPending:   versionRange(300, 601),
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			store, failed := newMemoryStore(), &failures{}
			publisher := &recordingPublisher{
				refuse: func(event streams.Event) error {
					if event.Version == testCase.refuse {
						return errors.New("refused")
					}
					return nil
				},
			}
			sender := newOutbox(store, publisher, failed)
			streamID := uuid.New()

			testCase.send(t, sender, store, streamID)
			if reads := store.readRanges(); !slices.Equal(reads, testCase.wantReads) {
				t.Fatalf("read %v, want %v", reads, testCase.wantReads)
			}
			requireVersions(t, "published", publisher.versions(streamID),
				testCase.wantPublished...)
			requireVersions(t, "still pending", store.pendingVersions(streamID),
				testCase.wantPending...)
		})
	}
}

// versionRange is the versions first to last
func versionRange(first int32, last int32) []int32 {
	var versions []int32
	for version := first; version <= last; version++ {
		versions = append(versions, version)
	}
	return versions
}

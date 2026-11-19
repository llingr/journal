// SPDX-FileCopyrightText: Copyright (c) 2026 The journal Authors
// SPDX-License-Identifier: Apache-2.0

package journal_test

import (
	"context"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/google/uuid"

	"github.com/llingr/journal"
	"github.com/llingr/journal/codec"
	"github.com/llingr/journal/codec/compress"
	"github.com/llingr/journal/codec/encrypt"
	"github.com/llingr/journal/internal/memory"
	"github.com/llingr/journal/persistence"
	"github.com/llingr/journal/streams"
)

// TestWithSweepIntervalSetsWhenTheSweepRuns: a store set to sweep every minute publishes what a
// crash left at its next sweep after the event is a minute old, where the default would wait
// three hours
func TestWithSweepIntervalSetsWhenTheSweepRuns(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		backend := memory.NewBackend()
		publisher := &countingPublisher{}
		eventStore := journal.NewStore(backend, journal.WithPublisher(publisher),
			journal.WithSweepInterval(time.Minute))
		eventStore.Start(t.Context())
		synctest.Wait()

		streamID := uuid.New()
		_, err := backend.Append(context.Background(), persistence.AppendRequest{
			StreamID: streamID,
			Events: []persistence.PersistedEvent{
				{
					StreamID:        streamID,
					Version:         1,
					EventType:       "LEFT",
					EventData:       []byte(`{}`),
					DataFormat:      codec.JSON,
					DataCompression: compress.None,
					DataEncryption:  encrypt.None,
				},
			},
			AppendTs: time.Now(),
			Publish:  true,
		})
		if err != nil {
			t.Fatalf("append what a crash left: %v", err)
		}

		time.Sleep(time.Minute + time.Second)
		synctest.Wait()
		if published := publisher.count(); published != 0 {
			t.Fatalf("published %d events at the first sweep, while the event was under a "+
				"minute old, want none", published)
		}
		time.Sleep(time.Minute)
		synctest.Wait()
		if published := publisher.count(); published != 1 {
			t.Fatalf("published %d events by the second sweep, want 1", published)
		}

		if err = eventStore.Shutdown(context.Background()); err != nil {
			t.Fatalf("shut down: %v", err)
		}
	})
}

// countingPublisher counts the events it publishes, from any goroutine
type countingPublisher struct {
	mutex     sync.Mutex
	published int
}

func (c *countingPublisher) PublishAll(_ context.Context, _ string,
	events []streams.Event) (int, error) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.published += len(events)
	return len(events), nil
}

func (c *countingPublisher) count() int {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	return c.published
}

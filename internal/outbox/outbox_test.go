// SPDX-FileCopyrightText: Copyright (c) 2026 The journal Authors
// SPDX-License-Identifier: Apache-2.0

package outbox_test

import (
	"context"
	"errors"
	"slices"
	"testing"
	"testing/synctest"
	"time"

	"github.com/google/uuid"

	"github.com/llingr/journal/codec"
	"github.com/llingr/journal/internal/outbox"
	"github.com/llingr/journal/publish"
	"github.com/llingr/journal/streams"
)

// TestPublishSendsTheCommittedEventsWithoutReadingThem: the events just written go as they
// were written, in order, and leave the store; nothing is read back.
func TestPublishSendsTheCommittedEventsWithoutReadingThem(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store, publisher, failed := newMemoryStore(), &recordingPublisher{}, &failures{}
		sender := newOutbox(store, publisher, failed)
		streamID := uuid.New()
		committed := store.append(streamID, time.Now(), 1, 2, 3)

		sender.Publish(t.Context(), streamID, 0, committed)
		requireVersions(t, "published", publisher.versions(streamID), 1, 2, 3)
		requireVersions(t, "still pending", store.pendingVersions(streamID))
		requireNoFailures(t, failed)
		if reads := store.readRanges(); len(reads) != 0 {
			t.Fatalf("events read back: %v, want none", reads)
		}
	})
}

// TestPublishReadsOnlyTheEarlierVersionsStillPending: what an earlier failed publish left
// goes first, read from the store, and the committed events follow from memory.
func TestPublishReadsOnlyTheEarlierVersionsStillPending(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store, publisher, failed := newMemoryStore(), &recordingPublisher{}, &failures{}
		sender := newOutbox(store, publisher, failed)
		streamID := uuid.New()
		store.append(streamID, time.Now(), 1, 2)
		committed := store.append(streamID, time.Now(), 3, 4)

		sender.Publish(t.Context(), streamID, 1, committed)
		requireVersions(t, "published", publisher.versions(streamID), 1, 2, 3, 4)
		requireVersions(t, "still pending", store.pendingVersions(streamID))
		if reads := store.readRanges(); !slices.Equal(reads, []string{"1 to 2"}) {
			t.Fatalf("events read back: %v, want versions 1 to 2 alone", reads)
		}
	})
}

// TestPublishOutlivesItsCaller: a caller that goes away after its commit, such as a request
// whose client hung up, does not hold its events back.
func TestPublishOutlivesItsCaller(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store, publisher, failed := newMemoryStore(), &recordingPublisher{}, &failures{}
		sender := newOutbox(store, publisher, failed)
		streamID := uuid.New()
		committed := store.append(streamID, time.Now(), 1)

		gone, cancel := context.WithCancel(t.Context())
		cancel()
		sender.Publish(gone, streamID, 0, committed)
		requireVersions(t, "published", publisher.versions(streamID), 1)
	})
}

// TestPublishStopsAtARefusedVersion: a refusal that is not a timeout stops the stream there,
// so nothing after it overtakes it, what was sent is cleared, and the hook hears of it.
func TestPublishStopsAtARefusedVersion(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store, failed := newMemoryStore(), &failures{}
		publisher := &recordingPublisher{
			refuse: func(event streams.Event) error {
				if event.Version == 2 {
					return errRefused
				}
				return nil
			},
		}
		sender := newOutbox(store, publisher, failed)
		streamID := uuid.New()
		committed := store.append(streamID, time.Now(), 1, 2, 3)

		sender.Publish(t.Context(), streamID, 0, committed)
		requireVersions(t, "published", publisher.versions(streamID), 1)
		requireVersions(t, "still pending", store.pendingVersions(streamID), 2, 3)
		streams, errs := failed.reported()
		if len(errs) != 1 || streams[0] != streamID || !errors.Is(errs[0], errRefused) {
			t.Fatalf("failures reported: %v for %v, want the refusal for the stream", errs, streams)
		}
	})
}

// TestPublishRecoversAPanickingPublisher: foreign code that panics is a failure, not a crash,
// and its event stays pending.
func TestPublishRecoversAPanickingPublisher(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store, failed := newMemoryStore(), &failures{}
		publisher := &recordingPublisher{
			refuse: func(streams.Event) error {
				panic("broker client bug")
			},
		}
		sender := newOutbox(store, publisher, failed)
		streamID := uuid.New()
		committed := store.append(streamID, time.Now(), 1)

		sender.Publish(t.Context(), streamID, 0, committed)
		_, errs := failed.reported()
		if len(errs) != 1 || !errors.Is(errs[0], publish.ErrPublisherPanic) {
			t.Fatalf("failures reported: %v, want ErrPublisherPanic", errs)
		}
		requireVersions(t, "still pending", store.pendingVersions(streamID), 1)
	})
}

// TestTheSweepRunsAtStartThenEveryThreeHours: by default a stream nobody appends to again is
// healed at the next start or within three hours, and a row less than a minute old, which a
// live writer may be publishing, is left to it.
func TestTheSweepRunsAtStartThenEveryThreeHours(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store, publisher, failed := newMemoryStore(), &recordingPublisher{}, &failures{}
		sender := newOutbox(store, publisher, failed)
		leftOver, inFlight, later := uuid.New(), uuid.New(), uuid.New()
		store.append(leftOver, time.Now().Add(-2*time.Minute), 1)
		store.append(inFlight, time.Now().Add(-30*time.Second), 1)

		sender.Start(t.Context())
		defer stopOutbox(t, sender)
		synctest.Wait()
		requireVersions(t, "published at start, left over", publisher.versions(leftOver), 1)
		requireVersions(t, "published at start, in flight", publisher.versions(inFlight))

		time.Sleep(time.Hour)
		store.append(later, time.Now(), 1)
		time.Sleep(2*time.Hour - time.Millisecond)
		synctest.Wait()
		requireVersions(t, "published before three hours", publisher.versions(later))

		time.Sleep(2 * time.Millisecond)
		synctest.Wait()
		requireVersions(t, "published at three hours, in flight", publisher.versions(inFlight), 1)
		requireVersions(t, "published at three hours, later", publisher.versions(later), 1)
	})
}

// TestATimedSweepReachesBackThreeIntervals: consecutive sweeps overlap, an older row waits for
// an operator, and Sweep by hand reaches as far back as it is told.
func TestATimedSweepReachesBackThreeIntervals(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store, publisher, failed := newMemoryStore(), &recordingPublisher{}, &failures{}
		sender := newOutbox(store, publisher, failed)
		inWindow, beyond := uuid.New(), uuid.New()
		store.append(inWindow, time.Now().Add(-8*time.Hour), 1)
		store.append(beyond, time.Now().Add(-10*time.Hour), 1)

		sender.Start(t.Context())
		defer stopOutbox(t, sender)
		synctest.Wait()
		requireVersions(t, "published within nine hours", publisher.versions(inWindow), 1)
		requireVersions(t, "published beyond nine hours", publisher.versions(beyond))

		swept, err := sender.Sweep(t.Context(), 24*time.Hour)
		if err != nil || swept != 1 {
			t.Fatalf("sweep a day back: %d streams, %v; want 1", swept, err)
		}
		requireVersions(t, "published by hand", publisher.versions(beyond), 1)
	})
}

// TestASweepStopsAtTheFirstFailure: a destination that refuses one stream is likely down, so
// the sweep leaves the rest where they are for a later one rather than hammer it.
func TestASweepStopsAtTheFirstFailure(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store, failed := newMemoryStore(), &failures{}
		attempts := 0
		publisher := &recordingPublisher{
			refuse: func(streams.Event) error {
				attempts++
				return errRefused
			},
		}
		sender := newOutbox(store, publisher, failed)
		first, second := uuid.New(), uuid.New()
		store.append(first, time.Now().Add(-time.Hour), 1)
		store.append(second, time.Now().Add(-time.Hour), 1)

		swept, err := sender.Sweep(t.Context(), time.Hour+time.Minute)
		if !errors.Is(err, errRefused) || swept != 0 {
			t.Fatalf("sweep against a refusing destination: %d streams, %v; want 0 and the "+
				"refusal", swept, err)
		}
		if attempts != 1 {
			t.Fatalf("publish attempts: %d, want 1", attempts)
		}
		requireVersions(t, "still pending, first", store.pendingVersions(first), 1)
		requireVersions(t, "still pending, second", store.pendingVersions(second), 1)
	})
}

// TestAPublishGivesUpAtItsTimeout: a destination that never answers fails the publish within
// ten seconds by default, so a writer is not held for an outage, and the events stay pending.
func TestAPublishGivesUpAtItsTimeout(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store, failed := newMemoryStore(), &failures{}
		pipeline, err := codec.NewPipeline()
		if err != nil {
			t.Fatalf("build the pipeline: %v", err)
		}
		sender := outbox.New(store, pipeline, hangingPublisher{}, publish.Hooks{
			Failed: failed.hook,
		})
		streamID := uuid.New()
		committed := store.append(streamID, time.Now(), 1)

		started := time.Now()
		sender.Publish(t.Context(), streamID, 0, committed)
		if waited := time.Since(started); waited != outbox.DefaultPublishTimeout {
			t.Fatalf("publish gave up after %s, want %s", waited, outbox.DefaultPublishTimeout)
		}
		_, errs := failed.reported()
		if len(errs) != 1 || !errors.Is(errs[0], context.DeadlineExceeded) {
			t.Fatalf("failures reported: %v, want the deadline", errs)
		}
		requireVersions(t, "still pending", store.pendingVersions(streamID), 1)
	})

	for _, timeout := range []time.Duration{outbox.MinimumPublishTimeout,
		outbox.MaximumPublishTimeout} {
		outbox.WithPublishTimeout(timeout)
	}
	for _, timeout := range []time.Duration{time.Second - 1, time.Minute + 1} {
		func() {
			defer func() {
				if recover() == nil {
					t.Errorf("WithPublishTimeout(%s) did not panic", timeout)
				}
			}()
			outbox.WithPublishTimeout(timeout)
		}()
	}
}

// hangingPublisher answers only when its context ends, as a broker that has gone away
type hangingPublisher struct{}

func (hangingPublisher) PublishAll(ctx context.Context, _ string, _ []streams.Event) (int,
	error) {
	<-ctx.Done()
	return 0, ctx.Err()
}

// TestWithSweepIntervalChangesTheInterval: a host that wants faster recovery says so, within
// fifteen seconds to a week, and anything outside that stops the program where it is set.
func TestWithSweepIntervalChangesTheInterval(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store, publisher, failed := newMemoryStore(), &recordingPublisher{}, &failures{}
		sender := newOutbox(store, publisher, failed, outbox.WithSweepInterval(time.Minute))
		streamID := uuid.New()
		sender.Start(t.Context())
		defer stopOutbox(t, sender)

		store.append(streamID, time.Now(), 1)
		time.Sleep(2*time.Minute + time.Millisecond)
		synctest.Wait()
		requireVersions(t, "published by the second one-minute sweep",
			publisher.versions(streamID), 1)
	})

	for _, interval := range []time.Duration{outbox.MinimumSweepInterval,
		outbox.MaximumSweepInterval} {
		outbox.WithSweepInterval(interval)
	}
	for _, interval := range []time.Duration{14 * time.Second, 8 * 24 * time.Hour} {
		func() {
			defer func() {
				if recover() == nil {
					t.Errorf("WithSweepInterval(%s) did not panic", interval)
				}
			}()
			outbox.WithSweepInterval(interval)
		}()
	}
}

// TestSweptSaysWhenTheSweepHadWorkToDo: recovery is rare by design, so a sweep that published
// anything is reported, and one with nothing to do is silent.
func TestSweptSaysWhenTheSweepHadWorkToDo(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store, publisher := newMemoryStore(), &recordingPublisher{}
		pipeline, err := codec.NewPipeline()
		if err != nil {
			t.Fatalf("build the pipeline: %v", err)
		}
		var swept []int
		sender := outbox.New(store, pipeline, publisher, publish.Hooks{
			Swept: func(streams int) {
				swept = append(swept, streams)
			},
		})
		store.append(uuid.New(), time.Now().Add(-time.Hour), 1, 2)
		store.append(uuid.New(), time.Now().Add(-time.Hour), 1)

		sender.Start(t.Context())
		defer stopOutbox(t, sender)
		synctest.Wait()
		time.Sleep(outbox.DefaultSweepInterval)
		synctest.Wait()
		if !slices.Equal(swept, []int{2}) {
			t.Fatalf("swept reported %v, want 2 streams once and nothing for the empty sweep",
				swept)
		}
	})
}

// TestStartSweepsAtOnceAndReportsFailures: a restart publishes what the last run left, without
// holding up the start, and a sweep's error reaches the Failed hook.
func TestStartSweepsAtOnceAndReportsFailures(t *testing.T) {
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
		streamID := uuid.New()
		store.append(streamID, time.Now().Add(-time.Hour), 1) // left by the last run

		sender.Start(t.Context())
		defer stopOutbox(t, sender)
		synctest.Wait()
		requireVersions(t, "published while the publisher is blocked", publisher.versions(streamID))
		close(release)
		synctest.Wait()
		requireVersions(t, "published once the publisher returns", publisher.versions(streamID), 1)

		store.mutex.Lock()
		store.pendingStreamsErr = errors.New("connection refused")
		store.mutex.Unlock()
		time.Sleep(outbox.DefaultSweepInterval)
		synctest.Wait()
		streams, _ := failed.reported()
		if len(streams) != 1 || streams[0] != uuid.Nil {
			t.Fatalf("failures reported for %v, want one, for no stream", streams)
		}
	})
}

// TestShutdownStopsTheSweeps: nothing is read after a shutdown, however long the service
// takes to exit.
func TestShutdownStopsTheSweeps(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store, publisher, failed := newMemoryStore(), &recordingPublisher{}, &failures{}
		sender := newOutbox(store, publisher, failed)
		sender.Start(t.Context())
		synctest.Wait()

		stopOutbox(t, sender)
		store.mutex.Lock()
		calls := store.pendingStreamCalls
		store.mutex.Unlock()
		time.Sleep(24 * time.Hour)
		synctest.Wait()

		store.mutex.Lock()
		defer store.mutex.Unlock()
		if store.pendingStreamCalls != calls {
			t.Fatalf("the store was read %d times after shutdown, want none",
				store.pendingStreamCalls-calls)
		}
	})
}

func stopOutbox(t *testing.T, sender outbox.Outbox) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	err := sender.Shutdown(ctx)
	if err != nil {
		t.Fatalf("shutdown: %v", err)
	}
}

func requireVersions(t *testing.T, what string, got []int32, want ...int32) {
	t.Helper()
	if !slices.Equal(got, want) {
		t.Fatalf("%s: got versions %v, want %v", what, got, want)
	}
}

func requireNoFailures(t *testing.T, failed *failures) {
	t.Helper()
	_, errs := failed.reported()
	if len(errs) != 0 {
		t.Fatalf("failures reported: %v, want none", errs)
	}
}

// TestEventsGoInBatchesOfOneKey: what an earlier publish left goes first under the key it was
// appended with, the committed events follow under theirs, and no call holds two keys.
func TestEventsGoInBatchesOfOneKey(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store, publisher, failed := newMemoryStore(), &recordingPublisher{}, &failures{}
		sender := newOutbox(store, publisher, failed)
		streamID := uuid.New()
		store.appendKeyed(streamID, time.Now(), "customer-1", 1, 2)
		committed := store.appendKeyed(streamID, time.Now(), "customer-2", 3, 4)

		sender.Publish(t.Context(), streamID, 1, committed)
		want := []string{"customer-1 1 to 2", "customer-2 3 to 4"}
		if calls := publisher.calls(); !slices.Equal(calls, want) {
			t.Fatalf("publisher calls %v, want %v", calls, want)
		}
		requireVersions(t, "still pending", store.pendingVersions(streamID))
		requireNoFailures(t, failed)
	})
}

// TestABatchHoldsAtMostTheBatchSize: a backlog goes out a batch at a time, 25 by default or as
// WithPublishBatchSize says, within one to a thousand.
func TestABatchHoldsAtMostTheBatchSize(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store, publisher, failed := newMemoryStore(), &recordingPublisher{}, &failures{}
		sender := newOutbox(store, publisher, failed, outbox.WithPublishBatchSize(2))
		streamID := uuid.New()
		committed := store.appendKeyed(streamID, time.Now(), "k", 1, 2, 3, 4, 5)

		sender.Publish(t.Context(), streamID, 0, committed)
		want := []string{"k 1 to 2", "k 3 to 4", "k 5 to 5"}
		if calls := publisher.calls(); !slices.Equal(calls, want) {
			t.Fatalf("publisher calls %v, want %v", calls, want)
		}
	})

	for _, size := range []int{outbox.MinimumPublishBatchSize, outbox.MaximumPublishBatchSize} {
		outbox.WithPublishBatchSize(size)
	}
	for _, size := range []int{0, outbox.MaximumPublishBatchSize + 1} {
		func() {
			defer func() {
				if recover() == nil {
					t.Errorf("WithPublishBatchSize(%d) did not panic", size)
				}
			}()
			outbox.WithPublishBatchSize(size)
		}()
	}
}

// TestAPartlyDurableBatchClearsWhatIsDurable: a batch that fails part way clears the events
// before the failure and leaves the rest pending, so a retry sends only those.
func TestAPartlyDurableBatchClearsWhatIsDurable(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store, failed := newMemoryStore(), &failures{}
		publisher := &recordingPublisher{
			refuse: func(event streams.Event) error {
				if event.Version == 3 {
					return errRefused
				}
				return nil
			},
		}
		sender := newOutbox(store, publisher, failed)
		streamID := uuid.New()
		committed := store.append(streamID, time.Now(), 1, 2, 3, 4)

		sender.Publish(t.Context(), streamID, 0, committed)
		requireVersions(t, "published", publisher.versions(streamID), 1, 2)
		requireVersions(t, "still pending", store.pendingVersions(streamID), 3, 4)
	})
}

// TestASweepPublishesUnderTheStoredKey: the sweep has no stream state, so it publishes under
// the key each row was appended with, and skips a version whose row an operator deleted.
func TestASweepPublishesUnderTheStoredKey(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store, publisher, failed := newMemoryStore(), &recordingPublisher{}, &failures{}
		sender := newOutbox(store, publisher, failed)
		streamID := uuid.New()
		store.appendKeyed(streamID, time.Now().Add(-time.Hour), "customer-7", 1, 2, 3)
		store.deletePending(streamID, 2)

		swept, err := sender.Sweep(t.Context(), 2*time.Hour)
		if err != nil || swept != 1 {
			t.Fatalf("sweep: %d streams, %v, want 1", swept, err)
		}
		requireVersions(t, "published", publisher.versions(streamID), 1, 3)
		if calls := publisher.calls(); !slices.Equal(calls, []string{"customer-7 1 to 3"}) {
			t.Fatalf("publisher calls %v, want one under customer-7", calls)
		}
	})
}

// TestThePublisherGetsTheDecodedEvent: the publisher receives the plain payload, the event's
// type, format, version and append time, and its metadata for headers.
func TestThePublisherGetsTheDecodedEvent(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store, publisher, failed := newMemoryStore(), &recordingPublisher{}, &failures{}
		sender := newOutbox(store, publisher, failed)
		streamID := uuid.New()
		appendTs := time.Now()
		committed := store.append(streamID, appendTs, 1)
		committed[0].Metadata = []byte(`{"correlationId":"c-1"}`)

		sender.Publish(t.Context(), streamID, 0, committed)
		got := publisher.published[0].event
		if got.StreamID != streamID || got.Version != 1 || got.EventType != "Happened" ||
			got.DataFormat != codec.Text || !got.AppendTs.Equal(appendTs) ||
			string(got.Payload) != "version 1" ||
			string(got.Metadata) != `{"correlationId":"c-1"}` {
			t.Fatalf("published %+v, want the stored event decoded", got)
		}
	})
}

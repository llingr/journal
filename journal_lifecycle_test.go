// SPDX-FileCopyrightText: Copyright (c) 2026 The journal Authors
// SPDX-License-Identifier: Apache-2.0

package journal_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/google/uuid"

	"github.com/llingr/journal"
	"github.com/llingr/journal/internal/memory"
	"github.com/llingr/journal/persistence"
)

// TestASecondStartRunsNoSecondSweep: a repeated Start neither doubles the sweeps nor leaves
// one running past Shutdown
func TestASecondStartRunsNoSecondSweep(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sweeps := newHeldSweeps()
		eventStore := storeOver(sweeps)

		eventStore.Start(t.Context())
		eventStore.Start(t.Context())
		synctest.Wait()
		requireSweeps(t, sweeps, "after two Starts", 1, 1)

		requireShutdown(t, eventStore)
		requireSweeps(t, sweeps, "after Shutdown", 1, 0)
	})
}

// TestAStartAfterShutdownStartsNothing: a store shut down stays down
func TestAStartAfterShutdownStartsNothing(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sweeps := newHeldSweeps()
		eventStore := storeOver(sweeps)

		eventStore.Start(t.Context())
		synctest.Wait()
		requireShutdown(t, eventStore)
		eventStore.Start(t.Context())
		synctest.Wait()
		requireSweeps(t, sweeps, "after a Start that followed Shutdown", 1, 0)
	})
}

// TestAShutdownBeforeStartMeansNoSweepEver: a service stopping before it finished starting
// never sweeps
func TestAShutdownBeforeStartMeansNoSweepEver(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sweeps := newHeldSweeps()
		eventStore := storeOver(sweeps)

		requireShutdown(t, eventStore)
		eventStore.Start(t.Context())
		synctest.Wait()
		requireSweeps(t, sweeps, "after a Start that followed Shutdown", 0, 0)
	})
}

// TestShutdownWaitsAgainAfterGivingUp: a Shutdown that reached its deadline leaves the sweep
// stopping, and a later one waits for it to end
func TestShutdownWaitsAgainAfterGivingUp(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sweeps := newHeldSweeps()
		sweeps.release = make(chan struct{})
		eventStore := storeOver(sweeps)

		eventStore.Start(t.Context())
		synctest.Wait()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		err := eventStore.Shutdown(ctx)
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("shutdown with the sweep held: got %v, want the deadline", err)
		}
		requireSweeps(t, sweeps, "after a Shutdown gave up", 1, 1)

		close(sweeps.release)
		requireShutdown(t, eventStore)
		requireSweeps(t, sweeps, "after the second Shutdown", 1, 0)
		requireShutdown(t, eventStore)
	})
}

// TestTheSweepsEndWithStartsContext: cancelling the context Start was given stops the sweeps,
// and a Shutdown after that returns at once
func TestTheSweepsEndWithStartsContext(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sweeps := newHeldSweeps()
		eventStore := storeOver(sweeps)

		ctx, cancel := context.WithCancel(t.Context())
		eventStore.Start(ctx)
		synctest.Wait()
		cancel()
		synctest.Wait()
		requireSweeps(t, sweeps, "after Start's context ended", 1, 0)
		requireShutdown(t, eventStore)
	})
}

// TestStartAndShutdownFromManyGoroutines: Starts and Shutdowns racing on one store start at
// most one sweep, and no sweep runs once any Shutdown has returned
func TestStartAndShutdownFromManyGoroutines(t *testing.T) {
	for range 200 {
		synctest.Test(t, func(t *testing.T) {
			sweeps := newHeldSweeps()
			eventStore := storeOver(sweeps)

			begin := make(chan struct{})
			var callers sync.WaitGroup
			for caller := range 16 {
				callers.Go(func() {
					<-begin
					if caller%2 == 0 {
						eventStore.Start(t.Context())
						return
					}
					err := eventStore.Shutdown(context.Background())
					started, running := sweeps.counts()
					if err != nil || running != 0 {
						t.Errorf("a Shutdown returned %v with %d of %d sweeps running, want "+
							"nil and none", err, running, started)
					}
				})
			}
			close(begin)
			callers.Wait()
			synctest.Wait()

			started, running := sweeps.counts()
			if started > 1 || running != 0 {
				t.Fatalf("after racing Starts and Shutdowns: %d sweeps started and %d running, "+
					"want at most 1 and none", started, running)
			}
		})
	}
}

// heldSweeps is the memory backend holding each sweep at its first read until the sweep's
// context ends, then until release is closed, so a test sees how many sweeps started and how
// many still run
type heldSweeps struct {
	persistence.Backend
	release chan struct{}

	mutex   sync.Mutex
	started int
	running int
}

func newHeldSweeps() *heldSweeps {
	released := make(chan struct{})
	close(released)
	return &heldSweeps{
		Backend: memory.NewBackend(),
		release: released,
	}
}

func (h *heldSweeps) QueryPendingStreams(ctx context.Context, _ time.Time, _ time.Time,
	_ int) ([]uuid.UUID, error) {
	h.mutex.Lock()
	h.started++
	h.running++
	h.mutex.Unlock()

	<-ctx.Done()
	<-h.release

	h.mutex.Lock()
	h.running--
	h.mutex.Unlock()
	return nil, ctx.Err()
}

func (h *heldSweeps) counts() (started int, running int) {
	h.mutex.Lock()
	defer h.mutex.Unlock()
	return h.started, h.running
}

// storeOver is a store publishing over the backend, so it sweeps
func storeOver(backend persistence.Backend) *journal.Store {
	return journal.NewStore(backend, journal.WithPublisher(&countingPublisher{}))
}

func requireSweeps(t *testing.T, sweeps *heldSweeps, when string, wantStarted int,
	wantRunning int) {
	t.Helper()
	started, running := sweeps.counts()
	if started != wantStarted || running != wantRunning {
		t.Fatalf("%s: %d sweeps started and %d running, want %d and %d", when, started,
			running, wantStarted, wantRunning)
	}
}

func requireShutdown(t *testing.T, eventStore *journal.Store) {
	t.Helper()
	err := eventStore.Shutdown(context.Background())
	if err != nil {
		t.Fatalf("shut down: %v", err)
	}
}

// SPDX-FileCopyrightText: Copyright (c) 2026 The journal Authors
// SPDX-License-Identifier: Apache-2.0

package outbox

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/google/uuid"
)

// Start runs a sweep at once, then one every sweepInterval, unless it has started or shut
// down already
func (o *outbox) Start(ctx context.Context) {
	o.lifecycle.Lock()
	defer o.lifecycle.Unlock()
	if o.stop != nil || o.isShutDown {
		return
	}
	sweepCtx, stop := context.WithCancel(ctx)
	o.stop = stop
	go o.runSweeps(sweepCtx)
}

// Shutdown stops the sweeps for good and waits for the one in hand
func (o *outbox) Shutdown(ctx context.Context) error {
	o.lifecycle.Lock()
	o.isShutDown = true
	stop := o.stop
	o.lifecycle.Unlock()

	if stop == nil {
		return nil
	}
	stop()
	select {
	case <-o.stopped:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("stop the event sweeps: %w", ctx.Err())
	}
}

func (o *outbox) runSweeps(ctx context.Context) {
	defer close(o.stopped)

	o.timedSweep(ctx)
	ticker := time.NewTicker(o.sweepInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			o.timedSweep(ctx)
		}
	}
}

// timedSweep sweeps sweepWindows intervals back, so consecutive sweeps overlap, and reports
// to the hooks, as no caller is there to receive the result
func (o *outbox) timedSweep(ctx context.Context) {
	swept, failedStream, err := o.sweep(ctx, sweepWindows*o.sweepInterval)
	if err != nil {
		o.failed(ctx, failedStream, err)
	}
	if swept > 0 && o.hooks.Swept != nil {
		o.hooks.Swept(swept)
	}
}

// Sweep publishes the streams with rows appended within window, for an operator
func (o *outbox) Sweep(ctx context.Context, window time.Duration) (int, error) {
	swept, _, err := o.sweep(ctx, window)
	return swept, err
}

// sweep publishes every stream with a row appended within window and more than sweepAge ago,
// until none is left; it stops at the first failure, as the destination is likely down,
// leaves the rest for a later sweep and names the stream it stopped at, uuid.Nil when it
// could not list them
func (o *outbox) sweep(ctx context.Context, window time.Duration) (int, uuid.UUID, error) {
	now := time.Now()
	appendedAfter, appendedBefore := now.Add(-window), now.Add(-sweepAge)
	swept := 0
	for ctx.Err() == nil {
		streams, err := o.store.QueryPendingStreams(ctx, appendedAfter, appendedBefore, sweepStreams)
		if err != nil {
			return swept, uuid.Nil, fmt.Errorf("list pending streams: %w", err)
		}
		if len(streams) == 0 {
			return swept, uuid.Nil, nil
		}
		for _, streamID := range streams {
			var sent bool
			sent, err = o.publishPending(ctx, streamID)
			if err != nil {
				return swept, streamID, err
			}
			if sent {
				swept++
			}
		}
	}
	return swept, uuid.Nil, ctx.Err()
}

// publishPending publishes every pending row of one stream, and is whether it sent any
func (o *outbox) publishPending(ctx context.Context, streamID uuid.UUID) (bool, error) {
	pending, err := o.pendingThrough(ctx, streamID, math.MaxInt32)
	if err != nil || len(pending) == 0 {
		return false, err
	}
	_, err = o.sendPending(ctx, streamID, pending, nil)
	return true, err
}

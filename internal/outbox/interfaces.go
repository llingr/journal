// SPDX-FileCopyrightText: Copyright (c) 2026 The journal Authors
// SPDX-License-Identifier: Apache-2.0

// Package outbox publishes committed events through the application's Publisher, clears
// each from the outbox once it is published, sweeps for what a crash or a failed publish
// left, and republishes on request. It names no transport and no database driver: the
// Publisher and the Store are injected.
package outbox

import (
	"context"
	"time"

	"github.com/google/uuid"

	"github.com/llingr/journal/internal/ports"
	"github.com/llingr/journal/persistence"
	"github.com/llingr/journal/publish"
)

// Backend is the outbox side of a persistence.Backend. It exists so publishing needs no database
// driver.
type Backend interface {
	// QueryPendingStreams lists streams with an event appended from appendedAfter to before
	// appendedBefore and still unpublished, in no order
	QueryPendingStreams(ctx context.Context, appendedAfter time.Time, appendedBefore time.Time,
		limit int) ([]uuid.UUID, error)

	// QueryPending returns one stream's unpublished versions in order
	QueryPending(ctx context.Context, streamID uuid.UUID) ([]int32, error)

	// QueryHead returns the stream's last version, 0 for a stream with no events, so a
	// republish knows where the stream ends
	QueryHead(ctx context.Context, streamID uuid.UUID) (int32, error)

	// QueryStreamRange returns one stream's events from fromVersion through throughVersion
	QueryStreamRange(ctx context.Context, streamID uuid.UUID, fromVersion int32,
		throughVersion int32) ([]persistence.PersistedEvent, error)

	// DeletePublished removes the stream's pending rows up to and including throughVersion
	DeletePublished(ctx context.Context, streamID uuid.UUID, throughVersion int32) error
}

// Outbox publishes committed events and clears each once it is published
type Outbox interface {
	ports.Outbox

	// Start runs the sweep on its own goroutine, at once and then on its interval; it is
	// called once
	Start(ctx context.Context)

	// Sweep publishes the streams with rows appended in the last window and more than a
	// minute old, stopping at the first failure; it exists for an operator to run by hand,
	// such as once a broker is back, and returns the streams it published
	Sweep(ctx context.Context, window time.Duration) (int, error)

	// RepublishEvents sends a stream's events from fromVersion on again, such as to a
	// downstream that lost them: those still unpublished with IfUnpublished, every one to the
	// stream's head with Always. It returns how many are durable, and refuses with
	// ErrEarlierUnpublished while a version below fromVersion is unpublished.
	RepublishEvents(ctx context.Context, streamID uuid.UUID, fromVersion int32,
		condition publish.RepublishCondition) (int, error)

	// RepublishEvent sends one version again: only while it is unpublished with
	// IfUnpublished, ErrAlreadyPublished otherwise, and whether or not with Always. It refuses with
	// ErrEarlierUnpublished while an earlier version is unpublished.
	RepublishEvent(ctx context.Context, streamID uuid.UUID, version int32,
		condition publish.RepublishCondition) error

	// Shutdown stops the sweeps, waiting for the one in hand until ctx ends
	Shutdown(ctx context.Context) error
}

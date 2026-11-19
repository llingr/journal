// SPDX-FileCopyrightText: Copyright (c) 2026 The journal Authors
// SPDX-License-Identifier: Apache-2.0

package persistence

import (
	"context"
	"time"

	"github.com/google/uuid"
)

// Backend persists events and the outbox they are published from, the one port a backend
// implements. Its Query methods only read and Append and DeletePublished write. It sees
// payload bytes and column values, never domain event types, and returns the streams errors,
// such as streams.ErrVersionConflict, for a caller to act on.
type Backend interface {
	// QueryStream returns up to limit of one stream's events in ascending version order,
	// starting at fromVersion, which is 1 or above, and streams.ErrVersionMissing when a
	// version from there on is not stored. Fewer than limit means the stream ends there.
	QueryStream(ctx context.Context, streamID uuid.UUID, fromVersion int32,
		limit int) ([]PersistedEvent, error)

	// QueryHead returns the stream's last version, 0 for a stream with no events, so a
	// caller can append without reading every event.
	QueryHead(ctx context.Context, streamID uuid.UUID) (int32, error)

	// QueryState returns the stream's stored state, the zero PersistedState when it has
	// none, and the stream's last version, both read at one moment.
	QueryState(ctx context.Context, streamID uuid.UUID) (state PersistedState, head int32,
		err error)

	// Append writes the request as one atomic unit and returns the stream's lowest version
	// still unpublished before it, 0 when there is none. The first event must be the version
	// after the stream's head: one the stream stores, which would overwrite it, or one past
	// the head, which would leave a gap, is streams.ErrVersionConflict and writes nothing.
	Append(ctx context.Context, request AppendRequest) (earliestPending int32, err error)

	// SaveState stores a state folded from the stream's events, as when the stored one was
	// folded by another version of the application, unless the stored one is folded to a
	// later version, and reports whether it was stored.
	SaveState(ctx context.Context, state PersistedState) (bool, error)

	// QueryPendingStreams lists streams with an event appended from appendedAfter to before
	// appendedBefore and still unpublished, in no order, for the sweeps
	QueryPendingStreams(ctx context.Context, appendedAfter time.Time, appendedBefore time.Time,
		limit int) ([]uuid.UUID, error)

	// QueryPending returns one stream's unpublished versions in order
	QueryPending(ctx context.Context, streamID uuid.UUID) ([]int32, error)

	// QueryStreamRange returns one stream's events from fromVersion through throughVersion, to
	// publish what an earlier publish left
	QueryStreamRange(ctx context.Context, streamID uuid.UUID, fromVersion int32,
		throughVersion int32) ([]PersistedEvent, error)

	// DeletePublished removes the stream's unpublished rows up to and including
	// throughVersion, once the destination has them
	DeletePublished(ctx context.Context, streamID uuid.UUID, throughVersion int32) error
}

// AppendRequest is one stream's events, their outbox rows and its state, all written or none
type AppendRequest struct {
	StreamID uuid.UUID
	Events   []PersistedEvent // contiguous versions from the stream's head
	AppendTs time.Time        // one value for the whole request

	// State replaces the stream's stored state, folded to the last of Events; nil for a
	// repository that stores none
	State *PersistedState

	// Publish writes the outbox rows the events are published from; with nothing to publish
	// to, no rows are written
	Publish bool
}

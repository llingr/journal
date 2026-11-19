// SPDX-FileCopyrightText: Copyright (c) 2026 The journal Authors
// SPDX-License-Identifier: Apache-2.0

// Package memory is a backend that keeps every stream in the process's memory, for the
// library's own tests. It passes the contract every backend passes, in persistence/backendtest,
// so a test over it sees what the library over a database sees.
package memory

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/llingr/journal/persistence"
)

// errStateVersions is a state folded to no event, with a negative state version, or not
// folded to the events appended with it
var errStateVersions = errors.New("the state's versions cannot be stored")

// backend holds every stream behind one lock, so each call is atomic and sees the others
// whole
type backend struct {
	mutex   sync.Mutex
	streams map[uuid.UUID]*stream
}

// stream is one stream's events, versions 1 to its head at their indexes, its outbox rows in
// version order, and its state
type stream struct {
	events  []persistence.PersistedEvent
	pending []pendingRow
	state   *persistence.PersistedState
}

// pendingRow is one version still to publish, appended at appendTs
type pendingRow struct {
	version  int32
	appendTs time.Time
}

// NewBackend returns an empty persistence.Backend in memory, safe for concurrent use
func NewBackend() persistence.Backend {
	return &backend{
		streams: make(map[uuid.UUID]*stream),
	}
}

// streamFor returns the stream, adding it when it has nothing yet; the caller holds the lock
func (b *backend) streamFor(streamID uuid.UUID) *stream {
	found, exists := b.streams[streamID]
	if !exists {
		found = &stream{}
		b.streams[streamID] = found
	}
	return found
}

// head is the stream's last version, 0 with no events
func (s *stream) head() int32 {
	return int32(len(s.events))
}

// storedTime is a time as a database column keeps it, to the microsecond
func storedTime(value time.Time) time.Time {
	return value.Round(time.Microsecond).UTC()
}

// storedBytes is a copy, so a caller changing its slice changes nothing stored; no bytes
// are stored as empty rather than nil, as a column keeps them
func storedBytes(value []byte) []byte {
	if value == nil {
		return []byte{}
	}
	return slices.Clone(value)
}

// contextError refuses a cancelled call as a database does, before it touches anything
func contextError(ctx context.Context, action string) error {
	err := ctx.Err()
	if err != nil {
		return fmt.Errorf("%s: %w", action, err)
	}
	return nil
}

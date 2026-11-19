// SPDX-FileCopyrightText: Copyright (c) 2026 The journal Authors
// SPDX-License-Identifier: Apache-2.0

package memory

import (
	"context"
	"fmt"
	"math"

	"github.com/google/uuid"

	"github.com/llingr/journal/persistence"
	"github.com/llingr/journal/streams"
)

// QueryStream returns up to limit of the stream's events from fromVersion
func (b *backend) QueryStream(ctx context.Context, streamID uuid.UUID, fromVersion int32,
	limit int) ([]persistence.PersistedEvent, error) {
	err := contextError(ctx, fmt.Sprintf("read stream %s", streamID))
	if err != nil {
		return nil, err
	}
	if fromVersion < 1 {
		return nil, fmt.Errorf("read stream %s from version %d: %w", streamID, fromVersion,
			streams.ErrVersionsNotConsecutive)
	}
	return b.readRange(streamID, fromVersion, math.MaxInt32, limit), nil
}

// QueryStreamRange returns the stream's events from fromVersion through throughVersion
func (b *backend) QueryStreamRange(ctx context.Context, streamID uuid.UUID, fromVersion int32,
	throughVersion int32) ([]persistence.PersistedEvent, error) {
	err := contextError(ctx, fmt.Sprintf("read stream %s", streamID))
	if err != nil {
		return nil, err
	}
	if fromVersion < 1 {
		return nil, fmt.Errorf("read stream %s from version %d: %w", streamID, fromVersion,
			streams.ErrVersionsNotConsecutive)
	}
	return b.readRange(streamID, fromVersion, throughVersion, math.MaxInt), nil
}

// readRange returns up to limit events from fromVersion, 1 or above, through throughVersion.
// Appends only ever extend the head, so no version is missing.
func (b *backend) readRange(streamID uuid.UUID, fromVersion int32, throughVersion int32,
	limit int) []persistence.PersistedEvent {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	found, exists := b.streams[streamID]
	if !exists {
		return nil
	}

	var read []persistence.PersistedEvent
	lastVersion := min(throughVersion, found.head())
	for version := fromVersion; version <= lastVersion && len(read) < limit; version++ {
		read = append(read, readEvent(found.events[version-1]))
	}
	return read
}

// QueryHead returns the stream's last version, 0 with no events
func (b *backend) QueryHead(ctx context.Context, streamID uuid.UUID) (int32, error) {
	err := contextError(ctx, fmt.Sprintf("read the head of stream %s", streamID))
	if err != nil {
		return 0, err
	}

	b.mutex.Lock()
	defer b.mutex.Unlock()
	found, exists := b.streams[streamID]
	if !exists {
		return 0, nil
	}
	return found.head(), nil
}

// QueryState returns the stream's state, the zero value when it has none, and its last
// version, both under one lock
func (b *backend) QueryState(ctx context.Context,
	streamID uuid.UUID) (persistence.PersistedState, int32, error) {
	err := contextError(ctx, fmt.Sprintf("read the state of stream %s", streamID))
	if err != nil {
		return persistence.PersistedState{}, 0, err
	}

	b.mutex.Lock()
	defer b.mutex.Unlock()
	found, exists := b.streams[streamID]
	if !exists {
		return persistence.PersistedState{}, 0, nil
	}
	if found.state == nil {
		return persistence.PersistedState{}, found.head(), nil
	}
	state := *found.state
	state.StateData = storedBytes(state.StateData)
	return state, found.head(), nil
}

// readEvent is a stored event with its own copies of the bytes, so a caller changing them
// changes nothing stored
func readEvent(stored persistence.PersistedEvent) persistence.PersistedEvent {
	read := stored
	read.EventData = storedBytes(stored.EventData)
	read.Metadata = storedBytes(stored.Metadata)
	return read
}

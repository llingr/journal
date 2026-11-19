// SPDX-FileCopyrightText: Copyright (c) 2026 The journal Authors
// SPDX-License-Identifier: Apache-2.0

package memory

import (
	"context"
	"fmt"

	"github.com/llingr/journal/persistence"
	"github.com/llingr/journal/streams"
)

// Append checks the whole request before it writes any of it, and takes events only from the
// version after the head
func (b *backend) Append(ctx context.Context, request persistence.AppendRequest) (int32, error) {
	err := contextError(ctx, fmt.Sprintf("append to stream %s", request.StreamID))
	if err != nil || len(request.Events) == 0 {
		return 0, err
	}
	firstVersion := request.Events[0].Version
	for index, event := range request.Events {
		if firstVersion < 1 || event.Version != firstVersion+int32(index) {
			return 0, fmt.Errorf("append to stream %s: %w: version %d at position %d",
				request.StreamID, streams.ErrVersionsNotConsecutive, event.Version, index)
		}
	}
	lastVersion := request.Events[len(request.Events)-1].Version
	if request.State != nil {
		err = checkState(*request.State)
		if err != nil {
			return 0, err
		}
		if request.State.Version != lastVersion || request.State.StreamID != request.StreamID {
			return 0, fmt.Errorf("append to stream %s: %w: the state is folded to stream %s "+
				"version %d, not version %d", request.StreamID, errStateVersions,
				request.State.StreamID, request.State.Version, lastVersion)
		}
	}

	b.mutex.Lock()
	defer b.mutex.Unlock()
	appended := b.streamFor(request.StreamID)
	head := appended.head()
	switch {
	case firstVersion <= head:
		return 0, fmt.Errorf("append to stream %s: %w: version %d is stored", request.StreamID,
			streams.ErrVersionConflict, firstVersion)
	case firstVersion > head+1:
		return 0, fmt.Errorf("append to stream %s: %w: version %d is past the head, %d",
			request.StreamID, streams.ErrVersionConflict, firstVersion, head)
	}

	appendTs := storedTime(request.AppendTs)
	for _, event := range request.Events {
		stored := event
		stored.StreamID = request.StreamID
		stored.EventAppendTs = appendTs
		stored.EventData = storedBytes(event.EventData)
		stored.Metadata = storedBytes(event.Metadata)
		appended.events = append(appended.events, stored)
	}

	var earliestPending int32
	if request.Publish {
		if len(appended.pending) > 0 {
			earliestPending = appended.pending[0].version
		}
		for _, event := range request.Events {
			appended.pending = append(appended.pending, pendingRow{
				version:  event.Version,
				appendTs: appendTs,
			})
		}
	}

	if request.State != nil {
		appended.saveState(*request.State)
	}
	return earliestPending, nil
}

// SaveState stores the state unless the stored one is folded to a later version
func (b *backend) SaveState(ctx context.Context, state persistence.PersistedState) (bool, error) {
	err := contextError(ctx, fmt.Sprintf("save the state of stream %s", state.StreamID))
	if err != nil {
		return false, err
	}
	err = checkState(state)
	if err != nil {
		return false, err
	}

	b.mutex.Lock()
	defer b.mutex.Unlock()
	return b.streamFor(state.StreamID).saveState(state), nil
}

// saveState replaces the stored state unless that is folded to a later version, keeping the
// partition key the first state was stored with, and is whether it stored it
func (s *stream) saveState(state persistence.PersistedState) bool {
	if s.state != nil && s.state.Version > state.Version {
		return false
	}
	stored := state
	stored.LastAppendTs = storedTime(state.LastAppendTs)
	stored.StateData = storedBytes(state.StateData)
	if s.state != nil {
		stored.PartitionKey = s.state.PartitionKey
	}
	s.state = &stored
	return true
}

// checkState refuses a state folded to no event, or with a negative state version
func checkState(state persistence.PersistedState) error {
	if state.Version < 1 || state.StateVersion < 0 {
		return fmt.Errorf("save the state of stream %s: %w: version %d, state version %d",
			state.StreamID, errStateVersions, state.Version, state.StateVersion)
	}
	return nil
}

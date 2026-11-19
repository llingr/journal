// SPDX-FileCopyrightText: Copyright (c) 2026 The journal Authors
// SPDX-License-Identifier: Apache-2.0

package memory

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// QueryPendingStreams lists streams with an outbox row appended from appendedAfter to before
// appendedBefore, reading at most limit rows, so a stream with several such rows is listed
// once and the list may be shorter. Streams come in no order, as a database's would.
func (b *backend) QueryPendingStreams(ctx context.Context, appendedAfter time.Time,
	appendedBefore time.Time, limit int) ([]uuid.UUID, error) {
	err := contextError(ctx, "list pending streams")
	if err != nil {
		return nil, err
	}

	b.mutex.Lock()
	defer b.mutex.Unlock()
	var listed []uuid.UUID
	rows := 0
	for streamID, found := range b.streams {
		isListed := false
		for _, row := range found.pending {
			if rows == limit {
				return listed, nil
			}
			if row.appendTs.Before(appendedAfter) || !row.appendTs.Before(appendedBefore) {
				continue
			}
			rows++
			if !isListed {
				isListed = true
				listed = append(listed, streamID)
			}
		}
	}
	return listed, nil
}

// QueryPending returns the stream's unpublished versions in order
func (b *backend) QueryPending(ctx context.Context, streamID uuid.UUID) ([]int32, error) {
	err := contextError(ctx, fmt.Sprintf("read pending rows of stream %s", streamID))
	if err != nil {
		return nil, err
	}

	b.mutex.Lock()
	defer b.mutex.Unlock()
	found, exists := b.streams[streamID]
	if !exists {
		return nil, nil
	}
	pending := make([]int32, 0, len(found.pending))
	for _, row := range found.pending {
		pending = append(pending, row.version)
	}
	return pending, nil
}

// DeletePublished removes the stream's outbox rows up to and including throughVersion
func (b *backend) DeletePublished(ctx context.Context, streamID uuid.UUID,
	throughVersion int32) error {
	err := contextError(ctx, fmt.Sprintf("clear stream %s through version %d", streamID,
		throughVersion))
	if err != nil {
		return err
	}

	b.mutex.Lock()
	defer b.mutex.Unlock()
	found, exists := b.streams[streamID]
	if !exists {
		return nil
	}
	published := 0
	for published < len(found.pending) && found.pending[published].version <= throughVersion {
		published++
	}
	found.pending = found.pending[published:]
	return nil
}

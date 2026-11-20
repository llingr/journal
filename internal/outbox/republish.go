// SPDX-FileCopyrightText: Copyright (c) 2026 The journal Authors
// SPDX-License-Identifier: Apache-2.0

package outbox

import (
	"context"
	"fmt"
	"strings"

	"github.com/google/uuid"

	"github.com/llingr/journal/persistence"
	"github.com/llingr/journal/publish"
	"github.com/llingr/journal/streams"
)

// republishPageSize is how many versions a republish of every version reads at a time
const republishPageSize = 250

func (o *outbox) RepublishEvents(ctx context.Context, streamID uuid.UUID, fromVersion int32,
	condition publish.RepublishCondition) (int, error) {
	pending, err := o.pendingFrom(ctx, streamID, fromVersion)
	if err != nil {
		return 0, err
	}

	switch condition {
	case publish.IfUnpublished:
		var toSend []persistence.PersistedEvent
		toSend, err = o.readVersions(ctx, streamID, pending)
		if err != nil {
			return 0, err
		}
		return o.sendAll(ctx, streamID, toSend)
	case publish.Always:
		return o.sendToHead(ctx, streamID, fromVersion)
	}
	panic(fmt.Sprintf("publish: unknown republish condition %d", condition))
}

func (o *outbox) RepublishEvent(ctx context.Context, streamID uuid.UUID, version int32,
	condition publish.RepublishCondition) error {
	pending, err := o.pendingFrom(ctx, streamID, version)
	if err != nil {
		return err
	}

	switch condition {
	case publish.IfUnpublished:
		if len(pending) == 0 || pending[0] != version {
			return fmt.Errorf("republish stream %s version %d: %w", streamID, version,
				publish.ErrAlreadyPublished)
		}
	case publish.Always:
	default:
		panic(fmt.Sprintf("publish: unknown republish condition %d", condition))
	}

	var stored []persistence.PersistedEvent
	stored, err = o.store.QueryStreamRange(ctx, streamID, version, version)
	if err != nil {
		return fmt.Errorf("republish stream %s version %d: %w", streamID, version, err)
	}
	if len(stored) == 0 {
		return fmt.Errorf("republish stream %s version %d: %w", streamID, version,
			streams.ErrVersionMissing)
	}
	_, err = o.sendAll(ctx, streamID, stored)
	return err
}

// pendingFrom is the stream's unpublished versions from version on, refusing while any
// before it is unpublished
func (o *outbox) pendingFrom(ctx context.Context, streamID uuid.UUID,
	version int32) ([]int32, error) {
	pending, err := o.store.QueryPending(ctx, streamID)
	if err != nil {
		return nil, fmt.Errorf("republish stream %s: %w", streamID, err)
	}
	earlier := 0
	for earlier < len(pending) && pending[earlier] < version {
		earlier++
	}
	if earlier > 0 {
		return nil, fmt.Errorf("republish stream %s from version %d: %w: %s first",
			streamID, version, publish.ErrEarlierUnpublished, describeVersions(pending[:earlier]))
	}
	return pending, nil
}

// sendToHead sends the stream's events from fromVersion to its last, reading and sending
// republishPageSize at a time so a long stream is never held whole, and stops at the first
// failure; it returns how many are durable
func (o *outbox) sendToHead(ctx context.Context, streamID uuid.UUID,
	fromVersion int32) (int, error) {
	head, err := o.store.QueryHead(ctx, streamID)
	if err != nil {
		return 0, fmt.Errorf("republish stream %s: %w", streamID, err)
	}
	if fromVersion > head {
		return 0, fmt.Errorf("republish stream %s from version %d, its last is %d: %w",
			streamID, fromVersion, head, streams.ErrVersionMissing)
	}

	sent := 0
	pageStart := fromVersion
	for {
		pageEnd := head
		if head-pageStart >= republishPageSize {
			pageEnd = pageStart + republishPageSize - 1
		}
		var page []persistence.PersistedEvent
		page, err = o.store.QueryStreamRange(ctx, streamID, pageStart, pageEnd)
		if err != nil {
			return sent, fmt.Errorf("republish stream %s: %w", streamID, err)
		}
		var pageSent int
		pageSent, err = o.sendAll(ctx, streamID, page)
		sent += pageSent
		if err != nil || pageEnd == head {
			return sent, err
		}
		pageStart = pageEnd + 1
	}
}

// describeVersions names versions in ascending order as the ranges a reader acts on, such
// as "publish versions 3 to 4, 7"
func describeVersions(versions []int32) string {
	var ranges []string
	for start := 0; start < len(versions); {
		end := start
		for end+1 < len(versions) && versions[end+1] == versions[end]+1 {
			end++
		}
		if end == start {
			ranges = append(ranges, fmt.Sprintf("%d", versions[start]))
		} else {
			ranges = append(ranges, fmt.Sprintf("%d to %d", versions[start], versions[end]))
		}
		start = end + 1
	}
	if len(versions) == 1 {
		return "publish version " + ranges[0]
	}
	return "publish versions " + strings.Join(ranges, ", ")
}

// SPDX-FileCopyrightText: Copyright (c) 2026 The journal Authors
// SPDX-License-Identifier: Apache-2.0

// Package publish is what an application provides to send committed events wherever they go,
// such as a Kafka topic: a Publisher, the Hooks that observe it, and the conditions and
// errors of a republish.
//
// The repository publishes after each committed write, and a stream's next write sends
// what an earlier one left. A sweep at start and every three hours by default is the
// backstop for a stream with no next write, after a crash between commit and publish or a
// failed publish.
package publish

import (
	"context"

	"github.com/google/uuid"

	"github.com/llingr/journal/streams"
)

// Publisher sends events wherever they go, and is the application's to write, so the library
// names no transport. PublishAll is called concurrently, from every goroutine that appends
// and from the sweep, so it must be safe for concurrent use. Two calls may carry the same
// stream: a version can be sent again, but never for the first time before a lower one.
type Publisher interface {
	// PublishAll sends one stream's events, in version order, under partitionKey. A nil
	// error means every event is durable at the destination; with an error, the count is how
	// many, from the first, are durable, and the rest are published again later.
	PublishAll(ctx context.Context, partitionKey string, events []streams.Event) (int, error)
}

// Hooks observe publishing. Each runs inline on the goroutine that published, the writer's
// after a commit and the sweeps' own otherwise, so a slow hook slows publishing; an unset
// hook costs a nil check.
type Hooks struct {
	// Published fires after a stream's versions fromVersion to toVersion are published
	Published func(streamID uuid.UUID, fromVersion int32, toVersion int32)

	// Failed fires for every publish that failed, whose events stay unpublished for the next
	// write or sweep; streamID is uuid.Nil when a sweep could not list the unpublished
	// streams. A failed publish does not fail the write, so this is its only report.
	Failed func(streamID uuid.UUID, err error)

	// Swept fires after a timed sweep that published the events of one or more streams,
	// which means a publish after a commit failed or never ran since the sweep before
	Swept func(streams int)
}

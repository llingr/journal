// SPDX-FileCopyrightText: Copyright (c) 2026 The journal Authors
// SPDX-License-Identifier: Apache-2.0

// Package ports holds the interface the repository publishes through after a commit, which
// the outbox implements, so neither package imports the other.
package ports

import (
	"context"

	"github.com/google/uuid"

	"github.com/llingr/journal/persistence"
)

// Outbox publishes a stream's committed events and clears them once published. It exists so
// the repository can publish after a write without importing a transport; outbox.New
// returns one.
type Outbox interface {
	// Publish sends the events just committed, as they were written, after any of the
	// stream's earlier versions still unpublished from earliestPending; a failure is the
	// outbox's to report, and the events stay pending.
	Publish(ctx context.Context, streamID uuid.UUID, earliestPending int32,
		committed []persistence.PersistedEvent)
}

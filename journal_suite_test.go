// SPDX-FileCopyrightText: Copyright (c) 2026 The journal Authors
// SPDX-License-Identifier: Apache-2.0

package journal_test

import (
	"testing"

	"github.com/llingr/journal/internal/memory"
	"github.com/llingr/journal/journaltest"
	"github.com/llingr/journal/persistence"
)

// TestTheLibraryOverTheMemoryBackend runs everything the library promises an application
func TestTheLibraryOverTheMemoryBackend(t *testing.T) {
	journaltest.RunSuite(t, func(*testing.T) persistence.Backend {
		return memory.NewBackend()
	})
}

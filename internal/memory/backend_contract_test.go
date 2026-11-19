// SPDX-FileCopyrightText: Copyright (c) 2026 The journal Authors
// SPDX-License-Identifier: Apache-2.0

package memory_test

import (
	"testing"

	"github.com/llingr/journal/internal/memory"
	"github.com/llingr/journal/persistence"
	"github.com/llingr/journal/persistence/backendtest"
)

// TestMemoryBackendContract runs the contract every backend passes, so the memory backend
// behaves as a database does
func TestMemoryBackendContract(t *testing.T) {
	backendtest.RunContract(t, func(*testing.T) persistence.Backend {
		return memory.NewBackend()
	})
}

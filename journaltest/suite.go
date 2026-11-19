// SPDX-FileCopyrightText: Copyright (c) 2026 The journal Authors
// SPDX-License-Identifier: Apache-2.0

// Package journaltest is the suite of everything the library promises an application, run
// through its public API over one backend: streams and their errors, payloads, partition
// keys, publishing, sweeps, republishing and stored state. The journal module runs it over
// the memory backend, and a backend runs it over itself beside the contract in
// persistence/backendtest:
//
//	func TestLibraryOverMyBackend(t *testing.T) {
//		journaltest.RunSuite(t, func(t *testing.T) persistence.Backend {
//			return newBackend(t)
//		})
//	}
//
// The suite looks inside the backend only through persistence.Backend, so it holds for any
// backend that passes the contract.
package journaltest

import (
	"testing"

	"github.com/llingr/journal/persistence"
)

// NewBackend returns a backend with no events in it, for one subtest, which it can skip or
// fail and register its teardown with
type NewBackend func(t *testing.T) persistence.Backend

// suiteCase is one test of the suite, given a backend of its own
type suiteCase struct {
	name string
	run  func(t *testing.T, backend persistence.Backend)
}

// RunSuite runs every test of the library over backends from newBackend, a fresh one each
func RunSuite(t *testing.T, newBackend NewBackend) {
	t.Helper()

	for _, cases := range [][]suiteCase{
		streamCases,
		payloadCases,
		partitionKeyCases,
		publishCases,
		sweepCases,
		republishCases,
		snapshotCases,
		failureCases,
	} {
		for _, testCase := range cases {
			t.Run(testCase.name, func(t *testing.T) {
				testCase.run(t, newBackend(t))
			})
		}
	}
}

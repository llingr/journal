// SPDX-FileCopyrightText: Copyright (c) 2026 The journal Authors
// SPDX-License-Identifier: Apache-2.0

package streams

import "context"

// AppendOption shapes one Create, CreateAll, Append or AppendAll
type AppendOption func(options *appendOptions)

type appendOptions struct {
	afterCommit []func(ctx context.Context)
}

// AfterCommit runs afterCommit once the write has committed, such as to invalidate a cache.
// It runs after the attempt to publish, whether or not that succeeded, as a failed publish is
// retried later and reported to publish.Hooks.Failed. It is not run when the write fails.
func AfterCommit(afterCommit func(ctx context.Context)) AppendOption {
	return func(options *appendOptions) {
		options.afterCommit = append(options.afterCommit, afterCommit)
	}
}

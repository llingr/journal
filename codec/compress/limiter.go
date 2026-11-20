// SPDX-FileCopyrightText: Copyright (c) 2026 The journal Authors
// SPDX-License-Identifier: Apache-2.0

package compress

import (
	"errors"
	"fmt"
)

// The calls a compressor that keeps state between calls runs at once; each holds a writer or
// a reader, so this bounds how many it ever holds, however many goroutines call it
const (
	DefaultConcurrency = 16
	MinimumConcurrency = 1
	MaximumConcurrency = 256
)

var ErrConcurrency = errors.New("unsupported concurrency")

// limiter is a slot per call a compressor may run at once
type limiter chan struct{}

// newLimiter panics with ErrConcurrency outside MinimumConcurrency to MaximumConcurrency, as
// that is a wiring mistake
func newLimiter(concurrency int) limiter {
	if concurrency < MinimumConcurrency || concurrency > MaximumConcurrency {
		panic(fmt.Errorf("%w: %d is outside %d to %d", ErrConcurrency, concurrency,
			MinimumConcurrency, MaximumConcurrency))
	}
	return make(limiter, concurrency)
}

// acquire takes a slot, waiting while every one is taken
func (l limiter) acquire() {
	l <- struct{}{}
}

// release frees the slot acquire took
func (l limiter) release() {
	<-l
}

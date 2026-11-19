// SPDX-FileCopyrightText: Copyright (c) 2026 The journal Authors
// SPDX-License-Identifier: Apache-2.0

package publish

import "errors"

// RepublishCondition says which versions a republish sends
type RepublishCondition int

const (
	// IfUnpublished sends only versions still unpublished
	IfUnpublished RepublishCondition = iota + 1

	// Always sends versions whether or not they were published before
	Always
)

// ErrEarlierUnpublished is a republish that would send a version before an earlier one is
// published, which would break the stream's order
var ErrEarlierUnpublished = errors.New("earlier versions are unpublished")

// ErrAlreadyPublished is an IfUnpublished republish of a version that is already published
var ErrAlreadyPublished = errors.New("the version is already published")

// ErrPublisherPanic is a Publisher that panicked instead of returning an error, reported to
// Hooks.Failed
var ErrPublisherPanic = errors.New("publisher panicked")

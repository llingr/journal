// SPDX-FileCopyrightText: Copyright (c) 2026 The journal Authors
// SPDX-License-Identifier: Apache-2.0

package streams

import (
	"errors"

	"github.com/llingr/journal/codec"
)

// ErrStreamNotFolded is an append, on a repository that folds or stores its state, to a stream
// read by Head, which has no state to fold the new events onto
var ErrStreamNotFolded = errors.New("stream has no folded state")

// StateCodec is how a repository stores its state S beside the events, so Get reads one row
// rather than folding every event. The state marshals as a domain event does, and its bytes
// are compressed and encrypted as the events' are.
type StateCodec[S any] struct {
	// Version is the application's version of its fold and of the state's form, 1 or above.
	// A change to either increases it, and Get folds again from the events a stream whose
	// state another version stored.
	Version int32

	Format    codec.DataFormat
	Marshal   func(state S) ([]byte, error)
	Unmarshal func(encoded []byte) (S, error)
}

// valid for a version of 1 or above, a known format and both functions
func (c StateCodec[S]) valid() bool {
	return c.Version >= 1 && c.Format.Valid() && c.Marshal != nil && c.Unmarshal != nil
}

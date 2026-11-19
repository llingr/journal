// SPDX-FileCopyrightText: Copyright (c) 2026 The journal Authors
// SPDX-License-Identifier: Apache-2.0

package persistence

import (
	"time"

	"github.com/google/uuid"

	"github.com/llingr/journal/codec"
	"github.com/llingr/journal/codec/compress"
	"github.com/llingr/journal/codec/encrypt"
)

// PersistedState is a stream's state as stored beside its events, its bytes still compressed
// and encrypted as the columns say: one per stream, replaced by every append, never
// published. The zero value, Version 0, is a stream with no state stored.
type PersistedState struct {
	StreamID     uuid.UUID
	Version      int32 // the stream's version the state is folded to
	StateVersion int32 // the application's version of its fold and of the state's form

	LastEventType string    // the type of the event at Version
	LastAppendTs  time.Time // when the event at Version was appended

	StateData         []byte
	DataFormat        codec.DataFormat
	DataCompression   compress.DataCompression
	DataEncryption    encrypt.DataEncryption
	DataEncryptionKid string // empty for encrypt.None
	PartitionKey      string // the stream's key; empty for the stream id
}

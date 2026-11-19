// SPDX-FileCopyrightText: Copyright (c) 2026 The journal Authors
// SPDX-License-Identifier: Apache-2.0

// Package persistence is the port a backend implements to store events and their outbox,
// such as journal-postgres: Backend, the PersistedEvent rows it keeps, the PersistedState
// beside them, and the AppendRequest that writes them. An application never names it: it
// passes a backend, such as journal-postgres's NewBackend, straight to journal.NewStore.
package persistence

import (
	"time"

	"github.com/google/uuid"

	"github.com/llingr/journal/codec"
	"github.com/llingr/journal/codec/compress"
	"github.com/llingr/journal/codec/encrypt"
)

// PersistedEvent is one row of the events table as stored, its payload still compressed and
// encrypted as the columns say. Only the library and a Store see it; a host sees
// streams.Event.
type PersistedEvent struct {
	StreamID          uuid.UUID
	Version           int32
	EventType         string
	EventData         []byte
	DataFormat        codec.DataFormat         // e.g. json, avro, yaml...
	DataCompression   compress.DataCompression // e.g. none, gzip, lz4...
	DataEncryption    encrypt.DataEncryption   // e.g. none, aes-gcm...
	DataEncryptionKid string                   // empty for encrypt.None
	EventAppendTs     time.Time
	Metadata          []byte
	PartitionKey      string // the key it is published under; empty for the stream id
}

// PublishKey is the key the event is published under: its PartitionKey, or the stream id
// when that is empty
func (e PersistedEvent) PublishKey() string {
	if e.PartitionKey == "" {
		return e.StreamID.String()
	}
	return e.PartitionKey
}

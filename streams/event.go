// SPDX-FileCopyrightText: Copyright (c) 2026 The journal Authors
// SPDX-License-Identifier: Apache-2.0

// Package streams holds the event store's streams: State, one stream at a version, the
// Repository that reads and writes them, the DomainEvent an application defines, the Event
// it is stored as, and the errors a caller acts on.
package streams

import (
	"time"

	"github.com/google/uuid"

	"github.com/llingr/journal/codec"
	"github.com/llingr/journal/persistence"
)

// Event is one stored event with its payload decoded: what State.Events returns and what a
// Publisher is sent
type Event struct {
	StreamID   uuid.UUID
	Version    int32
	EventType  string
	DataFormat codec.DataFormat // json, avro, proto...
	AppendTs   time.Time
	Payload    []byte // decompressed and decrypted
	Metadata   []byte // as stored, empty for none
}

// decodedEvent is the persisted event with its payload as plain
func decodedEvent(persisted persistence.PersistedEvent, plain []byte) Event {
	return Event{
		StreamID:   persisted.StreamID,
		Version:    persisted.Version,
		EventType:  persisted.EventType,
		DataFormat: persisted.DataFormat,
		AppendTs:   persisted.EventAppendTs,
		Payload:    plain,
		Metadata:   persisted.Metadata,
	}
}

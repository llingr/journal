// SPDX-FileCopyrightText: Copyright (c) 2026 The journal Authors
// SPDX-License-Identifier: Apache-2.0

package streams

import (
	"time"

	"github.com/google/uuid"
)

// Stream is one stream at a version: its events and the state they fold to. Its fields are
// read through methods, so its version is always one the store gave back.
type Stream[S any] struct {
	streamID      uuid.UUID
	version       int32
	state         S
	lastEventType string
	lastAppendTs  time.Time
	events        []Event
	domainEvents  []DomainEvent
	folded        bool // the state is the events folded, false for a stream read by Head
}

// StreamID is the stream's id
func (s Stream[S]) StreamID() uuid.UUID {
	return s.streamID
}

// Version is the stream's last version, 0 before its first event
func (s Stream[S]) Version() int32 {
	return s.version
}

// State is the stream's events folded, the zero state for a stream read by Head
func (s Stream[S]) State() S {
	return s.state
}

// LastEventType is the type of the event at Version, empty before the first event and for a
// stream read by Head
func (s Stream[S]) LastEventType() string {
	return s.lastEventType
}

// LastAppendTs is when the event at Version was appended, the zero time before the first event
// and for a stream read by Head
func (s Stream[S]) LastAppendTs() time.Time {
	return s.lastAppendTs
}

// Events are the stream's events as read or written, each Payload the bytes its domain event
// marshalled. A stream read from its stored state has only those written since, and one read
// by Head has none.
func (s Stream[S]) Events() []Event {
	return s.events
}

// DomainEvents are the same events as the domain event types that wrote them, such as
// *OrderPlaced, in the same order as Events
func (s Stream[S]) DomainEvents() []DomainEvent {
	return s.domainEvents
}

// SPDX-FileCopyrightText: Copyright (c) 2026 The journal Authors
// SPDX-License-Identifier: Apache-2.0

package streams

import (
	"errors"

	"github.com/llingr/journal/codec"
)

// ErrRepeatedEventType is two domain event types claiming one event type.
var ErrRepeatedEventType = errors.New("event type claimed by two domain event types")

// DomainEvent is an event the application defines, such as OrderPlaced: what it provides to
// be appended. It marshals itself into the stored Event's payload, so nothing in the library
// holds a table of formats or decides which marshaller a type gets.
type DomainEvent interface {
	EventType() string
	DataFormat() codec.DataFormat
	MarshalPayload() ([]byte, error)
	UnmarshalPayload(encoded []byte) error
}

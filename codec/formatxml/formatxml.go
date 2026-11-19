// SPDX-FileCopyrightText: Copyright (c) 2026 The journal Authors
// SPDX-License-Identifier: Apache-2.0

// Package formatxml marshals payloads as XML.
//
// A domain event type calls these from its own MarshalPayload and UnmarshalPayload.
package formatxml

import (
	"encoding/xml"

	"github.com/llingr/journal/codec"
)

// Format is the value a payload using this package reports as its DataFormat.
const Format = codec.XML

func Marshal(value any) ([]byte, error) {
	return xml.Marshal(value)
}

func Unmarshal(encoded []byte, target any) error {
	return xml.Unmarshal(encoded, target)
}

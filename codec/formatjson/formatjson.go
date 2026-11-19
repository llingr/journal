// SPDX-FileCopyrightText: Copyright (c) 2026 The journal Authors
// SPDX-License-Identifier: Apache-2.0

// Package formatjson marshals payloads as JSON.
//
// A domain event type calls these from its own MarshalPayload and UnmarshalPayload.
// Nothing registers this package anywhere: a payload reaches it by importing
// it, and a payload that wants a different JSON library imports that instead.
package formatjson

import (
	"encoding/json"

	"github.com/llingr/journal/codec"
)

// Format is the value a payload using this package reports as its DataFormat.
const Format = codec.JSON

func Marshal(value any) ([]byte, error) {
	return json.Marshal(value)
}

func Unmarshal(encoded []byte, target any) error {
	return json.Unmarshal(encoded, target)
}

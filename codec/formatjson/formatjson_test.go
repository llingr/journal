// SPDX-FileCopyrightText: Copyright (c) 2026 The journal Authors
// SPDX-License-Identifier: Apache-2.0

package formatjson_test

import (
	"reflect"
	"strings"
	"testing"

	"github.com/llingr/journal/codec"
	"github.com/llingr/journal/codec/formatjson"
)

type order struct {
	ID       string            `json:"id"`
	Amount   int64             `json:"amount"`
	Note     string            `json:"note"`
	Lines    []line            `json:"lines"`
	Labels   map[string]string `json:"labels"`
	Optional *string           `json:"optional"`
}

type line struct {
	Sku      string `json:"sku"`
	Quantity int    `json:"quantity"`
}

// TestFormatIsJSON: a payload reporting Format is stored as json, a value the schema accepts
func TestFormatIsJSON(t *testing.T) {
	if formatjson.Format != codec.JSON || !formatjson.Format.Valid() {
		t.Fatalf("Format is %q, want the valid %q", formatjson.Format, codec.JSON)
	}
}

// TestAPayloadRoundTrips: what is marshalled unmarshals to an equal value, with characters
// JSON escapes, characters outside ASCII, the extremes of an int64 and a nil pointer
func TestAPayloadRoundTrips(t *testing.T) {
	for name, written := range map[string]order{
		"escaped characters": {
			ID:   "o-1",
			Note: `<script>"quoted" & 'single' \ back/slash` + "\n\t",
		},
		"outside ASCII": {
			ID:   "o-2",
			Note: "naïve café, 東京, 🚚",
		},
		"int64 extremes": {
			ID:     "o-3",
			Amount: -9223372036854775808,
			Lines: []line{
				{Sku: "max", Quantity: 2147483647},
			},
		},
		"nested and nil": {
			ID: "o-4",
			Lines: []line{
				{Sku: "a", Quantity: 1},
				{Sku: "b", Quantity: 2},
			},
			Labels: map[string]string{
				"region": "eu",
			},
		},
	} {
		encoded, err := formatjson.Marshal(written)
		if err != nil {
			t.Fatalf("%s: marshal: %v", name, err)
		}
		var read order
		if err = formatjson.Unmarshal(encoded, &read); err != nil {
			t.Fatalf("%s: unmarshal %s: %v", name, encoded, err)
		}
		if !reflect.DeepEqual(read, written) {
			t.Fatalf("%s: read back %+v, want %+v", name, read, written)
		}
	}
}

// TestUnmarshalRefusesWhatIsNotThePayload: malformed bytes, a value of the wrong type and a
// truncated payload are errors, never a partly filled value taken as read
func TestUnmarshalRefusesWhatIsNotThePayload(t *testing.T) {
	for name, encoded := range map[string]string{
		"malformed":  `{"id": "o-1",`,
		"wrong type": `{"id": 7}`,
		"truncated":  `{"id": "o-1", "lines": [{"sku": "a"`,
		"empty":      ``,
	} {
		var read order
		if err := formatjson.Unmarshal([]byte(encoded), &read); err == nil {
			t.Fatalf("%s: unmarshal of %q succeeded, want an error", name, encoded)
		}
	}
}

// TestMarshalRefusesWhatJSONCannotHold: a value JSON has no form for is an error, not an empty
// payload
func TestMarshalRefusesWhatJSONCannotHold(t *testing.T) {
	_, err := formatjson.Marshal(map[string]any{
		"channel": make(chan int),
	})
	if err == nil || !strings.Contains(err.Error(), "chan") {
		t.Fatalf("marshal of a channel: got %v, want an error naming it", err)
	}
}

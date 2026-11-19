// SPDX-FileCopyrightText: Copyright (c) 2026 The journal Authors
// SPDX-License-Identifier: Apache-2.0

package codec

import (
	"errors"
	"testing"
)

// TestAllListsEachOnce: every format exactly once, the empty string excluded.
func TestAllListsEachOnce(t *testing.T) {
	seen := make(map[DataFormat]bool, len(AllFormats))
	for _, format := range AllFormats {
		if seen[format] {
			t.Errorf("AllFormats lists %s twice", format)
		}
		seen[format] = true
	}
	if seen[""] {
		t.Error("AllFormats lists the empty string")
	}
	if len(AllFormats) != 9 {
		t.Errorf("AllFormats has %d entries, want 9", len(AllFormats))
	}
	if DataFormat("").Valid() {
		t.Error(`DataFormat("") reports valid`)
	}
}

func TestDataFormatString(t *testing.T) {
	testCases := []struct {
		format DataFormat
		want   string
	}{
		{JSON, "json"},
		{Avro, "avro"},
		{CBOR, "cbor"},
		{Ion, "ion"},
		{MsgPack, "msgpack"},
		{Proto, "proto"},
		{Text, "text"},
		{XML, "xml"},
		{YAML, "yaml"},
	}

	for _, testCase := range testCases {
		t.Run(testCase.want, func(t *testing.T) {
			if got := testCase.format.String(); got != testCase.want {
				t.Errorf("String() = %q, want %q", got, testCase.want)
			}
		})
	}
}

func TestDataFormatValid(t *testing.T) {
	testCases := []struct {
		name   string
		format DataFormat
		want   bool
	}{
		{"zero value is not a valid format", DataFormat(""), false},
		{"json is valid", JSON, true},
		{"yaml is valid", YAML, true},
		{"an unlisted name is not valid", DataFormat("toml"), false},
		{"a listed name in capitals is not valid", DataFormat("JSON"), false},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			if got := testCase.format.Valid(); got != testCase.want {
				t.Errorf("Valid() = %v, want %v", got, testCase.want)
			}
		})
	}
}

func TestParseFormat(t *testing.T) {
	for _, format := range AllFormats {
		t.Run(format.String(), func(t *testing.T) {
			parsed, err := ParseFormat(format.String())
			if err != nil {
				t.Fatalf("ParseFormat(%q) returned error: %v", format.String(), err)
			}
			if parsed != format {
				t.Errorf("ParseFormat(%q) = %q, want %q", format.String(), parsed, format)
			}
		})
	}
}

func TestParseFormatUnknown(t *testing.T) {
	_, err := ParseFormat("not-a-format")
	if err == nil {
		t.Fatal("ParseFormat with an unknown name returned a nil error")
	}
	if !errors.Is(err, ErrUnknownDataFormat) {
		t.Errorf("errors.Is(err, ErrUnknownDataFormat) = false, want true; got %v", err)
	}
}

func TestParseFormatCaseSensitive(t *testing.T) {
	_, err := ParseFormat("JSON")
	if !errors.Is(err, ErrUnknownDataFormat) {
		t.Errorf("ParseFormat is case-sensitive and must reject \"JSON\"; got %v", err)
	}
}

// SPDX-FileCopyrightText: Copyright (c) 2026 The journal Authors
// SPDX-License-Identifier: Apache-2.0

package compress

import (
	"errors"
	"testing"
)

// TestAllListsEachOnce: every compression exactly once.
func TestAllListsEachOnce(t *testing.T) {
	seen := make(map[DataCompression]bool, len(All))
	for _, compression := range All {
		if seen[compression] {
			t.Errorf("compress.All lists %s twice", compression)
		}
		seen[compression] = true
	}
	if len(All) != 5 {
		t.Errorf("compress.All has %d entries, want 5", len(All))
	}
}

func TestDataCompressionString(t *testing.T) {
	testCases := []struct {
		compression DataCompression
		want        string
	}{
		{None, "none"},
		{Gzip, "gzip"},
		{LZ4, "lz4"},
		{Snappy, "snappy"},
		{Zstd, "zstd"},
	}

	for _, testCase := range testCases {
		t.Run(testCase.want, func(t *testing.T) {
			if got := testCase.compression.String(); got != testCase.want {
				t.Errorf("String() = %q, want %q", got, testCase.want)
			}
		})
	}
}

func TestParse(t *testing.T) {
	for _, compression := range All {
		t.Run(compression.String(), func(t *testing.T) {
			parsed, err := Parse(compression.String())
			if err != nil {
				t.Fatalf("Parse(%q) returned error: %v", compression.String(), err)
			}
			if parsed != compression {
				t.Errorf("Parse(%q) = %q, want %q", compression.String(), parsed, compression)
			}
		})
	}
}

func TestParseUnknown(t *testing.T) {
	_, err := Parse("not-a-compression")
	if err == nil {
		t.Fatal("Parse with an unknown name returned a nil error")
	}
	if !errors.Is(err, ErrUnknownDataCompression) {
		t.Errorf("errors.Is(err, ErrUnknownDataCompression) = false, want true; got %v", err)
	}
}

func TestParseCaseSensitive(t *testing.T) {
	_, err := Parse("GZIP")
	if !errors.Is(err, ErrUnknownDataCompression) {
		t.Errorf("Parse is case-sensitive and must reject \"GZIP\"; got %v", err)
	}
}

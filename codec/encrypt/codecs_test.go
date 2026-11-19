// SPDX-FileCopyrightText: Copyright (c) 2026 The journal Authors
// SPDX-License-Identifier: Apache-2.0

package encrypt

import (
	"errors"
	"testing"
)

// TestAllListsEachOnce: every encryption exactly once.
func TestAllListsEachOnce(t *testing.T) {
	seen := make(map[DataEncryption]bool, len(All))
	for _, encryption := range All {
		if seen[encryption] {
			t.Errorf("encrypt.All lists %s twice", encryption)
		}
		seen[encryption] = true
	}
	if len(All) != 3 {
		t.Errorf("encrypt.All has %d entries, want 3", len(All))
	}
}

func TestDataEncryptionString(t *testing.T) {
	testCases := []struct {
		encryption DataEncryption
		want       string
	}{
		{None, "none"},
		{AESGCM, "aes-gcm"},
		{XChaCha20Poly1305, "xchacha20-poly1305"},
	}

	for _, testCase := range testCases {
		t.Run(testCase.want, func(t *testing.T) {
			if got := testCase.encryption.String(); got != testCase.want {
				t.Errorf("String() = %q, want %q", got, testCase.want)
			}
		})
	}
}

func TestParse(t *testing.T) {
	for _, encryption := range All {
		t.Run(encryption.String(), func(t *testing.T) {
			parsed, err := Parse(encryption.String())
			if err != nil {
				t.Fatalf("Parse(%q) returned error: %v", encryption.String(), err)
			}
			if parsed != encryption {
				t.Errorf("Parse(%q) = %q, want %q", encryption.String(), parsed, encryption)
			}
		})
	}
}

func TestParseUnknown(t *testing.T) {
	_, err := Parse("not-an-encryption")
	if err == nil {
		t.Fatal("Parse with an unknown name returned a nil error")
	}
	if !errors.Is(err, ErrUnknownDataEncryption) {
		t.Errorf("errors.Is(err, ErrUnknownDataEncryption) = false, want true; got %v", err)
	}
}

func TestParseCaseSensitive(t *testing.T) {
	_, err := Parse("AES-GCM")
	if !errors.Is(err, ErrUnknownDataEncryption) {
		t.Errorf("Parse is case-sensitive and must reject \"AES-GCM\"; got %v", err)
	}
}

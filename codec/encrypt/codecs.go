// SPDX-FileCopyrightText: Copyright (c) 2026 The journal Authors
// SPDX-License-Identifier: Apache-2.0

package encrypt

import (
	"errors"
	"fmt"
	"slices"
)

// DataEncryption used on an event value, stored as its name
type DataEncryption string

const (
	None              DataEncryption = "none"
	AESGCM            DataEncryption = "aes-gcm"
	XChaCha20Poly1305 DataEncryption = "xchacha20-poly1305"
)

// ErrUnknownDataEncryption is a name Parse does not know
var ErrUnknownDataEncryption = errors.New("unknown data encryption")

// All in the order of the events_data_encryption enum
var All = []DataEncryption{
	None,
	AESGCM,
	XChaCha20Poly1305,
}

// String is the stored name
func (e DataEncryption) String() string {
	return string(e)
}

// Parse returns the encryption with the stored name, such as "aes-gcm"; the name is
// case-sensitive, and an unknown one is ErrUnknownDataEncryption
func Parse(name string) (DataEncryption, error) {
	encryption := DataEncryption(name)
	if slices.Contains(All, encryption) {
		return encryption, nil
	}
	return "", fmt.Errorf("%w: %q", ErrUnknownDataEncryption, name)
}

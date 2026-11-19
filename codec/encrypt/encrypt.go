// SPDX-FileCopyrightText: Copyright (c) 2026 The journal Authors
// SPDX-License-Identifier: Apache-2.0

// Package encrypt seals and opens event payloads: the Encryptor interface, AES-256-GCM and
// XChaCha20-Poly1305 implementations, the Keyring they draw keys from, and the DataEncryption
// names stored with each event.
package encrypt

import (
	"context"
)

// Encryptor seals payload bytes and opens them again.
type Encryptor interface {
	// Encryption is recorded beside every payload this Encryptor produces.
	Encryption() DataEncryption

	// Seal returns the sealed bytes and the id of the key they were sealed
	// under.
	Seal(ctx context.Context, plain []byte) (sealed []byte, keyId string, err error)

	// Open reverses Seal, using the key id stored beside the sealed bytes.
	Open(ctx context.Context, keyId string, sealed []byte) (plain []byte, err error)
}

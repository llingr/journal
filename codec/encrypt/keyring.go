// SPDX-FileCopyrightText: Copyright (c) 2026 The journal Authors
// SPDX-License-Identifier: Apache-2.0

package encrypt

import "context"

// Keyring supplies encryption keys.
type Keyring interface {
	// Current names the key new payloads are sealed under. Rotation is this
	// returning a different id.
	Current(ctx context.Context) (keyId string, key []byte, err error)

	// Key resolves the id stored beside a sealed payload.
	Key(ctx context.Context, keyId string) ([]byte, error)
}

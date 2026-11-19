// SPDX-FileCopyrightText: Copyright (c) 2026 The journal Authors
// SPDX-License-Identifier: Apache-2.0

package encrypt

import "errors"

// ErrUnknownKeyId is returned by a Keyring asked for a key id it does not
// hold.
var ErrUnknownKeyId = errors.New("unknown key id")

// ErrCiphertextTruncated is returned when sealed bytes are shorter than the
// framing the Encryptor wrote.
var ErrCiphertextTruncated = errors.New("sealed payload is truncated")

// ErrKeyLength is returned when a Keyring supplies a key of the wrong size for
// the cipher.
var ErrKeyLength = errors.New("key is the wrong length for the cipher")

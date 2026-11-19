// SPDX-FileCopyrightText: Copyright (c) 2026 The journal Authors
// SPDX-License-Identifier: Apache-2.0

package codec

import "errors"

// ErrNoCompressor is returned by a CompressorSource that cannot resolve a
// DataCompression. Decoding a payload whose compression cannot be resolved
// fails here rather than returning the compressed bytes.
var ErrNoCompressor = errors.New("no compressor for the data compression")

// ErrNoEncryptor is returned by an EncryptorSource that cannot resolve a
// DataEncryption.
var ErrNoEncryptor = errors.New("no encryptor for the data encryption")

// ErrInvalidCodecValue is returned when a codec value is not one of the listed names.
var ErrInvalidCodecValue = errors.New("invalid codec value")

// ErrMissingKeyId is returned when an Encryptor seals a payload without naming
// the key it used. data_encryption_kid is not empty for every encrypted row.
var ErrMissingKeyId = errors.New("encryptor sealed without naming a key")

// ErrEncryptionKeyIdMismatch is returned when a stored payload pairs the none
// encryption with a key id, or an encryption with no key id. The pairing
// mirrors chk_events_data_encryption_none.
var ErrEncryptionKeyIdMismatch = errors.New("data encryption and key id disagree")

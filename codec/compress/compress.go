// SPDX-FileCopyrightText: Copyright (c) 2026 The journal Authors
// SPDX-License-Identifier: Apache-2.0

// Package compress compresses and decompresses event payloads: the Compressor interface, gzip,
// LZ4, Snappy and zstd implementations, and the DataCompression names stored with each event.
package compress

import "errors"

// ErrEmptyInput from Decompress: no codec here produces an empty blob
var ErrEmptyInput = errors.New("empty compressed input")

// Compressor shrinks/restores payload bytes
type Compressor interface {

	// Compress bytes using the implemented algorithm
	Compress(plain []byte) ([]byte, error)

	// Decompress bytes
	Decompress(compressed []byte) ([]byte, error)

	// Compression algorithm used
	Compression() DataCompression
}

// SPDX-FileCopyrightText: Copyright (c) 2026 The journal Authors
// SPDX-License-Identifier: Apache-2.0

package compress

import (
	"github.com/klauspost/compress/snappy"
)

// snappyCompressor is stateless; block format, readable by any snappy implementation
type snappyCompressor struct{}

// NewSnappy has no options and no pool: snappy encodes in one pass with no reusable state
func NewSnappy() Compressor {
	return &snappyCompressor{}
}

func (s *snappyCompressor) Compression() DataCompression {
	return Snappy
}

// Compress into a fresh slice: a pooled destination would alias the result
func (s *snappyCompressor) Compress(plain []byte) ([]byte, error) {
	return snappy.Encode(nil, plain), nil
}

func (s *snappyCompressor) Decompress(compressed []byte) ([]byte, error) {
	if len(compressed) == 0 {
		return nil, ErrEmptyInput
	}
	return snappy.Decode(nil, compressed)
}

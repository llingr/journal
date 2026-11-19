// SPDX-FileCopyrightText: Copyright (c) 2026 The journal Authors
// SPDX-License-Identifier: Apache-2.0

package compress

import (
	"fmt"

	"github.com/klauspost/compress/zstd"
)

// zstdCompressor shares one encoder and one decoder; the library pools internally
type zstdCompressor struct {
	encoder *zstd.Encoder
	decoder *zstd.Decoder
}

// NewZstd at the supplied zstd.EncoderLevel, normally zstd.SpeedDefault; panics on a level
// zstd does not support, as that is a wiring mistake
func NewZstd(level zstd.EncoderLevel) Compressor {
	compressor := &zstdCompressor{}

	var err error
	compressor.encoder, err = zstd.NewWriter(nil, zstd.WithEncoderLevel(level))
	if err != nil {
		panic(fmt.Errorf("zstd level %d: %w", level, err))
	}
	compressor.decoder, err = zstd.NewReader(nil)
	if err != nil {
		panic(fmt.Errorf("zstd decoder: %w", err))
	}
	return compressor
}

func (z *zstdCompressor) Compression() DataCompression {
	return Zstd
}

// Compress with the one encoder: EncodeAll is safe for concurrent use
func (z *zstdCompressor) Compress(plain []byte) ([]byte, error) {
	return z.encoder.EncodeAll(plain, nil), nil
}

func (z *zstdCompressor) Decompress(compressed []byte) ([]byte, error) {
	if len(compressed) == 0 {
		return nil, ErrEmptyInput
	}
	return z.decoder.DecodeAll(compressed, nil)
}

// SPDX-FileCopyrightText: Copyright (c) 2026 The journal Authors
// SPDX-License-Identifier: Apache-2.0

package compress

import (
	"fmt"
	"runtime"

	"github.com/klauspost/compress/zstd"
)

// zstdCompressor shares one encoder and one decoder. The encoder builds its state for every
// call it may run at once on first use, so it is given no more than the bound or GOMAXPROCS,
// whichever is smaller; the decoder keeps state for at most four.
type zstdCompressor struct {
	limit   limiter
	encoder *zstd.Encoder
	decoder *zstd.Decoder
}

// NewZstd at the supplied zstd.EncoderLevel, normally zstd.SpeedDefault, running at most
// concurrency calls at once; panics on a level zstd does not support, and with ErrConcurrency
// outside MinimumConcurrency to MaximumConcurrency, as either is a wiring mistake
func NewZstd(level zstd.EncoderLevel, concurrency int) Compressor {
	compressor := &zstdCompressor{
		limit: newLimiter(concurrency),
	}

	var err error
	compressor.encoder, err = zstd.NewWriter(nil, zstd.WithEncoderLevel(level),
		zstd.WithEncoderConcurrency(min(concurrency, runtime.GOMAXPROCS(0))))
	if err != nil {
		panic(fmt.Errorf("zstd level %d: %w", level, err))
	}
	compressor.decoder, err = zstd.NewReader(nil)
	if err != nil {
		panic(fmt.Errorf("zstd decoder: %w", err))
	}
	return compressor
}

func (zs *zstdCompressor) Compression() DataCompression {
	return Zstd
}

// Compress with the one encoder: EncodeAll is safe for concurrent use
func (zs *zstdCompressor) Compress(plain []byte) ([]byte, error) {
	zs.limit.acquire()
	defer zs.limit.release()
	return zs.encoder.EncodeAll(plain, nil), nil
}

func (zs *zstdCompressor) Decompress(compressed []byte) ([]byte, error) {
	if len(compressed) == 0 {
		return nil, ErrEmptyInput
	}
	zs.limit.acquire()
	defer zs.limit.release()
	return zs.decoder.DecodeAll(compressed, nil)
}

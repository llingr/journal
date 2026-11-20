// SPDX-FileCopyrightText: Copyright (c) 2026 The journal Authors
// SPDX-License-Identifier: Apache-2.0

package compress

import (
	"bytes"
	"fmt"
	"io"
	"sync"

	"github.com/pierrec/lz4/v4"
)

// lz4Compressor pools the single-use writers and readers, at most one per slot in use; frame
// format, readable by any lz4 tool
type lz4Compressor struct {
	limit   limiter
	options []lz4.Option // applied to each writer on Compress
	writers sync.Pool    // *lz4.Writer
	readers sync.Pool    // *lz4.Reader
}

// NewLZ4 with the supplied lz4.Options, running at most concurrency calls at once. Blocks are
// 64 KB unless an option says otherwise: each writer and reader holds buffers of a block, and
// lz4's default of 4 MB costs 8 MB a call for a payload of any size. Panics on an option lz4
// rejects, and with ErrConcurrency outside MinimumConcurrency to MaximumConcurrency, as either
// is a wiring mistake.
func NewLZ4(concurrency int, options ...lz4.Option) Compressor {
	options = append([]lz4.Option{lz4.BlockSizeOption(lz4.Block64Kb)}, options...)
	err := lz4.NewWriter(io.Discard).Apply(options...)
	if err != nil {
		panic(fmt.Errorf("lz4 options: %w", err))
	}
	compressor := &lz4Compressor{
		limit:   newLimiter(concurrency),
		options: options,
	}

	// A zero lz4.Writer panics on Reset
	compressor.writers.New = func() any {
		return lz4.NewWriter(io.Discard)
	}

	compressor.readers.New = func() any {
		return lz4.NewReader(nil)
	}

	return compressor
}

func (lz *lz4Compressor) Compression() DataCompression {
	return LZ4
}

func (lz *lz4Compressor) Compress(plain []byte) ([]byte, error) {
	lz.limit.acquire()
	defer lz.limit.release()
	var compressed bytes.Buffer

	writer := lz.writers.Get().(*lz4.Writer)
	writer.Reset(&compressed)

	err := writer.Apply(lz.options...)
	if err != nil {
		return nil, err
	}
	if _, err = writer.Write(plain); err != nil {
		return nil, err
	}
	if err = writer.Close(); err != nil {
		return nil, err
	}

	// Pooled only after a clean Close
	lz.writers.Put(writer)
	return compressed.Bytes(), nil
}

func (lz *lz4Compressor) Decompress(compressed []byte) ([]byte, error) {
	if len(compressed) == 0 {
		return nil, ErrEmptyInput
	}
	lz.limit.acquire()
	defer lz.limit.release()
	reader := lz.readers.Get().(*lz4.Reader)
	reader.Reset(bytes.NewReader(compressed))

	plain, err := io.ReadAll(reader)
	if err != nil {
		return nil, err
	}

	lz.readers.Put(reader)
	return plain, nil
}

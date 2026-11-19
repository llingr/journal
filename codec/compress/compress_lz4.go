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

// lz4Compressor pools the single-use writers and readers; frame format, readable by any lz4 tool
type lz4Compressor struct {
	options []lz4.Option // applied to each writer on Compress
	writers sync.Pool    // *lz4.Writer
	readers sync.Pool    // *lz4.Reader
}

// NewLZ4 with the supplied lz4.Options; panics on one lz4 rejects, as that is a wiring
// mistake
func NewLZ4(options ...lz4.Option) Compressor {
	err := lz4.NewWriter(io.Discard).Apply(options...)
	if err != nil {
		panic(fmt.Errorf("lz4 options: %w", err))
	}
	compressor := &lz4Compressor{
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
	var compressed bytes.Buffer

	writer := lz.writers.Get().(*lz4.Writer)
	writer.Reset(&compressed)

	var err error
	if len(lz.options) > 0 {
		if err = writer.Apply(lz.options...); err != nil {
			return nil, err
		}
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
	reader := lz.readers.Get().(*lz4.Reader)
	reader.Reset(bytes.NewReader(compressed))

	plain, err := io.ReadAll(reader)
	if err != nil {
		return nil, err
	}

	lz.readers.Put(reader)
	return plain, nil
}

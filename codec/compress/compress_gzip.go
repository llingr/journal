// SPDX-FileCopyrightText: Copyright (c) 2026 The journal Authors
// SPDX-License-Identifier: Apache-2.0

package compress

import (
	"bytes"
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"sync"
)

// gzipCompressor pools the single-use gzip writers and readers
type gzipCompressor struct {
	writers sync.Pool // *gzip.Writer at the constructed level
	readers sync.Pool // *gzip.Reader
}

var ErrGzipLevel = errors.New("unsupported gzip level")

// NewGzip with the supplied compress/gzip level; panics with ErrGzipLevel on a level gzip
// does not support, as that is a wiring mistake
func NewGzip(level int) Compressor {
	isInRange := level >= gzip.DefaultCompression && level <= gzip.BestCompression
	if level != gzip.HuffmanOnly && !isInRange {
		panic(fmt.Errorf("%w: %d", ErrGzipLevel, level))
	}
	compressor := &gzipCompressor{}

	// A zero gzip.Writer resets to NoCompression, so the level is set at construction
	compressor.writers.New = func() any {
		writer, err := gzip.NewWriterLevel(io.Discard, level)
		if err != nil {
			return nil
		}
		return writer
	}

	compressor.readers.New = func() any {
		return new(gzip.Reader)
	}

	return compressor
}

func (gz *gzipCompressor) Compression() DataCompression {
	return Gzip
}

func (gz *gzipCompressor) Compress(plain []byte) ([]byte, error) {
	var compressed bytes.Buffer

	writer := gz.writers.Get().(*gzip.Writer)
	writer.Reset(&compressed)

	var err error
	if _, err = writer.Write(plain); err != nil {
		return nil, err
	}
	if err = writer.Close(); err != nil {
		return nil, err
	}

	// Pooled only after a clean Close
	gz.writers.Put(writer)
	return compressed.Bytes(), nil
}

func (gz *gzipCompressor) Decompress(compressed []byte) ([]byte, error) {
	if len(compressed) == 0 {
		return nil, ErrEmptyInput
	}
	reader := gz.readers.Get().(*gzip.Reader)
	err := reader.Reset(bytes.NewReader(compressed))
	if err != nil {
		return nil, err
	}

	var plain []byte
	if plain, err = io.ReadAll(reader); err != nil {
		return nil, err
	}
	if err = reader.Close(); err != nil {
		return nil, err
	}

	gz.readers.Put(reader)
	return plain, nil
}

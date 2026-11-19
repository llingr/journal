// SPDX-FileCopyrightText: Copyright (c) 2026 The journal Authors
// SPDX-License-Identifier: Apache-2.0

// Package codec turns a marshalled payload into the bytes stored for an event and back: the
// Pipeline that compresses and encrypts, and the DataFormat names stored with each event. The
// compress and encrypt packages hold the algorithms.
package codec

import (
	"context"
	"fmt"

	"github.com/llingr/journal/codec/compress"
	"github.com/llingr/journal/codec/encrypt"
)

// Encoded is a payload as stored: the bytes, and the four columns describing
// how they were produced.
type Encoded struct {
	Data            []byte
	Format          DataFormat
	Compression     compress.DataCompression
	Encryption      encrypt.DataEncryption
	EncryptionKeyId string
}

// Pipeline compresses and encrypts already-marshalled payload bytes, and
// reverses both on the way back.
//
// It never marshals. A payload marshals itself, so the framework holds no
// table of formats and DataFormat is a label written beside the bytes rather
// than a dispatch key. What it composes is injected.
type Pipeline struct {
	compressWriter compress.Compressor
	encryptWriter  encrypt.Encryptor
	compressors    CompressorSource
	encryptors     EncryptorSource
	selector       Selector
}

// Selector decides, per payload, which of the configured stages apply. A stage
// that is not configured is skipped whatever the Selector returns.
type Selector func(eventType string, plainSize int) (compress, encrypt bool)

// Option supplies one collaborator to a Pipeline under construction.
type Option func(pipeline *Pipeline) error

// NewPipeline builds a Pipeline. With no options every payload is stored
// exactly as it arrives.
func NewPipeline(options ...Option) (*Pipeline, error) {
	pipeline := &Pipeline{
		compressors: compressorMapOf(nil),
		encryptors:  encryptorMapOf(nil),
	}

	for _, option := range options {
		err := option(pipeline)
		if err != nil {
			return nil, err
		}
	}
	return pipeline, nil
}

// CompressWith sets the Compressor new events are written with. It is
// understood on read as well, so it never has to appear in DecompressWith.
func CompressWith(compressor compress.Compressor) Option {
	return func(pipeline *Pipeline) error {
		pipeline.compressWriter = compressor
		return nil
	}
}

// DecompressWith names every Compressor this binary understands on read,
// including algorithms it no longer writes.
func DecompressWith(compressors ...compress.Compressor) Option {
	return func(pipeline *Pipeline) error {
		pipeline.compressors = compressorMapOf(compressors)
		return nil
	}
}

// EncryptWith sets the Encryptor new events are sealed with. It is understood
// on read as well.
func EncryptWith(encryptor encrypt.Encryptor) Option {
	return func(pipeline *Pipeline) error {
		pipeline.encryptWriter = encryptor
		return nil
	}
}

// DecryptWith names every Encryptor this binary understands on read, including
// ciphers it no longer writes.
func DecryptWith(encryptors ...encrypt.Encryptor) Option {
	return func(pipeline *Pipeline) error {
		pipeline.encryptors = encryptorMapOf(encryptors)
		return nil
	}
}

// WithCompressorSource resolves stored compressions with the caller's own
// source, for a deployment that does not want a fixed set.
func WithCompressorSource(compressors CompressorSource) Option {
	return func(pipeline *Pipeline) error {
		pipeline.compressors = compressors
		return nil
	}
}

// WithEncryptorSource resolves stored encryptions with the caller's own
// source.
func WithEncryptorSource(encryptors EncryptorSource) Option {
	return func(pipeline *Pipeline) error {
		pipeline.encryptors = encryptors
		return nil
	}
}

// WithSelector narrows which payloads the configured stages apply to. Without
// one, every payload is compressed if CompressWith was given and sealed if
// EncryptWith was given.
func WithSelector(selector Selector) Option {
	return func(pipeline *Pipeline) error {
		pipeline.selector = selector
		return nil
	}
}

// Encode applies the configured compression and encryption to bytes a payload
// has already produced.
//
// The returned Encoded describes the bytes it carries: compression that did
// not shrink the payload is discarded and recorded as none.
func (p *Pipeline) Encode(ctx context.Context, eventType string, format DataFormat,
	plain []byte) (Encoded, error) {
	if !format.Valid() {
		return Encoded{}, fmt.Errorf("%w: data format %s", ErrInvalidCodecValue, format)
	}

	stored := Encoded{
		Data:        plain,
		Format:      format,
		Compression: compress.None,
		Encryption:  encrypt.None,
	}

	doCompress := p.compressWriter != nil
	doEncrypt := p.encryptWriter != nil
	if p.selector != nil {
		wantCompress, wantEncrypt := p.selector(eventType, len(plain))
		doCompress = doCompress && wantCompress
		doEncrypt = doEncrypt && wantEncrypt
	}

	var err error
	if doCompress {
		compression := p.compressWriter.Compression()
		var compressed []byte
		compressed, err = p.compressWriter.Compress(plain)
		if err != nil {
			return Encoded{}, fmt.Errorf("compress %s: %w", compression, err)
		}

		if len(compressed) < len(plain) {
			stored.Data = compressed
			stored.Compression = compression
		}
	}

	if doEncrypt {
		encryption := p.encryptWriter.Encryption()
		var sealed []byte
		var keyId string
		sealed, keyId, err = p.encryptWriter.Seal(ctx, stored.Data)
		if err != nil {
			return Encoded{}, fmt.Errorf("seal %s: %w", encryption, err)
		}
		if keyId == "" {
			return Encoded{}, fmt.Errorf("%w: %s", ErrMissingKeyId, encryption)
		}

		stored.Data = sealed
		stored.Encryption = encryption
		stored.EncryptionKeyId = keyId
	}

	return stored, nil
}

// Decode reverses Encode, returning the bytes the payload marshalled.
//
// Each stage is chosen by what the stored payload says was applied to it. The
// Selector is not consulted, so changing it never invalidates anything already
// written.
func (p *Pipeline) Decode(ctx context.Context, stored Encoded) ([]byte, error) {
	if (stored.Encryption == encrypt.None) != (stored.EncryptionKeyId == "") {
		return nil, fmt.Errorf("%w: %s with key id %q",
			ErrEncryptionKeyIdMismatch, stored.Encryption, stored.EncryptionKeyId)
	}

	payload := stored.Data
	var err error

	if stored.Encryption != encrypt.None {
		var encryptor encrypt.Encryptor
		encryptor, err = p.encryptorFor(stored.Encryption)
		if err != nil {
			return nil, err
		}
		payload, err = encryptor.Open(ctx, stored.EncryptionKeyId, payload)
		if err != nil {
			return nil, fmt.Errorf("open %s: %w", stored.Encryption, err)
		}
	}

	if stored.Compression != compress.None {
		var compressor compress.Compressor
		compressor, err = p.compressorFor(stored.Compression)
		if err != nil {
			return nil, err
		}
		payload, err = compressor.Decompress(payload)
		if err != nil {
			return nil, fmt.Errorf("decompress %s: %w", stored.Compression, err)
		}
	}

	return payload, nil
}

// compressorFor is the written Compressor when it produced the compression, so the pipeline
// always reads what it writes, and otherwise the one the read source resolves
func (p *Pipeline) compressorFor(compression compress.DataCompression) (compress.Compressor,
	error) {
	if p.compressWriter != nil && p.compressWriter.Compression() == compression {
		return p.compressWriter, nil
	}
	return p.compressors.Compressor(compression)
}

// encryptorFor is the written Encryptor when it sealed the encryption, and otherwise the one
// the read source resolves
func (p *Pipeline) encryptorFor(encryption encrypt.DataEncryption) (encrypt.Encryptor, error) {
	if p.encryptWriter != nil && p.encryptWriter.Encryption() == encryption {
		return p.encryptWriter, nil
	}
	return p.encryptors.Encryptor(encryption)
}

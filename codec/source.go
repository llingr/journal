// SPDX-FileCopyrightText: Copyright (c) 2026 The journal Authors
// SPDX-License-Identifier: Apache-2.0

package codec

import (
	"fmt"

	"github.com/llingr/journal/codec/compress"
	"github.com/llingr/journal/codec/encrypt"
)

// CompressorSource resolves the Compressor that produced a stored payload.
//
// The Pipeline holds no set of known compressions of its own. DecompressWith
// backs this with a map over the implementations handed to it; a deployment
// wanting a switch, a plugin loader or a call to something else implements
// this and passes WithCompressorSource instead.
type CompressorSource interface {
	Compressor(compression compress.DataCompression) (compress.Compressor, error)
}

// EncryptorSource resolves the Encryptor that sealed a stored payload.
type EncryptorSource interface {
	Encryptor(encryption encrypt.DataEncryption) (encrypt.Encryptor, error)
}

type compressorMap struct {
	byCompression map[compress.DataCompression]compress.Compressor
}

func compressorMapOf(compressors []compress.Compressor) CompressorSource {
	byCompression := make(map[compress.DataCompression]compress.Compressor, len(compressors))
	for _, compressor := range compressors {
		byCompression[compressor.Compression()] = compressor
	}
	return &compressorMap{
		byCompression: byCompression,
	}
}

func (cm *compressorMap) Compressor(compression compress.DataCompression) (compress.Compressor,
	error) {
	compressor, found := cm.byCompression[compression]
	if !found {
		return nil, fmt.Errorf("%w: %s", ErrNoCompressor, compression)
	}
	return compressor, nil
}

type encryptorMap struct {
	byEncryption map[encrypt.DataEncryption]encrypt.Encryptor
}

func encryptorMapOf(encryptors []encrypt.Encryptor) EncryptorSource {
	byEncryption := make(map[encrypt.DataEncryption]encrypt.Encryptor, len(encryptors))
	for _, encryptor := range encryptors {
		byEncryption[encryptor.Encryption()] = encryptor
	}
	return &encryptorMap{
		byEncryption: byEncryption,
	}
}

func (em *encryptorMap) Encryptor(encryption encrypt.DataEncryption) (encrypt.Encryptor,
	error) {
	encryptor, found := em.byEncryption[encryption]
	if !found {
		return nil, fmt.Errorf("%w: %s", ErrNoEncryptor, encryption)
	}
	return encryptor, nil
}

// SPDX-FileCopyrightText: Copyright (c) 2026 The journal Authors
// SPDX-License-Identifier: Apache-2.0

package codec_test

import (
	"compress/gzip"
	"context"
	"errors"
	"testing"

	"github.com/llingr/journal/codec"
	"github.com/llingr/journal/codec/compress"
	"github.com/llingr/journal/codec/encrypt"
)

// TestDecryptWithReadsACipherNoLongerWritten: switching the written cipher keeps the old rows
// readable when the old one is named for reads, and unreadable when it is not
func TestDecryptWithReadsACipherNoLongerWritten(t *testing.T) {
	keyring := newKeyring()
	before := newPipeline(t, codec.EncryptWith(encrypt.NewAESGCM(keyring)))
	written := encode(t, before, "Happened", compressible)

	after := newPipeline(t,
		codec.EncryptWith(encrypt.NewXChaCha20(keyring)),
		codec.DecryptWith(encrypt.NewAESGCM(keyring)))
	requireDecodes(t, after, written, compressible)
	if stored := encode(t, after, "Happened", compressible); stored.Encryption !=
		encrypt.XChaCha20Poly1305 {
		t.Fatalf("after the switch new rows are %s, want xchacha20-poly1305", stored.Encryption)
	}

	forgetful := newPipeline(t, codec.EncryptWith(encrypt.NewXChaCha20(keyring)))
	_, err := forgetful.Decode(context.Background(), written)
	if !errors.Is(err, codec.ErrNoEncryptor) {
		t.Fatalf("decode of aes-gcm by a pipeline that never names it: got %v, want "+
			"ErrNoEncryptor", err)
	}
}

// TestAnAlteredPayloadNeverDecodes: a sealed payload with one byte changed, a compressed one
// with its frame damaged, and one sealed under a key the keyring does not hold all fail,
// rather than decode to something other than what was written
func TestAnAlteredPayloadNeverDecodes(t *testing.T) {
	ctx := context.Background()
	keyring := newKeyring()
	sealing := newPipeline(t,
		codec.CompressWith(compress.NewLZ4(compress.DefaultConcurrency)),
		codec.EncryptWith(encrypt.NewAESGCM(keyring)))
	compressing := newPipeline(t, codec.CompressWith(compress.NewLZ4(compress.DefaultConcurrency)))

	sealed := encode(t, sealing, "Happened", compressible)
	flipped := sealed
	flipped.Data = append([]byte(nil), sealed.Data...)
	flipped.Data[len(flipped.Data)/2] ^= 0x01
	compressed := encode(t, compressing, "Happened", compressible)
	damaged := compressed
	damaged.Data = append([]byte(nil), compressed.Data[:len(compressed.Data)/2]...)
	unknownKey := sealed
	unknownKey.EncryptionKeyId = "key-2"

	for name, testCase := range map[string]struct {
		pipeline *codec.Pipeline
		stored   codec.Encoded
		want     error
	}{
		"a sealed byte flipped":  {sealing, flipped, nil},
		"a compressed frame cut": {compressing, damaged, nil},
		"an unknown key":         {sealing, unknownKey, encrypt.ErrUnknownKeyId},
	} {
		plain, err := testCase.pipeline.Decode(ctx, testCase.stored)
		if err == nil {
			t.Fatalf("%s: decoded %d bytes, want an error", name, len(plain))
		}
		if testCase.want != nil && !errors.Is(err, testCase.want) {
			t.Fatalf("%s: got %v, want %v", name, err, testCase.want)
		}
	}
}

// TestAFailedSealFailsTheEncode: a keyring that cannot name its current key fails the encode,
// so nothing reaches a row unsealed when sealing was asked for
func TestAFailedSealFailsTheEncode(t *testing.T) {
	errVault := errors.New("vault unreachable")
	pipeline := newPipeline(t, codec.EncryptWith(encrypt.NewAESGCM(failingKeyring{
		err: errVault,
	})))

	stored, err := pipeline.Encode(context.Background(), "Happened", codec.JSON, compressible)
	if !errors.Is(err, errVault) {
		t.Fatalf("encode with the keyring down: %+v, %v; want the keyring's error", stored, err)
	}
}

// TestSourcesResolveWhatWasStored: a caller's own sources decode what they hold and report
// what they do not, so a deployment can resolve algorithms its own way
func TestSourcesResolveWhatWasStored(t *testing.T) {
	keyring := newKeyring()
	written := encode(t, newPipeline(t,
		codec.CompressWith(compress.NewLZ4(compress.DefaultConcurrency)),
		codec.EncryptWith(encrypt.NewXChaCha20(keyring))), "Happened", compressible)

	resolving := newPipeline(t,
		codec.WithCompressorSource(compressorSource{
			compress.LZ4: compress.NewLZ4(compress.DefaultConcurrency),
		}),
		codec.WithEncryptorSource(encryptorSource{
			encrypt.XChaCha20Poly1305: encrypt.NewXChaCha20(keyring),
		}))
	requireDecodes(t, resolving, written, compressible)

	empty := newPipeline(t,
		codec.WithCompressorSource(compressorSource{}),
		codec.WithEncryptorSource(encryptorSource{
			encrypt.XChaCha20Poly1305: encrypt.NewXChaCha20(keyring),
		}))
	_, err := empty.Decode(context.Background(), written)
	if !errors.Is(err, codec.ErrNoCompressor) {
		t.Fatalf("decode through a source without lz4: got %v, want ErrNoCompressor", err)
	}
}

// TestAWrittenAlgorithmKeepsTheCallersSource: naming the algorithm to write never drops the
// source the caller gave for reads, whichever option comes first
func TestAWrittenAlgorithmKeepsTheCallersSource(t *testing.T) {
	keyring := newKeyring()
	written := encode(t, newPipeline(t,
		codec.CompressWith(compress.NewLZ4(compress.DefaultConcurrency)),
		codec.EncryptWith(encrypt.NewAESGCM(keyring))), "Happened", compressible)

	for name, options := range map[string][]codec.Option{
		"sources first": {
			codec.WithCompressorSource(compressorSource{
				compress.LZ4: compress.NewLZ4(compress.DefaultConcurrency),
			}),
			codec.WithEncryptorSource(encryptorSource{
				encrypt.AESGCM: encrypt.NewAESGCM(keyring),
			}),
			codec.CompressWith(compress.NewGzip(gzip.BestSpeed, compress.DefaultConcurrency)),
			codec.EncryptWith(encrypt.NewXChaCha20(keyring)),
		},
		"writers first": {
			codec.CompressWith(compress.NewGzip(gzip.BestSpeed, compress.DefaultConcurrency)),
			codec.EncryptWith(encrypt.NewXChaCha20(keyring)),
			codec.WithCompressorSource(compressorSource{
				compress.LZ4: compress.NewLZ4(compress.DefaultConcurrency),
			}),
			codec.WithEncryptorSource(encryptorSource{
				encrypt.AESGCM: encrypt.NewAESGCM(keyring),
			}),
		},
	} {
		pipeline := newPipeline(t, options...)
		plain, err := pipeline.Decode(context.Background(), written)
		if err != nil || string(plain) != string(compressible) {
			t.Fatalf("%s: decode of an lz4, aes-gcm row the sources hold: %d bytes, %v",
				name, len(plain), err)
		}
		requireDecodes(t, pipeline, encode(t, pipeline, "Happened", compressible), compressible)
	}
}

// failingKeyring cannot reach its keys
type failingKeyring struct {
	err error
}

func (k failingKeyring) Current(context.Context) (string, []byte, error) {
	return "", nil, k.err
}

func (k failingKeyring) Key(context.Context, string) ([]byte, error) {
	return nil, k.err
}

// compressorSource resolves from its own map
type compressorSource map[compress.DataCompression]compress.Compressor

func (s compressorSource) Compressor(compression compress.DataCompression) (
	compress.Compressor, error) {
	compressor, found := s[compression]
	if !found {
		return nil, codec.ErrNoCompressor
	}
	return compressor, nil
}

// encryptorSource resolves from its own map
type encryptorSource map[encrypt.DataEncryption]encrypt.Encryptor

func (s encryptorSource) Encryptor(encryption encrypt.DataEncryption) (encrypt.Encryptor,
	error) {
	encryptor, found := s[encryption]
	if !found {
		return nil, codec.ErrNoEncryptor
	}
	return encryptor, nil
}

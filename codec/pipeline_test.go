// SPDX-FileCopyrightText: Copyright (c) 2026 The journal Authors
// SPDX-License-Identifier: Apache-2.0

package codec_test

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/rand"
	"errors"
	"testing"

	"github.com/llingr/journal/codec"
	"github.com/llingr/journal/codec/compress"
	"github.com/llingr/journal/codec/encrypt"
)

// compressible is a payload gzip shrinks
var compressible = bytes.Repeat([]byte(`{"field":"a value that repeats"}`), 64)

// TestWithNoOptionsPayloadsAreStoredAsTheyArrive: the default pipeline changes nothing.
func TestWithNoOptionsPayloadsAreStoredAsTheyArrive(t *testing.T) {
	pipeline := newPipeline(t)

	stored := encode(t, pipeline, "Happened", compressible)
	if !bytes.Equal(stored.Data, compressible) || stored.Compression != compress.None ||
		stored.Encryption != encrypt.None || stored.EncryptionKeyId != "" {
		t.Fatalf("stored %+v, want the payload as it arrived", stored)
	}
	requireDecodes(t, pipeline, stored, compressible)
}

// TestCompressionThatDoesNotShrinkIsDiscarded: random bytes grow under gzip, so they are
// stored as they came and recorded as uncompressed, which is what the columns must say.
func TestCompressionThatDoesNotShrinkIsDiscarded(t *testing.T) {
	pipeline := newPipeline(t,
		codec.CompressWith(compress.NewGzip(gzip.BestSpeed, compress.DefaultConcurrency)))
	random := make([]byte, 512)
	_, _ = rand.Read(random)

	stored := encode(t, pipeline, "Happened", random)
	if stored.Compression != compress.None || !bytes.Equal(stored.Data, random) {
		t.Fatalf("incompressible bytes stored as %s, %d bytes, want none and the bytes as sent",
			stored.Compression, len(stored.Data))
	}
	requireDecodes(t, pipeline, stored, random)

	stored = encode(t, pipeline, "Happened", compressible)
	if stored.Compression != compress.Gzip || len(stored.Data) >= len(compressible) {
		t.Fatalf("compressible bytes stored as %s, %d bytes, want gzip and fewer than %d",
			stored.Compression, len(stored.Data), len(compressible))
	}
	requireDecodes(t, pipeline, stored, compressible)
}

// TestASelectorThatRefusesSkipsTheStage: the selector narrows the stages per event type, and a
// read never consults it, so changing it leaves what was written readable.
func TestASelectorThatRefusesSkipsTheStage(t *testing.T) {
	keyring := newKeyring()
	selector := func(eventType string, plainSize int) (bool, bool) {
		return eventType != "Small", eventType == "Secret"
	}
	pipeline := newPipeline(t,
		codec.CompressWith(compress.NewGzip(gzip.BestSpeed, compress.DefaultConcurrency)),
		codec.EncryptWith(encrypt.NewAESGCM(keyring)),
		codec.WithSelector(selector))

	small := encode(t, pipeline, "Small", compressible)
	if small.Compression != compress.None || small.Encryption != encrypt.None {
		t.Fatalf("a refused event stored as %s and %s, want neither stage",
			small.Compression, small.Encryption)
	}
	secret := encode(t, pipeline, "Secret", compressible)
	if secret.Compression != compress.Gzip ||
		secret.Encryption != encrypt.AESGCM ||
		secret.EncryptionKeyId != "key-1" {
		t.Fatalf("a secret event stored as %+v, want gzip then sealed under key-1", secret)
	}

	unselective := newPipeline(t,
		codec.CompressWith(compress.NewGzip(gzip.BestSpeed, compress.DefaultConcurrency)),
		codec.EncryptWith(encrypt.NewAESGCM(keyring)))
	requireDecodes(t, unselective, small, compressible)
	requireDecodes(t, unselective, secret, compressible)
}

// TestDecodeRefusesColumnsThatDisagree: the stored columns choose each stage, so a row whose
// columns cannot have been written by Encode is an error, never bytes passed through.
func TestDecodeRefusesColumnsThatDisagree(t *testing.T) {
	ctx := context.Background()
	pipeline := newPipeline(t)

	cases := []struct {
		name   string
		stored codec.Encoded
		want   error
	}{
		{
			name: "no encryption with a key id",
			stored: codec.Encoded{
				Data:            compressible,
				Format:          codec.JSON,
				Compression:     compress.None,
				Encryption:      encrypt.None,
				EncryptionKeyId: "key-1",
			},
			want: codec.ErrEncryptionKeyIdMismatch,
		},
		{
			name: "encryption with no key id",
			stored: codec.Encoded{
				Data:       compressible,
				Format:     codec.JSON,
				Encryption: encrypt.AESGCM,
			},
			want: codec.ErrEncryptionKeyIdMismatch,
		},
		{
			name: "a compression this binary does not read",
			stored: codec.Encoded{
				Data:        compressible,
				Format:      codec.JSON,
				Compression: compress.Zstd,
				Encryption:  encrypt.None,
			},
			want: codec.ErrNoCompressor,
		},
		{
			name: "an encryption this binary does not read",
			stored: codec.Encoded{
				Data:            compressible,
				Format:          codec.JSON,
				Encryption:      encrypt.AESGCM,
				EncryptionKeyId: "key-1",
			},
			want: codec.ErrNoEncryptor,
		},
	}
	for _, decodeCase := range cases {
		t.Run(decodeCase.name, func(t *testing.T) {
			_, err := pipeline.Decode(ctx, decodeCase.stored)
			if !errors.Is(err, decodeCase.want) {
				t.Fatalf("decode: got %v, want %v", err, decodeCase.want)
			}
		})
	}
}

// TestEncodeRefusesWhatCannotBeStored: an invalid format and a seal with no key id are refused
// before they reach a row the schema would reject.
func TestEncodeRefusesWhatCannotBeStored(t *testing.T) {
	ctx := context.Background()

	_, err := newPipeline(t).Encode(ctx, "Happened", codec.DataFormat("toml"), compressible)
	if !errors.Is(err, codec.ErrInvalidCodecValue) {
		t.Fatalf("encode with format toml: got %v, want ErrInvalidCodecValue", err)
	}

	anonymous := newPipeline(t, codec.EncryptWith(anonymousEncryptor{}))
	_, err = anonymous.Encode(ctx, "Happened", codec.JSON, compressible)
	if !errors.Is(err, codec.ErrMissingKeyId) {
		t.Fatalf("encode through a seal naming no key: got %v, want ErrMissingKeyId", err)
	}
}

// TestDecompressWithReadsAnAlgorithmNoLongerWritten: switching the written algorithm keeps the
// old rows readable when the old one is named for reads.
func TestDecompressWithReadsAnAlgorithmNoLongerWritten(t *testing.T) {
	before := newPipeline(t, codec.CompressWith(compress.NewLZ4(compress.DefaultConcurrency)))
	written := encode(t, before, "Happened", compressible)

	after := newPipeline(t,
		codec.CompressWith(compress.NewGzip(gzip.BestSpeed, compress.DefaultConcurrency)),
		codec.DecompressWith(compress.NewLZ4(compress.DefaultConcurrency)))
	requireDecodes(t, after, written, compressible)
	if stored := encode(t, after, "Happened", compressible); stored.Compression != compress.Gzip {
		t.Fatalf("after the switch new rows are %s, want gzip", stored.Compression)
	}
}

// anonymousEncryptor seals without naming the key it used
type anonymousEncryptor struct{}

func (anonymousEncryptor) Encryption() encrypt.DataEncryption {
	return encrypt.AESGCM
}

func (anonymousEncryptor) Seal(_ context.Context, plain []byte) ([]byte, string, error) {
	return plain, "", nil
}

func (anonymousEncryptor) Open(_ context.Context, _ string, sealed []byte) ([]byte, error) {
	return sealed, nil
}

// singleKeyring holds key-1 alone
type singleKeyring struct {
	key []byte
}

func newKeyring() *singleKeyring {
	keyring := &singleKeyring{
		key: make([]byte, 32),
	}
	_, _ = rand.Read(keyring.key)
	return keyring
}

func (k *singleKeyring) Current(context.Context) (string, []byte, error) {
	return "key-1", k.key, nil
}

func (k *singleKeyring) Key(_ context.Context, keyId string) ([]byte, error) {
	if keyId != "key-1" {
		return nil, encrypt.ErrUnknownKeyId
	}
	return k.key, nil
}

func newPipeline(t *testing.T, options ...codec.Option) *codec.Pipeline {
	t.Helper()
	pipeline, err := codec.NewPipeline(options...)
	if err != nil {
		t.Fatalf("build the pipeline: %v", err)
	}
	return pipeline
}

func encode(t *testing.T, pipeline *codec.Pipeline, eventType string, plain []byte) codec.Encoded {
	t.Helper()
	stored, err := pipeline.Encode(context.Background(), eventType, codec.JSON, plain)
	if err != nil {
		t.Fatalf("encode %s: %v", eventType, err)
	}
	return stored
}

func requireDecodes(t *testing.T, pipeline *codec.Pipeline, stored codec.Encoded, want []byte) {
	t.Helper()
	plain, err := pipeline.Decode(context.Background(), stored)
	if err != nil || !bytes.Equal(plain, want) {
		t.Fatalf("decode %s and %s: %d bytes, %v; want the %d bytes encoded",
			stored.Compression, stored.Encryption, len(plain), err, len(want))
	}
}

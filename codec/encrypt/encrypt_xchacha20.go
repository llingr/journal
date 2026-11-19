// SPDX-FileCopyrightText: Copyright (c) 2026 The journal Authors
// SPDX-License-Identifier: Apache-2.0

package encrypt

import (
	"context"
	"crypto/cipher"
	"crypto/rand"
	"fmt"
	"sync"

	"golang.org/x/crypto/chacha20poly1305"
)

type xChaCha20Encryptor struct {
	keyring Keyring
	aeads   sync.Map
}

// NewXChaCha20 returns an XChaCha20-Poly1305 Encryptor drawing its keys from the given
// Keyring. Its 24-byte nonce is random per payload, with no practical collision risk however
// many events a stream holds, and prefixed to the ciphertext.
func NewXChaCha20(keyring Keyring) Encryptor {
	return &xChaCha20Encryptor{
		keyring: keyring,
	}
}

func (xc *xChaCha20Encryptor) Encryption() DataEncryption {
	return XChaCha20Poly1305
}

func (xc *xChaCha20Encryptor) Seal(ctx context.Context, plain []byte) ([]byte, string, error) {
	keyId, key, err := xc.keyring.Current(ctx)
	if err != nil {
		return nil, "", fmt.Errorf("current key: %w", err)
	}

	aead, err := xc.aead(keyId, key)
	if err != nil {
		return nil, "", err
	}

	nonce := make([]byte, aead.NonceSize())
	if _, err = rand.Read(nonce); err != nil {
		return nil, "", fmt.Errorf("nonce: %w", err)
	}

	return aead.Seal(nonce, nonce, plain, nil), keyId, nil
}

func (xc *xChaCha20Encryptor) Open(ctx context.Context, keyId string,
	sealed []byte) ([]byte, error) {
	key, err := xc.keyring.Key(ctx, keyId)
	if err != nil {
		return nil, err
	}

	aead, err := xc.aead(keyId, key)
	if err != nil {
		return nil, err
	}

	if len(sealed) < aead.NonceSize() {
		return nil, ErrCiphertextTruncated
	}

	nonce, ciphertext := sealed[:aead.NonceSize()], sealed[aead.NonceSize():]
	return aead.Open(nil, nonce, ciphertext, nil)
}

// aead returns the AEAD for one key id, building it once. See the note in
// encrypt_aesgcm.go on why this is a cache rather than a pool.
func (xc *xChaCha20Encryptor) aead(keyId string, key []byte) (cipher.AEAD, error) {
	if cached, found := xc.aeads.Load(keyId); found {
		return cached.(cipher.AEAD), nil
	}

	if len(key) != chacha20poly1305.KeySize {
		return nil, fmt.Errorf("%w: got %d bytes, want %d",
			ErrKeyLength, len(key), chacha20poly1305.KeySize)
	}

	built, err := chacha20poly1305.NewX(key)
	if err != nil {
		return nil, err
	}

	cached, _ := xc.aeads.LoadOrStore(keyId, built)
	return cached.(cipher.AEAD), nil
}

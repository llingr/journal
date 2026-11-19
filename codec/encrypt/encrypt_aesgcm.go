// SPDX-FileCopyrightText: Copyright (c) 2026 The journal Authors
// SPDX-License-Identifier: Apache-2.0

package encrypt

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"fmt"
	"sync"
)

const aesKeyLength = 32

type aesGcmEncryptor struct {
	keyring Keyring
	aeads   sync.Map
}

// NewAESGCM returns an AES-256-GCM Encryptor drawing its keys from the given Keyring. The
// nonce is random per payload and prefixed to the ciphertext, so event_data carries its own
// framing and the schema holds only the algorithm and the key id.
func NewAESGCM(keyring Keyring) Encryptor {
	return &aesGcmEncryptor{
		keyring: keyring,
	}
}

func (ae *aesGcmEncryptor) Encryption() DataEncryption {
	return AESGCM
}

func (ae *aesGcmEncryptor) Seal(ctx context.Context, plain []byte) ([]byte, string, error) {
	keyId, key, err := ae.keyring.Current(ctx)
	if err != nil {
		return nil, "", fmt.Errorf("current key: %w", err)
	}

	aead, err := ae.aead(keyId, key)
	if err != nil {
		return nil, "", err
	}

	nonce := make([]byte, aead.NonceSize())
	if _, err = rand.Read(nonce); err != nil {
		return nil, "", fmt.Errorf("nonce: %w", err)
	}

	return aead.Seal(nonce, nonce, plain, nil), keyId, nil
}

func (ae *aesGcmEncryptor) Open(ctx context.Context, keyId string, sealed []byte) ([]byte, error) {
	key, err := ae.keyring.Key(ctx, keyId)
	if err != nil {
		return nil, err
	}

	aead, err := ae.aead(keyId, key)
	if err != nil {
		return nil, err
	}

	if len(sealed) < aead.NonceSize() {
		return nil, ErrCiphertextTruncated
	}

	nonce, ciphertext := sealed[:aead.NonceSize()], sealed[aead.NonceSize():]
	return aead.Open(nil, nonce, ciphertext, nil)
}

// aead returns the AEAD for one key id, building it once.
//
// A cache and not a sync.Pool: a cipher.AEAD is safe for concurrent use, so one
// instance serves every goroutine, and a pool would hand back an AEAD built for
// a different key.
func (ae *aesGcmEncryptor) aead(keyId string, key []byte) (cipher.AEAD, error) {
	if cached, found := ae.aeads.Load(keyId); found {
		return cached.(cipher.AEAD), nil
	}

	if len(key) != aesKeyLength {
		return nil, fmt.Errorf("%w: got %d bytes, want %d", ErrKeyLength, len(key), aesKeyLength)
	}

	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}

	built, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}

	cached, _ := ae.aeads.LoadOrStore(keyId, built)
	return cached.(cipher.AEAD), nil
}

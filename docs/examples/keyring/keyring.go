// SPDX-FileCopyrightText: Copyright (c) 2026 The journal Authors
// SPDX-License-Identifier: Apache-2.0

// Package keyring is an in-memory encrypt.Keyring for the examples. It keeps
// every key it has been given, so payloads sealed before a rotation still
// open after it. A deployment would put a KMS or a vault behind the same
// interface.
package keyring

import (
	"context"
	"crypto/rand"
	"fmt"
	"sync"

	"github.com/llingr/journal/codec/encrypt"
)

// Rotating holds every key it has been given and seals under the newest.
type Rotating struct {
	mu        sync.RWMutex
	currentId string
	keys      map[string][]byte
}

var _ encrypt.Keyring = (*Rotating)(nil)

// New returns a Rotating keyring sealing under keyId.
func New(keyId string, key []byte) *Rotating {
	return &Rotating{
		currentId: keyId,
		keys:      map[string][]byte{keyId: clone(key)},
	}
}

// NewKey returns 32 random bytes, the key length both ciphers take.
func NewKey() []byte {
	key := make([]byte, 32)
	rand.Read(key)
	return key
}

// Rotate makes keyId the key new payloads are sealed under. Earlier keys are
// kept for Open. An id is never reused: encryptors cache ciphers by id, so the
// same id under different bytes would open with the wrong key.
func (r *Rotating) Rotate(keyId string, key []byte) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.keys[keyId]; exists {
		return fmt.Errorf("key id %q already used", keyId)
	}
	r.keys[keyId] = clone(key)
	r.currentId = keyId
	return nil
}

func (r *Rotating) Current(ctx context.Context) (string, []byte, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.currentId, r.keys[r.currentId], nil
}

func (r *Rotating) Key(ctx context.Context, keyId string) ([]byte, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	key, found := r.keys[keyId]
	if !found {
		return nil, fmt.Errorf("%w: %q", encrypt.ErrUnknownKeyId, keyId)
	}
	return key, nil
}

func clone(key []byte) []byte {
	return append([]byte(nil), key...)
}

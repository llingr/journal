// SPDX-FileCopyrightText: Copyright (c) 2026 The journal Authors
// SPDX-License-Identifier: Apache-2.0

package encrypt_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"testing"

	"github.com/llingr/journal/codec/encrypt"
)

// testKeyring holds keys by id; current names the one new payloads are sealed under
type testKeyring struct {
	current string
	keys    map[string][]byte
}

func (k *testKeyring) Current(context.Context) (string, []byte, error) {
	return k.current, k.keys[k.current], nil
}

func (k *testKeyring) Key(_ context.Context, keyId string) ([]byte, error) {
	key, found := k.keys[keyId]
	if !found {
		return nil, fmt.Errorf("%w: %s", encrypt.ErrUnknownKeyId, keyId)
	}
	return key, nil
}

func newKeyring(keyIds ...string) *testKeyring {
	keyring := &testKeyring{
		current: keyIds[0],
		keys:    make(map[string][]byte),
	}
	for _, keyId := range keyIds {
		key := make([]byte, 32)
		_, _ = rand.Read(key)
		keyring.keys[keyId] = key
	}
	return keyring
}

var encryptors = []struct {
	name string
	new  func(keyring encrypt.Keyring) encrypt.Encryptor
}{
	{name: "aes-gcm", new: encrypt.NewAESGCM},
	{name: "xchacha20-poly1305", new: encrypt.NewXChaCha20},
}

// TestSealedBytesOpenOnlyAsSealed: anything other than the exact bytes under the exact key is
// refused, so a stored payload cannot be altered or misread without an error.
func TestSealedBytesOpenOnlyAsSealed(t *testing.T) {
	ctx := context.Background()
	plain := []byte(`{"amount":"12.50","currency":"GBP"}`)

	for _, under := range encryptors {
		t.Run(under.name, func(t *testing.T) {
			keyring := newKeyring("key-1", "key-2")
			encryptor := under.new(keyring)
			sealed, keyId, err := encryptor.Seal(ctx, plain)
			if err != nil {
				t.Fatalf("seal: %v", err)
			}
			if keyId != "key-1" || bytes.Contains(sealed, plain) {
				t.Fatalf("sealed under %q with the plain bytes visible: %t", keyId,
					bytes.Contains(sealed, plain))
			}
			opened, err := encryptor.Open(ctx, keyId, sealed)
			if err != nil || !bytes.Equal(opened, plain) {
				t.Fatalf("open what was sealed: %q, %v", opened, err)
			}

			again, _, err := encryptor.Seal(ctx, plain)
			if err != nil || bytes.Equal(again, sealed) {
				t.Fatalf("two seals of one payload are identical (%v), so the nonce repeats", err)
			}

			for index := range sealed {
				flipped := bytes.Clone(sealed)
				flipped[index] ^= 0x01
				_, err = encryptor.Open(ctx, keyId, flipped)
				if err == nil {
					t.Fatalf("a flipped bit at byte %d of %d opened", index, len(sealed))
				}
			}
			for _, length := range []int{0, 1, 23, len(sealed) - 1} {
				_, err = encryptor.Open(ctx, keyId, sealed[:length])
				if err == nil {
					t.Fatalf("sealed bytes cut to %d of %d opened", length, len(sealed))
				}
			}
			_, err = encryptor.Open(ctx, "key-2", sealed)
			if err == nil {
				t.Fatal("bytes sealed under key-1 opened under key-2")
			}
			_, err = encryptor.Open(ctx, "key-3", sealed)
			if !errors.Is(err, encrypt.ErrUnknownKeyId) {
				t.Fatalf("open under a key id the keyring lacks: got %v, want ErrUnknownKeyId", err)
			}
		})
	}
}

// TestAKeyOfTheWrongLengthIsRefused: a keyring handing out a short key fails the call that
// needs it, not with a cipher built on less key than the algorithm names.
func TestAKeyOfTheWrongLengthIsRefused(t *testing.T) {
	ctx := context.Background()

	for _, under := range encryptors {
		t.Run(under.name, func(t *testing.T) {
			keyring := newKeyring("short")
			keyring.keys["short"] = keyring.keys["short"][:16]
			encryptor := under.new(keyring)

			_, _, err := encryptor.Seal(ctx, []byte("payload"))
			if !errors.Is(err, encrypt.ErrKeyLength) {
				t.Fatalf("seal under a 16-byte key: got %v, want ErrKeyLength", err)
			}
			_, err = encryptor.Open(ctx, "short", make([]byte, 64))
			if !errors.Is(err, encrypt.ErrKeyLength) {
				t.Fatalf("open under a 16-byte key: got %v, want ErrKeyLength", err)
			}
		})
	}
}

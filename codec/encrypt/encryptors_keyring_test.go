// SPDX-FileCopyrightText: Copyright (c) 2026 The journal Authors
// SPDX-License-Identifier: Apache-2.0

package encrypt_test

import (
	"context"
	"errors"
	"testing"

	"github.com/llingr/journal/codec/encrypt"
)

// TestEachEncryptorNamesItsCipher: the id stored beside a sealed payload is the cipher that
// sealed it, so a row is never opened with the other one
func TestEachEncryptorNamesItsCipher(t *testing.T) {
	keyring := newKeyring("key-1")
	for _, testCase := range []struct {
		encryptor encrypt.Encryptor
		want      encrypt.DataEncryption
	}{
		{encrypt.NewAESGCM(keyring), encrypt.AESGCM},
		{encrypt.NewXChaCha20(keyring), encrypt.XChaCha20Poly1305},
	} {
		if got := testCase.encryptor.Encryption(); got != testCase.want {
			t.Fatalf("encryptor names %s, want %s", got, testCase.want)
		}
	}
}

// TestAKeyringThatFailsFailsTheCall: a keyring that cannot supply a key fails the seal or the
// open with its own error, rather than sealing under no key or returning bytes unopened
func TestAKeyringThatFailsFailsTheCall(t *testing.T) {
	ctx := context.Background()
	errVault := errors.New("vault unreachable")

	for _, under := range encryptors {
		t.Run(under.name, func(t *testing.T) {
			encryptor := under.new(failingKeyring{
				err: errVault,
			})
			sealed, keyId, err := encryptor.Seal(ctx, []byte("payload"))
			if !errors.Is(err, errVault) || sealed != nil || keyId != "" {
				t.Fatalf("seal with the keyring down: %d bytes under %q, %v; want the "+
					"keyring's error and nothing sealed", len(sealed), keyId, err)
			}
			opened, err := encryptor.Open(ctx, "key-1", make([]byte, 64))
			if !errors.Is(err, errVault) || opened != nil {
				t.Fatalf("open with the keyring down: %d bytes, %v; want the keyring's error",
					len(opened), err)
			}
		})
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

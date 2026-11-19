// SPDX-FileCopyrightText: Copyright (c) 2026 The journal Authors
// SPDX-License-Identifier: Apache-2.0

// Package decode turns a stored event's payload back into the plain bytes its domain event
// marshalled, for the repository and the outbox alike.
package decode

import (
	"context"
	"fmt"

	"github.com/llingr/journal/codec"
	"github.com/llingr/journal/persistence"
)

// Payload decompresses and decrypts the event's payload as its columns say
func Payload(ctx context.Context, pipeline *codec.Pipeline,
	event persistence.PersistedEvent) ([]byte, error) {
	plain, err := pipeline.Decode(ctx, codec.Encoded{
		Data:            event.EventData,
		Format:          event.DataFormat,
		Compression:     event.DataCompression,
		Encryption:      event.DataEncryption,
		EncryptionKeyId: event.DataEncryptionKid,
	})
	if err != nil {
		return nil, fmt.Errorf("decode stream %s version %d: %w", event.StreamID, event.Version,
			err)
	}
	return plain, nil
}

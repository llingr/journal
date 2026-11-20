// SPDX-FileCopyrightText: Copyright (c) 2026 The journal Authors
// SPDX-License-Identifier: Apache-2.0

// Command codec runs the payload pipeline and prints what each of the four
// binding strategies refuses.
//
// It touches no database: the pipeline sits above the store port, so
// everything below is bytes and four column values.
package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/xml"
	"fmt"
	"os"
	"text/tabwriter"

	"github.com/google/uuid"
	"github.com/llingr/journal/docs/examples/keyring"

	"github.com/llingr/journal/codec"
	"github.com/llingr/journal/codec/compress"
	"github.com/llingr/journal/codec/encrypt"
	"github.com/llingr/journal/codec/formatjson"
	"github.com/llingr/journal/codec/formatxml"
)

const (
	accountCreatedEventType  = "ACCOUNT_CREATED"
	orderDispatchedEventType = "ORDER_DISPATCHED"
)

type accountCreated struct {
	XMLName   xml.Name `json:"-" xml:"accountCreated"`
	AccountID string   `json:"accountId" xml:"accountId"`
	Tier      string   `json:"tier" xml:"tier"`
}

type orderDispatched struct {
	XMLName     xml.Name `json:"-" xml:"orderDispatched"`
	Carrier     string   `json:"carrier" xml:"carrier"`
	Consignment string   `json:"consignment" xml:"consignment"`
	Parcels     []parcel `json:"parcels" xml:"parcels>parcel"`
}

type parcel struct {
	Reference   string `json:"reference" xml:"reference"`
	Description string `json:"description" xml:"description"`
	Grams       int    `json:"grams" xml:"grams"`
}

func main() {
	if err := run(context.Background()); err != nil {
		fmt.Fprintf(os.Stderr, "codec example failed: %v\n", err)
		os.Exit(1)
	}
}

func run(ctx context.Context) error {
	keys := keyring.New("key-2026-09", keyring.NewKey())

	if err := roundTrip(ctx, keys); err != nil {
		return err
	}
	if err := selectPerEventType(ctx, keys); err != nil {
		return err
	}
	if err := rotateKey(ctx, keys); err != nil {
		return err
	}
	return nil
}

// roundTrip encodes three payloads and decodes them again, printing what the
// stored columns say happened to each.
func roundTrip(ctx context.Context, keyring encrypt.Keyring) error {
	pipeline, err := codec.NewPipeline(
		codec.CompressWith(compress.NewGzip(gzip.BestSpeed, compress.DefaultConcurrency)),
		codec.EncryptWith(encrypt.NewAESGCM(keyring)),
	)
	if err != nil {
		return err
	}

	fmt.Println("round trip, with gzip and encryption on everything")
	fmt.Println()

	table := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	_, _ = fmt.Fprintln(table, "  event type\tformat\tplain\tstored\tcompression\tencryption\tkey id")

	payloads := []struct {
		eventType string
		format    codec.DataFormat
		value     any
	}{
		{
			eventType: accountCreatedEventType,
			format:    codec.JSON,
			value:     &accountCreated{AccountID: uuid.New().String(), Tier: "STANDARD"},
		},
		{
			eventType: orderDispatchedEventType,
			format:    codec.JSON,
			value:     sampleOrder(40),
		},
		{
			eventType: orderDispatchedEventType,
			format:    codec.XML,
			value:     sampleOrder(40),
		},
	}

	for _, payload := range payloads {
		stored, plainByteCount, encodeErr := encodeAndDecode(
			ctx, pipeline, payload.eventType, payload.format, payload.value)
		if encodeErr != nil {
			return encodeErr
		}

		_, _ = fmt.Fprintf(table, "  %s\t%s\t%d\t%d\t%s\t%s\t%s\n",
			payload.eventType, stored.Format, plainByteCount, len(stored.Data),
			stored.Compression, stored.Encryption, keyIdOrNone(stored.EncryptionKeyId))
	}

	if err = table.Flush(); err != nil {
		return err
	}

	fmt.Println()
	fmt.Println("  The small payload asked for gzip and got none: gzip made it bigger, so the")
	fmt.Println("  plain bytes were stored and the column says what the bytes are.")
	fmt.Println()
	return nil
}

// selectPerEventType shows a Selector: one event type is sealed, the other is
// stored in the clear, and decode is told nothing.
func selectPerEventType(ctx context.Context, keyring encrypt.Keyring) error {
	pipeline, err := codec.NewPipeline(
		codec.CompressWith(compress.NewGzip(gzip.BestSpeed, compress.DefaultConcurrency)),
		codec.EncryptWith(encrypt.NewAESGCM(keyring)),
		codec.WithSelector(sealSensitiveEvents),
	)
	if err != nil {
		return err
	}

	fmt.Println("selector per event type: ACCOUNT_CREATED sealed, ORDER_DISPATCHED not")
	fmt.Println()

	table := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	_, _ = fmt.Fprintln(table, "  event type\tcompression\tencryption\tkey id")

	payloads := []struct {
		eventType string
		value     any
	}{
		{
			eventType: accountCreatedEventType,
			value:     &accountCreated{AccountID: uuid.New().String(), Tier: "PREMIUM"},
		},
		{
			eventType: orderDispatchedEventType,
			value:     sampleOrder(40),
		},
	}

	for _, payload := range payloads {
		stored, _, encodeErr := encodeAndDecode(
			ctx, pipeline, payload.eventType, codec.JSON, payload.value)
		if encodeErr != nil {
			return encodeErr
		}

		_, _ = fmt.Fprintf(table, "  %s\t%s\t%s\t%s\n",
			payload.eventType, stored.Compression, stored.Encryption,
			keyIdOrNone(stored.EncryptionKeyId))
	}

	if err = table.Flush(); err != nil {
		return err
	}
	fmt.Println()
	return nil
}

// rotateKey seals one payload, rotates the key, seals another, and opens both:
// each row carries the id of the key it was sealed under.
func rotateKey(ctx context.Context, keys *keyring.Rotating) error {
	pipeline, err := codec.NewPipeline(
		codec.EncryptWith(encrypt.NewAESGCM(keys)),
	)
	if err != nil {
		return err
	}

	plain, err := marshal(codec.JSON,
		&accountCreated{AccountID: uuid.New().String(), Tier: "STANDARD"})
	if err != nil {
		return err
	}

	before, err := pipeline.Encode(ctx, accountCreatedEventType, codec.JSON, plain)
	if err != nil {
		return err
	}

	if err = keys.Rotate("key-2026-10", keyring.NewKey()); err != nil {
		return err
	}

	after, err := pipeline.Encode(ctx, accountCreatedEventType, codec.JSON, plain)
	if err != nil {
		return err
	}

	fmt.Println("key rotation: sealed under key-2026-09, rotated, sealed again")
	fmt.Println()

	table := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	_, _ = fmt.Fprintln(table, "  sealed\tkey id\topens")

	for _, stored := range []struct {
		label  string
		stored codec.Encoded
	}{
		{label: "before rotation", stored: before},
		{label: "after rotation", stored: after},
	} {
		decoded, decodeErr := pipeline.Decode(ctx, stored.stored)
		if decodeErr != nil {
			return fmt.Errorf("decode %s: %w", stored.label, decodeErr)
		}
		_, _ = fmt.Fprintf(table, "  %s\t%s\t%t\n",
			stored.label, stored.stored.EncryptionKeyId, bytes.Equal(decoded, plain))
	}

	if err = table.Flush(); err != nil {
		return err
	}
	fmt.Println()
	return nil
}

// encodeAndDecode marshals one payload, runs the bytes through the pipeline
// and back, and returns the stored form and the byte count before compression.
func encodeAndDecode(
	ctx context.Context,
	pipeline *codec.Pipeline,
	eventType string,
	format codec.DataFormat,
	value any,
) (codec.Encoded, int, error) {
	plain, err := marshal(format, value)
	if err != nil {
		return codec.Encoded{}, 0, err
	}

	stored, err := pipeline.Encode(ctx, eventType, format, plain)
	if err != nil {
		return codec.Encoded{}, 0, fmt.Errorf("encode %s: %w", eventType, err)
	}

	decoded, err := pipeline.Decode(ctx, stored)
	if err != nil {
		return codec.Encoded{}, 0, fmt.Errorf("decode %s: %w", eventType, err)
	}
	if !bytes.Equal(decoded, plain) {
		return codec.Encoded{}, 0, fmt.Errorf("%s: decoded bytes differ from the marshalled bytes",
			eventType)
	}
	return stored, len(plain), nil
}

// sealSensitiveEvents seals one event type, and compresses anything large
// enough to be worth compressing.
func sealSensitiveEvents(eventType string, plainSize int) (compress, encrypt bool) {
	return plainSize >= 1024, eventType == accountCreatedEventType
}

// marshal is the caller's business, not the pipeline's: a payload marshals
// itself, so this stands in for the payload's own MarshalPayload.
func marshal(format codec.DataFormat, value any) ([]byte, error) {
	switch format {
	case codec.JSON:
		return formatjson.Marshal(value)
	case codec.XML:
		return formatxml.Marshal(value)
	default:
		return nil, fmt.Errorf("%w: %s", codec.ErrInvalidCodecValue, format)
	}
}

func keyIdOrNone(keyId string) string {
	if keyId == "" {
		return "none"
	}
	return keyId
}

// sampleOrder builds a payload repetitive enough for gzip to earn its place.
func sampleOrder(parcelCount int) *orderDispatched {
	order := &orderDispatched{
		Carrier:     "DPD",
		Consignment: "15442290871",
	}

	for parcelNumber := 1; parcelNumber <= parcelCount; parcelNumber++ {
		order.Parcels = append(order.Parcels, parcel{
			Reference:   fmt.Sprintf("PARCEL-15442290871-%04d", parcelNumber),
			Description: "refrigerated consignment, handle upright, do not stack",
			Grams:       1200 + parcelNumber,
		})
	}
	return order
}

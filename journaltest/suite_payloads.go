// SPDX-FileCopyrightText: Copyright (c) 2026 The journal Authors
// SPDX-License-Identifier: Apache-2.0

package journaltest

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/google/uuid"

	"github.com/llingr/journal"
	"github.com/llingr/journal/codec"
	"github.com/llingr/journal/codec/compress"
	"github.com/llingr/journal/codec/encrypt"
	"github.com/llingr/journal/persistence"
	"github.com/llingr/journal/streams"
)

var payloadCases = []suiteCase{
	{"PayloadsAreStoredCompressedAndSeenPlain", runPayloadsAreStoredCompressedAndSeenPlain},
	{"EveryCompressionReadsBack", runEveryCompressionReadsBack},
	{"ZstdIsTheDefaultFrom128Bytes", runZstdIsTheDefaultFrom128Bytes},
	{"EachEventKeepsItsDataFormat", runEachEventKeepsItsDataFormat},
	{"OneCompressionSlotServesEveryWriterAndReader",
		runOneCompressionSlotServesEveryWriterAndReader},
}

// runOneCompressionSlotServesEveryWriterAndReader: with WithCompressionConcurrency(1), writers
// and readers racing on every bounded compression share the one slot, and each of them
// finishes with its own payload
func runOneCompressionSlotServesEveryWriterAndReader(t *testing.T, backend persistence.Backend) {
	ctx := context.Background()
	for _, compression := range []compress.DataCompression{compress.Gzip, compress.LZ4,
		compress.Zstd} {
		publisher := &recordingPublisher{}
		orders := newOrders(storeWith(backend, publisher, nil,
			journal.WithDataCompression(compression), journal.WithCompressionConcurrency(1)))

		const writers = 16
		var racing sync.WaitGroup
		for writer := range writers {
			racing.Go(func() {
				orderID := uuid.New()
				note := strings.Repeat(fmt.Sprintf("writer %d ", writer), 100)
				_, err := orders.Create(ctx, orderID, &placed{Amount: writer, Note: note})
				if err != nil {
					t.Errorf("%s: create for writer %d: %v", compression, writer, err)
					return
				}
				read, err := orders.Get(ctx, orderID)
				if err != nil {
					t.Errorf("%s: get for writer %d: %v", compression, writer, err)
					return
				}
				got := read.DomainEvents()[0].(*placed)
				if got.Amount != writer || got.Note != note {
					t.Errorf("%s: writer %d read back amount %d and a note of %d bytes",
						compression, writer, got.Amount, len(got.Note))
				}
				stored, err := backend.QueryStream(ctx, orderID, 1, 1)
				if err != nil || len(stored) != 1 || stored[0].DataCompression != compression {
					t.Errorf("%s: writer %d's event read back as %+v, %v; want one stored "+
						"with %s", compression, writer, stored, err, compression)
				}
			})
		}
		racing.Wait()
		if published := len(publisher.published()); published != writers {
			t.Errorf("%s: %d events published, want %d", compression, published, writers)
		}
	}
}

// runEachEventKeepsItsDataFormat: events of different formats in one stream are each stored,
// read and published under the format they declared, with their own payloads
func runEachEventKeepsItsDataFormat(t *testing.T, backend persistence.Backend) {
	ctx := context.Background()
	publisher := &recordingPublisher{}
	orders := newOrders(storeWith(backend, publisher, nil))
	orderID := uuid.New()

	_, err := orders.CreateAll(ctx, orderID, []streams.DomainEvent{
		&placed{Amount: 1},
		&annotated{Text: `gift wrap & <ribbon>`},
		&marked{},
	})
	requireNoError(t, err, "create with json, xml and proto events")
	reread, err := orders.Get(ctx, orderID)
	requireNoError(t, err, "get")

	want := []codec.DataFormat{codec.JSON, codec.XML, codec.Proto}
	stored := storedEvents(t, backend, orderID)
	published := publisher.published()
	for index, format := range want {
		if stored[index].DataFormat != format || reread.Events()[index].DataFormat != format ||
			published[index].DataFormat != format {
			t.Fatalf("version %d stored as %s, read as %s and published as %s; want %s",
				index+1, stored[index].DataFormat, reread.Events()[index].DataFormat,
				published[index].DataFormat, format)
		}
	}
	note, isAnnotated := reread.DomainEvents()[1].(*annotated)
	if !isAnnotated || note.Text != `gift wrap & <ribbon>` || reread.State().Events != 3 {
		t.Fatalf("read back %#v and %d events, want the XML note and 3",
			reread.DomainEvents()[1], reread.State().Events)
	}
}

// runPayloadsAreStoredCompressedAndSeenPlain: a payload is stored compressed and not
// encrypted, while the stream written, the stream read and the publisher all see the plain
// payload, the same bytes
func runPayloadsAreStoredCompressedAndSeenPlain(t *testing.T, backend persistence.Backend) {
	ctx := context.Background()
	publisher := &recordingPublisher{}
	orders := newOrders(storeWith(backend, publisher, nil,
		journal.WithDataCompression(compress.Zstd)))
	orderID := uuid.New()
	note := strings.Repeat("compressible ", 50)

	created, err := orders.Create(ctx, orderID, &placed{Amount: 7, Note: note})
	requireNoError(t, err, "create")
	appended, err := orders.Append(ctx, created, &placed{Amount: 3, Note: note})
	requireNoError(t, err, "append")
	reread, err := orders.Get(ctx, orderID)
	requireNoError(t, err, "get")

	for _, stored := range storedEvents(t, backend, orderID) {
		if stored.DataCompression != compress.Zstd || stored.DataEncryption != encrypt.None ||
			stored.DataEncryptionKid != "" || stored.DataFormat != codec.JSON {
			t.Fatalf("version %d stored as %s, %s, %s with key %q; want json, zstd and none",
				stored.Version, stored.DataFormat, stored.DataCompression,
				stored.DataEncryption, stored.DataEncryptionKid)
		}
		if bytes.Contains(stored.EventData, []byte(note)) {
			t.Fatalf("version %d is stored plain", stored.Version)
		}
	}
	published := publisher.published()
	for name, events := range map[string][]streams.Event{
		"appended": appended.Events(),
		"reread":   reread.Events(),
	} {
		if len(events) != 2 || len(published) != 2 {
			t.Fatalf("%s holds %d events and %d were published, want 2 of each", name,
				len(events), len(published))
		}
		for index, event := range events {
			var decoded placed
			if json.Unmarshal(event.Payload, &decoded) != nil || decoded.Note != note ||
				!bytes.Equal(event.Payload, published[index].Payload) ||
				!event.AppendTs.Equal(published[index].AppendTs) {
				t.Fatalf("%s event %d is not the plain payload published", name, index)
			}
		}
	}
}

// runEveryCompressionReadsBack: a stream written with each compression reads back through a
// store set to any other, or to none, so changing the setting leaves older events readable
func runEveryCompressionReadsBack(t *testing.T, backend persistence.Backend) {
	ctx := context.Background()
	compressions := []compress.DataCompression{
		compress.Zstd, compress.Gzip, compress.LZ4, compress.Snappy, compress.None,
	}
	note := strings.Repeat("compressible ", 50)

	written := make(map[compress.DataCompression]uuid.UUID)
	for _, compression := range compressions {
		orderID := uuid.New()
		_, err := newOrders(storeWith(backend, &recordingPublisher{}, nil,
			journal.WithDataCompression(compression))).Create(ctx, orderID,
			&placed{Amount: 7, Note: note})
		requireNoError(t, err, "create with %s", compression)
		if stored := storedEvents(t, backend, orderID)[0]; stored.DataCompression != compression {
			t.Fatalf("stored with %s, want %s", stored.DataCompression, compression)
		}
		written[compression] = orderID
	}
	for _, readerCompression := range compressions {
		reader := newOrders(storeWith(backend, &recordingPublisher{}, nil,
			journal.WithDataCompression(readerCompression)))
		for writerCompression, orderID := range written {
			read, err := reader.Get(ctx, orderID)
			if err != nil || read.State().Amount != 7 {
				t.Fatalf("read %s through a store set to %s: %+v, %v", writerCompression,
					readerCompression, read.State(), err)
			}
		}
	}
}

// runZstdIsTheDefaultFrom128Bytes: with no options a payload of 128 bytes or more is stored
// compressed with zstd and a smaller one as it is; WithCompressionMinimumBytes moves the
// threshold, and compress.None turns compression off
func runZstdIsTheDefaultFrom128Bytes(t *testing.T, backend persistence.Backend) {
	ctx := context.Background()
	// {"amount":7,"note":""} is 22 bytes, so the note makes up the rest
	payloadOf := func(size int) *placed {
		return &placed{
			Amount: 7,
			Note:   strings.Repeat("a", size-22),
		}
	}

	for _, testCase := range []struct {
		name    string
		options []journal.StoreOption
		size    int
		want    compress.DataCompression
	}{
		{name: "default at 127 bytes", size: 127, want: compress.None},
		{name: "default at 128 bytes", size: 128, want: compress.Zstd},
		{name: "minimum of 200 at 199 bytes",
			options: []journal.StoreOption{journal.WithCompressionMinimumBytes(200)}, size: 199,
			want: compress.None},
		{name: "minimum of 200 at 200 bytes",
			options: []journal.StoreOption{journal.WithCompressionMinimumBytes(200)}, size: 200,
			want: compress.Zstd},
		{name: "minimum of 1 at 23 bytes",
			options: []journal.StoreOption{journal.WithCompressionMinimumBytes(1)}, size: 23,
			want: compress.None},
		{name: "gzip at 128 bytes",
			options: []journal.StoreOption{journal.WithDataCompression(compress.Gzip)}, size: 128,
			want: compress.Gzip},
		{name: "none at 500 bytes",
			options: []journal.StoreOption{journal.WithDataCompression(compress.None)}, size: 500,
			want: compress.None},
	} {
		orderID := uuid.New()
		payload := payloadOf(testCase.size)
		plain, err := payload.MarshalPayload()
		if err != nil || len(plain) != testCase.size {
			t.Fatalf("%s: payload is %d bytes, want %d: %v", testCase.name, len(plain),
				testCase.size, err)
		}
		_, err = newOrders(storeWith(backend, &recordingPublisher{}, nil,
			testCase.options...)).Create(ctx, orderID, payload)
		requireNoError(t, err, "%s: create", testCase.name)
		if got := storedEvents(t, backend, orderID)[0].DataCompression; got != testCase.want {
			t.Fatalf("%s: stored with %s, want %s", testCase.name, got, testCase.want)
		}
	}
}

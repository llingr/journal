// SPDX-FileCopyrightText: Copyright (c) 2026 The journal Authors
// SPDX-License-Identifier: Apache-2.0

package formatxml_test

import (
	"encoding/xml"
	"reflect"
	"testing"

	"github.com/llingr/journal/codec"
	"github.com/llingr/journal/codec/formatxml"
)

type order struct {
	XMLName xml.Name `xml:"order"`
	ID      string   `xml:"id,attr"`
	Amount  int64    `xml:"amount"`
	Note    string   `xml:"note"`
	Lines   []line   `xml:"lines>line"`
}

type line struct {
	Sku      string `xml:"sku,attr"`
	Quantity int    `xml:",chardata"`
}

// TestFormatIsXML: a payload reporting Format is stored as xml, a value the schema accepts
func TestFormatIsXML(t *testing.T) {
	if formatxml.Format != codec.XML || !formatxml.Format.Valid() {
		t.Fatalf("Format is %q, want the valid %q", formatxml.Format, codec.XML)
	}
}

// TestAPayloadRoundTrips: what is marshalled unmarshals to an equal value, with the
// characters XML escapes in text and in attributes, and characters outside ASCII
func TestAPayloadRoundTrips(t *testing.T) {
	for name, written := range map[string]order{
		"escaped characters": {
			ID:   `o-1 "quoted" & <angled>`,
			Note: `<b>bold</b> & 'single' "double"`,
		},
		"outside ASCII": {
			ID:   "o-2",
			Note: "naïve café, 東京, 🚚",
		},
		"repeated elements": {
			ID:     "o-3",
			Amount: -9223372036854775808,
			Lines: []line{
				{Sku: "a", Quantity: 1},
				{Sku: "b", Quantity: 2},
			},
		},
	} {
		encoded, err := formatxml.Marshal(written)
		if err != nil {
			t.Fatalf("%s: marshal: %v", name, err)
		}
		var read order
		if err = formatxml.Unmarshal(encoded, &read); err != nil {
			t.Fatalf("%s: unmarshal %s: %v", name, encoded, err)
		}
		read.XMLName, written.XMLName = xml.Name{}, xml.Name{}
		if !reflect.DeepEqual(read, written) {
			t.Fatalf("%s: read back %+v, want %+v", name, read, written)
		}
	}
}

// TestUnmarshalRefusesWhatIsNotThePayload: unclosed elements, a value of the wrong type and
// an empty payload are errors
func TestUnmarshalRefusesWhatIsNotThePayload(t *testing.T) {
	for name, encoded := range map[string]string{
		"unclosed":   `<order id="o-1"><note>open`,
		"wrong type": `<order id="o-1"><amount>many</amount></order>`,
		"empty":      ``,
	} {
		var read order
		if err := formatxml.Unmarshal([]byte(encoded), &read); err == nil {
			t.Fatalf("%s: unmarshal of %q succeeded, want an error", name, encoded)
		}
	}
}

// TestMarshalRefusesWhatXMLCannotHold: a map has no XML form, so marshalling one is an error,
// not an empty payload
func TestMarshalRefusesWhatXMLCannotHold(t *testing.T) {
	if _, err := formatxml.Marshal(map[string]string{"id": "o-1"}); err == nil {
		t.Fatal("marshal of a map succeeded, want an error")
	}
}

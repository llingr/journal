// SPDX-FileCopyrightText: Copyright (c) 2026 The journal Authors
// SPDX-License-Identifier: Apache-2.0

package codec

import (
	"errors"
	"fmt"
	"slices"
)

// DataFormat used on an event or state value, stored as its name
type DataFormat string

const (
	JSON    DataFormat = "json"
	Avro    DataFormat = "avro"
	CBOR    DataFormat = "cbor"
	Ion     DataFormat = "ion"
	MsgPack DataFormat = "msgpack"
	Proto   DataFormat = "proto"
	Text    DataFormat = "text"
	XML     DataFormat = "xml"
	YAML    DataFormat = "yaml"
)

// ErrUnknownDataFormat is a name ParseFormat does not know
var ErrUnknownDataFormat = errors.New("unknown data format")

// AllFormats in the order of the events_data_format enum
var AllFormats = []DataFormat{
	JSON,
	Avro,
	CBOR,
	Ion,
	MsgPack,
	Proto,
	Text,
	XML,
	YAML,
}

// String is the stored name
func (d DataFormat) String() string {
	return string(d)
}

// Valid for the listed formats only
func (d DataFormat) Valid() bool {
	return slices.Contains(AllFormats, d)
}

// ParseFormat returns the format with the stored name, such as "json"; the name is
// case-sensitive, and an unknown one is ErrUnknownDataFormat
func ParseFormat(name string) (DataFormat, error) {
	format := DataFormat(name)
	if format.Valid() {
		return format, nil
	}
	return "", fmt.Errorf("%w: %q", ErrUnknownDataFormat, name)
}

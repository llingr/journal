// SPDX-FileCopyrightText: Copyright (c) 2026 The journal Authors
// SPDX-License-Identifier: Apache-2.0

package compress

import (
	"errors"
	"fmt"
	"slices"
)

// DataCompression used in table 'events', stored as its name
type DataCompression string

const (
	None   DataCompression = "none"
	Gzip   DataCompression = "gzip"
	LZ4    DataCompression = "lz4"
	Snappy DataCompression = "snappy"
	Zstd   DataCompression = "zstd"
)

var All = []DataCompression{None, Zstd, Snappy, LZ4, Gzip} // most common first

// String is the stored name
func (c DataCompression) String() string {
	return string(c)
}

// ErrUnknownDataCompression is a name Parse does not know
var ErrUnknownDataCompression = errors.New("unknown data compression")

// Parse returns the compression with the stored name, such as "zstd"; the name is
// case-sensitive, and an unknown one is ErrUnknownDataCompression
func Parse(name string) (DataCompression, error) {
	compression := DataCompression(name)
	if slices.Contains(All, compression) {
		return compression, nil
	}
	return "", fmt.Errorf("%w: %q", ErrUnknownDataCompression, name)
}

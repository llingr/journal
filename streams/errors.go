// SPDX-FileCopyrightText: Copyright (c) 2026 The journal Authors
// SPDX-License-Identifier: Apache-2.0

package streams

import "errors"

// ErrVersionConflict is a version the stream cannot take next: taken by another writer, or
// not contiguous from the head. Either way the caller re-reads the head.
var ErrVersionConflict = errors.New("version is not the stream's next")

// ErrVersionsNotConsecutive is an append whose versions do not start at 1 or above and run
// on one at a time, which only a programming error produces
var ErrVersionsNotConsecutive = errors.New("versions are not consecutive")

// ErrNotPermitted is the role lacking a privilege the call needs
var ErrNotPermitted = errors.New("role lacks the privilege")

// ErrVersionMissing is a version a read needs that is not stored, such as one trimmed or
// deleted, so the read would return only part of the stream
var ErrVersionMissing = errors.New("version is not stored")

// ErrNoPartitionKey is a partition key function that returned an empty key, refused before
// anything is written
var ErrNoPartitionKey = errors.New("no partition key")

// ErrStreamExists is a Create of a stream that already has events
var ErrStreamExists = errors.New("stream already exists")

// ErrStreamNotCreated is an Append to a stream at version 0, which only Create can start
var ErrStreamNotCreated = errors.New("stream has not been created")

// ErrNoEvents is a Create or an Append with no events
var ErrNoEvents = errors.New("no events to write")

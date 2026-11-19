// SPDX-FileCopyrightText: Copyright (c) 2026 The journal Authors
// SPDX-License-Identifier: Apache-2.0

package journaltest

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/llingr/journal"
	"github.com/llingr/journal/codec"
	"github.com/llingr/journal/codec/compress"
	"github.com/llingr/journal/codec/encrypt"
	"github.com/llingr/journal/persistence"
	"github.com/llingr/journal/publish"
	"github.com/llingr/journal/streams"
)

var failureCases = []suiteCase{
	{"AnEventThatFailsToMarshalWritesNothing", runAnEventThatFailsToMarshalWritesNothing},
	{"AnEventOfAnUnknownFormatWritesNothing", runAnEventOfAnUnknownFormatWritesNothing},
	{"AStateThatFailsToMarshalWritesNothing", runAStateThatFailsToMarshalWritesNothing},
	{"AnEventThatFailsToUnmarshalFailsTheRead", runAnEventThatFailsToUnmarshalFailsTheRead},
	{"ATransitionRefusingAStoredEventFailsTheRead",
		runATransitionRefusingAStoredEventFailsTheRead},
	{"ACorruptedPayloadFailsTheReadAndThePublish",
		runACorruptedPayloadFailsTheReadAndThePublish},
	{"ACorruptedStateFailsTheRead", runACorruptedStateFailsTheRead},
}

// errMarshal is a payload or state that cannot be marshalled
var errMarshal = errors.New("cannot marshal")

// errUnmarshal is a payload that cannot be read back
var errUnmarshal = errors.New("cannot unmarshal")

// runAnEventThatFailsToMarshalWritesNothing: an event whose payload cannot be marshalled
// fails the write with its error, and nothing of the write is stored or published
func runAnEventThatFailsToMarshalWritesNothing(t *testing.T, backend persistence.Backend) {
	publisher := &recordingPublisher{}
	orderID := uuid.New()

	// a repository without transitions, which folds nothing, so the marshal is what refuses
	_, err := journal.NewRepository[struct{}](storeWith(backend, publisher, nil)).CreateAll(
		context.Background(), orderID, []streams.DomainEvent{
			&placed{Amount: 1},
			&unmarshallable{},
		})
	requireError(t, err, errMarshal, "create with an event that cannot be marshalled")
	requireHead(t, backend, orderID, 0)
	requireVersions(t, "published", publisher.versions())
}

// runAnEventOfAnUnknownFormatWritesNothing: an event naming a data format the library does
// not know is refused before anything is stored
func runAnEventOfAnUnknownFormatWritesNothing(t *testing.T, backend persistence.Backend) {
	orderID := uuid.New()

	_, err := journal.NewRepository[struct{}](storeWith(backend, &recordingPublisher{}, nil)).
		Create(context.Background(), orderID, &tomlEvent{})
	requireError(t, err, codec.ErrInvalidCodecValue, "create with a toml event")
	requireHead(t, backend, orderID, 0)
}

// runAStateThatFailsToMarshalWritesNothing: a state the codec cannot marshal fails the write,
// so events are never stored without the state a repository that keeps it promises
func runAStateThatFailsToMarshalWritesNothing(t *testing.T, backend persistence.Backend) {
	publisher := &recordingPublisher{}
	broken := orderCodec(1)
	broken.Marshal = func(order) ([]byte, error) {
		return nil, errMarshal
	}
	orderID := uuid.New()

	_, err := newOrders(storeWith(backend, publisher, nil), journal.WithSnapshot(broken)).Create(
		context.Background(), orderID, &placed{Amount: 1})
	requireError(t, err, errMarshal, "create through a codec that cannot marshal the state")
	requireHead(t, backend, orderID, 0)
	requireVersions(t, "published", publisher.versions())
}

// runAnEventThatFailsToUnmarshalFailsTheRead: a stored event its type cannot read fails Get
// with that error, rather than folding the stream without it
func runAnEventThatFailsToUnmarshalFailsTheRead(t *testing.T, backend persistence.Backend) {
	ctx := context.Background()
	eventStore := storeWith(backend, &recordingPublisher{}, nil)
	orderID := uuid.New()
	_, err := newOrders(eventStore).Create(ctx, orderID, &placed{Amount: 1})
	requireNoError(t, err, "create")

	unreadable := journal.NewRepository(eventStore, journal.WithTransitions(
		journal.Transition(func(state order, _ *unreadablePlaced) (order, error) {
			return state, nil
		}),
	))
	_, err = unreadable.Get(ctx, orderID)
	requireError(t, err, errUnmarshal, "get through an event type that cannot read the payload")
}

// runATransitionRefusingAStoredEventFailsTheRead: a transition that refuses an event already
// stored, such as one tightened since, fails Get with its error rather than skip the event
func runATransitionRefusingAStoredEventFailsTheRead(t *testing.T, backend persistence.Backend) {
	ctx := context.Background()
	eventStore := storeWith(backend, &recordingPublisher{}, nil)
	orderID := uuid.New()
	_, err := newOrders(eventStore).Create(ctx, orderID, &placed{Amount: 1})
	requireNoError(t, err, "create")

	errTightened := errors.New("amounts below 10 are no longer accepted")
	tightened := journal.NewRepository(eventStore, journal.WithTransitions(
		journal.Transition(func(state order, event *placed) (order, error) {
			if event.Amount < 10 {
				return state, errTightened
			}
			return state, nil
		}),
	))
	_, err = tightened.Get(ctx, orderID)
	requireError(t, err, errTightened, "get through a transition refusing the stored event")
}

// runACorruptedPayloadFailsTheReadAndThePublish: a stored payload whose bytes do not
// decompress fails Get, and its publish fails rather than send the damaged bytes; the event
// stays unpublished
func runACorruptedPayloadFailsTheReadAndThePublish(t *testing.T, backend persistence.Backend) {
	ctx := context.Background()
	orderID := uuid.New()
	corrupted := seededEvent(orderID, 1, "")
	corrupted.DataCompression = compress.Zstd
	corrupted.EventData = []byte("not a zstd frame")
	_, err := backend.Append(ctx, persistence.AppendRequest{
		StreamID: orderID,
		Events:   []persistence.PersistedEvent{corrupted},
		AppendTs: minutesAgo(5),
		Publish:  true,
	})
	requireNoError(t, err, "append a corrupted payload")
	publisher := &recordingPublisher{}
	eventStore := storeWith(backend, publisher, nil)

	_, err = newOrders(eventStore).Get(ctx, orderID)
	if err == nil {
		t.Fatal("get of a corrupted payload succeeded, want an error")
	}
	swept, err := eventStore.Sweep(ctx, time.Hour)
	if err == nil || swept != 0 {
		t.Fatalf("sweep of a corrupted payload: %d streams, %v; want an error", swept, err)
	}
	_, err = eventStore.RepublishEvents(ctx, orderID, 1, publish.Always)
	if err == nil {
		t.Fatal("republish of a corrupted payload succeeded, want an error")
	}
	requireVersions(t, "published", publisher.versions())
	requirePending(t, backend, orderID, 1)
}

// runACorruptedStateFailsTheRead: a stored state whose bytes do not decompress fails Get, as
// a codec bug or a damaged row would be hidden by folding the events again
func runACorruptedStateFailsTheRead(t *testing.T, backend persistence.Backend) {
	ctx := context.Background()
	orders := newOrders(journal.NewStore(backend), journal.WithSnapshot(orderCodec(1)))
	orderID := uuid.New()
	_, err := orders.Create(ctx, orderID, &placed{Amount: 1})
	requireNoError(t, err, "create")

	stored, _, err := backend.QueryState(ctx, orderID)
	requireNoError(t, err, "read the stored state")
	stored.StateData = []byte("not a zstd frame")
	stored.DataCompression = compress.Zstd
	stored.DataEncryption = encrypt.None
	saved, err := backend.SaveState(ctx, stored)
	if err != nil || !saved {
		t.Fatalf("store a corrupted state: %t, %v", saved, err)
	}

	_, err = orders.Get(ctx, orderID)
	if err == nil {
		t.Fatal("get of a corrupted state succeeded, want an error")
	}
}

// unmarshallable is an event whose payload cannot be marshalled
type unmarshallable struct{}

func (u *unmarshallable) EventType() string {
	return "UNMARSHALLABLE"
}

func (u *unmarshallable) DataFormat() codec.DataFormat {
	return codec.JSON
}

func (u *unmarshallable) MarshalPayload() ([]byte, error) {
	return nil, errMarshal
}

func (u *unmarshallable) UnmarshalPayload([]byte) error {
	return nil
}

// tomlEvent names a data format the library does not store
type tomlEvent struct{}

func (e *tomlEvent) EventType() string {
	return "TOML"
}

func (e *tomlEvent) DataFormat() codec.DataFormat {
	return codec.DataFormat("toml")
}

func (e *tomlEvent) MarshalPayload() ([]byte, error) {
	return []byte(`amount = 1`), nil
}

func (e *tomlEvent) UnmarshalPayload([]byte) error {
	return nil
}

// unreadablePlaced reads PLACED events and cannot
type unreadablePlaced struct{}

func (u *unreadablePlaced) EventType() string {
	return "PLACED"
}

func (u *unreadablePlaced) DataFormat() codec.DataFormat {
	return codec.JSON
}

func (u *unreadablePlaced) MarshalPayload() ([]byte, error) {
	return json.Marshal(u)
}

func (u *unreadablePlaced) UnmarshalPayload([]byte) error {
	return errUnmarshal
}

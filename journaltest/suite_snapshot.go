// SPDX-FileCopyrightText: Copyright (c) 2026 The journal Authors
// SPDX-License-Identifier: Apache-2.0

package journaltest

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/llingr/journal"
	"github.com/llingr/journal/codec/compress"
	"github.com/llingr/journal/persistence"
	"github.com/llingr/journal/streams"
)

var snapshotCases = []suiteCase{
	{"TheStateIsStoredBesideItsEvents", runTheStateIsStoredBesideItsEvents},
	{"ALongStreamFoldsInPagesThenStoresItsState", runALongStreamFoldsInPagesThenStoresItsState},
	{"AStateBehindTheStreamIsFoldedAgain", runAStateBehindTheStreamIsFoldedAgain},
	{"AStateThatDoesNotUnmarshalFailsTheRead", runAStateThatDoesNotUnmarshalFailsTheRead},
	{"AnAppendFromHeadIsRefusedWhenTheStateIsStored",
		runAnAppendFromHeadIsRefusedWhenTheStateIsStored},
	{"AGetOfAnUnwrittenStreamStoresNothing", runAGetOfAnUnwrittenStreamStoresNothing},
}

// runTheStateIsStoredBesideItsEvents: every write stores the state it folds, compressed as an
// event is, at the codec's version; nothing publishes it, and Get reads it alone
func runTheStateIsStoredBesideItsEvents(t *testing.T, backend persistence.Backend) {
	ctx := context.Background()
	publisher := &recordingPublisher{}
	orders := newOrders(storeWith(backend, publisher, nil,
		journal.WithDataCompression(compress.Zstd), journal.WithCompressionMinimumBytes(1)),
		journal.WithSnapshot(orderCodec(3)))
	orderID := uuid.New()
	note := strings.Repeat("compressible ", 20)

	created, err := orders.Create(ctx, orderID, &placed{Amount: 1, Note: note})
	requireNoError(t, err, "create")
	written, err := orders.AppendAll(ctx, created, []streams.DomainEvent{
		&placed{Amount: 2, Note: note},
		&shipped{},
	})
	requireNoError(t, err, "append")

	stored, head, err := backend.QueryState(ctx, orderID)
	requireNoError(t, err, "read the stored state")
	if head != 3 || stored.Version != 3 || stored.StateVersion != 3 ||
		stored.LastEventType != "SHIPPED" || !stored.LastAppendTs.Equal(written.LastAppendTs()) {
		t.Fatalf("stored state %+v at head %d, want version 3, state version 3, last SHIPPED",
			stored, head)
	}
	requireVersions(t, "published", publisher.versions(), 1, 2, 3)

	read, err := orders.Get(ctx, orderID)
	requireNoError(t, err, "get")
	if read.State() != written.State() || read.Version() != 3 || len(read.Events()) != 0 ||
		read.LastEventType() != "SHIPPED" || !read.LastAppendTs().Equal(written.LastAppendTs()) {
		t.Fatalf("read %+v at version %d with %d events, last %s; want %+v at 3 from the state "+
			"alone, last SHIPPED", read.State(), read.Version(), len(read.Events()),
			read.LastEventType(), written.State())
	}
	appended, err := orders.Append(ctx, read, &placed{Amount: 4})
	if err != nil || appended.State().Amount != 7 {
		t.Fatalf("append to a stream read from its state: %+v, %v", appended.State(), err)
	}
}

// runALongStreamFoldsInPagesThenStoresItsState: a stream written without its state, over
// several pages, folds whole when a repository that stores it first reads it, and stores the
// result; a new state version folds it again
func runALongStreamFoldsInPagesThenStoresItsState(t *testing.T, backend persistence.Backend) {
	ctx := context.Background()
	eventStore := journal.NewStore(backend)
	orderID := uuid.New()
	// 501 events, as three pages of 250 with the last of one event
	payloads := make([]streams.DomainEvent, 501)
	for index := range payloads {
		payloads[index] = &placed{Amount: 1}
	}
	_, err := newOrders(eventStore).CreateAll(ctx, orderID, payloads)
	requireNoError(t, err, "create 501 events")

	for _, stateVersion := range []int32{1, 1, 2} {
		read, err := newOrders(eventStore, journal.WithSnapshot(orderCodec(stateVersion))).Get(
			ctx, orderID)
		requireNoError(t, err, "get at state version %d", stateVersion)
		if read.Version() != 501 || read.State().Amount != 501 {
			t.Fatalf("state version %d: amount %d at version %d, want 501 at 501", stateVersion,
				read.State().Amount, read.Version())
		}
		stored, _, err := backend.QueryState(ctx, orderID)
		requireNoError(t, err, "read the stored state")
		if stored.Version != 501 || stored.StateVersion != stateVersion {
			t.Fatalf("after a get at state version %d the stored state is version %d at state "+
				"version %d", stateVersion, stored.Version, stored.StateVersion)
		}
	}
}

// runAStateBehindTheStreamIsFoldedAgain: a writer that stores no state leaves the stored one
// behind the stream, and the next Get folds the events rather than return it, then stores
// what it folded
func runAStateBehindTheStreamIsFoldedAgain(t *testing.T, backend persistence.Backend) {
	ctx := context.Background()
	eventStore := journal.NewStore(backend)
	stored := newOrders(eventStore, journal.WithSnapshot(orderCodec(1)))
	unstored := newOrders(eventStore)
	orderID := uuid.New()

	created, err := stored.Create(ctx, orderID, &placed{Amount: 1})
	requireNoError(t, err, "create with the state")
	_, err = unstored.Append(ctx, created, &placed{Amount: 2})
	requireNoError(t, err, "append without the state")

	read, err := stored.Get(ctx, orderID)
	if err != nil || read.State().Amount != 3 || read.Version() != 2 {
		t.Fatalf("get: amount %d at version %d, %v; want 3 at version 2", read.State().Amount,
			read.Version(), err)
	}
	state, head, err := backend.QueryState(ctx, orderID)
	requireNoError(t, err, "read the stored state")
	if state.Version != 2 || head != 2 {
		t.Fatalf("after the get the stored state is at version %d of %d, want 2", state.Version,
			head)
	}
}

// runAStateThatDoesNotUnmarshalFailsTheRead: a stored state the codec cannot read is a bug to
// see, not one to hide by folding the events again
func runAStateThatDoesNotUnmarshalFailsTheRead(t *testing.T, backend persistence.Backend) {
	ctx := context.Background()
	eventStore := journal.NewStore(backend)
	orderID := uuid.New()
	_, err := newOrders(eventStore, journal.WithSnapshot(orderCodec(1))).Create(ctx, orderID,
		&placed{Amount: 1})
	requireNoError(t, err, "create")

	errUnreadable := errors.New("unreadable state")
	broken := orderCodec(1)
	broken.Unmarshal = func([]byte) (order, error) {
		return order{}, errUnreadable
	}
	_, err = newOrders(eventStore, journal.WithSnapshot(broken)).Get(ctx, orderID)
	requireError(t, err, errUnreadable, "get through a codec that cannot read the state")
}

// runAnAppendFromHeadIsRefusedWhenTheStateIsStored: a stream read by Head holds no state, so
// storing the fold of its new events would store the wrong state
func runAnAppendFromHeadIsRefusedWhenTheStateIsStored(t *testing.T, backend persistence.Backend) {
	ctx := context.Background()
	orders := newOrders(journal.NewStore(backend), journal.WithSnapshot(orderCodec(1)))
	orderID := uuid.New()
	_, err := orders.Create(ctx, orderID, &placed{Amount: 1})
	requireNoError(t, err, "create")

	head, err := orders.Head(ctx, orderID)
	requireNoError(t, err, "head")
	_, err = orders.Append(ctx, head, &shipped{})
	requireError(t, err, streams.ErrStreamNotFolded, "append from the head")
	requireHead(t, backend, orderID, 1)
}

// runAGetOfAnUnwrittenStreamStoresNothing: a stream with no events is the zero state, and no
// state is stored for it
func runAGetOfAnUnwrittenStreamStoresNothing(t *testing.T, backend persistence.Backend) {
	ctx := context.Background()
	orderID := uuid.New()

	read, err := newOrders(journal.NewStore(backend), journal.WithSnapshot(orderCodec(1))).Get(ctx,
		orderID)
	if err != nil || read.Version() != 0 || read.State() != (order{}) {
		t.Fatalf("get of an unwritten stream: %+v at version %d, %v; want the zero state",
			read.State(), read.Version(), err)
	}
	stored, _, err := backend.QueryState(ctx, orderID)
	requireNoError(t, err, "read the stored state")
	if stored.Version != 0 {
		t.Fatalf("a state was stored at version %d for an unwritten stream", stored.Version)
	}
}

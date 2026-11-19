// SPDX-FileCopyrightText: Copyright (c) 2026 The journal Authors
// SPDX-License-Identifier: Apache-2.0

package journaltest

import (
	"context"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"slices"
	"sync"
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

// order is the aggregate every test folds: what was placed, whether it shipped, and how many
// events it took
type order struct {
	Amount int    `json:"amount"`
	Status string `json:"status"`
	Events int    `json:"events"`
}

// errNegativeAmount is the aggregate refusing an event
var errNegativeAmount = errors.New("an amount cannot be negative")

// errNotPlaced is the aggregate refusing to ship an order not placed
var errNotPlaced = errors.New("the order is not placed")

// errBrokerDown is a publisher's failure
var errBrokerDown = errors.New("broker down")

// placed adds to an order, with a note to make its payload as long as a test needs
type placed struct {
	Amount int    `json:"amount"`
	Note   string `json:"note,omitempty"`
}

func (p *placed) EventType() string {
	return "PLACED"
}

func (p *placed) DataFormat() codec.DataFormat {
	return codec.JSON
}

func (p *placed) MarshalPayload() ([]byte, error) {
	return json.Marshal(p)
}

func (p *placed) UnmarshalPayload(encoded []byte) error {
	return json.Unmarshal(encoded, p)
}

// shipped ships a placed order
type shipped struct{}

func (s *shipped) EventType() string {
	return "SHIPPED"
}

func (s *shipped) DataFormat() codec.DataFormat {
	return codec.JSON
}

func (s *shipped) MarshalPayload() ([]byte, error) {
	return json.Marshal(s)
}

func (s *shipped) UnmarshalPayload(encoded []byte) error {
	return json.Unmarshal(encoded, s)
}

// marked is an event whose payload marshals to nothing, as an empty protobuf message does
type marked struct{}

func (m *marked) EventType() string {
	return "MARKED"
}

func (m *marked) DataFormat() codec.DataFormat {
	return codec.Proto
}

func (m *marked) MarshalPayload() ([]byte, error) {
	return nil, nil
}

func (m *marked) UnmarshalPayload(encoded []byte) error {
	if len(encoded) != 0 {
		return fmt.Errorf("a marked event has no payload, got %d bytes", len(encoded))
	}
	return nil
}

// annotated notes an order, in XML
type annotated struct {
	XMLName xml.Name `xml:"annotated"`
	Text    string   `xml:"text"`
}

func (a *annotated) EventType() string {
	return "ANNOTATED"
}

func (a *annotated) DataFormat() codec.DataFormat {
	return codec.XML
}

func (a *annotated) MarshalPayload() ([]byte, error) {
	return xml.Marshal(a)
}

func (a *annotated) UnmarshalPayload(encoded []byte) error {
	return xml.Unmarshal(encoded, a)
}

// orderTransitions are how each event moves an order
func orderTransitions() journal.RepositoryOption[order] {
	return journal.WithTransitions(
		journal.Transition(func(state order, event *placed) (order, error) {
			if event.Amount < 0 {
				return state, errNegativeAmount
			}
			state.Amount += event.Amount
			state.Status = "PLACED"
			state.Events++
			return state, nil
		}),
		journal.Transition(func(state order, _ *shipped) (order, error) {
			if state.Status != "PLACED" {
				return state, errNotPlaced
			}
			state.Status = "SHIPPED"
			state.Events++
			return state, nil
		}),
		journal.Transition(func(state order, _ *marked) (order, error) {
			state.Events++
			return state, nil
		}),
		journal.Transition(func(state order, _ *annotated) (order, error) {
			state.Events++
			return state, nil
		}),
	)
}

// newOrders is a repository of orders over the store, with every transition
func newOrders(eventStore *journal.Store,
	options ...journal.RepositoryOption[order]) *streams.Repository[order] {
	return journal.NewRepository(eventStore, append(options, orderTransitions())...)
}

// orderCodec stores an order as JSON at the state version given
func orderCodec(stateVersion int32) streams.StateCodec[order] {
	return streams.StateCodec[order]{
		Version: stateVersion,
		Format:  codec.JSON,
		Marshal: func(state order) ([]byte, error) {
			return json.Marshal(state)
		},
		Unmarshal: func(encoded []byte) (order, error) {
			var state order
			err := json.Unmarshal(encoded, &state)
			return state, err
		},
	}
}

// publishCall is one call to a publisher: the key, and the events it reported durable
type publishCall struct {
	partitionKey string
	events       []streams.Event
}

// recordingPublisher records what it publishes, from any goroutine. Without respond every
// event is durable; with it, respond decides each call, numbered from 0, and only the events
// it reports durable are recorded.
type recordingPublisher struct {
	mutex    sync.Mutex
	attempts int
	calls    []publishCall
	respond  func(ctx context.Context, call int, events []streams.Event) (int, error)
}

func (r *recordingPublisher) PublishAll(ctx context.Context, partitionKey string,
	events []streams.Event) (int, error) {
	r.mutex.Lock()
	call := r.attempts
	r.attempts++
	r.mutex.Unlock()

	durable, err := len(events), error(nil)
	if r.respond != nil {
		durable, err = r.respond(ctx, call, events)
	}
	r.mutex.Lock()
	defer r.mutex.Unlock()
	if durable > 0 {
		r.calls = append(r.calls, publishCall{
			partitionKey: partitionKey,
			events:       slices.Clone(events[:durable]),
		})
	}
	return durable, err
}

// published is every durable event, in the order published
func (r *recordingPublisher) published() []streams.Event {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	var events []streams.Event
	for _, call := range r.calls {
		events = append(events, call.events...)
	}
	return events
}

// versions is the version of every durable event, in the order published
func (r *recordingPublisher) versions() []int32 {
	var versions []int32
	for _, event := range r.published() {
		versions = append(versions, event.Version)
	}
	return versions
}

// keys is the key of every call that published anything
func (r *recordingPublisher) keys() []string {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	keys := make([]string, 0, len(r.calls))
	for _, call := range r.calls {
		keys = append(keys, call.partitionKey)
	}
	return keys
}

// batchSizes is how many events each call that published anything carried
func (r *recordingPublisher) batchSizes() []int {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	sizes := make([]int, 0, len(r.calls))
	for _, call := range r.calls {
		sizes = append(sizes, len(call.events))
	}
	return sizes
}

// attemptCount is how many calls the publisher was given, durable or not
func (r *recordingPublisher) attemptCount() int {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	return r.attempts
}

// failFirst refuses the first call and takes every later one
func failFirst(_ context.Context, call int, events []streams.Event) (int, error) {
	if call == 0 {
		return 0, errBrokerDown
	}
	return len(events), nil
}

// failAlways refuses every call
func failAlways(context.Context, int, []streams.Event) (int, error) {
	return 0, errBrokerDown
}

// failures records what Hooks.Failed reports, from any goroutine
type failures struct {
	mutex  sync.Mutex
	errors []error
}

func (f *failures) hook(_ uuid.UUID, err error) {
	f.mutex.Lock()
	defer f.mutex.Unlock()
	f.errors = append(f.errors, err)
}

func (f *failures) reported() []error {
	f.mutex.Lock()
	defer f.mutex.Unlock()
	return slices.Clone(f.errors)
}

// seededEvent is a PLACED event as the library stores it uncompressed, for a test that writes
// through the backend what a crash or an earlier writer left
func seededEvent(streamID uuid.UUID, version int32,
	partitionKey string) persistence.PersistedEvent {
	return persistence.PersistedEvent{
		StreamID:        streamID,
		Version:         version,
		EventType:       "PLACED",
		EventData:       []byte(`{"amount":1}`),
		DataFormat:      codec.JSON,
		DataCompression: compress.None,
		DataEncryption:  encrypt.None,
		PartitionKey:    partitionKey,
	}
}

// seed writes PLACED events of the versions straight to the backend, appended at appendedAt,
// with outbox rows when published is set, as a writer that crashed before publishing leaves
// them
func seed(t *testing.T, backend persistence.Backend, streamID uuid.UUID, appendedAt time.Time,
	published bool, partitionKey string, versions ...int32) {
	t.Helper()

	seeded := make([]persistence.PersistedEvent, 0, len(versions))
	for _, version := range versions {
		seeded = append(seeded, seededEvent(streamID, version, partitionKey))
	}
	_, err := backend.Append(context.Background(), persistence.AppendRequest{
		StreamID: streamID,
		Events:   seeded,
		AppendTs: appendedAt.UTC().Truncate(time.Microsecond),
		Publish:  published,
	})
	requireNoError(t, err, "seed versions %v", versions)
}

// minutesAgo is a time the given minutes back, as the library stores it
func minutesAgo(minutes int) time.Time {
	return time.Now().Add(-time.Duration(minutes) * time.Minute).UTC().Truncate(time.Microsecond)
}

// storedEvents is every stored event of the stream, as the backend holds it
func storedEvents(t *testing.T, backend persistence.Backend,
	streamID uuid.UUID) []persistence.PersistedEvent {
	t.Helper()

	stored, err := backend.QueryStream(context.Background(), streamID, 1, 1_000_000)
	requireNoError(t, err, "read stream %s from the backend", streamID)
	return stored
}

// requirePending fails unless the stream's unpublished versions are want
func requirePending(t *testing.T, backend persistence.Backend, streamID uuid.UUID,
	want ...int32) {
	t.Helper()

	pending, err := backend.QueryPending(context.Background(), streamID)
	requireNoError(t, err, "read the pending versions of stream %s", streamID)
	if !slices.Equal(pending, want) && (len(pending) != 0 || len(want) != 0) {
		t.Fatalf("pending versions %v, want %v", pending, want)
	}
}

// requireHead fails unless the stream's last stored version is want
func requireHead(t *testing.T, backend persistence.Backend, streamID uuid.UUID, want int32) {
	t.Helper()

	head, err := backend.QueryHead(context.Background(), streamID)
	requireNoError(t, err, "read the head of stream %s", streamID)
	if head != want {
		t.Fatalf("stream %s is at version %d, want %d", streamID, head, want)
	}
}

// requireVersions fails unless got is want, naming what was compared
func requireVersions(t *testing.T, what string, got []int32, want ...int32) {
	t.Helper()

	if !slices.Equal(got, want) && (len(got) != 0 || len(want) != 0) {
		t.Fatalf("%s: versions %v, want %v", what, got, want)
	}
}

// requireError fails unless err is want
func requireError(t *testing.T, err error, want error, action string, arguments ...any) {
	t.Helper()

	if !errors.Is(err, want) {
		t.Fatalf("%s: got %v, want %v", fmt.Sprintf(action, arguments...), err, want)
	}
}

func requireNoError(t *testing.T, err error, action string, arguments ...any) {
	t.Helper()

	if err != nil {
		t.Fatalf("%s: %v", fmt.Sprintf(action, arguments...), err)
	}
}

// eventually waits up to ten seconds for isDone, for what runs on the sweep's own goroutine
func eventually(t *testing.T, what string, isDone func() bool) {
	t.Helper()

	deadline := time.Now().Add(10 * time.Second)
	for !isDone() {
		if time.Now().After(deadline) {
			t.Fatalf("%s did not happen within ten seconds", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// storeWith is a store over the backend publishing to publisher, with the failures reported
func storeWith(backend persistence.Backend, publisher publish.Publisher, failed *failures,
	options ...journal.StoreOption) *journal.Store {
	hooks := publish.Hooks{}
	if failed != nil {
		hooks.Failed = failed.hook
	}
	options = append([]journal.StoreOption{
		journal.WithPublisher(publisher),
		journal.WithHooks(hooks),
	}, options...)
	return journal.NewStore(backend, options...)
}

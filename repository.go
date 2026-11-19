// SPDX-FileCopyrightText: Copyright (c) 2026 The journal Authors
// SPDX-License-Identifier: Apache-2.0

package journal

import (
	"errors"
	"fmt"
	"reflect"

	"github.com/llingr/journal/internal/ports"
	"github.com/llingr/journal/streams"
)

// ErrNoTransition is an event of a type the repository has no transition for. Get returns it
// as well as Append, so a transition for a retired event type is kept while streams hold it.
var ErrNoTransition = errors.New("no transition for the event type")

// RepositoryOption configures the streams.Repository NewRepository returns
type RepositoryOption[S any] func(settings *repositorySettings[S])

type repositorySettings[S any] struct {
	transitions  []StateTransition[S]
	partitionKey func(state streams.Stream[S]) string
	stateCodec   *streams.StateCodec[S]
}

// StateTransition is how one type of event moves the state, built by Transition
type StateTransition[S any] struct {
	eventType      string
	newDomainEvent func() streams.DomainEvent
	apply          func(state S, payload streams.DomainEvent) (S, error)
}

// Transition is how events of type E move the state S, both read from the function, as
//
//	journal.Transition(func(order Order, shipped *OrderShipped) (Order, error) { ... })
//
// E is a pointer to the event's struct; panics otherwise, as that is a wiring mistake.
func Transition[S any, E streams.DomainEvent](
	apply func(state S, event E) (S, error)) StateTransition[S] {
	eventPointer := reflect.TypeFor[E]()
	if eventPointer.Kind() != reflect.Pointer {
		panic(fmt.Sprintf("journal.Transition needs a pointer event type, got %s", eventPointer))
	}
	newDomainEvent := func() streams.DomainEvent {
		return reflect.New(eventPointer.Elem()).Interface().(E)
	}
	return StateTransition[S]{
		eventType:      newDomainEvent().EventType(),
		newDomainEvent: newDomainEvent,
		apply: func(state S, payload streams.DomainEvent) (S, error) {
			return apply(state, payload.(E))
		},
	}
}

// WithTransitions gives the repository its transitions, one per event type; an event of
// another type is ErrNoTransition. Panics on two for one event type.
func WithTransitions[S any](transitions ...StateTransition[S]) RepositoryOption[S] {
	return func(settings *repositorySettings[S]) {
		settings.transitions = append(settings.transitions, transitions...)
	}
}

// WithPartitionKey publishes a stream's events under the key chosen from the stream, with
// the new events' state, rather than under the stream id; an empty key fails the write
// before anything is saved. A stream's key must never change, as a destination such as Kafka
// keeps order only within one key.
func WithPartitionKey[S any](
	partitionKey func(stream streams.Stream[S]) string) RepositoryOption[S] {
	return func(settings *repositorySettings[S]) {
		settings.partitionKey = partitionKey
	}
}

// WithSnapshot stores each stream's state beside its events, rewritten in the transaction of
// every append, so Get reads one row rather than folding the stream:
//
//	journal.WithSnapshot(streams.StateCodec[Order]{
//		Version:   1,
//		Format:    formatjson.Format,
//		Marshal:   func(order Order) ([]byte, error) { return formatjson.Marshal(order) },
//		Unmarshal: func(encoded []byte) (Order, error) { ... },
//	})
//
// Increase the codec's Version whenever the transitions or the state's form change: Get then
// folds each stream again from its events the first time it reads it, and stores the result.
// Panics on a version below 1, an unknown format or a missing function.
func WithSnapshot[S any](stateCodec streams.StateCodec[S]) RepositoryOption[S] {
	return func(settings *repositorySettings[S]) {
		settings.stateCodec = &stateCodec
	}
}

// NewRepository returns the repository of one aggregate over the store. Without transitions
// its state never changes, for a host whose state lives elsewhere and that appends from Head,
// as journal.NewRepository[struct{}](eventStore); Get on it fails with ErrNoTransition at the
// first stored event.
func NewRepository[S any](store *Store, options ...RepositoryOption[S]) *streams.Repository[S] {
	if store == nil {
		panic("journal.NewRepository needs a store")
	}
	var settings repositorySettings[S]
	for _, option := range options {
		option(&settings)
	}

	byEventType := make(map[string]StateTransition[S], len(settings.transitions))
	for _, transition := range settings.transitions {
		_, repeated := byEventType[transition.eventType]
		if repeated {
			panic(fmt.Errorf("journal.WithTransitions: %w: %s",
				streams.ErrRepeatedEventType, transition.eventType))
		}
		byEventType[transition.eventType] = transition
	}

	newDomainEvent := func(eventType string) (streams.DomainEvent, error) {
		transition, found := byEventType[eventType]
		if !found {
			return nil, fmt.Errorf("%w: %s", ErrNoTransition, eventType)
		}
		return transition.newDomainEvent(), nil
	}
	apply := func(state S, payload streams.DomainEvent) (S, error) {
		if len(byEventType) == 0 {
			return state, nil
		}
		transition, found := byEventType[payload.EventType()]
		if !found {
			return state, fmt.Errorf("%w: %s", ErrNoTransition, payload.EventType())
		}
		return transition.apply(state, payload)
	}
	// a nil outbox, not a typed nil, so the repository knows nothing is published
	var committed ports.Outbox
	if store.outbox != nil {
		committed = store.outbox
	}
	return streams.NewRepository(store.backend, store.pipeline, committed, newDomainEvent, apply,
		settings.partitionKey, settings.stateCodec)
}

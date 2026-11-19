// SPDX-FileCopyrightText: Copyright (c) 2026 The journal Authors
// SPDX-License-Identifier: Apache-2.0

package journal_test

import (
	"testing"
	"time"

	"github.com/llingr/journal"
	"github.com/llingr/journal/codec"
	"github.com/llingr/journal/internal/memory"
)

// TestWiringMistakesStopAtConstruction: a wiring mistake panics where it is written, not at
// the first call
func TestWiringMistakesStopAtConstruction(t *testing.T) {
	store := journal.NewStore(memory.NewBackend())
	mistakes := map[string]func(){
		"no backend": func() {
			journal.NewStore(nil)
		},
		"no store": func() {
			journal.NewRepository[struct{}](nil)
		},
		"an unknown compression": func() {
			journal.WithDataCompression("brotli")
		},
		"a compression minimum of 0 bytes": func() {
			journal.WithCompressionMinimumBytes(0)
		},
		"a compression minimum over 2048 bytes": func() {
			journal.WithCompressionMinimumBytes(2049)
		},
		"a batch size of 0": func() {
			journal.WithPublishBatchSize(0)
		},
		"a publish timeout over a minute": func() {
			journal.WithPublishTimeout(time.Minute + time.Second)
		},
		"a sweep interval under fifteen seconds": func() {
			journal.WithSweepInterval(14 * time.Second)
		},
		"two transitions for one event": func() {
			journal.NewRepository(store, journal.WithTransitions(
				journal.Transition(func(state struct{}, _ *pointerEvent) (struct{}, error) {
					return state, nil
				}),
				journal.Transition(func(state struct{}, _ *pointerEvent) (struct{}, error) {
					return state, nil
				}),
			))
		},
		"an event type that is not a pointer": func() {
			journal.Transition(func(state struct{}, _ valueEvent) (struct{}, error) {
				return state, nil
			})
		},
	}
	for name, mistake := range mistakes {
		t.Run(name, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Fatal("did not panic")
				}
			}()
			mistake()
		})
	}
}

// pointerEvent implements DomainEvent on its pointer, as a transition needs
type pointerEvent struct{}

func (*pointerEvent) EventType() string {
	return "POINTER"
}

func (*pointerEvent) DataFormat() codec.DataFormat {
	return codec.JSON
}

func (*pointerEvent) MarshalPayload() ([]byte, error) {
	return []byte(`{}`), nil
}

func (*pointerEvent) UnmarshalPayload([]byte) error {
	return nil
}

// valueEvent implements DomainEvent on its value, which a transition cannot decode into
type valueEvent struct{}

func (valueEvent) EventType() string {
	return "VALUE"
}

func (valueEvent) DataFormat() codec.DataFormat {
	return codec.JSON
}

func (valueEvent) MarshalPayload() ([]byte, error) {
	return []byte(`{}`), nil
}

func (valueEvent) UnmarshalPayload([]byte) error {
	return nil
}

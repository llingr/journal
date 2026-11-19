// SPDX-FileCopyrightText: Copyright (c) 2026 The journal Authors
// SPDX-License-Identifier: Apache-2.0

package streams_test

import (
	"fmt"
	"reflect"

	"github.com/llingr/journal/streams"
)

// domainEvents is the lookup journal.New builds from its transitions, here from empty
// prototypes of each domain event type
func domainEvents(prototypes ...streams.DomainEvent) func(string) (streams.DomainEvent, error) {
	typeByEventType := make(map[string]reflect.Type, len(prototypes))
	for _, prototype := range prototypes {
		typeByEventType[prototype.EventType()] = reflect.TypeOf(prototype).Elem()
	}
	return func(eventType string) (streams.DomainEvent, error) {
		domainType, found := typeByEventType[eventType]
		if !found {
			return nil, fmt.Errorf("no domain event type for %s", eventType)
		}
		return reflect.New(domainType).Interface().(streams.DomainEvent), nil
	}
}

// SPDX-FileCopyrightText: Copyright (c) 2026 The journal Authors
// SPDX-License-Identifier: Apache-2.0

package streams

// WithMetadata attaches metadata to one event, such as a trace id to send as Kafka headers:
//
//	orders.Append(ctx, order, streams.WithMetadata(&OrderShipped{}, headers))
//
// The library stores the bytes with the event and returns them unread in Event.Metadata, so
// their encoding is the application's. A transition still receives the event itself.
func WithMetadata(event DomainEvent, metadata []byte) DomainEvent {
	return eventWithMetadata{
		DomainEvent: event,
		metadata:    metadata,
	}
}

type eventWithMetadata struct {
	DomainEvent
	metadata []byte
}

// withoutMetadata returns the event WithMetadata wrapped, and its metadata, nil for none;
// for an event wrapped twice the outer metadata wins
func withoutMetadata(event DomainEvent) (DomainEvent, []byte) {
	var metadata []byte
	wrapped, isWrapped := event.(eventWithMetadata)
	for isWrapped {
		if metadata == nil {
			metadata = wrapped.metadata
		}
		event = wrapped.DomainEvent
		wrapped, isWrapped = event.(eventWithMetadata)
	}
	return event, metadata
}

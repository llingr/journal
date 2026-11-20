# journal

journal is an event-sourcing library for Go with a transactional outbox. Each aggregate, such
as an order, is stored as the stream of events that changed it, and its state is folded from
those events. Every event is published once it is committed, to Kafka or anywhere else, with
no two-phase commit between the database and the broker.

What it guarantees:

- An append succeeds only from the version the writer read. A writer that lost a race gets
  `streams.ErrVersionConflict`, reads the stream again and retries; no event is overwritten.
- An event is published only after it is committed, and a stream's events are published in
  version order.
- Publishing is at least once. A publish that fails, or never runs because the process
  crashed, is retried by the stream's next append and by a sweep at start and every three
  hours, so a consumer can see an event twice.
- A failed publish never fails or undoes the write that committed the event.

The cost is an outbox row per published event until it is sent, and consumers that ignore an
event they have already seen.

Until version 1, journal's API can change between minor versions.

## Install

```sh
go get github.com/llingr/journal
go get github.com/llingr/journal-postgres
```

It needs Go 1.26 or later. Each database is a backend in a module of its own, so an
application depends only on the one it uses; journal-postgres is the Postgres backend.

## Quick Start

[journal-postgres's `docs/examples/quickstart`](https://github.com/llingr/journal-postgres/tree/main/docs/examples/quickstart)
is a complete program: it applies the schema, then creates an order, ships it and reads it
back, publishing each event to the log. Its parts:

```go
// Order is the state an order's events fold to
type Order struct {
	Amount  int
	Shipped bool
}

// OrderPlaced is the event that starts an order; OrderShipped is written the same way
type OrderPlaced struct {
	Amount int `json:"amount"`
}

func (e *OrderPlaced) EventType() string {
	return "ORDER_PLACED"
}

func (e *OrderPlaced) DataFormat() codec.DataFormat {
	return codec.JSON
}

func (e *OrderPlaced) MarshalPayload() ([]byte, error) {
	return json.Marshal(e)
}

func (e *OrderPlaced) UnmarshalPayload(encoded []byte) error {
	return json.Unmarshal(encoded, e)
}
```

```go
eventStore := journal.NewStore(postgres.NewBackend(appPool),
	journal.WithPublisher(logPublisher{}))
eventStore.Start(ctx)
defer eventStore.Shutdown(ctx)

orders := journal.NewRepository(eventStore, journal.WithTransitions(
	journal.Transition(func(order Order, placed *OrderPlaced) (Order, error) {
		order.Amount = placed.Amount
		return order, nil
	}),
	journal.Transition(func(order Order, _ *OrderShipped) (Order, error) {
		if order.Shipped {
			return order, errors.New("the order has shipped already")
		}
		order.Shipped = true
		return order, nil
	}),
))

orderID := uuid.New()
order, err := orders.Create(ctx, orderID, &OrderPlaced{Amount: 120})
_, err = orders.Append(ctx, order, &OrderShipped{})
order, err = orders.Get(ctx, orderID)
```

To run it, create its two roles and database as journal-postgres's README shows, then from a
checkout of journal-postgres:

```sh
OWNER_DATABASE_URL=postgres://journal_owner:owner-password@localhost:5432/orders \
APP_DATABASE_URL=postgres://journal_app:app-password@localhost:5432/orders \
go run ./docs/examples/quickstart
```

## Events, Repositories and Streams

**An event** is a struct implementing `streams.DomainEvent`. `EventType` names it and is stored
with every event, so it must never change: a stored event whose type no transition names fails
`Get` with `journal.ErrNoTransition`. `DataFormat`, `MarshalPayload` and `UnmarshalPayload` say
how its payload is stored; the payload is compressed with zstd from 128 bytes unless the store
is set otherwise.

**A store** is built once per database, with `journal.NewStore`, and **a repository** once per
aggregate, with `journal.NewRepository`. A repository's transitions say how each event type
moves the state. A transition that returns an error refuses the event, and nothing of that
write is saved. Keep a transition for every event type the streams hold, including retired
ones. The store and its repositories are safe for concurrent use.

**A stream** is what each call returns, a `streams.Stream[Order]`. `State()` is the order
folded to `Version()`, `DomainEvents()` are the events read or written as your types, and
`Events()` are the same events with their stored payloads.

| Call | Does |
|---|---|
| `Create`, `CreateAll` | start a stream with one event or several |
| `Append`, `AppendAll` | add events to a stream you have read |
| `Get` | read a stream at its last version; an unwritten one is the zero state at version 0 |
| `Head` | read only the last version, for an application whose state lives elsewhere |

An `Append` from a stream another writer has appended to since you read it returns
`streams.ErrVersionConflict`: read the stream again and retry.

## Publishing

`journal.WithPublisher` takes your implementation of `publish.Publisher`. Without one, events
are saved and never published.

```go
type Publisher interface {
	// PublishAll sends one stream's events, in order, under partitionKey. A nil error means
	// all are durable; with an error, the count is how many, from the first, are durable.
	PublishAll(ctx context.Context, partitionKey string, events []streams.Event) (int, error)
}
```

Each `streams.Event` carries the stream id, version, event type, data format, append time,
payload and metadata, which map onto a Kafka record's key, value and headers. `PublishAll` is
called from every goroutine that appends and from the sweep, so it must be safe for
concurrent use.

Events go under their stream's id unless the repository chooses a partition key from the
stream. A stream's key must not change, as Kafka keeps order only within a key, and an empty
key fails the write with `streams.ErrNoPartitionKey` before anything is saved:

```go
orders := journal.NewRepository(eventStore, journal.WithTransitions(...),
	journal.WithPartitionKey(func(order streams.Stream[Order]) string {
		return order.State().CustomerID
	}),
)
```

`journal.WithHooks` reports each publish, each failure and each sweep that found work, for
your logs and alerts; the library itself never logs.

## Options

| Store option | Default | Range |
|---|---|---|
| `WithPublisher(publisher)` | none: events are saved, not published | |
| `WithDataCompression(compression)` | `compress.Zstd` | `Zstd`, `Gzip`, `LZ4`, `Snappy`, `None` |
| `WithCompressionMinimumBytes(bytes)`, the smallest payload compressed | 128 | 1 to 2048 |
| `WithCompressionConcurrency(calls)`, the payloads each of gzip, lz4 and zstd compresses or decompresses at once | 16 | 1 to 256 |
| `WithPublishTimeout(timeout)`, for each call to the publisher | 10 seconds | 1 second to 1 minute |
| `WithPublishBatchSize(size)`, the most events in one call | 25 | 1 to 1000 |
| `WithSweepInterval(interval)`, how often failed publishes are retried | 3 hours | 15 seconds to 1 week |
| `WithHooks(publish.Hooks{...})`, observing publishing | none | |

The compressions are in `github.com/llingr/journal/codec/compress`. An option outside its range
panics when the store is built, as does any other wiring mistake, such as two transitions for
one event type.

## Optional Features

### Stored State

With `WithSnapshot`, every write also stores the state it folds to, in the same transaction,
and `Get` reads that one row instead of every event:

```go
orders := journal.NewRepository(eventStore,
	journal.WithTransitions(...),
	journal.WithSnapshot(streams.StateCodec[Order]{
		Version: 1,
		Format:  codec.JSON,
		Marshal: func(order Order) ([]byte, error) {
			return json.Marshal(order)
		},
		Unmarshal: func(encoded []byte) (Order, error) {
			var order Order
			err := json.Unmarshal(encoded, &order)
			return order, err
		},
	}),
)
```

Increase `Version` whenever the transitions or `Order` change. Each stream is then folded from
its events again the first time it is read, and the result is stored. A stream read from its
stored state has no `Events()`; `LastEventType()` and `LastAppendTs()` describe its last event.

### Metadata

`streams.WithMetadata` stores bytes with one event, such as trace headers for its Kafka record.
They are stored as given and returned in `Event.Metadata`:

```go
order, err = orders.Append(ctx, order, streams.WithMetadata(&OrderShipped{}, headers))
```

### Code After a Commit

`streams.AfterCommit` runs a function once the write has committed and its events have gone to
the publisher, such as one invalidating a cache. It never runs for a write that failed:

```go
order, err = orders.Append(ctx, order, &OrderShipped{}, streams.AfterCommit(invalidate))
```

### Sweeps and Republishing

The sweep publishes whatever earlier publishes left behind. It runs at start and then every
sweep interval, over events appended in the last three intervals. `eventStore.Sweep(ctx,
window)` reaches further back by hand, such as after a broker outage longer than that. Neither
touches an event appended in the last minute, which the write that appended it may still be
publishing.

To send events again, such as to a consumer that lost them:

```go
sent, err := eventStore.RepublishEvents(ctx, streamID, fromVersion, publish.Always)
err = eventStore.RepublishEvent(ctx, streamID, version, publish.IfUnpublished)
```

`publish.Always` sends the versions whether or not they were published; `RepublishEvents`
sends from `fromVersion` to the stream's last and returns how many are durable.
`publish.IfUnpublished` sends only versions not yet published. Either sends each event under
the partition key it was saved with, and neither sends a version while an earlier one is
unpublished, so a stream's events never overtake each other.

An event the publisher always rejects holds back every later event of its stream until its
outbox row is deleted; journal-postgres's README gives the SQL.

## Errors

| Error | Returned by | What to do |
|---|---|---|
| `streams.ErrVersionConflict` | `Append`, `AppendAll` | read the stream again and retry |
| `streams.ErrStreamExists` | `Create`, `CreateAll` | the stream exists; use `Get` and `Append` |
| `streams.ErrStreamNotCreated` | `Append`, `AppendAll` | the stream has no events; use `Create` |
| `streams.ErrStreamNotFolded` | `Append`, `AppendAll` | the stream was read by `Head`; use `Get` |
| `streams.ErrNoEvents` | `CreateAll`, `AppendAll` | pass at least one event |
| `streams.ErrNoPartitionKey` | any write | the partition key function returned "" |
| `streams.ErrVersionMissing` | `Get`, republishing | a version was deleted from the stream |
| `streams.ErrNotPermitted` | any call | the database role lacks a privilege |
| `journal.ErrNoTransition` | `Get`, any write | add a transition for the event type |
| `journal.ErrNoPublisher` | republishing | build the store `WithPublisher` |
| `publish.ErrEarlierUnpublished` | republishing | publish the versions it names first |
| `publish.ErrAlreadyPublished` | `RepublishEvent` | use `publish.Always` to send it again |
| `publish.ErrPublisherPanic` | `Hooks.Failed` | your publisher panicked; fix it |

## Other Databases

A backend implements `persistence.Backend` and passes two suites: the contract every backend
keeps, from `persistence/backendtest`, and everything journal promises an application, run
over that backend, from `journaltest`:

```go
func TestBackend(t *testing.T) {
	newTestBackend := func(t *testing.T) persistence.Backend {
		return newBackend(t)
	}
	t.Run("Contract", func(t *testing.T) {
		backendtest.RunContract(t, newTestBackend)
	})
	t.Run("Library", func(t *testing.T) {
		journaltest.RunSuite(t, newTestBackend)
	})
}
```

journal-postgres is the backend to follow.

## Tests

```sh
make test
make lint
```

Both suites run over a backend in memory, so the tests need no database; journal-postgres runs
them over Postgres.

## More Examples

- [journal-postgres's `docs/examples/accounts-service`](https://github.com/llingr/journal-postgres/tree/main/docs/examples/accounts-service):
  a complete HTTP service on journal, anvil and llingr-pgx
- `docs/examples/codec`: the `codec` packages on their own, including encryption, which the
  store does not offer

## Licence

Apache License, Version 2.0. See LICENSE, COPYRIGHT and AUTHORS.

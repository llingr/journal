// SPDX-FileCopyrightText: Copyright (c) 2026 The journal Authors
// SPDX-License-Identifier: Apache-2.0

// Package journal is event sourcing for Go. A Store is one database's events, over a backend
// such as journal-postgres, with the publisher every committed event goes to; a Repository
// creates, appends to and folds the streams of one aggregate through its typed transitions.
//
//	eventStore := journal.NewStore(postgres.NewBackend(appPool),
//		journal.WithPublisher(kafkaPublisher))
//	orders := journal.NewRepository(eventStore, journal.WithTransitions(
//		journal.Transition(func(order Order, placed *OrderPlaced) (Order, error) { ... }),
//	))
//	order, err := orders.Create(ctx, orderID, &OrderPlaced{...})
package journal

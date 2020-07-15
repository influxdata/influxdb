// package kv
//
// The KV package is a set of services and abstractions built around key value storage.
// There exist in-memory and persisted implementations of the core `Store` family of
// interfaces outside of this package (see `inmem` and `bolt` packages).
//
// The `Store` interface exposes transactional access to a backing kv persistence layer.
// It allows for read-only (View) and read-write (Update) transactions to be opened.
// These methods take a function which is passed an implementation of the transaction interface (Tx).
// This interface exposes a way to manipulate namespaced keys and values (Buckets).
//
// All keys and values are namespaced (grouped) using buckets. Buckets can only be created on
// implementations of the `SchemaStore` interface. This is a superset of the `Store` interface,
// which has the additional bucket creation and deletion methods.
//
// Bucket creation and deletion should be facilitated via a migration (see `kv/migration`).
package kv

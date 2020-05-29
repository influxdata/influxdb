package influxdb

import (
	"context"
	"time"
)

// KeyValueLog is a generic type logs key-value pairs. This interface is intended to be used to construct other
// higher-level log-like resources such as an oplog or audit log.
//
// The idea is to create a log who values can be accessed at the key k:
// k -> [(v0,t0) (v1,t1) ... (vn,tn)]
//
// Logs may be retrieved in ascending or descending time order and support limits and offsets.
type KeyValueLog interface {
	// AddLogEntry adds an entry (v,t) to the log defined for the key k.
	AddLogEntry(ctx context.Context, k []byte, v []byte, t time.Time) error

	// ForEachLogEntry iterates through all the log entries at key k and applies the function fn for each record.
	ForEachLogEntry(ctx context.Context, k []byte, opts FindOptions, fn func(v []byte, t time.Time) error) error

	// FirstLogEntry is used to retrieve the first entry in the log at key k.
	FirstLogEntry(ctx context.Context, k []byte) ([]byte, time.Time, error)

	// LastLogEntry is used to retrieve the last entry in the log at key k.
	LastLogEntry(ctx context.Context, k []byte) ([]byte, time.Time, error)
}

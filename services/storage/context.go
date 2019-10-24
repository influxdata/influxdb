package storage

import (
	"context"
)

type key int

const (
	readOptionsKey key = iota
)

// ReadOptions are additional options that may be passed with context.Context
// to configure the behavior of a storage read request.
type ReadOptions struct {
	NodeID uint64
}

// NewContextWithRequestOptions returns a new Context with nodeID added.
func NewContextWithReadOptions(ctx context.Context, opts *ReadOptions) context.Context {
	return context.WithValue(ctx, readOptionsKey, opts)
}

// ReadOptionsFromContext returns the ReadOptions associated with the context
// or nil if no additional options have been specified.
func ReadOptionsFromContext(ctx context.Context) *ReadOptions {
	opts, _ := ctx.Value(readOptionsKey).(*ReadOptions)
	return opts
}

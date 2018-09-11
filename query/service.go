package query

import (
	"context"
	"io"

	"github.com/influxdata/flux"
)

// QueryService represents a type capable of performing queries.
type QueryService interface {
	// Query submits a query for execution returning a results iterator.
	// Cancel must be called on any returned results to free resources.
	Query(ctx context.Context, req *Request) (flux.ResultIterator, error)
}

// AsyncQueryService represents a service for performing queries where the results are delivered asynchronously.
type AsyncQueryService interface {
	// Query submits a query for execution returning immediately.
	// Done must be called on any returned Query objects.
	Query(ctx context.Context, req *Request) (flux.Query, error)
}

// ProxyQueryService performs queries and encodes the result into a writer.
// The results are opaque to a ProxyQueryService.
type ProxyQueryService interface {
	// Query performs the requested query and encodes the results into w.
	// The number of bytes written to w is returned __independent__ of any error.
	Query(ctx context.Context, w io.Writer, req *ProxyRequest) (int64, error)
}

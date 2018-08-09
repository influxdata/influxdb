package platform

import (
	"context"
	"io"
)

// TODO(desa): These files are possibly a temporary. This is needed
// as a part of the source work that is being done.
// See https://github.com/influxdata/platform/issues/594 for more info.

// SourceQuery is a query for a source.
type SourceQuery struct {
	Query string `json:"query"`
	Type  string `json:"type"`
}

// SourceQueryResult is a result of a source query.
type SourceQueryResult struct {
	Reader io.Reader
}

// SourceQuerier allows for the querying of sources.
type SourceQuerier interface {
	Query(ctx context.Context, q *SourceQuery) (*SourceQueryResult, error)
}

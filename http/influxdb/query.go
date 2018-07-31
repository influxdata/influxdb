package influxdb

import (
	"bytes"
	"context"
	"fmt"

	"github.com/influxdata/platform"
	"github.com/influxdata/platform/chronograf"
)

// SourceQuerier connects to Influx via HTTP using tokens to manage buckets
type SourceQuerier struct {
	Source *platform.Source
}

func (s *SourceQuerier) Query(ctx context.Context, q *platform.SourceQuery) (*platform.SourceQueryResult, error) {
	switch q.Type {
	case "influxql":
		return s.influxQuery(ctx, q)
	case "flux":
		return s.fluxQuery(ctx, q)
	}

	return nil, fmt.Errorf("unsupport language %v", q.Type)
}

func (s *SourceQuerier) influxQuery(ctx context.Context, q *platform.SourceQuery) (*platform.SourceQueryResult, error) {
	c, err := newClient(s.Source)
	if err != nil {
		return nil, err
	}

	query := chronograf.Query{
		Command: q.Query,
		// TODO(specify database)
	}

	res, err := c.Query(ctx, query)
	if err != nil {
		return nil, err
	}

	b, err := res.MarshalJSON()
	if err != nil {
		return nil, err
	}

	return &platform.SourceQueryResult{
		Reader: bytes.NewReader(b),
	}, nil
}

func (s *SourceQuerier) fluxQuery(ctx context.Context, q *platform.SourceQuery) (*platform.SourceQueryResult, error) {
	panic("not implemented")
}

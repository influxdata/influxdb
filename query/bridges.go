package query

import (
	"context"
	"io"

	"github.com/influxdata/flux"
)

// QueryServiceBridge implements the QueryService interface while consuming the AsyncQueryService interface.
type QueryServiceBridge struct {
	AsyncQueryService AsyncQueryService
}

func (b QueryServiceBridge) Query(ctx context.Context, req *Request) (flux.ResultIterator, error) {
	query, err := b.AsyncQueryService.Query(ctx, req)
	if err != nil {
		return nil, err
	}
	return flux.NewResultIteratorFromQuery(query), nil
}

// ProxyQueryServiceBridge implements ProxyQueryService while consuming a QueryService interface.
type ProxyQueryServiceBridge struct {
	QueryService QueryService
}

func (b ProxyQueryServiceBridge) Query(ctx context.Context, w io.Writer, req *ProxyRequest) (int64, error) {
	results, err := b.QueryService.Query(ctx, &req.Request)
	if err != nil {
		return 0, err
	}
	defer results.Cancel()
	encoder := req.Dialect.Encoder()
	return encoder.Encode(w, results)
}

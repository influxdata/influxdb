package query

import (
	"context"
	"io"

	"github.com/influxdata/flux"
	"github.com/influxdata/platform"
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

// REPLQuerier implements the repl.Querier interface while consuming a QueryService
type REPLQuerier struct {
	// Authorization is the authorization to provide for all requests
	Authorization *platform.Authorization
	// OrganizationID is the ID to provide for all requests
	OrganizationID platform.ID
	QueryService   QueryService
}

func (q *REPLQuerier) Query(ctx context.Context, compiler flux.Compiler) (flux.ResultIterator, error) {
	req := &Request{
		Authorization:  q.Authorization,
		OrganizationID: q.OrganizationID,
		Compiler:       compiler,
	}
	return q.QueryService.Query(ctx, req)
}

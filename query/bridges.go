package query

import (
	"context"
	"io"

	"github.com/influxdata/flux"
	"github.com/influxdata/flux/csv"
	platform "github.com/influxdata/influxdb"
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

// QueryServiceProxyBridge implements QueryService while consuming a ProxyQueryService interface.
type QueryServiceProxyBridge struct {
	ProxyQueryService ProxyQueryService
}

func (b QueryServiceProxyBridge) Query(ctx context.Context, req *Request) (flux.ResultIterator, error) {
	d := csv.Dialect{ResultEncoderConfig: csv.DefaultEncoderConfig()}
	preq := &ProxyRequest{
		Request: *req,
		Dialect: d,
	}

	r, w := io.Pipe()
	statsChan := make(chan flux.Statistics, 1)

	go func() {
		stats, err := b.ProxyQueryService.Query(ctx, w, preq)
		_ = w.CloseWithError(err)
		statsChan <- stats
	}()

	dec := csv.NewMultiResultDecoder(csv.ResultDecoderConfig{})
	ri, err := dec.Decode(r)
	return asyncStatsResultIterator{
		ResultIterator: ri,
		statsChan:      statsChan,
	}, err
}

type asyncStatsResultIterator struct {
	flux.ResultIterator
	statsChan chan flux.Statistics
	stats     flux.Statistics
}

func (i asyncStatsResultIterator) Release() {
	i.ResultIterator.Release()
	i.stats = <-i.statsChan
}

func (i asyncStatsResultIterator) Statistics() flux.Statistics {
	return i.stats
}

// ProxyQueryServiceAsyncBridge implements ProxyQueryService while consuming an AsyncQueryService
type ProxyQueryServiceAsyncBridge struct {
	AsyncQueryService AsyncQueryService
}

func (b ProxyQueryServiceAsyncBridge) Query(ctx context.Context, w io.Writer, req *ProxyRequest) (flux.Statistics, error) {
	q, err := b.AsyncQueryService.Query(ctx, &req.Request)
	if err != nil {
		return flux.Statistics{}, err
	}

	results := flux.NewResultIteratorFromQuery(q)
	defer results.Release()

	encoder := req.Dialect.Encoder()
	_, err = encoder.Encode(w, results)
	if err != nil {
		return flux.Statistics{}, err
	}

	stats := results.Statistics()
	return stats, nil
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

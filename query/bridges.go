package query

import (
	"bufio"
	"context"
	"io"

	platform2 "github.com/influxdata/influxdb/v2/kit/platform"

	"github.com/influxdata/flux"
	"github.com/influxdata/flux/csv"
	platform "github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kit/check"
	"github.com/influxdata/influxdb/v2/kit/tracing"
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

// Check returns the status of this query service.  Since this bridge consumes an AsyncQueryService,
// which is not available over the network, this check always passes.
func (QueryServiceBridge) Check(context.Context) check.Response {
	return check.Response{Name: "Query Service", Status: check.StatusPass}
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
	asri := &asyncStatsResultIterator{
		r:          newBufferedReadCloser(r),
		statsReady: make(chan struct{}),
	}

	go func() {
		stats, err := b.ProxyQueryService.Query(ctx, w, preq)
		_ = w.CloseWithError(err)
		asri.stats = stats
		close(asri.statsReady)
	}()

	return asri, nil
}

func (b QueryServiceProxyBridge) Check(ctx context.Context) check.Response {
	return b.ProxyQueryService.Check(ctx)
}

type asyncStatsResultIterator struct {
	flux.ResultIterator

	// The buffered reader and any error that has been
	// encountered when reading.
	r   *bufferedReadCloser
	err error

	// Channel that is closed when stats have been written.
	statsReady chan struct{}

	// Statistics gathered from calling the proxy query service.
	// This field must not be read until statsReady is closed.
	stats flux.Statistics
}

func (i *asyncStatsResultIterator) More() bool {
	if i.ResultIterator == nil {
		// Peek into the read. If there is an error
		// before reading any bytes, do not use the
		// result decoder and use the error that is
		// returned as the error for this result iterator.
		if _, err := i.r.Peek(1); err != nil {
			// Only an error if this is not an EOF.
			if err != io.EOF {
				i.err = err
			}
			return false
		}

		// At least one byte could be read so create a result
		// iterator using the reader.
		dec := csv.NewMultiResultDecoder(csv.ResultDecoderConfig{})
		ri, err := dec.Decode(i.r)
		if err != nil {
			i.err = err
			return false
		}
		i.ResultIterator = ri
	}
	return i.ResultIterator.More()
}

func (i *asyncStatsResultIterator) Err() error {
	if i.err != nil {
		return i.err
	}
	return i.ResultIterator.Err()
}

func (i *asyncStatsResultIterator) Release() {
	if i.ResultIterator != nil {
		i.ResultIterator.Release()
	}
}

func (i *asyncStatsResultIterator) Statistics() flux.Statistics {
	<-i.statsReady
	return i.stats
}

// ProxyQueryServiceAsyncBridge implements ProxyQueryService while consuming an AsyncQueryService
type ProxyQueryServiceAsyncBridge struct {
	AsyncQueryService AsyncQueryService
}

func (b ProxyQueryServiceAsyncBridge) Query(ctx context.Context, w io.Writer, req *ProxyRequest) (flux.Statistics, error) {
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	q, err := b.AsyncQueryService.Query(ctx, &req.Request)
	if err != nil {
		return flux.Statistics{}, tracing.LogError(span, err)
	}

	results := flux.NewResultIteratorFromQuery(q)
	defer results.Release()

	encoder := req.Dialect.Encoder()
	_, err = encoder.Encode(w, results)
	// Release the results and collect the statistics regardless of the error.
	results.Release()
	stats := results.Statistics()
	if err != nil {
		return stats, tracing.LogError(span, err)
	}

	if results, err := q.ProfilerResults(); err != nil {
		return stats, tracing.LogError(span, err)
	} else if results != nil {
		_, err = encoder.Encode(w, results)
		if err != nil {
			return stats, tracing.LogError(span, err)
		}
	}
	return stats, nil
}

// Check returns the status of this query service.  Since this bridge consumes an AsyncQueryService,
// which is not available over the network, this check always passes.
func (ProxyQueryServiceAsyncBridge) Check(context.Context) check.Response {
	return check.Response{Name: "Query Service", Status: check.StatusPass}
}

// REPLQuerier implements the repl.Querier interface while consuming a QueryService
type REPLQuerier struct {
	// Authorization is the authorization to provide for all requests
	Authorization *platform.Authorization
	// OrganizationID is the ID to provide for all requests
	OrganizationID platform2.ID
	QueryService   QueryService
}

// Query will pack a query to be sent to a remote server for execution.  deps may be safely ignored since
// they will be correctly initialized on the server side.
func (q *REPLQuerier) Query(ctx context.Context, deps flux.Dependencies, compiler flux.Compiler) (flux.ResultIterator, error) {
	req := &Request{
		Authorization:  q.Authorization,
		OrganizationID: q.OrganizationID,
		Compiler:       compiler,
	}
	return q.QueryService.Query(ctx, req)
}

// bufferedReadCloser is a bufio.Reader that implements io.ReadCloser.
type bufferedReadCloser struct {
	*bufio.Reader
	r io.ReadCloser
}

// newBufferedReadCloser constructs a new bufferedReadCloser.
func newBufferedReadCloser(r io.ReadCloser) *bufferedReadCloser {
	return &bufferedReadCloser{
		Reader: bufio.NewReader(r),
		r:      r,
	}
}

func (br *bufferedReadCloser) Close() error {
	return br.r.Close()
}

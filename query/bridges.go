package query

import (
	"context"
	"io"
	"sort"
)

// QueryServiceBridge implements the QueryService interface while consuming the AsyncQueryService interface.
type QueryServiceBridge struct {
	AsyncQueryService AsyncQueryService
}

func (b QueryServiceBridge) Query(ctx context.Context, req *Request) (ResultIterator, error) {
	query, err := b.AsyncQueryService.Query(ctx, req)
	if err != nil {
		return nil, err
	}
	return newResultIterator(query), nil
}

// resultIterator implements a ResultIterator while consuming a Query
type resultIterator struct {
	query   Query
	cancel  chan struct{}
	ready   bool
	results *MapResultIterator
}

func newResultIterator(q Query) *resultIterator {
	return &resultIterator{
		query:  q,
		cancel: make(chan struct{}),
	}
}

func (r *resultIterator) More() bool {
	if !r.ready {
		select {
		case <-r.cancel:
			goto DONE
		case results, ok := <-r.query.Ready():
			if !ok {
				goto DONE
			}
			r.ready = true
			r.results = NewMapResultIterator(results)
		}
	}
	if r.results.More() {
		return true
	}

DONE:
	r.query.Done()
	return false
}

func (r *resultIterator) Next() Result {
	return r.results.Next()
}

func (r *resultIterator) Cancel() {
	select {
	case <-r.cancel:
	default:
		close(r.cancel)
	}
	r.query.Cancel()
}

func (r *resultIterator) Err() error {
	return r.query.Err()
}

func (r *resultIterator) Statistics() Statistics {
	return r.query.Statistics()
}

type MapResultIterator struct {
	results map[string]Result
	order   []string
}

func NewMapResultIterator(results map[string]Result) *MapResultIterator {
	order := make([]string, 0, len(results))
	for k := range results {
		order = append(order, k)
	}
	sort.Strings(order)
	return &MapResultIterator{
		results: results,
		order:   order,
	}
}

func (r *MapResultIterator) More() bool {
	return len(r.order) > 0
}

func (r *MapResultIterator) Next() Result {
	next := r.order[0]
	r.order = r.order[1:]
	return r.results[next]
}

func (r *MapResultIterator) Cancel() {

}

func (r *MapResultIterator) Err() error {
	return nil
}

type SliceResultIterator struct {
	results []Result
}

func NewSliceResultIterator(results []Result) *SliceResultIterator {
	return &SliceResultIterator{
		results: results,
	}
}

func (r *SliceResultIterator) More() bool {
	return len(r.results) > 0
}

func (r *SliceResultIterator) Next() Result {
	next := r.results[0]
	r.results = r.results[1:]
	return next
}

func (r *SliceResultIterator) Cancel() {
	r.results = nil
}

func (r *SliceResultIterator) Err() error {
	return nil
}

// ProxyQueryServiceBridge implements ProxyQueryService while consuming a QueryService interface.
type ProxyQueryServiceBridge struct {
	QueryService QueryService
}

func (b ProxyQueryServiceBridge) Query(ctx context.Context, w io.Writer, req *ProxyRequest) (n int64, err error) {
	results, err := b.QueryService.Query(ctx, &req.Request)
	if err != nil {
		return 0, err
	}
	defer results.Cancel()
	encoder := req.Dialect.Encoder()
	n, err = encoder.Encode(w, results)
	if err != nil {
		return n, err
	}

	return n, nil
}

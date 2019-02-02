package cli

import (
	"context"

	"github.com/influxdata/flux"
	"github.com/influxdata/flux/csv"
	"github.com/influxdata/flux/repl"
	_ "github.com/influxdata/influxdb/flux/builtin"
	"github.com/influxdata/influxdb/flux/client"
)

// QueryService represents a type capable of performing queries.
type fluxClient interface {
	// Query submits a query for execution returning a results iterator.
	// Cancel must be called on any returned results to free resources.
	Query(ctx context.Context, req *client.ProxyRequest) (flux.ResultIterator, error)
}

// replQuerier implements the repl.Querier interface while consuming a fluxClient
type replQuerier struct {
	client fluxClient
}

func (q *replQuerier) Query(ctx context.Context, compiler flux.Compiler) (flux.ResultIterator, error) {
	req := &client.ProxyRequest{
		Compiler: compiler,
		Dialect:  csv.DefaultDialect(),
	}
	return q.client.Query(ctx, req)
}

func getFluxREPL(host string, port int, ssl bool) (*repl.REPL, error) {
	c, err := client.NewHTTP(host, port, ssl)
	if err != nil {
		return nil, err
	}
	return repl.New(&replQuerier{client: c}), nil
}

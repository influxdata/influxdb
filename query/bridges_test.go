package query_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/influxdata/flux"
	"github.com/influxdata/flux/csv"
	"github.com/influxdata/flux/execute/executetest"
	"github.com/influxdata/flux/metadata"
	"github.com/influxdata/influxdb/v2/query"
	"github.com/influxdata/influxdb/v2/query/mock"
)

type failWriter struct {
	Err error
}

// Write returns len(p)/2, w.Err,
// simulating a partial write with some error.
func (w failWriter) Write(p []byte) (int, error) {
	return len(p) / 2, w.Err
}

func TestProxyQueryServiceAsyncBridge_StatsOnClientDisconnect(t *testing.T) {
	q := mock.NewQuery()
	q.Metadata = metadata.Metadata{
		"foo": []interface{}{"bar"},
	}
	r := executetest.NewResult([]*executetest.Table{
		{},
	})
	r.Nm = "a"
	q.SetResults(r)

	expReq := &query.Request{OrganizationID: 0x1234}
	mockAsyncSvc := &mock.AsyncQueryService{
		QueryF: func(ctx context.Context, req *query.Request) (flux.Query, error) {
			if req.OrganizationID != 0x1234 {
				panic(fmt.Errorf("unexpected request: %v", req))
			}
			return q, nil
		},
	}

	// Use an io.Writer that returns a specific error on Write.
	w := failWriter{Err: errors.New("something went wrong with the write!")}

	bridge := query.ProxyQueryServiceAsyncBridge{
		AsyncQueryService: mockAsyncSvc,
	}
	stats, err := bridge.Query(context.Background(), w, &query.ProxyRequest{
		Request: *expReq,
		Dialect: csv.DefaultDialect(),
	})
	if !strings.Contains(err.Error(), w.Err.Error()) {
		t.Fatalf("Query should have failed with an error wrapping failWriter.Err, got %v", err)
	}

	// Even though there was an error, the statistics should be from the mock query.
	md := stats.Metadata
	if md["foo"] == nil || len(md["foo"]) != 1 || md["foo"][0] != "bar" {
		t.Fatalf("stats were missing or had wrong metadata: exp metadata[foo]=[bar], got %v", md)
	}
}

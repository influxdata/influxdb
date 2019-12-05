package query_test

import (
	"bytes"
	"context"
	"io"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/influxdata/flux"
	platform "github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/query"
	"github.com/influxdata/influxdb/query/mock"
	"go.uber.org/zap"
)

var orgID = MustIDBase16("ba55ba55ba55ba55")

// MustIDBase16 is an helper to ensure a correct ID is built during testing.
func MustIDBase16(s string) platform.ID {
	id, err := platform.IDFromString(s)
	if err != nil {
		panic(err)
	}
	return *id
}

var opts = []cmp.Option{
	cmpopts.IgnoreUnexported(query.ProxyRequest{}),
	cmpopts.IgnoreUnexported(query.Request{}),
}

func TestLoggingProxyQueryService(t *testing.T) {
	wantStats := flux.Statistics{
		TotalDuration:   time.Second,
		CompileDuration: time.Second,
		QueueDuration:   time.Second,
		PlanDuration:    time.Second,
		RequeueDuration: time.Second,
		ExecuteDuration: time.Second,
		Concurrency:     2,
		MaxAllocated:    2048,
	}
	wantBytes := 10
	pqs := &mock.ProxyQueryService{
		QueryF: func(ctx context.Context, w io.Writer, req *query.ProxyRequest) (flux.Statistics, error) {
			w.Write(make([]byte, wantBytes))
			return wantStats, nil
		},
	}
	var logs []query.Log
	logger := &mock.QueryLogger{
		LogFn: func(l query.Log) error {
			logs = append(logs, l)
			return nil
		},
	}

	wantTime := time.Now()
	lpqs := query.NewLoggingProxyQueryService(zap.NewNop(), logger, pqs)
	lpqs.SetNowFunctionForTesting(func() time.Time {
		return wantTime
	})

	var buf bytes.Buffer
	req := &query.ProxyRequest{
		Request: query.Request{
			Authorization:  nil,
			OrganizationID: orgID,
			Compiler:       nil,
		},
		Dialect: nil,
	}
	stats, err := lpqs.Query(context.Background(), &buf, req)
	if err != nil {
		t.Fatal(err)
	}
	if !cmp.Equal(wantStats, stats, opts...) {
		t.Errorf("unexpected query stats: -want/+got\n%s", cmp.Diff(wantStats, stats, opts...))
	}
	wantLogs := []query.Log{{
		Time:           wantTime,
		OrganizationID: orgID,
		Error:          nil,
		ProxyRequest:   req,
		ResponseSize:   int64(wantBytes),
		Statistics:     wantStats,
	}}
	if !cmp.Equal(wantLogs, logs, opts...) {
		t.Errorf("unexpected query logs: -want/+got\n%s", cmp.Diff(wantLogs, logs, opts...))
	}
}

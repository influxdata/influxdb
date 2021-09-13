package query_test

import (
	"bytes"
	"context"
	"io"
	"io/ioutil"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/influxdata/flux"
	"github.com/influxdata/flux/metadata"
	platform2 "github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/query"
	"github.com/influxdata/influxdb/v2/query/mock"
	"github.com/opentracing/opentracing-go"
	"github.com/uber/jaeger-client-go"
	"go.uber.org/zap"
)

var orgID = MustIDBase16("ba55ba55ba55ba55")

// MustIDBase16 is an helper to ensure a correct ID is built during testing.
func MustIDBase16(s string) platform2.ID {
	id, err := platform2.IDFromString(s)
	if err != nil {
		panic(err)
	}
	return *id
}

var opts = []cmp.Option{
	cmpopts.IgnoreUnexported(query.ProxyRequest{}),
	cmpopts.IgnoreUnexported(query.Request{}),
}

type contextKey string

const loggingCtxKey contextKey = "do-logging"

func TestLoggingProxyQueryService(t *testing.T) {
	// Set a Jaeger in-memory tracer to get span information in the query log.
	oldTracer := opentracing.GlobalTracer()
	defer opentracing.SetGlobalTracer(oldTracer)
	sampler := jaeger.NewConstSampler(true)
	reporter := jaeger.NewInMemoryReporter()
	tracer, closer := jaeger.NewTracer(t.Name(), sampler, reporter)
	defer closer.Close()
	opentracing.SetGlobalTracer(tracer)

	wantStats := flux.Statistics{
		TotalDuration:   time.Second,
		CompileDuration: time.Second,
		QueueDuration:   time.Second,
		PlanDuration:    time.Second,
		RequeueDuration: time.Second,
		ExecuteDuration: time.Second,
		Concurrency:     2,
		MaxAllocated:    2048,
		Metadata:        make(metadata.Metadata),
	}
	wantStats.Metadata.Add("some-mock-metadata", 42)
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

	req := &query.ProxyRequest{
		Request: query.Request{
			Authorization:  nil,
			OrganizationID: orgID,
			Compiler:       nil,
		},
		Dialect: nil,
	}

	t.Run("log", func(t *testing.T) {
		defer func() {
			logs = nil
		}()
		wantTime := time.Now()
		lpqs := query.NewLoggingProxyQueryService(zap.NewNop(), logger, pqs)
		lpqs.SetNowFunctionForTesting(func() time.Time {
			return wantTime
		})

		var buf bytes.Buffer
		stats, err := lpqs.Query(context.Background(), &buf, req)
		if err != nil {
			t.Fatal(err)
		}
		if !cmp.Equal(wantStats, stats, opts...) {
			t.Errorf("unexpected query stats: -want/+got\n%s", cmp.Diff(wantStats, stats, opts...))
		}
		traceID := reporter.GetSpans()[0].Context().(jaeger.SpanContext).TraceID().String()
		wantLogs := []query.Log{{
			Time:           wantTime,
			OrganizationID: orgID,
			TraceID:        traceID,
			Sampled:        true,
			Error:          nil,
			ProxyRequest:   req,
			ResponseSize:   int64(wantBytes),
			Statistics:     wantStats,
		}}
		if !cmp.Equal(wantLogs, logs, opts...) {
			t.Errorf("unexpected query logs: -want/+got\n%s", cmp.Diff(wantLogs, logs, opts...))
		}
	})

	t.Run("conditional logging", func(t *testing.T) {
		defer func() {
			logs = nil
		}()

		condLog := query.ConditionalLogging(func(ctx context.Context) bool {
			return ctx.Value(loggingCtxKey) != nil
		})

		lpqs := query.NewLoggingProxyQueryService(zap.NewNop(), logger, pqs, condLog)
		_, err := lpqs.Query(context.Background(), ioutil.Discard, req)
		if err != nil {
			t.Fatal(err)
		}

		if len(logs) != 0 {
			t.Fatal("expected query service not to log")
		}

		ctx := context.WithValue(context.Background(), loggingCtxKey, true)
		_, err = lpqs.Query(ctx, ioutil.Discard, req)
		if err != nil {
			t.Fatal(err)
		}

		if len(logs) != 1 {
			t.Fatal("expected query service to log")
		}
	})

	t.Run("require metadata key", func(t *testing.T) {
		defer func() {
			logs = nil
		}()

		reqMeta1 := query.RequireMetadataKey("this-metadata-wont-be-found")
		lpqs1 := query.NewLoggingProxyQueryService(zap.NewNop(), logger, pqs, reqMeta1)

		_, err := lpqs1.Query(context.Background(), ioutil.Discard, req)
		if err != nil {
			t.Fatal(err)
		}

		if len(logs) != 0 {
			t.Fatal("expected query service not to log")
		}

		reqMeta2 := query.RequireMetadataKey("some-mock-metadata")
		lpqs2 := query.NewLoggingProxyQueryService(zap.NewNop(), logger, pqs, reqMeta2)

		_, err = lpqs2.Query(context.Background(), ioutil.Discard, req)
		if err != nil {
			t.Fatal(err)
		}

		if len(logs) != 1 {
			t.Fatal("expected query service to log")
		}
	})
}

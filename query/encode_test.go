package query_test

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/flux"
	"github.com/influxdata/flux/csv"
	"github.com/influxdata/flux/execute"
	"github.com/influxdata/flux/execute/executetest"
	"github.com/influxdata/influxdb/v2/query"
	"github.com/influxdata/influxdb/v2/query/mock"
)

func TestReturnNoContent(t *testing.T) {
	getMockResult := func() flux.Result {
		// Some random data.
		r := executetest.NewResult([]*executetest.Table{{
			KeyCols: []string{"t1"},
			ColMeta: []flux.ColMeta{
				{Label: "_time", Type: flux.TTime},
				{Label: "_value", Type: flux.TFloat},
				{Label: "t1", Type: flux.TString},
				{Label: "t2", Type: flux.TString},
			},
			Data: [][]interface{}{
				{execute.Time(0), 1.0, "a", "y"},
				{execute.Time(10), 2.0, "a", "x"},
				{execute.Time(20), 3.0, "a", "y"},
				{execute.Time(30), 4.0, "a", "x"},
				{execute.Time(40), 5.0, "a", "y"},
			},
		}})
		r.Nm = "foo"
		return r
	}
	assertNoContent := func(t *testing.T, respBody []byte, stats flux.Statistics, reqErr error) {
		if reqErr != nil {
			t.Fatalf("unexpected error on query: %v", reqErr)
		}
		if body := string(respBody); len(body) > 0 {
			t.Fatalf("response body should be empty, but was: %s", body)
		}
	}

	testCases := []struct {
		name     string
		queryFn  func(ctx context.Context, req *query.Request) (flux.Query, error)
		dialect  flux.Dialect
		assertFn func(t *testing.T, respBody []byte, stats flux.Statistics, reqErr error)
	}{
		{
			name: "no-content - no error",
			queryFn: func(ctx context.Context, req *query.Request) (flux.Query, error) {
				q := mock.NewQuery()
				q.SetResults(getMockResult())
				return q, nil
			},
			dialect:  query.NewNoContentDialect(),
			assertFn: assertNoContent,
		},
		{
			name: "no-content - error",
			queryFn: func(ctx context.Context, req *query.Request) (flux.Query, error) {
				q := mock.NewQuery()
				q.SetResults(getMockResult())
				q.SetErr(fmt.Errorf("I am a runtime error"))
				return q, nil
			},
			dialect:  query.NewNoContentDialect(),
			assertFn: assertNoContent,
		},
		{
			name: "no-content-with-error - no error",
			queryFn: func(ctx context.Context, req *query.Request) (flux.Query, error) {
				q := mock.NewQuery()
				q.SetResults(getMockResult())
				return q, nil
			},
			dialect:  query.NewNoContentWithErrorDialect(),
			assertFn: assertNoContent,
		},
		{
			name: "no-content-with-error - error",
			queryFn: func(ctx context.Context, req *query.Request) (flux.Query, error) {
				q := mock.NewQuery()
				q.SetResults(getMockResult())
				q.SetErr(fmt.Errorf("I am a runtime error"))
				return q, nil
			},
			dialect: query.NewNoContentWithErrorDialect(),
			assertFn: func(t *testing.T, respBody []byte, stats flux.Statistics, reqErr error) {
				if reqErr != nil {
					t.Fatalf("unexpected error on query: %v", reqErr)
				}
				if len(respBody) == 0 {
					t.Fatalf("response body should not be empty, but it was")
				}
				_, err := csv.NewResultDecoder(csv.ResultDecoderConfig{}).Decode(bytes.NewReader(respBody))
				if err == nil {
					t.Fatalf("expected error got none")
				} else if diff := cmp.Diff(err.Error(), "I am a runtime error"); diff != "" {
					t.Fatalf("unexpected error, -want/+got:\n\t%s", diff)
				}
			},
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			mockAsyncSvc := &mock.AsyncQueryService{
				QueryF: tc.queryFn,
			}
			w := bytes.NewBuffer([]byte{})
			bridge := query.ProxyQueryServiceAsyncBridge{
				AsyncQueryService: mockAsyncSvc,
			}
			stats, err := bridge.Query(context.Background(), w, &query.ProxyRequest{
				Request: query.Request{},
				Dialect: tc.dialect,
			})
			tc.assertFn(t, w.Bytes(), stats, err)
		})
	}
}

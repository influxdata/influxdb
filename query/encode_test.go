package query_test

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/flux"
	"github.com/influxdata/flux/csv"
	"github.com/influxdata/flux/execute"
	"github.com/influxdata/flux/execute/executetest"
	"github.com/influxdata/influxdb/pkg/testing/assert"
	"github.com/influxdata/influxdb/query"
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
	assertNoContent := func(t *testing.T, respBody []byte, reqErr error) {
		if reqErr != nil {
			t.Fatalf("unexpected error on query: %v", reqErr)
		}
		if body := string(respBody); len(body) > 0 {
			t.Fatalf("response body should be empty, but was: %s", body)
		}
	}

	testCases := []struct {
		name     string
		query    flux.Query
		dialect  flux.Dialect
		assertFn func(t *testing.T, respBody []byte, reqErr error)
	}{
		{
			name:     "no-content - no error",
			query:    &mockQuery{Result: getMockResult()},
			dialect:  query.NewNoContentDialect(),
			assertFn: assertNoContent,
		},
		{
			name:     "no-content - error",
			query:    &mockQuery{Result: getMockResult(), Error: fmt.Errorf("I am a runtime error")},
			dialect:  query.NewNoContentDialect(),
			assertFn: assertNoContent,
		},
		{
			name:     "no-content-with-error - no error",
			query:    &mockQuery{Result: getMockResult()},
			dialect:  query.NewNoContentWithErrorDialect(),
			assertFn: assertNoContent,
		},
		{
			name:    "no-content-with-error - error",
			query:   &mockQuery{Result: getMockResult(), Error: fmt.Errorf("I am a runtime error")},
			dialect: query.NewNoContentWithErrorDialect(),
			assertFn: func(t *testing.T, respBody []byte, reqErr error) {
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
			results := flux.NewResultIteratorFromQuery(tc.query)
			defer results.Release()
			w := bytes.NewBuffer([]byte{})
			encoder := tc.dialect.Encoder()
			_, err := encoder.Encode(w, results)
			assert.NoError(t, err)
			tc.assertFn(t, w.Bytes(), err)
		})
	}
}

type mockQuery struct {
	Result flux.Result
	Error  error
	c      chan flux.Result
}

func (m *mockQuery) Results() <-chan flux.Result {
	if m.c != nil {
		return m.c
	}
	m.c = make(chan flux.Result, 1)
	m.c <- m.Result
	close(m.c)
	return m.c
}

func (m *mockQuery) Done() {
	// no-op
}

func (m *mockQuery) Cancel() {
	panic("not implemented")
}

func (m *mockQuery) Err() error {
	return m.Error
}

func (m *mockQuery) Statistics() flux.Statistics {
	panic("not implemented")
}

func (m *mockQuery) ProfilerResults() (flux.ResultIterator, error) {
	panic("not implemented")
}

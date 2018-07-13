package functions_test

import (
	"testing"

	"github.com/influxdata/platform/query"
	"github.com/influxdata/platform/query/execute"
	"github.com/influxdata/platform/query/execute/executetest"
	"github.com/influxdata/platform/query/functions"
	"github.com/influxdata/platform/query/querytest"
)

func TestMaxOperation_Marshaling(t *testing.T) {
	data := []byte(`{"id":"max","kind":"max","spec":{"column":"baz"}}`)
	op := &query.Operation{
		ID: "max",
		Spec: &functions.MaxOpSpec{
			SelectorConfig: execute.SelectorConfig{
				Column: "baz",
			},
		},
	}

	querytest.OperationMarshalingTestHelper(t, data, op)
}

func TestMax_Process(t *testing.T) {
	testCases := []struct {
		name string
		data *executetest.Table
		want []execute.Row
	}{
		{
			name: "first",
			data: &executetest.Table{
				KeyCols: []string{"t1"},
				ColMeta: []query.ColMeta{
					{Label: "_time", Type: query.TTime},
					{Label: "_value", Type: query.TFloat},
					{Label: "t1", Type: query.TString},
					{Label: "t2", Type: query.TString},
				},
				Data: [][]interface{}{
					{execute.Time(0), 10.0, "a", "y"},
					{execute.Time(10), 5.0, "a", "x"},
					{execute.Time(20), 9.0, "a", "y"},
					{execute.Time(30), 4.0, "a", "x"},
					{execute.Time(40), 6.0, "a", "y"},
					{execute.Time(50), 8.0, "a", "x"},
					{execute.Time(60), 1.0, "a", "y"},
					{execute.Time(70), 2.0, "a", "x"},
					{execute.Time(80), 3.0, "a", "y"},
					{execute.Time(90), 7.0, "a", "x"},
				},
			},
			want: []execute.Row{{
				Values: []interface{}{execute.Time(0), 10.0, "a", "y"},
			}},
		},
		{
			name: "last",
			data: &executetest.Table{
				KeyCols: []string{"t1"},
				ColMeta: []query.ColMeta{
					{Label: "_time", Type: query.TTime},
					{Label: "_value", Type: query.TFloat},
					{Label: "t1", Type: query.TString},
					{Label: "t2", Type: query.TString},
				},
				Data: [][]interface{}{
					{execute.Time(0), 7.0, "a", "y"},
					{execute.Time(10), 5.0, "a", "x"},
					{execute.Time(20), 9.0, "a", "y"},
					{execute.Time(30), 4.0, "a", "x"},
					{execute.Time(40), 6.0, "a", "y"},
					{execute.Time(50), 8.0, "a", "x"},
					{execute.Time(60), 1.0, "a", "y"},
					{execute.Time(70), 2.0, "a", "x"},
					{execute.Time(80), 3.0, "a", "y"},
					{execute.Time(90), 10.0, "a", "x"},
				},
			},
			want: []execute.Row{{
				Values: []interface{}{execute.Time(90), 10.0, "a", "x"},
			}},
		},
		{
			name: "middle",
			data: &executetest.Table{
				KeyCols: []string{"t1"},
				ColMeta: []query.ColMeta{
					{Label: "_time", Type: query.TTime},
					{Label: "_value", Type: query.TFloat},
					{Label: "t1", Type: query.TString},
					{Label: "t2", Type: query.TString},
				},
				Data: [][]interface{}{
					{execute.Time(0), 7.0, "a", "y"},
					{execute.Time(10), 5.0, "a", "x"},
					{execute.Time(20), 9.0, "a", "y"},
					{execute.Time(30), 4.0, "a", "x"},
					{execute.Time(40), 6.0, "a", "y"},
					{execute.Time(50), 10.0, "a", "x"},
					{execute.Time(60), 1.0, "a", "y"},
					{execute.Time(70), 2.0, "a", "x"},
					{execute.Time(80), 3.0, "a", "y"},
					{execute.Time(90), 8.0, "a", "x"},
				},
			},
			want: []execute.Row{{
				Values: []interface{}{execute.Time(50), 10.0, "a", "x"},
			}},
		},
	}
	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			executetest.RowSelectorFuncTestHelper(
				t,
				new(functions.MaxSelector),
				tc.data,
				tc.want,
			)
		})
	}
}

func BenchmarkMax(b *testing.B) {
	executetest.RowSelectorFuncBenchmarkHelper(b, new(functions.MaxSelector), NormalTable)
}

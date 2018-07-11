package functions_test

import (
	"math"
	"testing"

	"github.com/influxdata/platform/query"
	"github.com/influxdata/platform/query/execute/executetest"
	"github.com/influxdata/platform/query/functions"
	"github.com/influxdata/platform/query/querytest"
)

func TestMeanOperation_Marshaling(t *testing.T) {
	data := []byte(`{"id":"mean","kind":"mean"}`)
	op := &query.Operation{
		ID:   "mean",
		Spec: &functions.MeanOpSpec{},
	}

	querytest.OperationMarshalingTestHelper(t, data, op)
}

func TestMean_Process(t *testing.T) {
	testCases := []struct {
		name string
		data []float64
		want float64
	}{
		{
			name: "zero",
			data: []float64{0, 0, 0},
			want: 0.0,
		},
		{
			name: "nonzero",
			data: []float64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
			want: 4.5,
		},
		{
			name: "NaN",
			data: []float64{},
			want: math.NaN(),
		},
	}
	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			executetest.AggFuncTestHelper(
				t,
				new(functions.MeanAgg),
				tc.data,
				tc.want,
			)
		})
	}
}

func BenchmarkMean(b *testing.B) {
	executetest.AggFuncBenchmarkHelper(
		b,
		new(functions.MeanAgg),
		NormalData,
		10.00081696729983,
	)
}

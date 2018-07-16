package functions_test

import (
	"math"
	"testing"

	"github.com/influxdata/platform/query"
	"github.com/influxdata/platform/query/execute/executetest"
	"github.com/influxdata/platform/query/functions"
	"github.com/influxdata/platform/query/querytest"
)

func TestSkewOperation_Marshaling(t *testing.T) {
	data := []byte(`{"id":"skew","kind":"skew"}`)
	op := &query.Operation{
		ID:   "skew",
		Spec: &functions.SkewOpSpec{},
	}

	querytest.OperationMarshalingTestHelper(t, data, op)
}

func TestSkew_Process(t *testing.T) {
	testCases := []struct {
		name string
		data []float64
		want float64
	}{
		{
			name: "zero",
			data: []float64{1, 2, 3},
			want: 0.0,
		},
		{
			name: "nonzero",
			data: []float64{2, 2, 3},
			want: 0.7071067811865475,
		},
		{
			name: "nonzero",
			data: []float64{2, 2, 3, 4},
			want: 0.49338220021815854,
		},
		{
			name: "NaN short",
			data: []float64{1},
			want: math.NaN(),
		},
		{
			name: "NaN divide by zero",
			data: []float64{1, 1, 1},
			want: math.NaN(),
		},
	}
	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			executetest.AggFuncTestHelper(
				t,
				new(functions.SkewAgg),
				tc.data,
				tc.want,
			)
		})
	}
}

func BenchmarkSkew(b *testing.B) {
	executetest.AggFuncBenchmarkHelper(
		b,
		new(functions.SkewAgg),
		NormalData,
		0.0032200673020400935,
	)
}

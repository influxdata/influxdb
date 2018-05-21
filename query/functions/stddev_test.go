package functions_test

import (
	"math"
	"testing"

	"github.com/influxdata/ifql/functions"
	"github.com/influxdata/ifql/query"
	"github.com/influxdata/ifql/query/execute/executetest"
	"github.com/influxdata/ifql/query/querytest"
)

func TestStddevOperation_Marshaling(t *testing.T) {
	data := []byte(`{"id":"stddev","kind":"stddev"}`)
	op := &query.Operation{
		ID:   "stddev",
		Spec: &functions.StddevOpSpec{},
	}

	querytest.OperationMarshalingTestHelper(t, data, op)
}

func TestStddev_Process(t *testing.T) {
	testCases := []struct {
		name string
		data []float64
		want float64
	}{
		{
			name: "zero",
			data: []float64{1, 1, 1},
			want: 0.0,
		},
		{
			name: "nonzero",
			data: []float64{1, 2, 3},
			want: 1.0,
		},
		{
			name: "NaN",
			data: []float64{1},
			want: math.NaN(),
		},
	}
	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			executetest.AggFuncTestHelper(
				t,
				new(functions.StddevAgg),
				tc.data,
				tc.want,
			)
		})
	}
}

func BenchmarkStddev(b *testing.B) {
	executetest.AggFuncBenchmarkHelper(
		b,
		new(functions.StddevAgg),
		NormalData,
		2.998926113076968,
	)
}

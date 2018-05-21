package functions_test

import (
	"testing"

	"github.com/influxdata/platform/query/functions"
	"github.com/influxdata/platform/query"
	"github.com/influxdata/platform/query/execute/executetest"
	"github.com/influxdata/platform/query/querytest"
)

func TestSpreadOperation_Marshaling(t *testing.T) {
	data := []byte(`{"id":"spread","kind":"spread"}`)
	op := &query.Operation{
		ID:   "spread",
		Spec: &functions.SpreadOpSpec{},
	}

	querytest.OperationMarshalingTestHelper(t, data, op)
}

func TestSpread_Process(t *testing.T) {
	agg := new(functions.SpreadAgg)
	executetest.AggFuncTestHelper(t,
		agg,
		[]float64{
			0, 1, 2, 3, 4,
			5, 6, 7, 8, 9,
		},
		float64(9),
	)
}

func BenchmarkSpread(b *testing.B) {
	executetest.AggFuncBenchmarkHelper(
		b,
		new(functions.SpreadAgg),
		NormalData,
		28.227196461851847,
	)
}

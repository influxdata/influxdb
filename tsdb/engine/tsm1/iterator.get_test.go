package tsm1

import (
	"testing"

	"github.com/influxdata/influxdb/influxql"
	"github.com/influxdata/influxdb/query"
)

func BenchmarkIntegerIterator_Next(b *testing.B) {
	opt := query.IteratorOptions{
		Aux: []influxql.VarRef{{Val: "f1"}, {Val: "f1"}, {Val: "f1"}, {Val: "f1"}},
	}
	aux := []cursorAt{
		&literalValueCursor{value: "foo bar"},
		&literalValueCursor{value: int64(1e3)},
		&literalValueCursor{value: float64(1e3)},
		&literalValueCursor{value: true},
	}

	cur := newIntegerIterator("m0", query.Tags{}, opt, &infiniteIntegerCursor{}, aux, nil, nil)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		cur.Next()
	}
}

type infiniteIntegerCursor struct{}

func (*infiniteIntegerCursor) close() error {
	return nil
}

func (*infiniteIntegerCursor) next() (t int64, v interface{}) {
	return 0, 0
}

func (*infiniteIntegerCursor) nextInteger() (t int64, v int64) {
	return 0, 0
}

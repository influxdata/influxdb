package executetest

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/platform/query"
	"github.com/influxdata/platform/query/execute"
)

func RowSelectorFuncTestHelper(t *testing.T, selector execute.RowSelector, data query.Block, want []execute.Row) {
	t.Helper()

	s := selector.NewFloatSelector()
	valueIdx := execute.ColIdx(execute.DefaultValueColLabel, data.Cols())
	if valueIdx < 0 {
		t.Fatal("no _value column found")
	}
	data.Do(func(cr query.ColReader) error {
		s.DoFloat(cr.Floats(valueIdx), cr)
		return nil
	})

	got := s.Rows()

	if !cmp.Equal(want, got) {
		t.Errorf("unexpected value -want/+got\n%s", cmp.Diff(want, got))
	}
}

var rows []execute.Row

func RowSelectorFuncBenchmarkHelper(b *testing.B, selector execute.RowSelector, data query.Block) {
	b.Helper()

	valueIdx := execute.ColIdx(execute.DefaultValueColLabel, data.Cols())
	if valueIdx < 0 {
		b.Fatal("no _value column found")
	}

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		s := selector.NewFloatSelector()
		data.Do(func(cr query.ColReader) error {
			s.DoFloat(cr.Floats(valueIdx), cr)
			return nil
		})
		rows = s.Rows()
	}
}

func IndexSelectorFuncTestHelper(t *testing.T, selector execute.IndexSelector, data query.Block, want [][]int) {
	t.Helper()

	var got [][]int
	s := selector.NewFloatSelector()
	valueIdx := execute.ColIdx(execute.DefaultValueColLabel, data.Cols())
	if valueIdx < 0 {
		t.Fatal("no _value column found")
	}
	data.Do(func(cr query.ColReader) error {
		var cpy []int
		selected := s.DoFloat(cr.Floats(valueIdx))
		if len(selected) > 0 {
			cpy = make([]int, len(selected))
			copy(cpy, selected)
		}
		got = append(got, cpy)
		return nil
	})

	if !cmp.Equal(want, got) {
		t.Errorf("unexpected value -want/+got\n%s", cmp.Diff(want, got))
	}
}

func IndexSelectorFuncBenchmarkHelper(b *testing.B, selector execute.IndexSelector, data query.Block) {
	b.Helper()

	valueIdx := execute.ColIdx(execute.DefaultValueColLabel, data.Cols())
	if valueIdx < 0 {
		b.Fatal("no _value column found")
	}

	b.ResetTimer()
	var got [][]int
	for n := 0; n < b.N; n++ {
		s := selector.NewFloatSelector()
		data.Do(func(cr query.ColReader) error {
			got = append(got, s.DoFloat(cr.Floats(valueIdx)))
			return nil
		})
	}
}

package reads_test

import (
	"context"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/flux"
	"github.com/influxdata/flux/execute"
	"github.com/influxdata/flux/execute/executetest"
	"github.com/influxdata/flux/memory"
	"github.com/influxdata/influxdb/flux/stdlib/influxdata/influxdb"
	"github.com/influxdata/influxdb/mock"
	"github.com/influxdata/influxdb/storage/reads"
	"github.com/influxdata/influxdb/storage/reads/datatypes"
	"github.com/influxdata/influxdb/tsdb/cursors"
)

func TestDuplicateKeys_ReadFilter(t *testing.T) {
	closed := 0

	s := mock.NewStoreReader()
	s.ReadFilterFunc = func(ctx context.Context, req *datatypes.ReadFilterRequest) (reads.ResultSet, error) {
		inputs := make([]cursors.Cursor, 2)
		inputs[0] = func() cursors.Cursor {
			called := false
			cur := mock.NewFloatArrayCursor()
			cur.NextFunc = func() *cursors.FloatArray {
				if called {
					return &cursors.FloatArray{}
				}
				called = true
				return &cursors.FloatArray{
					Timestamps: []int64{0},
					Values:     []float64{1.0},
				}
			}
			cur.CloseFunc = func() {
				closed++
			}
			return cur
		}()
		inputs[1] = func() cursors.Cursor {
			called := false
			cur := mock.NewIntegerArrayCursor()
			cur.NextFunc = func() *cursors.IntegerArray {
				if called {
					return &cursors.IntegerArray{}
				}
				called = true
				return &cursors.IntegerArray{
					Timestamps: []int64{10},
					Values:     []int64{1},
				}
			}
			cur.CloseFunc = func() {
				closed++
			}
			return cur
		}()

		idx := -1
		rs := mock.NewResultSet()
		rs.NextFunc = func() bool {
			idx++
			return idx < len(inputs)
		}
		rs.CursorFunc = func() cursors.Cursor {
			return inputs[idx]
		}
		rs.CloseFunc = func() {
			idx = len(inputs)
		}
		return rs, nil
	}

	r := reads.NewReader(s)
	ti, err := r.ReadFilter(context.Background(), influxdb.ReadFilterSpec{
		Bounds: execute.Bounds{
			Start: execute.Time(0),
			Stop:  execute.Time(30),
		},
	}, &memory.Allocator{})
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	var got []*executetest.Table
	if err := ti.Do(func(tbl flux.Table) error {
		t, err := executetest.ConvertTable(tbl)
		if err != nil {
			return err
		}
		got = append(got, t)
		return nil
	}); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	want := []*executetest.Table{
		{
			ColMeta: []flux.ColMeta{
				{Label: "_start", Type: flux.TTime},
				{Label: "_stop", Type: flux.TTime},
				{Label: "_time", Type: flux.TTime},
				{Label: "_value", Type: flux.TFloat},
			},
			KeyCols: []string{"_start", "_stop"},
			Data: [][]interface{}{
				{execute.Time(0), execute.Time(30), execute.Time(0), 1.0},
			},
		},
	}
	for _, tbl := range want {
		tbl.Normalize()
	}
	if !cmp.Equal(want, got) {
		t.Fatalf("unexpected output:\n%s", cmp.Diff(want, got))
	}

	if want, got := closed, 2; want != got {
		t.Fatalf("unexpected count of closed cursors -want/+got:\n\t- %d\n\t+ %d", want, got)
	}
}

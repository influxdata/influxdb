package functions_test

import (
	"testing"
	"time"

	"github.com/influxdata/ifql/functions"
	"github.com/influxdata/ifql/query"
	"github.com/influxdata/ifql/query/execute"
	"github.com/influxdata/ifql/query/execute/executetest"
	"github.com/influxdata/ifql/query/querytest"
)

func TestShiftOperation_Marshaling(t *testing.T) {
	data := []byte(`{"id":"shift","kind":"shift","spec":{"shift":"1h"}}`)
	op := &query.Operation{
		ID: "shift",
		Spec: &functions.ShiftOpSpec{
			Shift: query.Duration(1 * time.Hour),
		},
	}
	querytest.OperationMarshalingTestHelper(t, data, op)
}

func TestShift_Process(t *testing.T) {
	cols := []execute.ColMeta{
		{Label: "t1", Type: execute.TString},
		{Label: execute.DefaultTimeColLabel, Type: execute.TTime},
		{Label: execute.DefaultValueColLabel, Type: execute.TFloat},
	}

	testCases := []struct {
		name string
		spec *functions.ShiftProcedureSpec
		data []execute.Block
		want []*executetest.Block
	}{
		{
			name: "one block",
			spec: &functions.ShiftProcedureSpec{
				Columns: []string{execute.DefaultTimeColLabel},
				Shift:   query.Duration(1),
			},
			data: []execute.Block{
				&executetest.Block{
					KeyCols: []string{"t1"},
					ColMeta: cols,
					Data: [][]interface{}{
						{"a", execute.Time(1), 2.0},
						{"a", execute.Time(2), 1.0},
					},
				},
			},
			want: []*executetest.Block{
				{
					KeyCols: []string{"t1"},
					ColMeta: cols,
					Data: [][]interface{}{
						{"a", execute.Time(2), 2.0},
						{"a", execute.Time(3), 1.0},
					},
				},
			},
		},
		{
			name: "multiple blocks",
			spec: &functions.ShiftProcedureSpec{
				Columns: []string{execute.DefaultTimeColLabel},
				Shift:   query.Duration(2),
			},
			data: []execute.Block{
				&executetest.Block{
					KeyCols: []string{"t1"},
					ColMeta: cols,
					Data: [][]interface{}{
						{"a", execute.Time(1), 2.0},
						{"a", execute.Time(2), 1.0},
					},
				},
				&executetest.Block{
					KeyCols: []string{"t1"},
					ColMeta: cols,
					Data: [][]interface{}{
						{"b", execute.Time(3), 3.0},
						{"b", execute.Time(4), 4.0},
					},
				},
			},
			want: []*executetest.Block{
				{
					KeyCols: []string{"t1"},
					ColMeta: cols,
					Data: [][]interface{}{
						{"a", execute.Time(3), 2.0},
						{"a", execute.Time(4), 1.0},
					},
				},
				{
					KeyCols: []string{"t1"},
					ColMeta: cols,
					Data: [][]interface{}{
						{"b", execute.Time(5), 3.0},
						{"b", execute.Time(6), 4.0},
					},
				},
			},
		},
	}
	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			executetest.ProcessTestHelper(
				t,
				tc.data,
				tc.want,
				func(d execute.Dataset, c execute.BlockBuilderCache) execute.Transformation {
					return functions.NewShiftTransformation(d, c, tc.spec)
				},
			)
		})
	}
}

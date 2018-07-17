package functions_test

import (
	"testing"

	"github.com/influxdata/platform/query"
	"github.com/influxdata/platform/query/execute"
	"github.com/influxdata/platform/query/execute/executetest"
	"github.com/influxdata/platform/query/functions"
	"github.com/influxdata/platform/query/querytest"
)

func TestDifferenceOperation_Marshaling(t *testing.T) {
	data := []byte(`{"id":"difference","kind":"difference","spec":{"non_negative":true}}`)
	op := &query.Operation{
		ID: "difference",
		Spec: &functions.DifferenceOpSpec{
			NonNegative: true,
		},
	}
	querytest.OperationMarshalingTestHelper(t, data, op)
}

func TestDifference_PassThrough(t *testing.T) {
	executetest.TransformationPassThroughTestHelper(t, func(d execute.Dataset, c execute.TableBuilderCache) execute.Transformation {
		s := functions.NewDifferenceTransformation(
			d,
			c,
			&functions.DifferenceProcedureSpec{},
		)
		return s
	})
}

func TestDifference_Process(t *testing.T) {
	testCases := []struct {
		name string
		spec *functions.DifferenceProcedureSpec
		data []query.Table
		want []*executetest.Table
	}{
		{
			name: "float",
			spec: &functions.DifferenceProcedureSpec{
				Columns: []string{execute.DefaultValueColLabel},
			},
			data: []query.Table{&executetest.Table{
				ColMeta: []query.ColMeta{
					{Label: "_time", Type: query.TTime},
					{Label: "_value", Type: query.TFloat},
				},
				Data: [][]interface{}{
					{execute.Time(1), 2.0},
					{execute.Time(2), 1.0},
				},
			}},
			want: []*executetest.Table{{
				ColMeta: []query.ColMeta{
					{Label: "_time", Type: query.TTime},
					{Label: "_value", Type: query.TFloat},
				},
				Data: [][]interface{}{
					{execute.Time(2), -1.0},
				},
			}},
		},
		{
			name: "int",
			spec: &functions.DifferenceProcedureSpec{
				Columns: []string{execute.DefaultValueColLabel},
			},
			data: []query.Table{&executetest.Table{
				ColMeta: []query.ColMeta{
					{Label: "_time", Type: query.TTime},
					{Label: "_value", Type: query.TInt},
				},
				Data: [][]interface{}{
					{execute.Time(1), int64(20)},
					{execute.Time(2), int64(10)},
				},
			}},
			want: []*executetest.Table{{
				ColMeta: []query.ColMeta{
					{Label: "_time", Type: query.TTime},
					{Label: "_value", Type: query.TInt},
				},
				Data: [][]interface{}{
					{execute.Time(2), int64(-10)},
				},
			}},
		},
		{
			name: "int non negative",
			spec: &functions.DifferenceProcedureSpec{
				Columns:     []string{execute.DefaultValueColLabel},
				NonNegative: true,
			},
			data: []query.Table{&executetest.Table{
				ColMeta: []query.ColMeta{
					{Label: "_time", Type: query.TTime},
					{Label: "_value", Type: query.TInt},
				},
				Data: [][]interface{}{
					{execute.Time(1), int64(20)},
					{execute.Time(2), int64(10)},
					{execute.Time(3), int64(20)},
				},
			}},
			want: []*executetest.Table{{
				ColMeta: []query.ColMeta{
					{Label: "_time", Type: query.TTime},
					{Label: "_value", Type: query.TInt},
				},
				Data: [][]interface{}{
					{execute.Time(2), int64(10)},
					{execute.Time(3), int64(10)},
				},
			}},
		},
		{
			name: "uint",
			spec: &functions.DifferenceProcedureSpec{
				Columns: []string{execute.DefaultValueColLabel},
			},
			data: []query.Table{&executetest.Table{
				ColMeta: []query.ColMeta{
					{Label: "_time", Type: query.TTime},
					{Label: "_value", Type: query.TUInt},
				},
				Data: [][]interface{}{
					{execute.Time(1), uint64(10)},
					{execute.Time(2), uint64(20)},
				},
			}},
			want: []*executetest.Table{{
				ColMeta: []query.ColMeta{
					{Label: "_time", Type: query.TTime},
					{Label: "_value", Type: query.TInt},
				},
				Data: [][]interface{}{
					{execute.Time(2), int64(10)},
				},
			}},
		},
		{
			name: "uint with negative result",
			spec: &functions.DifferenceProcedureSpec{
				Columns: []string{execute.DefaultValueColLabel},
			},
			data: []query.Table{&executetest.Table{
				ColMeta: []query.ColMeta{
					{Label: "_time", Type: query.TTime},
					{Label: "_value", Type: query.TUInt},
				},
				Data: [][]interface{}{
					{execute.Time(1), uint64(20)},
					{execute.Time(2), uint64(10)},
				},
			}},
			want: []*executetest.Table{{
				ColMeta: []query.ColMeta{
					{Label: "_time", Type: query.TTime},
					{Label: "_value", Type: query.TInt},
				},
				Data: [][]interface{}{
					{execute.Time(2), int64(-10)},
				},
			}},
		},
		{
			name: "uint with non negative",
			spec: &functions.DifferenceProcedureSpec{
				Columns:     []string{execute.DefaultValueColLabel},
				NonNegative: true,
			},
			data: []query.Table{&executetest.Table{
				ColMeta: []query.ColMeta{
					{Label: "_time", Type: query.TTime},
					{Label: "_value", Type: query.TUInt},
				},
				Data: [][]interface{}{
					{execute.Time(1), uint64(20)},
					{execute.Time(2), uint64(10)},
					{execute.Time(3), uint64(20)},
				},
			}},
			want: []*executetest.Table{{
				ColMeta: []query.ColMeta{
					{Label: "_time", Type: query.TTime},
					{Label: "_value", Type: query.TInt},
				},
				Data: [][]interface{}{
					{execute.Time(2), int64(10)},
					{execute.Time(3), int64(10)},
				},
			}},
		},
		{
			name: "non negative one table",
			spec: &functions.DifferenceProcedureSpec{
				Columns:     []string{execute.DefaultValueColLabel},
				NonNegative: true,
			},
			data: []query.Table{&executetest.Table{
				ColMeta: []query.ColMeta{
					{Label: "_time", Type: query.TTime},
					{Label: "_value", Type: query.TFloat},
				},
				Data: [][]interface{}{
					{execute.Time(1), 2.0},
					{execute.Time(2), 1.0},
					{execute.Time(3), 2.0},
				},
			}},
			want: []*executetest.Table{{
				ColMeta: []query.ColMeta{
					{Label: "_time", Type: query.TTime},
					{Label: "_value", Type: query.TFloat},
				},
				Data: [][]interface{}{
					{execute.Time(2), 1.0},
					{execute.Time(3), 1.0},
				},
			}},
		},
		{
			name: "float with tags",
			spec: &functions.DifferenceProcedureSpec{
				Columns: []string{execute.DefaultValueColLabel},
			},
			data: []query.Table{&executetest.Table{
				ColMeta: []query.ColMeta{
					{Label: "_time", Type: query.TTime},
					{Label: "_value", Type: query.TFloat},
					{Label: "t", Type: query.TString},
				},
				Data: [][]interface{}{
					{execute.Time(1), 2.0, "a"},
					{execute.Time(2), 1.0, "b"},
				},
			}},
			want: []*executetest.Table{{
				ColMeta: []query.ColMeta{
					{Label: "_time", Type: query.TTime},
					{Label: "_value", Type: query.TFloat},
					{Label: "t", Type: query.TString},
				},
				Data: [][]interface{}{
					{execute.Time(2), -1.0, "b"},
				},
			}},
		},
		{
			name: "float with multiple values",
			spec: &functions.DifferenceProcedureSpec{
				Columns: []string{"x", "y"},
			},
			data: []query.Table{&executetest.Table{
				ColMeta: []query.ColMeta{
					{Label: "_time", Type: query.TTime},
					{Label: "x", Type: query.TFloat},
					{Label: "y", Type: query.TFloat},
				},
				Data: [][]interface{}{
					{execute.Time(1), 2.0, 20.0},
					{execute.Time(2), 1.0, 10.0},
				},
			}},
			want: []*executetest.Table{{
				ColMeta: []query.ColMeta{
					{Label: "_time", Type: query.TTime},
					{Label: "x", Type: query.TFloat},
					{Label: "y", Type: query.TFloat},
				},
				Data: [][]interface{}{
					{execute.Time(2), -1.0, -10.0},
				},
			}},
		},
		{
			name: "float non negative with multiple values",
			spec: &functions.DifferenceProcedureSpec{
				Columns:     []string{"x", "y"},
				NonNegative: true,
			},
			data: []query.Table{&executetest.Table{
				ColMeta: []query.ColMeta{
					{Label: "_time", Type: query.TTime},
					{Label: "x", Type: query.TFloat},
					{Label: "y", Type: query.TFloat},
				},
				Data: [][]interface{}{
					{execute.Time(1), 2.0, 20.0},
					{execute.Time(2), 1.0, 10.0},
					{execute.Time(3), 2.0, 0.0},
				},
			}},
			want: []*executetest.Table{{
				ColMeta: []query.ColMeta{
					{Label: "_time", Type: query.TTime},
					{Label: "x", Type: query.TFloat},
					{Label: "y", Type: query.TFloat},
				},
				Data: [][]interface{}{
					{execute.Time(2), 1.0, 10.0},
					{execute.Time(3), 1.0, 0.0},
				},
			}},
		},
		{
			name: "float no values",
			spec: &functions.DifferenceProcedureSpec{
				Columns:     []string{"x", "y"},
				NonNegative: true,
			},
			data: []query.Table{&executetest.Table{
				ColMeta: []query.ColMeta{
					{Label: "_time", Type: query.TTime},
					{Label: "x", Type: query.TFloat},
					{Label: "y", Type: query.TFloat},
				},
				Data: [][]interface{}{},
			}},
			want: []*executetest.Table{{
				ColMeta: []query.ColMeta{
					{Label: "_time", Type: query.TTime},
					{Label: "x", Type: query.TFloat},
					{Label: "y", Type: query.TFloat},
				},
				Data: [][]interface{}(nil),
			}},
		},
		{
			name: "float single value",
			spec: &functions.DifferenceProcedureSpec{
				Columns:     []string{"x", "y"},
				NonNegative: true,
			},
			data: []query.Table{&executetest.Table{
				ColMeta: []query.ColMeta{
					{Label: "_time", Type: query.TTime},
					{Label: "x", Type: query.TFloat},
					{Label: "y", Type: query.TFloat},
				},
				Data: [][]interface{}{
					{execute.Time(3), 10.0, 20.0},
				},
			}},
			want: []*executetest.Table{{
				ColMeta: []query.ColMeta{
					{Label: "_time", Type: query.TTime},
					{Label: "x", Type: query.TFloat},
					{Label: "y", Type: query.TFloat},
				},
				Data: [][]interface{}(nil),
			}},
		},
	}
	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			executetest.ProcessTestHelper(
				t,
				tc.data,
				tc.want,
				nil,
				func(d execute.Dataset, c execute.TableBuilderCache) execute.Transformation {
					return functions.NewDifferenceTransformation(d, c, tc.spec)
				},
			)
		})
	}
}

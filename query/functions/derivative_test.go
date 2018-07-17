package functions_test

import (
	"testing"
	"time"

	"github.com/influxdata/platform/query"
	"github.com/influxdata/platform/query/execute"
	"github.com/influxdata/platform/query/execute/executetest"
	"github.com/influxdata/platform/query/functions"
	"github.com/influxdata/platform/query/querytest"
)

func TestDerivativeOperation_Marshaling(t *testing.T) {
	data := []byte(`{"id":"derivative","kind":"derivative","spec":{"unit":"1m","non_negative":true}}`)
	op := &query.Operation{
		ID: "derivative",
		Spec: &functions.DerivativeOpSpec{
			Unit:        query.Duration(time.Minute),
			NonNegative: true,
		},
	}
	querytest.OperationMarshalingTestHelper(t, data, op)
}

func TestDerivative_PassThrough(t *testing.T) {
	executetest.TransformationPassThroughTestHelper(t, func(d execute.Dataset, c execute.TableBuilderCache) execute.Transformation {
		s := functions.NewDerivativeTransformation(
			d,
			c,
			&functions.DerivativeProcedureSpec{},
		)
		return s
	})
}

func TestDerivative_Process(t *testing.T) {
	testCases := []struct {
		name string
		spec *functions.DerivativeProcedureSpec
		data []query.Table
		want []*executetest.Table
	}{
		{
			name: "float",
			spec: &functions.DerivativeProcedureSpec{
				Columns: []string{execute.DefaultValueColLabel},
				TimeCol: execute.DefaultTimeColLabel,
				Unit:    1,
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
			name: "float with units",
			spec: &functions.DerivativeProcedureSpec{
				Columns: []string{execute.DefaultValueColLabel},
				TimeCol: execute.DefaultTimeColLabel,
				Unit:    query.Duration(time.Second),
			},
			data: []query.Table{&executetest.Table{
				ColMeta: []query.ColMeta{
					{Label: "_time", Type: query.TTime},
					{Label: "_value", Type: query.TFloat},
				},
				Data: [][]interface{}{
					{execute.Time(1 * time.Second), 2.0},
					{execute.Time(3 * time.Second), 1.0},
				},
			}},
			want: []*executetest.Table{{
				ColMeta: []query.ColMeta{
					{Label: "_time", Type: query.TTime},
					{Label: "_value", Type: query.TFloat},
				},
				Data: [][]interface{}{
					{execute.Time(3 * time.Second), -0.5},
				},
			}},
		},
		{
			name: "int",
			spec: &functions.DerivativeProcedureSpec{
				Columns: []string{execute.DefaultValueColLabel},
				TimeCol: execute.DefaultTimeColLabel,
				Unit:    1,
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
					{Label: "_value", Type: query.TFloat},
				},
				Data: [][]interface{}{
					{execute.Time(2), -10.0},
				},
			}},
		},
		{
			name: "int with units",
			spec: &functions.DerivativeProcedureSpec{
				Columns: []string{execute.DefaultValueColLabel},
				TimeCol: execute.DefaultTimeColLabel,
				Unit:    query.Duration(time.Second),
			},
			data: []query.Table{&executetest.Table{
				ColMeta: []query.ColMeta{
					{Label: "_time", Type: query.TTime},
					{Label: "_value", Type: query.TInt},
				},
				Data: [][]interface{}{
					{execute.Time(1 * time.Second), int64(20)},
					{execute.Time(3 * time.Second), int64(10)},
				},
			}},
			want: []*executetest.Table{{
				ColMeta: []query.ColMeta{
					{Label: "_time", Type: query.TTime},
					{Label: "_value", Type: query.TFloat},
				},
				Data: [][]interface{}{
					{execute.Time(3 * time.Second), -5.0},
				},
			}},
		},
		{
			name: "int non negative",
			spec: &functions.DerivativeProcedureSpec{
				Columns:     []string{execute.DefaultValueColLabel},
				TimeCol:     execute.DefaultTimeColLabel,
				Unit:        1,
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
					{Label: "_value", Type: query.TFloat},
				},
				Data: [][]interface{}{
					{execute.Time(2), 10.0},
					{execute.Time(3), 10.0},
				},
			}},
		},
		{
			name: "uint",
			spec: &functions.DerivativeProcedureSpec{
				Columns: []string{execute.DefaultValueColLabel},
				TimeCol: execute.DefaultTimeColLabel,
				Unit:    1,
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
					{Label: "_value", Type: query.TFloat},
				},
				Data: [][]interface{}{
					{execute.Time(2), 10.0},
				},
			}},
		},
		{
			name: "uint with negative result",
			spec: &functions.DerivativeProcedureSpec{
				Columns: []string{execute.DefaultValueColLabel},
				TimeCol: execute.DefaultTimeColLabel,
				Unit:    1,
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
					{Label: "_value", Type: query.TFloat},
				},
				Data: [][]interface{}{
					{execute.Time(2), -10.0},
				},
			}},
		},
		{
			name: "uint with non negative",
			spec: &functions.DerivativeProcedureSpec{
				Columns:     []string{execute.DefaultValueColLabel},
				TimeCol:     execute.DefaultTimeColLabel,
				Unit:        1,
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
					{Label: "_value", Type: query.TFloat},
				},
				Data: [][]interface{}{
					{execute.Time(2), 10.0},
					{execute.Time(3), 10.0},
				},
			}},
		},
		{
			name: "uint with units",
			spec: &functions.DerivativeProcedureSpec{
				Columns: []string{execute.DefaultValueColLabel},
				TimeCol: execute.DefaultTimeColLabel,
				Unit:    query.Duration(time.Second),
			},
			data: []query.Table{&executetest.Table{
				ColMeta: []query.ColMeta{
					{Label: "_time", Type: query.TTime},
					{Label: "_value", Type: query.TUInt},
				},
				Data: [][]interface{}{
					{execute.Time(1 * time.Second), uint64(20)},
					{execute.Time(3 * time.Second), uint64(10)},
				},
			}},
			want: []*executetest.Table{{
				ColMeta: []query.ColMeta{
					{Label: "_time", Type: query.TTime},
					{Label: "_value", Type: query.TFloat},
				},
				Data: [][]interface{}{
					{execute.Time(3 * time.Second), -5.0},
				},
			}},
		},
		{
			name: "non negative one table",
			spec: &functions.DerivativeProcedureSpec{
				Columns:     []string{execute.DefaultValueColLabel},
				TimeCol:     execute.DefaultTimeColLabel,
				Unit:        1,
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
			spec: &functions.DerivativeProcedureSpec{
				Columns: []string{execute.DefaultValueColLabel},
				TimeCol: execute.DefaultTimeColLabel,
				Unit:    1,
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
			spec: &functions.DerivativeProcedureSpec{
				Columns: []string{"x", "y"},
				TimeCol: execute.DefaultTimeColLabel,
				Unit:    1,
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
			spec: &functions.DerivativeProcedureSpec{
				Columns:     []string{"x", "y"},
				TimeCol:     execute.DefaultTimeColLabel,
				Unit:        1,
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
					return functions.NewDerivativeTransformation(d, c, tc.spec)
				},
			)
		})
	}
}

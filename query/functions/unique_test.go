package functions_test

import (
	"testing"

	"github.com/influxdata/platform/query"
	"github.com/influxdata/platform/query/execute"
	"github.com/influxdata/platform/query/execute/executetest"
	"github.com/influxdata/platform/query/functions"
	"github.com/influxdata/platform/query/querytest"
)

func TestUniqueOperation_Marshaling(t *testing.T) {
	data := []byte(`{"id":"unique","kind":"unique","spec":{"column":"_value"}}`)
	op := &query.Operation{
		ID: "unique",
		Spec: &functions.UniqueOpSpec{
			Column: "_value",
		},
	}
	querytest.OperationMarshalingTestHelper(t, data, op)
}

func TestUnique_PassThrough(t *testing.T) {
	executetest.TransformationPassThroughTestHelper(t, func(d execute.Dataset, c execute.TableBuilderCache) execute.Transformation {
		s := functions.NewUniqueTransformation(
			d,
			c,
			&functions.UniqueProcedureSpec{
				Column: "_value",
			},
		)
		return s
	})
}

func TestUnique_Process(t *testing.T) {
	testCases := []struct {
		name string
		spec *functions.UniqueProcedureSpec
		data []query.Table
		want []*executetest.Table
	}{
		{
			name: "one table",
			spec: &functions.UniqueProcedureSpec{
				Column: "_value",
			},
			data: []query.Table{&executetest.Table{
				ColMeta: []query.ColMeta{
					{Label: "_time", Type: query.TTime},
					{Label: "_value", Type: query.TFloat},
				},
				Data: [][]interface{}{
					{execute.Time(1), 2.0},
					{execute.Time(2), 1.0},
					{execute.Time(3), 3.0},
					{execute.Time(4), 1.0},
				},
			}},
			want: []*executetest.Table{{
				ColMeta: []query.ColMeta{
					{Label: "_time", Type: query.TTime},
					{Label: "_value", Type: query.TFloat},
				},
				Data: [][]interface{}{
					{execute.Time(1), 2.0},
					{execute.Time(2), 1.0},
					{execute.Time(3), 3.0},
				},
			}},
		},
		{
			name: "unique tag",
			spec: &functions.UniqueProcedureSpec{
				Column: "t1",
			},
			data: []query.Table{&executetest.Table{
				ColMeta: []query.ColMeta{
					{Label: "_time", Type: query.TTime},
					{Label: "t1", Type: query.TString},
					{Label: "_value", Type: query.TFloat},
				},
				Data: [][]interface{}{
					{execute.Time(1), "a", 2.0},
					{execute.Time(2), "a", 1.0},
					{execute.Time(3), "b", 3.0},
					{execute.Time(4), "c", 1.0},
				},
			}},
			want: []*executetest.Table{{
				ColMeta: []query.ColMeta{
					{Label: "_time", Type: query.TTime},
					{Label: "t1", Type: query.TString},
					{Label: "_value", Type: query.TFloat},
				},
				Data: [][]interface{}{
					{execute.Time(1), "a", 2.0},
					{execute.Time(3), "b", 3.0},
					{execute.Time(4), "c", 1.0},
				},
			}},
		},
		{
			name: "unique times",
			spec: &functions.UniqueProcedureSpec{
				Column: "_time",
			},
			data: []query.Table{&executetest.Table{
				ColMeta: []query.ColMeta{
					{Label: "_time", Type: query.TTime},
					{Label: "t1", Type: query.TString},
					{Label: "_value", Type: query.TFloat},
				},
				Data: [][]interface{}{
					{execute.Time(1), "a", 2.0},
					{execute.Time(2), "a", 1.0},
					{execute.Time(3), "b", 3.0},
					{execute.Time(3), "c", 1.0},
				},
			}},
			want: []*executetest.Table{{
				ColMeta: []query.ColMeta{
					{Label: "_time", Type: query.TTime},
					{Label: "t1", Type: query.TString},
					{Label: "_value", Type: query.TFloat},
				},
				Data: [][]interface{}{
					{execute.Time(1), "a", 2.0},
					{execute.Time(2), "a", 1.0},
					{execute.Time(3), "b", 3.0},
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
					return functions.NewUniqueTransformation(d, c, tc.spec)
				},
			)
		})
	}
}

package functions_test

import (
	"testing"

	"github.com/influxdata/ifql/functions"
	"github.com/influxdata/ifql/query"
	"github.com/influxdata/ifql/query/execute"
	"github.com/influxdata/ifql/query/execute/executetest"
	"github.com/influxdata/ifql/query/querytest"
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
	executetest.TransformationPassThroughTestHelper(t, func(d execute.Dataset, c execute.BlockBuilderCache) execute.Transformation {
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
		data []execute.Block
		want []*executetest.Block
	}{
		{
			name: "one block",
			spec: &functions.UniqueProcedureSpec{
				Column: "_value",
			},
			data: []execute.Block{&executetest.Block{
				ColMeta: []execute.ColMeta{
					{Label: "_time", Type: execute.TTime},
					{Label: "_value", Type: execute.TFloat},
				},
				Data: [][]interface{}{
					{execute.Time(1), 2.0},
					{execute.Time(2), 1.0},
					{execute.Time(3), 3.0},
					{execute.Time(4), 1.0},
				},
			}},
			want: []*executetest.Block{{
				ColMeta: []execute.ColMeta{
					{Label: "_time", Type: execute.TTime},
					{Label: "_value", Type: execute.TFloat},
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
			data: []execute.Block{&executetest.Block{
				ColMeta: []execute.ColMeta{
					{Label: "_time", Type: execute.TTime},
					{Label: "t1", Type: execute.TString},
					{Label: "_value", Type: execute.TFloat},
				},
				Data: [][]interface{}{
					{execute.Time(1), "a", 2.0},
					{execute.Time(2), "a", 1.0},
					{execute.Time(3), "b", 3.0},
					{execute.Time(4), "c", 1.0},
				},
			}},
			want: []*executetest.Block{{
				ColMeta: []execute.ColMeta{
					{Label: "_time", Type: execute.TTime},
					{Label: "t1", Type: execute.TString},
					{Label: "_value", Type: execute.TFloat},
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
			data: []execute.Block{&executetest.Block{
				ColMeta: []execute.ColMeta{
					{Label: "_time", Type: execute.TTime},
					{Label: "t1", Type: execute.TString},
					{Label: "_value", Type: execute.TFloat},
				},
				Data: [][]interface{}{
					{execute.Time(1), "a", 2.0},
					{execute.Time(2), "a", 1.0},
					{execute.Time(3), "b", 3.0},
					{execute.Time(3), "c", 1.0},
				},
			}},
			want: []*executetest.Block{{
				ColMeta: []execute.ColMeta{
					{Label: "_time", Type: execute.TTime},
					{Label: "t1", Type: execute.TString},
					{Label: "_value", Type: execute.TFloat},
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
				func(d execute.Dataset, c execute.BlockBuilderCache) execute.Transformation {
					return functions.NewUniqueTransformation(d, c, tc.spec)
				},
			)
		})
	}
}

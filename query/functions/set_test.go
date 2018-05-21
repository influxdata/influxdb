package functions_test

import (
	"testing"

	"github.com/influxdata/platform/query/functions"
	"github.com/influxdata/platform/query"
	"github.com/influxdata/platform/query/execute"
	"github.com/influxdata/platform/query/execute/executetest"
	"github.com/influxdata/platform/query/querytest"
)

func TestSetOperation_Marshaling(t *testing.T) {
	data := []byte(`{"id":"set","kind":"set","spec":{"key":"t1","value":"v1"}}`)
	op := &query.Operation{
		ID: "set",
		Spec: &functions.SetOpSpec{
			Key:   "t1",
			Value: "v1",
		},
	}

	querytest.OperationMarshalingTestHelper(t, data, op)
}

func TestSet_Process(t *testing.T) {
	testCases := []struct {
		name string
		spec *functions.SetProcedureSpec
		data []execute.Block
		want []*executetest.Block
	}{
		{
			name: "new col",
			spec: &functions.SetProcedureSpec{
				Key:   "t1",
				Value: "bob",
			},
			data: []execute.Block{&executetest.Block{
				ColMeta: []execute.ColMeta{
					{Label: "_time", Type: execute.TTime},
					{Label: "_value", Type: execute.TFloat},
				},
				Data: [][]interface{}{
					{execute.Time(1), 2.0},
					{execute.Time(2), 1.0},
				},
			}},
			want: []*executetest.Block{{
				ColMeta: []execute.ColMeta{
					{Label: "_time", Type: execute.TTime},
					{Label: "_value", Type: execute.TFloat},
					{Label: "t1", Type: execute.TString},
				},
				Data: [][]interface{}{
					{execute.Time(1), 2.0, "bob"},
					{execute.Time(2), 1.0, "bob"},
				},
			}},
		},
		{
			name: "replace col",
			spec: &functions.SetProcedureSpec{
				Key:   "t1",
				Value: "bob",
			},
			data: []execute.Block{&executetest.Block{
				ColMeta: []execute.ColMeta{
					{Label: "_time", Type: execute.TTime},
					{Label: "_value", Type: execute.TFloat},
					{Label: "t1", Type: execute.TString},
				},
				Data: [][]interface{}{
					{execute.Time(1), 1.0, "jim"},
					{execute.Time(2), 2.0, "sue"},
				},
			}},
			want: []*executetest.Block{{
				ColMeta: []execute.ColMeta{
					{Label: "_time", Type: execute.TTime},
					{Label: "_value", Type: execute.TFloat},
					{Label: "t1", Type: execute.TString},
				},
				Data: [][]interface{}{
					{execute.Time(1), 1.0, "bob"},
					{execute.Time(2), 2.0, "bob"},
				},
			}},
		},
		{
			name: "replace key col",
			spec: &functions.SetProcedureSpec{
				Key:   "t1",
				Value: "bob",
			},
			data: []execute.Block{&executetest.Block{
				KeyCols: []string{"t1"},
				ColMeta: []execute.ColMeta{
					{Label: "_time", Type: execute.TTime},
					{Label: "_value", Type: execute.TFloat},
					{Label: "t1", Type: execute.TString},
					{Label: "t2", Type: execute.TString},
				},
				Data: [][]interface{}{
					{execute.Time(1), 1.0, "alice", "a"},
					{execute.Time(2), 1.0, "alice", "b"},
				},
			}},
			want: []*executetest.Block{{
				KeyCols: []string{"t1"},
				ColMeta: []execute.ColMeta{
					{Label: "_time", Type: execute.TTime},
					{Label: "_value", Type: execute.TFloat},
					{Label: "t1", Type: execute.TString},
					{Label: "t2", Type: execute.TString},
				},
				Data: [][]interface{}{
					{execute.Time(1), 1.0, "bob", "a"},
					{execute.Time(2), 1.0, "bob", "b"},
				},
			}},
		},
		{
			name: "replace common col, merging blocks",
			spec: &functions.SetProcedureSpec{
				Key:   "t1",
				Value: "bob",
			},
			data: []execute.Block{
				&executetest.Block{
					KeyCols: []string{"t1"},
					ColMeta: []execute.ColMeta{
						{Label: "_time", Type: execute.TTime},
						{Label: "_value", Type: execute.TFloat},
						{Label: "t1", Type: execute.TString},
					},
					Data: [][]interface{}{
						{execute.Time(1), 1.0, "alice"},
						{execute.Time(2), 1.0, "alice"},
					},
				},
				&executetest.Block{
					KeyCols: []string{"t1"},
					ColMeta: []execute.ColMeta{
						{Label: "_time", Type: execute.TTime},
						{Label: "_value", Type: execute.TFloat},
						{Label: "t1", Type: execute.TString},
					},
					Data: [][]interface{}{
						{execute.Time(3), 3.0, "sue"},
						{execute.Time(4), 5.0, "sue"},
					},
				},
			},
			want: []*executetest.Block{{
				KeyCols: []string{"t1"},
				ColMeta: []execute.ColMeta{
					{Label: "_time", Type: execute.TTime},
					{Label: "_value", Type: execute.TFloat},
					{Label: "t1", Type: execute.TString},
				},
				Data: [][]interface{}{
					{execute.Time(1), 1.0, "bob"},
					{execute.Time(2), 1.0, "bob"},
					{execute.Time(3), 3.0, "bob"},
					{execute.Time(4), 5.0, "bob"},
				},
			}},
		},
		{
			name: "new common col, multiple blocks",
			spec: &functions.SetProcedureSpec{
				Key:   "t2",
				Value: "bob",
			},
			data: []execute.Block{
				&executetest.Block{
					KeyCols: []string{"t1"},
					ColMeta: []execute.ColMeta{
						{Label: "_time", Type: execute.TTime},
						{Label: "_value", Type: execute.TFloat},
						{Label: "t1", Type: execute.TString},
					},
					Data: [][]interface{}{
						{execute.Time(1), 1.0, "alice"},
						{execute.Time(2), 1.0, "alice"},
					},
				},
				&executetest.Block{
					KeyCols: []string{"t1"},
					ColMeta: []execute.ColMeta{
						{Label: "_time", Type: execute.TTime},
						{Label: "_value", Type: execute.TFloat},
						{Label: "t1", Type: execute.TString},
					},
					Data: [][]interface{}{
						{execute.Time(3), 3.0, "sue"},
						{execute.Time(4), 5.0, "sue"},
					},
				},
			},
			want: []*executetest.Block{
				{
					KeyCols: []string{"t1"},
					ColMeta: []execute.ColMeta{
						{Label: "_time", Type: execute.TTime},
						{Label: "_value", Type: execute.TFloat},
						{Label: "t1", Type: execute.TString},
						{Label: "t2", Type: execute.TString},
					},
					Data: [][]interface{}{
						{execute.Time(1), 1.0, "alice", "bob"},
						{execute.Time(2), 1.0, "alice", "bob"},
					},
				},
				{
					KeyCols: []string{"t1"},
					ColMeta: []execute.ColMeta{
						{Label: "_time", Type: execute.TTime},
						{Label: "_value", Type: execute.TFloat},
						{Label: "t1", Type: execute.TString},
						{Label: "t2", Type: execute.TString},
					},
					Data: [][]interface{}{
						{execute.Time(3), 3.0, "sue", "bob"},
						{execute.Time(4), 5.0, "sue", "bob"},
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
					return functions.NewSetTransformation(d, c, tc.spec)
				},
			)
		})
	}
}

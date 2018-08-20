package functions_test

import (
	"sort"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/platform/query"
	"github.com/influxdata/platform/query/execute"
	"github.com/influxdata/platform/query/execute/executetest"
	"github.com/influxdata/platform/query/functions"
	"github.com/influxdata/platform/query/plan"
	"github.com/influxdata/platform/query/plan/plantest"
	"github.com/influxdata/platform/query/querytest"
)

func TestJoin_NewQuery(t *testing.T) {
	tests := []querytest.NewQueryTestCase{
		{
			Name: "basic two-way join",
			Raw: `
				a = from(db:"dbA") |> range(start:-1h)
				b = from(db:"dbB") |> range(start:-1h)
				join(tables:{a:a,b:b}, on:["host"])`,
			Want: &query.Spec{
				Operations: []*query.Operation{
					{
						ID: "from0",
						Spec: &functions.FromOpSpec{
							Database: "dbA",
						},
					},
					{
						ID: "range1",
						Spec: &functions.RangeOpSpec{
							Start: query.Time{
								Relative:   -1 * time.Hour,
								IsRelative: true,
							},
							Stop: query.Time{
								IsRelative: true,
							},
							TimeCol:  "_time",
							StartCol: "_start",
							StopCol:  "_stop",
						},
					},
					{
						ID: "from2",
						Spec: &functions.FromOpSpec{
							Database: "dbB",
						},
					},
					{
						ID: "range3",
						Spec: &functions.RangeOpSpec{
							Start: query.Time{
								Relative:   -1 * time.Hour,
								IsRelative: true,
							},
							Stop: query.Time{
								IsRelative: true,
							},
							TimeCol:  "_time",
							StartCol: "_start",
							StopCol:  "_stop",
						},
					},
					{
						ID: "join4",
						Spec: &functions.JoinOpSpec{
							On:         []string{"host"},
							TableNames: map[query.OperationID]string{"range1": "a", "range3": "b"},
							Method:     "inner",
						},
					},
				},
				Edges: []query.Edge{
					{Parent: "from0", Child: "range1"},
					{Parent: "from2", Child: "range3"},
					{Parent: "range1", Child: "join4"},
					{Parent: "range3", Child: "join4"},
				},
			},
		},
		{
			Name: "from with join with complex ast",
			Raw: `
				a = from(db:"flux") |> range(start:-1h)
				b = from(db:"flux") |> range(start:-1h)
				join(tables:{a:a,b:b}, on:["t1"])
			`,
			Want: &query.Spec{
				Operations: []*query.Operation{
					{
						ID: "from0",
						Spec: &functions.FromOpSpec{
							Database: "flux",
						},
					},
					{
						ID: "range1",
						Spec: &functions.RangeOpSpec{
							Start: query.Time{
								Relative:   -1 * time.Hour,
								IsRelative: true,
							},
							Stop: query.Time{
								IsRelative: true,
							},
							TimeCol:  "_time",
							StartCol: "_start",
							StopCol:  "_stop",
						},
					},
					{
						ID: "from2",
						Spec: &functions.FromOpSpec{
							Database: "flux",
						},
					},
					{
						ID: "range3",
						Spec: &functions.RangeOpSpec{
							Start: query.Time{
								Relative:   -1 * time.Hour,
								IsRelative: true,
							},
							Stop: query.Time{
								IsRelative: true,
							},
							TimeCol:  "_time",
							StartCol: "_start",
							StopCol:  "_stop",
						},
					},
					{
						ID: "join4",
						Spec: &functions.JoinOpSpec{
							On:         []string{"t1"},
							TableNames: map[query.OperationID]string{"range1": "a", "range3": "b"},
							Method:     "inner",
						},
					},
				},
				Edges: []query.Edge{
					{Parent: "from0", Child: "range1"},
					{Parent: "from2", Child: "range3"},
					{Parent: "range1", Child: "join4"},
					{Parent: "range3", Child: "join4"},
				},
			},
		},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.Name, func(t *testing.T) {
			t.Parallel()
			querytest.NewQueryTestHelper(t, tc)
		})
	}
}

func TestJoinOperation_Marshaling(t *testing.T) {
	data := []byte(`{
		"id":"join",
		"kind":"join",
		"spec":{
			"on":["t1","t2"],
			"tableNames":{"sum1":"a","count3":"b"}
		}
	}`)
	op := &query.Operation{
		ID: "join",
		Spec: &functions.JoinOpSpec{
			On:         []string{"t1", "t2"},
			TableNames: map[query.OperationID]string{"sum1": "a", "count3": "b"},
		},
	}
	querytest.OperationMarshalingTestHelper(t, data, op)
}

func TestMergeJoin_Process(t *testing.T) {
	parentID0 := plantest.RandomProcedureID()
	parentID1 := plantest.RandomProcedureID()
	tableNames := map[plan.ProcedureID]string{
		parentID0: "a",
		parentID1: "b",
	}
	testCases := []struct {
		skip  bool
		name  string
		spec  *functions.MergeJoinProcedureSpec
		data0 []*executetest.Table // data from parent 0
		data1 []*executetest.Table // data from parent 1
		want  []*executetest.Table
	}{
		{
			name: "simple inner",
			spec: &functions.MergeJoinProcedureSpec{
				On:         []string{"_time"},
				TableNames: tableNames,
			},
			data0: []*executetest.Table{
				{
					ColMeta: []query.ColMeta{
						{Label: "_time", Type: query.TTime},
						{Label: "_value", Type: query.TFloat},
					},
					Data: [][]interface{}{
						{execute.Time(1), 1.0},
						{execute.Time(2), 2.0},
						{execute.Time(3), 3.0},
					},
				},
			},
			data1: []*executetest.Table{
				{
					ColMeta: []query.ColMeta{
						{Label: "_time", Type: query.TTime},
						{Label: "_value", Type: query.TFloat},
					},
					Data: [][]interface{}{
						{execute.Time(1), 10.0},
						{execute.Time(2), 20.0},
						{execute.Time(3), 30.0},
					},
				},
			},
			want: []*executetest.Table{
				{
					ColMeta: []query.ColMeta{
						{Label: "_time", Type: query.TTime},
						{Label: "a__value", Type: query.TFloat},
						{Label: "b__value", Type: query.TFloat},
					},
					Data: [][]interface{}{
						{execute.Time(1), 1.0, 10.0},
						{execute.Time(2), 2.0, 20.0},
						{execute.Time(3), 3.0, 30.0},
					},
				},
			},
		},
		{
			name: "simple inner with ints",
			spec: &functions.MergeJoinProcedureSpec{
				On:         []string{"_time"},
				TableNames: tableNames,
			},
			data0: []*executetest.Table{
				{
					ColMeta: []query.ColMeta{
						{Label: "_time", Type: query.TTime},
						{Label: "_value", Type: query.TInt},
					},
					Data: [][]interface{}{
						{execute.Time(1), int64(1)},
						{execute.Time(2), int64(2)},
						{execute.Time(3), int64(3)},
					},
				},
			},
			data1: []*executetest.Table{
				{
					ColMeta: []query.ColMeta{
						{Label: "_time", Type: query.TTime},
						{Label: "_value", Type: query.TInt},
					},
					Data: [][]interface{}{
						{execute.Time(1), int64(10)},
						{execute.Time(2), int64(20)},
						{execute.Time(3), int64(30)},
					},
				},
			},
			want: []*executetest.Table{
				{
					ColMeta: []query.ColMeta{
						{Label: "_time", Type: query.TTime},
						{Label: "a__value", Type: query.TInt},
						{Label: "b__value", Type: query.TInt},
					},
					Data: [][]interface{}{
						{execute.Time(1), int64(1), int64(10)},
						{execute.Time(2), int64(2), int64(20)},
						{execute.Time(3), int64(3), int64(30)},
					},
				},
			},
		},
		{
			name: "inner with unsorted tables",
			spec: &functions.MergeJoinProcedureSpec{
				On:         []string{"_time"},
				TableNames: tableNames,
			},
			data0: []*executetest.Table{
				{
					ColMeta: []query.ColMeta{
						{Label: "_time", Type: query.TTime},
						{Label: "_value", Type: query.TFloat},
					},
					Data: [][]interface{}{
						{execute.Time(2), 1.0},
						{execute.Time(1), 2.0},
						{execute.Time(3), 3.0},
					},
				},
			},
			data1: []*executetest.Table{
				{
					ColMeta: []query.ColMeta{
						{Label: "_time", Type: query.TTime},
						{Label: "_value", Type: query.TFloat},
					},
					Data: [][]interface{}{
						{execute.Time(3), 10.0},
						{execute.Time(2), 30.0},
						{execute.Time(1), 20.0},
					},
				},
			},
			want: []*executetest.Table{
				{
					ColMeta: []query.ColMeta{
						{Label: "_time", Type: query.TTime},
						{Label: "a__value", Type: query.TFloat},
						{Label: "b__value", Type: query.TFloat},
					},
					Data: [][]interface{}{
						{execute.Time(1), 2.0, 20.0},
						{execute.Time(2), 1.0, 30.0},
						{execute.Time(3), 3.0, 10.0},
					},
				},
			},
		},
		{
			name: "inner with missing values",
			spec: &functions.MergeJoinProcedureSpec{
				On:         []string{"_time"},
				TableNames: tableNames,
			},
			data0: []*executetest.Table{
				{
					ColMeta: []query.ColMeta{
						{Label: "_time", Type: query.TTime},
						{Label: "_value", Type: query.TFloat},
					},
					Data: [][]interface{}{
						{execute.Time(1), 1.0},
						{execute.Time(2), 2.0},
						{execute.Time(3), 3.0},
					},
				},
			},
			data1: []*executetest.Table{
				{
					ColMeta: []query.ColMeta{
						{Label: "_time", Type: query.TTime},
						{Label: "_value", Type: query.TFloat},
					},
					Data: [][]interface{}{
						{execute.Time(1), 10.0},
						{execute.Time(3), 30.0},
					},
				},
			},
			want: []*executetest.Table{
				{
					ColMeta: []query.ColMeta{
						{Label: "_time", Type: query.TTime},
						{Label: "a__value", Type: query.TFloat},
						{Label: "b__value", Type: query.TFloat},
					},
					Data: [][]interface{}{
						{execute.Time(1), 1.0, 10.0},
						{execute.Time(3), 3.0, 30.0},
					},
				},
			},
		},
		{
			name: "inner with multiple matches",
			spec: &functions.MergeJoinProcedureSpec{
				On:         []string{"_time"},
				TableNames: tableNames,
			},
			data0: []*executetest.Table{
				{
					ColMeta: []query.ColMeta{
						{Label: "_time", Type: query.TTime},
						{Label: "_value", Type: query.TFloat},
					},
					Data: [][]interface{}{
						{execute.Time(1), 1.0},
						{execute.Time(2), 2.0},
						{execute.Time(3), 3.0},
					},
				},
			},
			data1: []*executetest.Table{
				{
					ColMeta: []query.ColMeta{
						{Label: "_time", Type: query.TTime},
						{Label: "_value", Type: query.TFloat},
					},
					Data: [][]interface{}{
						{execute.Time(1), 10.0},
						{execute.Time(1), 10.1},
						{execute.Time(2), 20.0},
						{execute.Time(3), 30.0},
						{execute.Time(3), 30.1},
					},
				},
			},
			want: []*executetest.Table{
				{
					ColMeta: []query.ColMeta{
						{Label: "_time", Type: query.TTime},
						{Label: "a__value", Type: query.TFloat},
						{Label: "b__value", Type: query.TFloat},
					},
					Data: [][]interface{}{
						{execute.Time(1), 1.0, 10.0},
						{execute.Time(1), 1.0, 10.1},
						{execute.Time(2), 2.0, 20.0},
						{execute.Time(3), 3.0, 30.0},
						{execute.Time(3), 3.0, 30.1},
					},
				},
			},
		},
		{
			name: "inner with common tags",
			spec: &functions.MergeJoinProcedureSpec{
				On:         []string{"_time", "t1"},
				TableNames: tableNames,
			},
			data0: []*executetest.Table{
				{
					KeyCols: []string{"t1"},
					ColMeta: []query.ColMeta{
						{Label: "_time", Type: query.TTime},
						{Label: "_value", Type: query.TFloat},
						{Label: "t1", Type: query.TString},
					},
					Data: [][]interface{}{
						{execute.Time(1), 1.0, "a"},
						{execute.Time(2), 2.0, "a"},
						{execute.Time(3), 3.0, "a"},
					},
				},
			},
			data1: []*executetest.Table{
				{
					KeyCols: []string{"t1"},
					ColMeta: []query.ColMeta{
						{Label: "_time", Type: query.TTime},
						{Label: "_value", Type: query.TFloat},
						{Label: "t1", Type: query.TString},
					},
					Data: [][]interface{}{
						{execute.Time(1), 10.0, "a"},
						{execute.Time(2), 20.0, "a"},
						{execute.Time(3), 30.0, "a"},
					},
				},
			},
			want: []*executetest.Table{
				{
					KeyCols: []string{"t1"},
					ColMeta: []query.ColMeta{
						{Label: "_time", Type: query.TTime},
						{Label: "a__value", Type: query.TFloat},
						{Label: "b__value", Type: query.TFloat},
						{Label: "t1", Type: query.TString},
					},
					Data: [][]interface{}{
						{execute.Time(1), 1.0, 10.0, "a"},
						{execute.Time(2), 2.0, 20.0, "a"},
						{execute.Time(3), 3.0, 30.0, "a"},
					},
				},
			},
		},
		{
			name: "inner with extra attributes",
			spec: &functions.MergeJoinProcedureSpec{
				On:         []string{"_time", "t1"},
				TableNames: tableNames,
			},
			data0: []*executetest.Table{
				{
					ColMeta: []query.ColMeta{
						{Label: "_time", Type: query.TTime},
						{Label: "_value", Type: query.TFloat},
						{Label: "t1", Type: query.TString},
					},
					Data: [][]interface{}{
						{execute.Time(1), 1.0, "a"},
						{execute.Time(1), 1.5, "b"},
						{execute.Time(2), 2.0, "a"},
						{execute.Time(2), 2.5, "b"},
						{execute.Time(3), 3.0, "a"},
						{execute.Time(3), 3.5, "b"},
					},
				},
			},
			data1: []*executetest.Table{
				{
					ColMeta: []query.ColMeta{
						{Label: "_time", Type: query.TTime},
						{Label: "_value", Type: query.TFloat},
						{Label: "t1", Type: query.TString},
					},
					Data: [][]interface{}{
						{execute.Time(1), 10.0, "a"},
						{execute.Time(1), 10.1, "b"},
						{execute.Time(2), 20.0, "a"},
						{execute.Time(2), 20.1, "b"},
						{execute.Time(3), 30.0, "a"},
						{execute.Time(3), 30.1, "b"},
					},
				},
			},
			want: []*executetest.Table{
				{
					ColMeta: []query.ColMeta{
						{Label: "_time", Type: query.TTime},
						{Label: "a__value", Type: query.TFloat},
						{Label: "b__value", Type: query.TFloat},
						{Label: "t1", Type: query.TString},
					},
					Data: [][]interface{}{
						{execute.Time(1), 1.0, 10.0, "a"},
						{execute.Time(1), 1.5, 10.1, "b"},
						{execute.Time(2), 2.0, 20.0, "a"},
						{execute.Time(2), 2.5, 20.1, "b"},
						{execute.Time(3), 3.0, 30.0, "a"},
						{execute.Time(3), 3.5, 30.1, "b"},
					},
				},
			},
		},
		{
			name: "inner with tags and extra attributes",
			spec: &functions.MergeJoinProcedureSpec{
				On:         []string{"_time", "t1", "t2"},
				TableNames: tableNames,
			},
			data0: []*executetest.Table{
				{
					KeyCols: []string{"t1"},
					ColMeta: []query.ColMeta{
						{Label: "_time", Type: query.TTime},
						{Label: "_value", Type: query.TFloat},
						{Label: "t1", Type: query.TString},
						{Label: "t2", Type: query.TString},
					},
					Data: [][]interface{}{
						{execute.Time(1), 1.0, "a", "x"},
						{execute.Time(1), 1.5, "a", "y"},
						{execute.Time(2), 2.0, "a", "x"},
						{execute.Time(2), 2.5, "a", "y"},
						{execute.Time(3), 3.0, "a", "x"},
						{execute.Time(3), 3.5, "a", "y"},
					},
				},
			},
			data1: []*executetest.Table{
				{
					KeyCols: []string{"t1"},
					ColMeta: []query.ColMeta{
						{Label: "_time", Type: query.TTime},
						{Label: "_value", Type: query.TFloat},
						{Label: "t1", Type: query.TString},
						{Label: "t2", Type: query.TString},
					},
					Data: [][]interface{}{
						{execute.Time(1), 10.0, "a", "x"},
						{execute.Time(1), 10.1, "a", "y"},
						{execute.Time(2), 20.0, "a", "x"},
						{execute.Time(2), 20.1, "a", "y"},
						{execute.Time(3), 30.0, "a", "x"},
						{execute.Time(3), 30.1, "a", "y"},
					},
				},
			},
			want: []*executetest.Table{
				{
					KeyCols: []string{"t1"},
					ColMeta: []query.ColMeta{
						{Label: "_time", Type: query.TTime},
						{Label: "a__value", Type: query.TFloat},
						{Label: "b__value", Type: query.TFloat},
						{Label: "t1", Type: query.TString},
						{Label: "t2", Type: query.TString},
					},
					Data: [][]interface{}{
						{execute.Time(1), 1.0, 10.0, "a", "x"},
						{execute.Time(1), 1.5, 10.1, "a", "y"},
						{execute.Time(2), 2.0, 20.0, "a", "x"},
						{execute.Time(2), 2.5, 20.1, "a", "y"},
						{execute.Time(3), 3.0, 30.0, "a", "x"},
						{execute.Time(3), 3.5, 30.1, "a", "y"},
					},
				},
			},
		},
		{
			name: "inner with multiple values, tags and extra attributes",
			spec: &functions.MergeJoinProcedureSpec{
				On:         []string{"_time", "t1", "t2"},
				TableNames: tableNames,
			},
			data0: []*executetest.Table{
				{
					KeyCols: []string{"t1"},
					ColMeta: []query.ColMeta{
						{Label: "_time", Type: query.TTime},
						{Label: "_value", Type: query.TFloat},
						{Label: "t1", Type: query.TString},
						{Label: "t2", Type: query.TString},
					},
					Data: [][]interface{}{
						{execute.Time(1), 1.0, "a", "x"},
						{execute.Time(2), 2.0, "a", "x"},
						{execute.Time(2), 2.5, "a", "y"},
						{execute.Time(3), 3.5, "a", "y"},
					},
				},
			},
			data1: []*executetest.Table{
				{
					KeyCols: []string{"t1"},
					ColMeta: []query.ColMeta{
						{Label: "_time", Type: query.TTime},
						{Label: "_value", Type: query.TFloat},
						{Label: "t1", Type: query.TString},
						{Label: "t2", Type: query.TString},
					},
					Data: [][]interface{}{
						{execute.Time(1), 10.0, "a", "x"},
						{execute.Time(1), 10.1, "a", "x"},
						{execute.Time(2), 20.0, "a", "x"},
						{execute.Time(2), 20.1, "a", "y"},
						{execute.Time(3), 30.0, "a", "y"},
						{execute.Time(3), 30.1, "a", "y"},
					},
				},
			},
			want: []*executetest.Table{
				{
					KeyCols: []string{"t1"},
					ColMeta: []query.ColMeta{
						{Label: "_time", Type: query.TTime},
						{Label: "a__value", Type: query.TFloat},
						{Label: "b__value", Type: query.TFloat},
						{Label: "t1", Type: query.TString},
						{Label: "t2", Type: query.TString},
					},
					Data: [][]interface{}{
						{execute.Time(1), 1.0, 10.0, "a", "x"},
						{execute.Time(1), 1.0, 10.1, "a", "x"},
						{execute.Time(2), 2.0, 20.0, "a", "x"},
						{execute.Time(2), 2.5, 20.1, "a", "y"},
						{execute.Time(3), 3.5, 30.0, "a", "y"},
						{execute.Time(3), 3.5, 30.1, "a", "y"},
					},
				},
			},
		},
		{
			name: "inner with multiple tables in each stream",
			spec: &functions.MergeJoinProcedureSpec{
				On:         []string{"_time"},
				TableNames: tableNames,
			},
			data0: []*executetest.Table{
				{
					KeyCols: []string{"_value"},
					ColMeta: []query.ColMeta{
						{Label: "_time", Type: query.TTime},
						{Label: "_value", Type: query.TFloat},
					},
					Data: [][]interface{}{
						{execute.Time(1), 1.0},
					},
				},
				{
					KeyCols: []string{"_value"},
					ColMeta: []query.ColMeta{
						{Label: "_time", Type: query.TTime},
						{Label: "_value", Type: query.TFloat},
					},
					Data: [][]interface{}{
						{execute.Time(2), 2.0},
					},
				},
				{
					KeyCols: []string{"_value"},
					ColMeta: []query.ColMeta{
						{Label: "_time", Type: query.TTime},
						{Label: "_value", Type: query.TFloat},
					},
					Data: [][]interface{}{
						{execute.Time(3), 3.0},
					},
				},
			},
			data1: []*executetest.Table{
				{
					KeyCols: []string{"_value"},
					ColMeta: []query.ColMeta{
						{Label: "_time", Type: query.TTime},
						{Label: "_value", Type: query.TFloat},
					},
					Data: [][]interface{}{
						{execute.Time(1), 1.0},
					},
				},
				{
					KeyCols: []string{"_value"},
					ColMeta: []query.ColMeta{
						{Label: "_time", Type: query.TTime},
						{Label: "_value", Type: query.TFloat},
					},
					Data: [][]interface{}{
						{execute.Time(2), 2.0},
					},
				},
				{
					KeyCols: []string{"_value"},
					ColMeta: []query.ColMeta{
						{Label: "_time", Type: query.TTime},
						{Label: "_value", Type: query.TFloat},
					},
					Data: [][]interface{}{
						{execute.Time(3), 3.0},
					},
				},
			},
			want: []*executetest.Table{
				{
					KeyCols: []string{"a__value", "b__value"},
					ColMeta: []query.ColMeta{
						{Label: "_time", Type: query.TTime},
						{Label: "a__value", Type: query.TFloat},
						{Label: "b__value", Type: query.TFloat},
					},
					Data: [][]interface{}{
						{execute.Time(1), 1.0, 1.0},
					},
				},
				{
					KeyCols: []string{"a__value", "b__value"},
					ColMeta: []query.ColMeta{
						{Label: "_time", Type: query.TTime},
						{Label: "a__value", Type: query.TFloat},
						{Label: "b__value", Type: query.TFloat},
					},
					Data: [][]interface{}{
						{execute.Time(2), 2.0, 2.0},
					},
				},
				{
					KeyCols: []string{"a__value", "b__value"},
					ColMeta: []query.ColMeta{
						{Label: "_time", Type: query.TTime},
						{Label: "a__value", Type: query.TFloat},
						{Label: "b__value", Type: query.TFloat},
					},
					Data: [][]interface{}{
						{execute.Time(3), 3.0, 3.0},
					},
				},
			},
		},
		{
			name: "inner with multiple unsorted tables in each stream",
			spec: &functions.MergeJoinProcedureSpec{
				On:         []string{"_time"},
				TableNames: tableNames,
			},
			data0: []*executetest.Table{
				{
					KeyCols: []string{"_key"},
					ColMeta: []query.ColMeta{
						{Label: "_time", Type: query.TTime},
						{Label: "_key", Type: query.TString},
					},
					Data: [][]interface{}{
						{execute.Time(3), "a"},
						{execute.Time(1), "a"},
					},
				},
				{
					KeyCols: []string{"_key"},
					ColMeta: []query.ColMeta{
						{Label: "_time", Type: query.TTime},
						{Label: "_key", Type: query.TString},
					},
					Data: [][]interface{}{
						{execute.Time(4), "b"},
						{execute.Time(2), "b"},
					},
				},
				{
					KeyCols: []string{"_key"},
					ColMeta: []query.ColMeta{
						{Label: "_time", Type: query.TTime},
						{Label: "_key", Type: query.TString},
					},
					Data: [][]interface{}{
						{execute.Time(5), "c"},
						{execute.Time(2), "c"},
					},
				},
			},
			data1: []*executetest.Table{
				{
					KeyCols: []string{"_key"},
					ColMeta: []query.ColMeta{
						{Label: "_time", Type: query.TTime},
						{Label: "_key", Type: query.TString},
					},
					Data: [][]interface{}{
						{execute.Time(8), "a"},
					},
				},
				{
					KeyCols: []string{"_key"},
					ColMeta: []query.ColMeta{
						{Label: "_time", Type: query.TTime},
						{Label: "_key", Type: query.TString},
					},
					Data: [][]interface{}{
						{execute.Time(5), "b"},
						{execute.Time(7), "b"},
						{execute.Time(6), "b"},
					},
				},
				{
					KeyCols: []string{"_key"},
					ColMeta: []query.ColMeta{
						{Label: "_time", Type: query.TTime},
						{Label: "_key", Type: query.TString},
					},
					Data: [][]interface{}{
						{execute.Time(1), "c"},
					},
				},
			},
			want: []*executetest.Table{
				{
					KeyCols: []string{"a__key", "b__key"},
					ColMeta: []query.ColMeta{
						{Label: "_time", Type: query.TTime},
						{Label: "a__key", Type: query.TString},
						{Label: "b__key", Type: query.TString},
					},
					Data: [][]interface{}{
						{execute.Time(1), "a", "c"},
					},
				},
				{
					KeyCols: []string{"a__key", "b__key"},
					ColMeta: []query.ColMeta{
						{Label: "_time", Type: query.TTime},
						{Label: "a__key", Type: query.TString},
						{Label: "b__key", Type: query.TString},
					},
					Data: [][]interface{}{
						{execute.Time(5), "c", "b"},
					},
				},
			},
		},
		{
			name: "inner with different (but intersecting) group keys",
			spec: &functions.MergeJoinProcedureSpec{
				On:         []string{"_time", "t2"},
				TableNames: tableNames,
			},
			data0: []*executetest.Table{
				{
					KeyCols: []string{"t1", "t2"},
					ColMeta: []query.ColMeta{
						{Label: "_time", Type: query.TTime},
						{Label: "_value", Type: query.TFloat},
						{Label: "t1", Type: query.TString},
						{Label: "t2", Type: query.TString},
					},
					Data: [][]interface{}{
						{execute.Time(1), 1.0, "a", "x"},
						{execute.Time(2), 2.0, "a", "x"},
						{execute.Time(3), 3.0, "a", "x"},
					},
				},
				{
					KeyCols: []string{"t1", "t2"},
					ColMeta: []query.ColMeta{
						{Label: "_time", Type: query.TTime},
						{Label: "_value", Type: query.TFloat},
						{Label: "t1", Type: query.TString},
						{Label: "t2", Type: query.TString},
					},
					Data: [][]interface{}{
						{execute.Time(1), 1.5, "a", "y"},
						{execute.Time(2), 2.5, "a", "y"},
						{execute.Time(3), 3.5, "a", "y"},
					},
				},
			},
			data1: []*executetest.Table{
				{
					KeyCols: []string{"t1"},
					ColMeta: []query.ColMeta{
						{Label: "_time", Type: query.TTime},
						{Label: "_value", Type: query.TFloat},
						{Label: "t1", Type: query.TString},
						{Label: "t2", Type: query.TString},
					},
					Data: [][]interface{}{
						{execute.Time(1), 10.0, "a", "x"},
						{execute.Time(1), 10.1, "a", "y"},
						{execute.Time(2), 20.0, "a", "x"},
						{execute.Time(2), 20.1, "a", "y"},
						{execute.Time(3), 30.0, "a", "x"},
						{execute.Time(3), 30.1, "a", "y"},
					},
				},
			},
			want: []*executetest.Table{
				{
					KeyCols: []string{"a_t1", "b_t1", "t2"},
					ColMeta: []query.ColMeta{
						{Label: "_time", Type: query.TTime},
						{Label: "a__value", Type: query.TFloat},
						{Label: "a_t1", Type: query.TString},
						{Label: "b__value", Type: query.TFloat},
						{Label: "b_t1", Type: query.TString},
						{Label: "t2", Type: query.TString},
					},
					Data: [][]interface{}{
						{execute.Time(1), 1.0, "a", 10.0, "a", "x"},
						{execute.Time(2), 2.0, "a", 20.0, "a", "x"},
						{execute.Time(3), 3.0, "a", 30.0, "a", "x"},
					},
				},
				{
					KeyCols: []string{"a_t1", "b_t1", "t2"},
					ColMeta: []query.ColMeta{
						{Label: "_time", Type: query.TTime},
						{Label: "a__value", Type: query.TFloat},
						{Label: "a_t1", Type: query.TString},
						{Label: "b__value", Type: query.TFloat},
						{Label: "b_t1", Type: query.TString},
						{Label: "t2", Type: query.TString},
					},
					Data: [][]interface{}{
						{execute.Time(1), 1.5, "a", 10.1, "a", "y"},
						{execute.Time(2), 2.5, "a", 20.1, "a", "y"},
						{execute.Time(3), 3.5, "a", 30.1, "a", "y"},
					},
				},
			},
		},
		{
			name: "inner with different (and not intersecting) group keys",
			spec: &functions.MergeJoinProcedureSpec{
				On:         []string{"_time", "t2"},
				TableNames: tableNames,
			},
			data0: []*executetest.Table{
				{
					KeyCols: []string{"t1"},
					ColMeta: []query.ColMeta{
						{Label: "_time", Type: query.TTime},
						{Label: "_value", Type: query.TFloat},
						{Label: "t1", Type: query.TString},
						{Label: "t2", Type: query.TString},
					},
					Data: [][]interface{}{
						{execute.Time(1), 1.0, "a", "x"},
						{execute.Time(2), 2.0, "a", "x"},
						{execute.Time(3), 3.0, "a", "x"},
						{execute.Time(1), 1.5, "a", "y"},
						{execute.Time(2), 2.5, "a", "y"},
						{execute.Time(3), 3.5, "a", "y"},
					},
				},
			},
			data1: []*executetest.Table{
				{
					KeyCols: []string{"t2"},
					ColMeta: []query.ColMeta{
						{Label: "_time", Type: query.TTime},
						{Label: "_value", Type: query.TFloat},
						{Label: "t1", Type: query.TString},
						{Label: "t2", Type: query.TString},
					},
					Data: [][]interface{}{
						{execute.Time(1), 10.0, "a", "x"},
						{execute.Time(2), 20.0, "a", "x"},
						{execute.Time(3), 30.0, "a", "x"},
					},
				},
				{
					KeyCols: []string{"t2"},
					ColMeta: []query.ColMeta{
						{Label: "_time", Type: query.TTime},
						{Label: "_value", Type: query.TFloat},
						{Label: "t1", Type: query.TString},
						{Label: "t2", Type: query.TString},
					},
					Data: [][]interface{}{
						{execute.Time(1), 10.1, "a", "y"},
						{execute.Time(2), 20.1, "a", "y"},
						{execute.Time(3), 30.1, "a", "y"},
					},
				},
			},
			want: []*executetest.Table{
				{
					KeyCols: []string{"a_t1", "t2"},
					ColMeta: []query.ColMeta{
						{Label: "_time", Type: query.TTime},
						{Label: "a__value", Type: query.TFloat},
						{Label: "a_t1", Type: query.TString},
						{Label: "b__value", Type: query.TFloat},
						{Label: "b_t1", Type: query.TString},
						{Label: "t2", Type: query.TString},
					},
					Data: [][]interface{}{
						{execute.Time(1), 1.0, "a", 10.0, "a", "x"},
						{execute.Time(2), 2.0, "a", 20.0, "a", "x"},
						{execute.Time(3), 3.0, "a", 30.0, "a", "x"},
					},
				},
				{
					KeyCols: []string{"a_t1", "t2"},
					ColMeta: []query.ColMeta{
						{Label: "_time", Type: query.TTime},
						{Label: "a__value", Type: query.TFloat},
						{Label: "a_t1", Type: query.TString},
						{Label: "b__value", Type: query.TFloat},
						{Label: "b_t1", Type: query.TString},
						{Label: "t2", Type: query.TString},
					},
					Data: [][]interface{}{
						{execute.Time(1), 1.5, "a", 10.1, "a", "y"},
						{execute.Time(2), 2.5, "a", 20.1, "a", "y"},
						{execute.Time(3), 3.5, "a", 30.1, "a", "y"},
					},
				},
			},
		},
		{
			name: "inner where join key does not intersect with group keys",
			spec: &functions.MergeJoinProcedureSpec{
				On:         []string{"_time"},
				TableNames: tableNames,
			},
			data0: []*executetest.Table{
				{
					KeyCols: []string{"t1"},
					ColMeta: []query.ColMeta{
						{Label: "_time", Type: query.TTime},
						{Label: "_value", Type: query.TFloat},
						{Label: "t1", Type: query.TString},
						{Label: "t2", Type: query.TString},
					},
					Data: [][]interface{}{
						{execute.Time(1), 1.0, "a", "x"},
						{execute.Time(2), 2.0, "a", "x"},
						{execute.Time(3), 3.0, "a", "x"},
					},
				},
			},
			data1: []*executetest.Table{
				{
					KeyCols: []string{"t1"},
					ColMeta: []query.ColMeta{
						{Label: "_time", Type: query.TTime},
						{Label: "_value", Type: query.TFloat},
						{Label: "t1", Type: query.TString},
						{Label: "t2", Type: query.TString},
					},
					Data: [][]interface{}{
						{execute.Time(1), 10.0, "a", "x"},
						{execute.Time(2), 20.0, "a", "x"},
						{execute.Time(3), 30.0, "a", "x"},
						{execute.Time(1), 10.1, "a", "y"},
						{execute.Time(2), 20.1, "a", "y"},
						{execute.Time(3), 30.1, "a", "y"},
					},
				},
			},
			want: []*executetest.Table{
				{
					KeyCols: []string{"a_t1", "b_t1"},
					ColMeta: []query.ColMeta{
						{Label: "_time", Type: query.TTime},
						{Label: "a__value", Type: query.TFloat},
						{Label: "a_t1", Type: query.TString},
						{Label: "a_t2", Type: query.TString},
						{Label: "b__value", Type: query.TFloat},
						{Label: "b_t1", Type: query.TString},
						{Label: "b_t2", Type: query.TString},
					},
					Data: [][]interface{}{
						{execute.Time(1), 1.0, "a", "x", 10.0, "a", "x"},
						{execute.Time(1), 1.0, "a", "x", 10.1, "a", "y"},
						{execute.Time(2), 2.0, "a", "x", 20.0, "a", "x"},
						{execute.Time(2), 2.0, "a", "x", 20.1, "a", "y"},
						{execute.Time(3), 3.0, "a", "x", 30.0, "a", "x"},
						{execute.Time(3), 3.0, "a", "x", 30.1, "a", "y"},
					},
				},
			},
		},
		{
			name: "inner with default on parameter",
			spec: &functions.MergeJoinProcedureSpec{
				TableNames: tableNames,
			},
			data0: []*executetest.Table{
				{
					ColMeta: []query.ColMeta{
						{Label: "_time", Type: query.TTime},
						{Label: "_value", Type: query.TFloat},
						{Label: "a_tag", Type: query.TString},
					},
					Data: [][]interface{}{
						{execute.Time(1), 1.0, "a"},
						{execute.Time(2), 2.0, "a"},
						{execute.Time(3), 3.0, "a"},
					},
				},
			},
			data1: []*executetest.Table{
				{
					ColMeta: []query.ColMeta{
						{Label: "_time", Type: query.TTime},
						{Label: "_value", Type: query.TFloat},
						{Label: "b_tag", Type: query.TString},
					},
					Data: [][]interface{}{
						{execute.Time(1), 1.0, "b"},
						{execute.Time(2), 2.0, "b"},
						{execute.Time(3), 3.0, "b"},
					},
				},
			},
			want: []*executetest.Table{
				{
					ColMeta: []query.ColMeta{
						{Label: "_time", Type: query.TTime},
						{Label: "_value", Type: query.TFloat},
						{Label: "a_tag", Type: query.TString},
						{Label: "b_tag", Type: query.TString},
					},
					Data: [][]interface{}{
						{execute.Time(1), 1.0, "a", "b"},
						{execute.Time(2), 2.0, "a", "b"},
						{execute.Time(3), 3.0, "a", "b"},
					},
				},
			},
		},
		{
			name: "inner satisfying eviction condition",
			spec: &functions.MergeJoinProcedureSpec{
				TableNames: tableNames,
				On:         []string{"_time", "tag"},
			},
			data0: []*executetest.Table{
				{
					KeyCols: []string{"tag"},
					ColMeta: []query.ColMeta{
						{Label: "_time", Type: query.TTime},
						{Label: "_value", Type: query.TFloat},
						{Label: "tag", Type: query.TString},
					},
					Data: [][]interface{}{
						{execute.Time(1), 1.0, "a"},
					},
				},
				{
					KeyCols: []string{"tag"},
					ColMeta: []query.ColMeta{
						{Label: "_time", Type: query.TTime},
						{Label: "_value", Type: query.TFloat},
						{Label: "tag", Type: query.TString},
					},
					Data: [][]interface{}{
						{execute.Time(2), 2.0, "b"},
					},
				},
				{
					KeyCols: []string{"tag"},
					ColMeta: []query.ColMeta{
						{Label: "_time", Type: query.TTime},
						{Label: "_value", Type: query.TFloat},
						{Label: "tag", Type: query.TString},
					},
					Data: [][]interface{}{
						{execute.Time(3), 3.0, "c"},
					},
				},
			},
			data1: []*executetest.Table{
				{
					KeyCols: []string{"tag"},
					ColMeta: []query.ColMeta{
						{Label: "_time", Type: query.TTime},
						{Label: "_value", Type: query.TFloat},
						{Label: "tag", Type: query.TString},
					},
					Data: [][]interface{}{
						{execute.Time(1), 1.0, "a"},
					},
				},
				{
					KeyCols: []string{"tag"},
					ColMeta: []query.ColMeta{
						{Label: "_time", Type: query.TTime},
						{Label: "_value", Type: query.TFloat},
						{Label: "tag", Type: query.TString},
					},
					Data: [][]interface{}{
						{execute.Time(2), 2.0, "b"},
					},
				},
				{
					KeyCols: []string{"tag"},
					ColMeta: []query.ColMeta{
						{Label: "_time", Type: query.TTime},
						{Label: "_value", Type: query.TFloat},
						{Label: "tag", Type: query.TString},
					},
					Data: [][]interface{}{
						{execute.Time(3), 3.0, "c"},
					},
				},
			},
			want: []*executetest.Table{
				{
					KeyCols: []string{"tag"},
					ColMeta: []query.ColMeta{
						{Label: "_time", Type: query.TTime},
						{Label: "a__value", Type: query.TFloat},
						{Label: "b__value", Type: query.TFloat},
						{Label: "tag", Type: query.TString},
					},
					Data: [][]interface{}{
						{execute.Time(1), 1.0, 1.0, "a"},
					},
				},
				{
					KeyCols: []string{"tag"},
					ColMeta: []query.ColMeta{
						{Label: "_time", Type: query.TTime},
						{Label: "a__value", Type: query.TFloat},
						{Label: "b__value", Type: query.TFloat},
						{Label: "tag", Type: query.TString},
					},
					Data: [][]interface{}{
						{execute.Time(2), 2.0, 2.0, "b"},
					},
				},
				{
					KeyCols: []string{"tag"},
					ColMeta: []query.ColMeta{
						{Label: "_time", Type: query.TTime},
						{Label: "a__value", Type: query.TFloat},
						{Label: "b__value", Type: query.TFloat},
						{Label: "tag", Type: query.TString},
					},
					Data: [][]interface{}{
						{execute.Time(3), 3.0, 3.0, "c"},
					},
				},
			},
		},
	}
	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			if tc.skip {
				t.Skip()
			}
			parents := []execute.DatasetID{execute.DatasetID(parentID0), execute.DatasetID(parentID1)}

			tableNames := make(map[execute.DatasetID]string, len(tc.spec.TableNames))
			for pid, name := range tc.spec.TableNames {
				tableNames[execute.DatasetID(pid)] = name
			}

			d := executetest.NewDataset(executetest.RandomDatasetID())
			c := functions.NewMergeJoinCache(executetest.UnlimitedAllocator, parents, tableNames, tc.spec.On)
			c.SetTriggerSpec(execute.DefaultTriggerSpec)
			jt := functions.NewMergeJoinTransformation(d, c, tc.spec, parents, tableNames)

			l := len(tc.data0)
			if len(tc.data1) > l {
				l = len(tc.data1)
			}
			for i := 0; i < l; i++ {
				if i < len(tc.data0) {
					if err := jt.Process(parents[0], tc.data0[i]); err != nil {
						t.Fatal(err)
					}
				}
				if i < len(tc.data1) {
					if err := jt.Process(parents[1], tc.data1[i]); err != nil {
						t.Fatal(err)
					}
				}
			}

			got, err := executetest.TablesFromCache(c)
			if err != nil {
				t.Fatal(err)
			}

			executetest.NormalizeTables(got)
			executetest.NormalizeTables(tc.want)

			sort.Sort(executetest.SortedTables(got))
			sort.Sort(executetest.SortedTables(tc.want))

			if !cmp.Equal(tc.want, got) {
				t.Errorf("unexpected tables -want/+got\n%s", cmp.Diff(tc.want, got))
			}
		})
	}
}

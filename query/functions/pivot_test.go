package functions_test

import (
	"errors"
	"testing"
	"time"

	"github.com/influxdata/platform/query"
	"github.com/influxdata/platform/query/execute"
	"github.com/influxdata/platform/query/execute/executetest"
	"github.com/influxdata/platform/query/functions"
	"github.com/influxdata/platform/query/querytest"
)

func TestPivot_NewQuery(t *testing.T) {
	tests := []querytest.NewQueryTestCase{
		{
			Name: "pivot [_measurement, _field] around _time",
			Raw:  `from(db:"testdb") |> range(start: -1h) |> pivot(rowKey: ["_time"], colKey: ["_measurement", "_field"], valueCol: "_value")`,
			Want: &query.Spec{
				Operations: []*query.Operation{
					{
						ID: "from0",
						Spec: &functions.FromOpSpec{
							Database: "testdb",
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
						ID: "pivot2",
						Spec: &functions.PivotOpSpec{
							RowKey:   []string{"_time"},
							ColKey:   []string{"_measurement", "_field"},
							ValueCol: "_value",
						},
					},
				},
				Edges: []query.Edge{
					{Parent: "from0", Child: "range1"},
					{Parent: "range1", Child: "pivot2"},
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

func TestPivotOperation_Marshaling(t *testing.T) {
	data := []byte(`{
		"id":"pivot",
		"kind":"pivot",
		"spec":{
			"row_key":["_time"],
			"col_key":["_measurement", "_field"], 
			"value_col":"_value"
		}
	}`)
	op := &query.Operation{
		ID: "pivot",
		Spec: &functions.PivotOpSpec{
			RowKey:   []string{"_time"},
			ColKey:   []string{"_measurement", "_field"},
			ValueCol: "_value",
		},
	}
	querytest.OperationMarshalingTestHelper(t, data, op)
}

func TestPivot_Process(t *testing.T) {
	testCases := []struct {
		name    string
		spec    *functions.PivotProcedureSpec
		data    []query.Table
		want    []*executetest.Table
		wantErr error
	}{
		{
			name: "overlapping rowKey and colKey",
			spec: &functions.PivotProcedureSpec{
				RowKey:   []string{"_time", "a"},
				ColKey:   []string{"_measurement", "_field", "a"},
				ValueCol: "_value",
			},
			data:    nil,
			wantErr: errors.New("column name found in both rowKey and colKey: a"),
		},
		{
			name: "_field flatten case one table",
			spec: &functions.PivotProcedureSpec{
				RowKey:   []string{"_time"},
				ColKey:   []string{"_field"},
				ValueCol: "_value",
			},
			data: []query.Table{
				&executetest.Table{
					KeyCols: []string{"_measurement"},
					ColMeta: []query.ColMeta{
						{Label: "_time", Type: query.TTime},
						{Label: "_value", Type: query.TFloat},
						{Label: "_measurement", Type: query.TString},
						{Label: "_field", Type: query.TString},
					},
					Data: [][]interface{}{
						{execute.Time(1), 1.0, "m1", "f1"},
						{execute.Time(1), 2.0, "m1", "f2"},
						{execute.Time(2), 3.0, "m1", "f1"},
						{execute.Time(2), 4.0, "m1", "f2"},
					},
				},
			},
			want: []*executetest.Table{
				{
					KeyCols: []string{"_measurement"},
					ColMeta: []query.ColMeta{
						{Label: "_time", Type: query.TTime},
						{Label: "_measurement", Type: query.TString},
						{Label: "f1", Type: query.TFloat},
						{Label: "f2", Type: query.TFloat},
					},
					Data: [][]interface{}{
						{execute.Time(1), "m1", 1.0, 2.0},
						{execute.Time(2), "m1", 3.0, 4.0},
					},
				},
			},
		},
		{
			name: "_field flatten case two tables",
			spec: &functions.PivotProcedureSpec{
				RowKey:   []string{"_time"},
				ColKey:   []string{"_field"},
				ValueCol: "_value",
			},
			data: []query.Table{
				&executetest.Table{
					KeyCols: []string{"_measurement"},
					ColMeta: []query.ColMeta{
						{Label: "_time", Type: query.TTime},
						{Label: "_value", Type: query.TFloat},
						{Label: "_measurement", Type: query.TString},
						{Label: "_field", Type: query.TString},
					},
					Data: [][]interface{}{
						{execute.Time(1), 1.0, "m1", "f1"},
						{execute.Time(1), 2.0, "m1", "f2"},
						{execute.Time(2), 3.0, "m1", "f1"},
						{execute.Time(2), 4.0, "m1", "f2"},
					},
				},
				&executetest.Table{
					KeyCols: []string{"_measurement"},
					ColMeta: []query.ColMeta{
						{Label: "_time", Type: query.TTime},
						{Label: "_value", Type: query.TFloat},
						{Label: "_measurement", Type: query.TString},
						{Label: "_field", Type: query.TString},
					},
					Data: [][]interface{}{
						{execute.Time(1), 1.0, "m2", "f3"},
						{execute.Time(1), 2.0, "m2", "f4"},
						{execute.Time(2), 3.0, "m2", "f3"},
						{execute.Time(2), 4.0, "m2", "f4"},
					},
				},
			},
			want: []*executetest.Table{
				{
					KeyCols: []string{"_measurement"},
					ColMeta: []query.ColMeta{
						{Label: "_time", Type: query.TTime},
						{Label: "_measurement", Type: query.TString},
						{Label: "f1", Type: query.TFloat},
						{Label: "f2", Type: query.TFloat},
					},
					Data: [][]interface{}{
						{execute.Time(1), "m1", 1.0, 2.0},
						{execute.Time(2), "m1", 3.0, 4.0},
					},
				},
				{
					KeyCols: []string{"_measurement"},
					ColMeta: []query.ColMeta{
						{Label: "_time", Type: query.TTime},
						{Label: "_measurement", Type: query.TString},
						{Label: "f3", Type: query.TFloat},
						{Label: "f4", Type: query.TFloat},
					},
					Data: [][]interface{}{
						{execute.Time(1), "m2", 1.0, 2.0},
						{execute.Time(2), "m2", 3.0, 4.0},
					},
				},
			},
		},
		{
			name: "duplicate rowKey + colKey",
			spec: &functions.PivotProcedureSpec{
				RowKey:   []string{"_time"},
				ColKey:   []string{"_measurement", "_field"},
				ValueCol: "_value",
			},
			data: []query.Table{
				&executetest.Table{
					KeyCols: []string{"_measurement"},
					ColMeta: []query.ColMeta{
						{Label: "_time", Type: query.TTime},
						{Label: "_value", Type: query.TFloat},
						{Label: "_measurement", Type: query.TString},
						{Label: "_field", Type: query.TString},
					},
					Data: [][]interface{}{
						{execute.Time(1), 1.0, "m1", "f1"},
						{execute.Time(1), 2.0, "m1", "f2"},
						{execute.Time(2), 3.0, "m1", "f1"},
						{execute.Time(2), 4.0, "m1", "f2"},
						{execute.Time(1), 5.0, "m1", "f1"},
					},
				},
			},
			want: []*executetest.Table{
				{
					KeyCols: nil,
					ColMeta: []query.ColMeta{
						{Label: "_time", Type: query.TTime},
						{Label: "m1_f1", Type: query.TFloat},
						{Label: "m1_f2", Type: query.TFloat},
					},
					Data: [][]interface{}{
						{execute.Time(1), 5.0, 2.0},
						{execute.Time(2), 3.0, 4.0},
					},
				},
			},
		},
		{
			name: "dropping a column not in rowKey or groupKey",
			spec: &functions.PivotProcedureSpec{
				RowKey:   []string{"_time"},
				ColKey:   []string{"_measurement", "_field"},
				ValueCol: "_value",
			},
			data: []query.Table{
				&executetest.Table{
					KeyCols: []string{"_measurement"},
					ColMeta: []query.ColMeta{
						{Label: "_time", Type: query.TTime},
						{Label: "_value", Type: query.TFloat},
						{Label: "_measurement", Type: query.TString},
						{Label: "_field", Type: query.TString},
						{Label: "droppedcol", Type: query.TInt},
					},
					Data: [][]interface{}{
						{execute.Time(1), 1.0, "m1", "f1", 1},
						{execute.Time(1), 2.0, "m1", "f2", 1},
						{execute.Time(2), 3.0, "m1", "f1", 1},
						{execute.Time(2), 4.0, "m1", "f2", 1},
						{execute.Time(1), 5.0, "m1", "f1", 1},
					},
				},
			},
			want: []*executetest.Table{
				{
					KeyCols: nil,
					ColMeta: []query.ColMeta{
						{Label: "_time", Type: query.TTime},
						{Label: "m1_f1", Type: query.TFloat},
						{Label: "m1_f2", Type: query.TFloat},
					},
					Data: [][]interface{}{
						{execute.Time(1), 5.0, 2.0},
						{execute.Time(2), 3.0, 4.0},
					},
				},
			},
		},
		{
			name: "group key doesn't change",
			spec: &functions.PivotProcedureSpec{
				RowKey:   []string{"_time"},
				ColKey:   []string{"_measurement", "_field"},
				ValueCol: "_value",
			},
			data: []query.Table{
				&executetest.Table{
					KeyCols: []string{"grouper"},
					ColMeta: []query.ColMeta{
						{Label: "_time", Type: query.TTime},
						{Label: "_value", Type: query.TFloat},
						{Label: "_measurement", Type: query.TString},
						{Label: "_field", Type: query.TString},
						{Label: "grouper", Type: query.TString},
					},
					Data: [][]interface{}{
						{execute.Time(1), 1.0, "m1", "f1", "A"},
						{execute.Time(1), 2.0, "m1", "f2", "A"},
						{execute.Time(2), 3.0, "m1", "f1", "A"},
						{execute.Time(2), 4.0, "m1", "f2", "A"},
						{execute.Time(1), 5.0, "m1", "f1", "A"},
					},
				},
			},
			want: []*executetest.Table{
				{
					KeyCols: []string{"grouper"},
					ColMeta: []query.ColMeta{
						{Label: "_time", Type: query.TTime},
						{Label: "grouper", Type: query.TString},
						{Label: "m1_f1", Type: query.TFloat},
						{Label: "m1_f2", Type: query.TFloat},
					},
					Data: [][]interface{}{
						{execute.Time(1), "A", 5.0, 2.0},
						{execute.Time(2), "A", 3.0, 4.0},
					},
				},
			},
		},
		{
			name: "group key loses a member",
			spec: &functions.PivotProcedureSpec{
				RowKey:   []string{"_time"},
				ColKey:   []string{"_measurement", "_field"},
				ValueCol: "_value",
			},
			data: []query.Table{
				&executetest.Table{
					KeyCols: []string{"grouper", "_field"},
					ColMeta: []query.ColMeta{
						{Label: "_time", Type: query.TTime},
						{Label: "_value", Type: query.TFloat},
						{Label: "_measurement", Type: query.TString},
						{Label: "_field", Type: query.TString},
						{Label: "grouper", Type: query.TString},
					},
					Data: [][]interface{}{
						{execute.Time(1), 1.0, "m1", "f1", "A"},
						{execute.Time(2), 3.0, "m1", "f1", "A"},
					},
				},
				&executetest.Table{
					KeyCols: []string{"grouper", "_field"},
					ColMeta: []query.ColMeta{
						{Label: "_time", Type: query.TTime},
						{Label: "_value", Type: query.TFloat},
						{Label: "_measurement", Type: query.TString},
						{Label: "_field", Type: query.TString},
						{Label: "grouper", Type: query.TString},
					},
					Data: [][]interface{}{
						{execute.Time(1), 2.0, "m1", "f2", "B"},
					},
				},
				&executetest.Table{
					KeyCols: []string{"grouper", "_field"},
					ColMeta: []query.ColMeta{
						{Label: "_time", Type: query.TTime},
						{Label: "_value", Type: query.TFloat},
						{Label: "_measurement", Type: query.TString},
						{Label: "_field", Type: query.TString},
						{Label: "grouper", Type: query.TString},
					},
					Data: [][]interface{}{
						{execute.Time(2), 4.0, "m1", "f2", "A"},
					},
				},
			},
			want: []*executetest.Table{
				{
					KeyCols: []string{"grouper"},
					ColMeta: []query.ColMeta{
						{Label: "_time", Type: query.TTime},
						{Label: "grouper", Type: query.TString},
						{Label: "m1_f1", Type: query.TFloat},
						{Label: "m1_f2", Type: query.TFloat},
					},
					Data: [][]interface{}{
						{execute.Time(1), "A", 1.0, 0.0},
						{execute.Time(2), "A", 3.0, 4.0},
					},
				},
				{
					KeyCols: []string{"grouper"},
					ColMeta: []query.ColMeta{
						{Label: "_time", Type: query.TTime},
						{Label: "grouper", Type: query.TString},
						{Label: "m1_f2", Type: query.TFloat},
					},
					Data: [][]interface{}{
						{execute.Time(1), "B", 2.0},
					},
				},
			},
		},
		{
			name: "group key loses all members. drops _value",
			spec: &functions.PivotProcedureSpec{
				RowKey:   []string{"_time"},
				ColKey:   []string{"_measurement", "_field"},
				ValueCol: "grouper",
			},
			data: []query.Table{
				&executetest.Table{
					KeyCols: []string{"grouper", "_field"},
					ColMeta: []query.ColMeta{
						{Label: "_time", Type: query.TTime},
						{Label: "_value", Type: query.TFloat},
						{Label: "_measurement", Type: query.TString},
						{Label: "_field", Type: query.TString},
						{Label: "grouper", Type: query.TString},
					},
					Data: [][]interface{}{
						{execute.Time(1), 1.0, "m1", "f1", "A"},
						{execute.Time(2), 3.0, "m1", "f1", "A"},
					},
				},
				&executetest.Table{
					KeyCols: []string{"grouper", "_field"},
					ColMeta: []query.ColMeta{
						{Label: "_time", Type: query.TTime},
						{Label: "_value", Type: query.TFloat},
						{Label: "_measurement", Type: query.TString},
						{Label: "_field", Type: query.TString},
						{Label: "grouper", Type: query.TString},
					},
					Data: [][]interface{}{
						{execute.Time(1), 2.0, "m1", "f2", "B"},
					},
				},
				&executetest.Table{
					KeyCols: []string{"grouper", "_field"},
					ColMeta: []query.ColMeta{
						{Label: "_time", Type: query.TTime},
						{Label: "_value", Type: query.TFloat},
						{Label: "_measurement", Type: query.TString},
						{Label: "_field", Type: query.TString},
						{Label: "grouper", Type: query.TString},
					},
					Data: [][]interface{}{
						{execute.Time(2), 4.0, "m1", "f2", "A"},
					},
				},
			},
			want: []*executetest.Table{
				{
					KeyCols: nil,
					ColMeta: []query.ColMeta{
						{Label: "_time", Type: query.TTime},
						{Label: "m1_f1", Type: query.TString},
						{Label: "m1_f2", Type: query.TString},
					},
					Data: [][]interface{}{
						{execute.Time(1), "A", "B"},
						{execute.Time(2), "A", "A"},
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
				tc.wantErr,
				func(d execute.Dataset, c execute.TableBuilderCache) execute.Transformation {
					return functions.NewPivotTransformation(d, c, tc.spec)
				},
			)
		})
	}
}

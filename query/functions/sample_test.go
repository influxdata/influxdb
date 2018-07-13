package functions_test

import (
	"testing"

	"github.com/influxdata/platform/query"
	"github.com/influxdata/platform/query/execute"
	"github.com/influxdata/platform/query/execute/executetest"
	"github.com/influxdata/platform/query/functions"
	"github.com/influxdata/platform/query/querytest"
)

func TestSampleOperation_Marshaling(t *testing.T) {
	data := []byte(`{"id":"sample","kind":"sample","spec":{"n":5, "pos":0}}`)
	op := &query.Operation{
		ID: "sample",
		Spec: &functions.SampleOpSpec{
			N:   5,
			Pos: 0,
		},
	}

	querytest.OperationMarshalingTestHelper(t, data, op)
}

func TestSample_Process(t *testing.T) {
	testCases := []struct {
		name   string
		data   query.Table
		want   [][]int
		fromor *functions.SampleSelector
	}{
		{
			fromor: &functions.SampleSelector{
				N:   1,
				Pos: 0,
			},
			name: "everything in separate Do calls",
			data: &executetest.Table{
				KeyCols: []string{"t1"},
				ColMeta: []query.ColMeta{
					{Label: "_time", Type: query.TTime},
					{Label: "_value", Type: query.TFloat},
					{Label: "t1", Type: query.TString},
					{Label: "t2", Type: query.TString},
				},
				Data: [][]interface{}{
					{execute.Time(0), 7.0, "a", "y"},
					{execute.Time(10), 5.0, "a", "x"},
					{execute.Time(20), 9.0, "a", "y"},
					{execute.Time(30), 4.0, "a", "x"},
					{execute.Time(40), 6.0, "a", "y"},
					{execute.Time(50), 8.0, "a", "x"},
					{execute.Time(60), 1.0, "a", "y"},
					{execute.Time(70), 2.0, "a", "x"},
					{execute.Time(80), 3.0, "a", "y"},
					{execute.Time(90), 10.0, "a", "x"},
				},
			},
			want: [][]int{
				{0},
				{0},
				{0},
				{0},
				{0},
				{0},
				{0},
				{0},
				{0},
				{0},
			},
		},
		{
			fromor: &functions.SampleSelector{
				N:   1,
				Pos: 0,
			},
			name: "everything in single Do call",
			data: execute.CopyTable(&executetest.Table{
				KeyCols: []string{"t1"},
				ColMeta: []query.ColMeta{
					{Label: "_time", Type: query.TTime},
					{Label: "_value", Type: query.TFloat},
					{Label: "t1", Type: query.TString},
					{Label: "t2", Type: query.TString},
				},
				Data: [][]interface{}{
					{execute.Time(0), 7.0, "a", "y"},
					{execute.Time(10), 5.0, "a", "x"},
					{execute.Time(20), 9.0, "a", "y"},
					{execute.Time(30), 4.0, "a", "x"},
					{execute.Time(40), 6.0, "a", "y"},
					{execute.Time(50), 8.0, "a", "x"},
					{execute.Time(60), 1.0, "a", "y"},
					{execute.Time(70), 2.0, "a", "x"},
					{execute.Time(80), 3.0, "a", "y"},
					{execute.Time(90), 10.0, "a", "x"},
				},
			}, executetest.UnlimitedAllocator),
			want: [][]int{{
				0,
				1,
				2,
				3,
				4,
				5,
				6,
				7,
				8,
				9,
			}},
		},
		{
			fromor: &functions.SampleSelector{
				N:   2,
				Pos: 0,
			},
			name: "every-other-even",
			data: execute.CopyTable(&executetest.Table{
				KeyCols: []string{"t1"},
				ColMeta: []query.ColMeta{
					{Label: "_time", Type: query.TTime},
					{Label: "_value", Type: query.TFloat},
					{Label: "t1", Type: query.TString},
					{Label: "t2", Type: query.TString},
				},
				Data: [][]interface{}{
					{execute.Time(0), 7.0, "a", "y"},
					{execute.Time(10), 5.0, "a", "x"},
					{execute.Time(20), 9.0, "a", "y"},
					{execute.Time(30), 4.0, "a", "x"},
					{execute.Time(40), 6.0, "a", "y"},
					{execute.Time(50), 8.0, "a", "x"},
					{execute.Time(60), 1.0, "a", "y"},
					{execute.Time(70), 2.0, "a", "x"},
					{execute.Time(80), 3.0, "a", "y"},
					{execute.Time(90), 10.0, "a", "x"},
				},
			}, executetest.UnlimitedAllocator),
			want: [][]int{{
				0,
				2,
				4,
				6,
				8,
			}},
		},
		{
			fromor: &functions.SampleSelector{
				N:   2,
				Pos: 1,
			},
			name: "every-other-odd",
			data: execute.CopyTable(&executetest.Table{
				KeyCols: []string{"t1"},
				ColMeta: []query.ColMeta{
					{Label: "_time", Type: query.TTime},
					{Label: "_value", Type: query.TFloat},
					{Label: "t1", Type: query.TString},
					{Label: "t2", Type: query.TString},
				},
				Data: [][]interface{}{
					{execute.Time(0), 7.0, "a", "y"},
					{execute.Time(10), 5.0, "a", "x"},
					{execute.Time(20), 9.0, "a", "y"},
					{execute.Time(30), 4.0, "a", "x"},
					{execute.Time(40), 6.0, "a", "y"},
					{execute.Time(50), 8.0, "a", "x"},
					{execute.Time(60), 1.0, "a", "y"},
					{execute.Time(70), 2.0, "a", "x"},
					{execute.Time(80), 3.0, "a", "y"},
					{execute.Time(90), 10.0, "a", "x"},
				},
			}, executetest.UnlimitedAllocator),
			want: [][]int{{
				1,
				3,
				5,
				7,
				9,
			}},
		},
		{
			fromor: &functions.SampleSelector{
				N:   3,
				Pos: 0,
			},
			name: "every-third-0",
			data: execute.CopyTable(&executetest.Table{
				KeyCols: []string{"t1"},
				ColMeta: []query.ColMeta{
					{Label: "_time", Type: query.TTime},
					{Label: "_value", Type: query.TFloat},
					{Label: "t1", Type: query.TString},
					{Label: "t2", Type: query.TString},
				},
				Data: [][]interface{}{
					{execute.Time(0), 7.0, "a", "y"},
					{execute.Time(10), 5.0, "a", "x"},
					{execute.Time(20), 9.0, "a", "y"},
					{execute.Time(30), 4.0, "a", "x"},
					{execute.Time(40), 6.0, "a", "y"},
					{execute.Time(50), 8.0, "a", "x"},
					{execute.Time(60), 1.0, "a", "y"},
					{execute.Time(70), 2.0, "a", "x"},
					{execute.Time(80), 3.0, "a", "y"},
					{execute.Time(90), 10.0, "a", "x"},
				},
			}, executetest.UnlimitedAllocator),
			want: [][]int{{
				0,
				3,
				6,
				9,
			}},
		},
		{
			fromor: &functions.SampleSelector{
				N:   3,
				Pos: 1,
			},
			name: "every-third-1",
			data: execute.CopyTable(&executetest.Table{
				KeyCols: []string{"t1"},
				ColMeta: []query.ColMeta{
					{Label: "_time", Type: query.TTime},
					{Label: "_value", Type: query.TFloat},
					{Label: "t1", Type: query.TString},
					{Label: "t2", Type: query.TString},
				},
				Data: [][]interface{}{
					{execute.Time(0), 7.0, "a", "y"},
					{execute.Time(10), 5.0, "a", "x"},
					{execute.Time(20), 9.0, "a", "y"},
					{execute.Time(30), 4.0, "a", "x"},
					{execute.Time(40), 6.0, "a", "y"},
					{execute.Time(50), 8.0, "a", "x"},
					{execute.Time(60), 1.0, "a", "y"},
					{execute.Time(70), 2.0, "a", "x"},
					{execute.Time(80), 3.0, "a", "y"},
					{execute.Time(90), 10.0, "a", "x"},
				},
			}, executetest.UnlimitedAllocator),
			want: [][]int{{
				1,
				4,
				7,
			}},
		},
		{
			fromor: &functions.SampleSelector{
				N:   3,
				Pos: 2,
			},
			name: "every-third-2",
			data: execute.CopyTable(&executetest.Table{
				KeyCols: []string{"t1"},
				ColMeta: []query.ColMeta{
					{Label: "_time", Type: query.TTime},
					{Label: "_value", Type: query.TFloat},
					{Label: "t1", Type: query.TString},
					{Label: "t2", Type: query.TString},
				},
				Data: [][]interface{}{
					{execute.Time(0), 7.0, "a", "y"},
					{execute.Time(10), 5.0, "a", "x"},
					{execute.Time(20), 9.0, "a", "y"},
					{execute.Time(30), 4.0, "a", "x"},
					{execute.Time(40), 6.0, "a", "y"},
					{execute.Time(50), 8.0, "a", "x"},
					{execute.Time(60), 1.0, "a", "y"},
					{execute.Time(70), 2.0, "a", "x"},
					{execute.Time(80), 3.0, "a", "y"},
					{execute.Time(90), 10.0, "a", "x"},
				},
			}, executetest.UnlimitedAllocator),
			want: [][]int{{
				2,
				5,
				8,
			}},
		},
		{
			fromor: &functions.SampleSelector{
				N:   3,
				Pos: 2,
			},
			name: "every-third-2 in separate Do calls",
			data: &executetest.Table{
				KeyCols: []string{"t1"},
				ColMeta: []query.ColMeta{
					{Label: "_time", Type: query.TTime},
					{Label: "_value", Type: query.TFloat},
					{Label: "t1", Type: query.TString},
					{Label: "t2", Type: query.TString},
				},
				Data: [][]interface{}{
					{execute.Time(0), 7.0, "a", "y"},
					{execute.Time(10), 5.0, "a", "x"},
					{execute.Time(20), 9.0, "a", "y"},
					{execute.Time(30), 4.0, "a", "x"},
					{execute.Time(40), 6.0, "a", "y"},
					{execute.Time(50), 8.0, "a", "x"},
					{execute.Time(60), 1.0, "a", "y"},
					{execute.Time(70), 2.0, "a", "x"},
					{execute.Time(80), 3.0, "a", "y"},
					{execute.Time(90), 10.0, "a", "x"},
				},
			},
			want: [][]int{
				nil,
				nil,
				{0},
				nil,
				nil,
				{0},
				nil,
				nil,
				{0},
				nil,
			},
		},
	}
	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			executetest.IndexSelectorFuncTestHelper(
				t,
				tc.fromor,
				tc.data,
				tc.want,
			)
		})
	}
}

func BenchmarkSample(b *testing.B) {
	ss := &functions.SampleSelector{
		N:   10,
		Pos: 0,
	}
	executetest.IndexSelectorFuncBenchmarkHelper(b, ss, NormalTable)
}

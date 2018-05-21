package functions_test

import (
	"testing"

	"github.com/influxdata/ifql/functions"
	"github.com/influxdata/ifql/query/execute"
	"github.com/influxdata/ifql/query/execute/executetest"
)

func TestKeys_Process(t *testing.T) {
	testCases := []struct {
		name string
		spec *functions.KeysProcedureSpec
		data []execute.Block
		want []*executetest.Block
	}{
		{
			name: "one block",
			spec: &functions.KeysProcedureSpec{},
			data: []execute.Block{
				&executetest.Block{
					ColMeta: []execute.ColMeta{
						{Label: "_time", Type: execute.TTime},
						{Label: "_value", Type: execute.TFloat},
						{Label: "tag0", Type: execute.TString},
						{Label: "tag1", Type: execute.TString},
					},
					Data: [][]interface{}{
						{execute.Time(1), 2.0},
					},
				},
			},
			want: []*executetest.Block{{
				ColMeta: []execute.ColMeta{
					{Label: "_value", Type: execute.TString},
				},
				Data: [][]interface{}{
					{"_time"},
					{"_value"},
					{"tag0"},
					{"tag1"},
				},
			}},
		},
		{
			name: "one block except",
			spec: &functions.KeysProcedureSpec{Except: []string{"_value", "_time"}},
			data: []execute.Block{
				&executetest.Block{
					ColMeta: []execute.ColMeta{
						{Label: "_time", Type: execute.TTime},
						{Label: "_value", Type: execute.TFloat},
						{Label: "tag0", Type: execute.TString},
						{Label: "tag1", Type: execute.TString},
					},
					Data: [][]interface{}{
						{execute.Time(1), 2.0},
					},
				},
			},
			want: []*executetest.Block{{
				ColMeta: []execute.ColMeta{
					{Label: "_value", Type: execute.TString},
				},
				Data: [][]interface{}{
					{"tag0"},
					{"tag1"},
				},
			}},
		},
		{
			name: "two blocks",
			spec: &functions.KeysProcedureSpec{},
			data: []execute.Block{
				&executetest.Block{
					KeyCols: []string{"tag0", "tag1"},
					ColMeta: []execute.ColMeta{
						{Label: "tag0", Type: execute.TString},
						{Label: "tag1", Type: execute.TString},
						{Label: "_time", Type: execute.TTime},
						{Label: "_value", Type: execute.TFloat},
					},
					Data: [][]interface{}{
						{"tag0-0", "tag1-0", execute.Time(1), 2.0},
					},
				},
				&executetest.Block{
					KeyCols: []string{"tag0", "tag2"},
					ColMeta: []execute.ColMeta{
						{Label: "tag0", Type: execute.TString},
						{Label: "tag2", Type: execute.TString},
						{Label: "_time", Type: execute.TTime},
						{Label: "_value", Type: execute.TFloat},
					},
					Data: [][]interface{}{
						{"tag0-0", "tag2-0", execute.Time(1), 2.0},
					},
				},
			},
			want: []*executetest.Block{
				{
					KeyCols: []string{"tag0", "tag1"},
					ColMeta: []execute.ColMeta{
						{Label: "tag0", Type: execute.TString},
						{Label: "tag1", Type: execute.TString},
						{Label: "_value", Type: execute.TString},
					},
					Data: [][]interface{}{
						{"tag0-0", "tag1-0", "_time"},
						{"tag0-0", "tag1-0", "_value"},
						{"tag0-0", "tag1-0", "tag0"},
						{"tag0-0", "tag1-0", "tag1"},
					},
				},
				{
					KeyCols: []string{"tag0", "tag2"},
					ColMeta: []execute.ColMeta{
						{Label: "tag0", Type: execute.TString},
						{Label: "tag2", Type: execute.TString},
						{Label: "_value", Type: execute.TString},
					},
					Data: [][]interface{}{
						{"tag0-0", "tag2-0", "_time"},
						{"tag0-0", "tag2-0", "_value"},
						{"tag0-0", "tag2-0", "tag0"},
						{"tag0-0", "tag2-0", "tag2"},
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
					return functions.NewKeysTransformation(d, c, tc.spec)
				},
			)
		})
	}
}

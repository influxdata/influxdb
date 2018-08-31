package functions_test

import (
	"testing"

	"github.com/influxdata/platform/query"
	"github.com/influxdata/platform/query/execute"
	"github.com/influxdata/platform/query/execute/executetest"
	"github.com/influxdata/platform/query/functions"
	"github.com/influxdata/platform/query/querytest"
)

func TestCovariance_NewQuery(t *testing.T) {
	tests := []querytest.NewQueryTestCase{
		{
			Name: "simple covariance",
			Raw:  `from(bucket:"mybucket") |> covariance(columns:["a","b"],)`,
			Want: &query.Spec{
				Operations: []*query.Operation{
					{
						ID: "from0",
						Spec: &functions.FromOpSpec{
							Bucket: "mybucket",
						},
					},
					{
						ID: "covariance1",
						Spec: &functions.CovarianceOpSpec{
							ValueDst: execute.DefaultValueColLabel,
							AggregateConfig: execute.AggregateConfig{
								TimeSrc: execute.DefaultStopColLabel,
								TimeDst: execute.DefaultTimeColLabel,
								Columns: []string{"a", "b"},
							},
						},
					},
				},
				Edges: []query.Edge{
					{Parent: "from0", Child: "covariance1"},
				},
			},
		},
		{
			Name: "pearsonr",
			Raw:  `from(bucket:"mybucket")|>covariance(columns:["a","b"],pearsonr:true)`,
			Want: &query.Spec{
				Operations: []*query.Operation{
					{
						ID: "from0",
						Spec: &functions.FromOpSpec{
							Bucket: "mybucket",
						},
					},
					{
						ID: "covariance1",
						Spec: &functions.CovarianceOpSpec{
							ValueDst:           execute.DefaultValueColLabel,
							PearsonCorrelation: true,
							AggregateConfig: execute.AggregateConfig{
								TimeSrc: execute.DefaultStopColLabel,
								TimeDst: execute.DefaultTimeColLabel,
								Columns: []string{"a", "b"},
							},
						},
					},
				},
				Edges: []query.Edge{
					{Parent: "from0", Child: "covariance1"},
				},
			},
		},
		{
			Name: "global covariance",
			Raw:  `cov(x: from(bucket:"mybucket"), y:from(bucket:"mybucket"), on:["host"], pearsonr:true)`,
			Want: &query.Spec{
				Operations: []*query.Operation{
					{
						ID: "from0",
						Spec: &functions.FromOpSpec{
							Bucket: "mybucket",
						},
					},
					{
						ID: "from1",
						Spec: &functions.FromOpSpec{
							Bucket: "mybucket",
						},
					},
					{
						ID: "join2",
						Spec: &functions.JoinOpSpec{
							On: []string{"host"},
							TableNames: map[query.OperationID]string{
								"from0": "x",
								"from1": "y",
							},
							Method: "inner",
						},
					},
					{
						ID: "covariance3",
						Spec: &functions.CovarianceOpSpec{
							ValueDst:           execute.DefaultValueColLabel,
							PearsonCorrelation: true,
							AggregateConfig: execute.AggregateConfig{
								TimeSrc: execute.DefaultStopColLabel,
								TimeDst: execute.DefaultTimeColLabel,
								Columns: []string{"x__value", "y__value"},
							},
						},
					},
				},
				Edges: []query.Edge{
					{Parent: "from0", Child: "join2"},
					{Parent: "from1", Child: "join2"},
					{Parent: "join2", Child: "covariance3"},
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

func TestCovarianceOperation_Marshaling(t *testing.T) {
	data := []byte(`{
		"id":"covariance",
		"kind":"covariance",
		"spec":{
			"pearsonr":true
		}
	}`)
	op := &query.Operation{
		ID: "covariance",
		Spec: &functions.CovarianceOpSpec{
			PearsonCorrelation: true,
		},
	}
	querytest.OperationMarshalingTestHelper(t, data, op)
}

func TestCovariance_Process(t *testing.T) {
	testCases := []struct {
		name string
		spec *functions.CovarianceProcedureSpec
		data []query.Table
		want []*executetest.Table
	}{
		{
			name: "variance",
			spec: &functions.CovarianceProcedureSpec{
				ValueLabel: execute.DefaultValueColLabel,
				AggregateConfig: execute.AggregateConfig{
					TimeSrc: execute.DefaultStopColLabel,
					TimeDst: execute.DefaultTimeColLabel,
					Columns: []string{"x", "y"},
				},
			},
			data: []query.Table{&executetest.Table{
				KeyCols: []string{"_start", "_stop"},
				ColMeta: []query.ColMeta{
					{Label: "_start", Type: query.TTime},
					{Label: "_stop", Type: query.TTime},
					{Label: "_time", Type: query.TTime},
					{Label: "x", Type: query.TFloat},
					{Label: "y", Type: query.TFloat},
				},
				Data: [][]interface{}{
					{execute.Time(0), execute.Time(5), execute.Time(0), 1.0, 1.0},
					{execute.Time(0), execute.Time(5), execute.Time(1), 2.0, 2.0},
					{execute.Time(0), execute.Time(5), execute.Time(2), 3.0, 3.0},
					{execute.Time(0), execute.Time(5), execute.Time(3), 4.0, 4.0},
					{execute.Time(0), execute.Time(5), execute.Time(4), 5.0, 5.0},
				},
			}},
			want: []*executetest.Table{{
				KeyCols: []string{"_start", "_stop"},
				ColMeta: []query.ColMeta{
					{Label: "_start", Type: query.TTime},
					{Label: "_stop", Type: query.TTime},
					{Label: "_time", Type: query.TTime},
					{Label: "_value", Type: query.TFloat},
				},
				Data: [][]interface{}{
					{execute.Time(0), execute.Time(5), execute.Time(5), 2.5},
				},
			}},
		},
		{
			name: "negative covariance",
			spec: &functions.CovarianceProcedureSpec{
				ValueLabel: execute.DefaultValueColLabel,
				AggregateConfig: execute.AggregateConfig{
					TimeSrc: execute.DefaultStopColLabel,
					TimeDst: execute.DefaultTimeColLabel,
					Columns: []string{"x", "y"},
				},
			},
			data: []query.Table{&executetest.Table{
				KeyCols: []string{"_start", "_stop"},
				ColMeta: []query.ColMeta{
					{Label: "_start", Type: query.TTime},
					{Label: "_stop", Type: query.TTime},
					{Label: "_time", Type: query.TTime},
					{Label: "x", Type: query.TFloat},
					{Label: "y", Type: query.TFloat},
				},
				Data: [][]interface{}{
					{execute.Time(0), execute.Time(5), execute.Time(0), 1.0, 5.0},
					{execute.Time(0), execute.Time(5), execute.Time(1), 2.0, 4.0},
					{execute.Time(0), execute.Time(5), execute.Time(2), 3.0, 3.0},
					{execute.Time(0), execute.Time(5), execute.Time(3), 4.0, 2.0},
					{execute.Time(0), execute.Time(5), execute.Time(4), 5.0, 1.0},
				},
			}},
			want: []*executetest.Table{{
				KeyCols: []string{"_start", "_stop"},
				ColMeta: []query.ColMeta{
					{Label: "_start", Type: query.TTime},
					{Label: "_stop", Type: query.TTime},
					{Label: "_time", Type: query.TTime},
					{Label: "_value", Type: query.TFloat},
				},
				Data: [][]interface{}{
					{execute.Time(0), execute.Time(5), execute.Time(5), -2.5},
				},
			}},
		},
		{
			name: "small covariance",
			spec: &functions.CovarianceProcedureSpec{
				ValueLabel: execute.DefaultValueColLabel,
				AggregateConfig: execute.AggregateConfig{
					TimeSrc: execute.DefaultStopColLabel,
					TimeDst: execute.DefaultTimeColLabel,
					Columns: []string{"x", "y"},
				},
			},
			data: []query.Table{&executetest.Table{
				KeyCols: []string{"_start", "_stop"},
				ColMeta: []query.ColMeta{
					{Label: "_start", Type: query.TTime},
					{Label: "_stop", Type: query.TTime},
					{Label: "_time", Type: query.TTime},
					{Label: "x", Type: query.TFloat},
					{Label: "y", Type: query.TFloat},
				},
				Data: [][]interface{}{
					{execute.Time(0), execute.Time(5), execute.Time(0), 1.0, 1.0},
					{execute.Time(0), execute.Time(5), execute.Time(1), 2.0, 1.0},
					{execute.Time(0), execute.Time(5), execute.Time(2), 3.0, 1.0},
					{execute.Time(0), execute.Time(5), execute.Time(3), 4.0, 1.0},
					{execute.Time(0), execute.Time(5), execute.Time(4), 5.0, 2.0},
				},
			}},
			want: []*executetest.Table{{
				KeyCols: []string{"_start", "_stop"},
				ColMeta: []query.ColMeta{
					{Label: "_start", Type: query.TTime},
					{Label: "_stop", Type: query.TTime},
					{Label: "_time", Type: query.TTime},
					{Label: "_value", Type: query.TFloat},
				},
				Data: [][]interface{}{
					{execute.Time(0), execute.Time(5), execute.Time(5), 0.5},
				},
			}},
		},
		{
			name: "pearson correlation",
			spec: &functions.CovarianceProcedureSpec{
				ValueLabel:         execute.DefaultValueColLabel,
				PearsonCorrelation: true,
				AggregateConfig: execute.AggregateConfig{
					TimeSrc: execute.DefaultStopColLabel,
					TimeDst: execute.DefaultTimeColLabel,
					Columns: []string{"x", "y"},
				},
			},
			data: []query.Table{&executetest.Table{
				KeyCols: []string{"_start", "_stop"},
				ColMeta: []query.ColMeta{
					{Label: "_start", Type: query.TTime},
					{Label: "_stop", Type: query.TTime},
					{Label: "_time", Type: query.TTime},
					{Label: "x", Type: query.TFloat},
					{Label: "y", Type: query.TFloat},
				},
				Data: [][]interface{}{
					{execute.Time(0), execute.Time(5), execute.Time(0), 1.0, 1.0},
					{execute.Time(0), execute.Time(5), execute.Time(1), 2.0, 2.0},
					{execute.Time(0), execute.Time(5), execute.Time(2), 3.0, 3.0},
					{execute.Time(0), execute.Time(5), execute.Time(3), 4.0, 4.0},
					{execute.Time(0), execute.Time(5), execute.Time(4), 5.0, 5.0},
				},
			}},
			want: []*executetest.Table{{
				KeyCols: []string{"_start", "_stop"},
				ColMeta: []query.ColMeta{
					{Label: "_start", Type: query.TTime},
					{Label: "_stop", Type: query.TTime},
					{Label: "_time", Type: query.TTime},
					{Label: "_value", Type: query.TFloat},
				},
				Data: [][]interface{}{
					{execute.Time(0), execute.Time(5), execute.Time(5), 1.0},
				},
			}},
		},
		{
			name: "pearson correlation opposite",
			spec: &functions.CovarianceProcedureSpec{
				ValueLabel:         execute.DefaultValueColLabel,
				PearsonCorrelation: true,
				AggregateConfig: execute.AggregateConfig{
					TimeSrc: execute.DefaultStopColLabel,
					TimeDst: execute.DefaultTimeColLabel,
					Columns: []string{"x", "y"},
				},
			},
			data: []query.Table{&executetest.Table{
				KeyCols: []string{"_start", "_stop"},
				ColMeta: []query.ColMeta{
					{Label: "_start", Type: query.TTime},
					{Label: "_stop", Type: query.TTime},
					{Label: "_time", Type: query.TTime},
					{Label: "x", Type: query.TFloat},
					{Label: "y", Type: query.TFloat},
				},
				Data: [][]interface{}{
					{execute.Time(0), execute.Time(5), execute.Time(0), 1.0, 5.0},
					{execute.Time(0), execute.Time(5), execute.Time(1), 2.0, 4.0},
					{execute.Time(0), execute.Time(5), execute.Time(2), 3.0, 3.0},
					{execute.Time(0), execute.Time(5), execute.Time(3), 4.0, 2.0},
					{execute.Time(0), execute.Time(5), execute.Time(4), 5.0, 1.0},
				},
			}},
			want: []*executetest.Table{{
				KeyCols: []string{"_start", "_stop"},
				ColMeta: []query.ColMeta{
					{Label: "_start", Type: query.TTime},
					{Label: "_stop", Type: query.TTime},
					{Label: "_time", Type: query.TTime},
					{Label: "_value", Type: query.TFloat},
				},
				Data: [][]interface{}{
					{execute.Time(0), execute.Time(5), execute.Time(5), -1.0},
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
					return functions.NewCovarianceTransformation(d, c, tc.spec)
				},
			)
		})
	}
}

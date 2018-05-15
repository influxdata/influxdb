package csv_test

import (
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/ifql/query/execute"
	"github.com/influxdata/ifql/query/execute/executetest"
	"github.com/influxdata/ifql/values"
	"github.com/influxdata/platform/query/csv"
)

func TestResultDecoder(t *testing.T) {
	testCases := []struct {
		name string
		data string
		want []*executetest.Block
	}{
		{
			name: "single block",
			data: `#datatype,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,tag,tag,double
,blkid,_start,_stop,_time,_measurement,host,_value
,0,2018-04-17T00:00:00Z,2018-04-17T00:05:00Z,2018-04-17T00:00:00Z,cpu,A,42.0
,0,2018-04-17T00:00:00Z,2018-04-17T00:05:00Z,2018-04-17T00:00:01Z,cpu,A,43.0
`,
			want: []*executetest.Block{{
				ColMeta: []execute.ColMeta{
					{Label: "_time", Kind: execute.TimeColKind, Type: execute.TTime},
					{Label: "_measurement", Kind: execute.TagColKind, Type: execute.TString},
					{Label: "host", Kind: execute.TagColKind, Type: execute.TString},
					{Label: "_value", Kind: execute.ValueColKind, Type: execute.TFloat},
				},
				Bnds: execute.Bounds{
					Start: values.ConvertTime(time.Date(2018, 4, 17, 0, 0, 0, 0, time.UTC)),
					Stop:  values.ConvertTime(time.Date(2018, 4, 17, 0, 5, 0, 0, time.UTC)),
				},
				Data: [][]interface{}{
					{values.ConvertTime(time.Date(2018, 4, 17, 0, 0, 0, 0, time.UTC)), "cpu", "A", 42.0},
					{values.ConvertTime(time.Date(2018, 4, 17, 0, 0, 1, 0, time.UTC)), "cpu", "A", 43.0},
				},
			}},
		},
		{
			name: "multiple blocks",
			data: `#datatype,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,tag,tag,double
,blkid,_start,_stop,_time,_measurement,host,_value
,0,2018-04-17T00:00:00Z,2018-04-17T00:05:00Z,2018-04-17T00:00:00Z,cpu,A,42.0
,0,2018-04-17T00:00:00Z,2018-04-17T00:05:00Z,2018-04-17T00:00:01Z,cpu,A,43.0
,1,2018-04-17T00:05:00Z,2018-04-17T00:10:00Z,2018-04-17T00:05:00Z,cpu,A,52.0
,1,2018-04-17T00:05:00Z,2018-04-17T00:10:00Z,2018-04-17T00:05:01Z,cpu,A,53.0
`,
			want: []*executetest.Block{
				{
					ColMeta: []execute.ColMeta{
						{Label: "_time", Kind: execute.TimeColKind, Type: execute.TTime},
						{Label: "_measurement", Kind: execute.TagColKind, Type: execute.TString},
						{Label: "host", Kind: execute.TagColKind, Type: execute.TString},
						{Label: "_value", Kind: execute.ValueColKind, Type: execute.TFloat},
					},
					Bnds: execute.Bounds{
						Start: values.ConvertTime(time.Date(2018, 4, 17, 0, 0, 0, 0, time.UTC)),
						Stop:  values.ConvertTime(time.Date(2018, 4, 17, 0, 5, 0, 0, time.UTC)),
					},
					Data: [][]interface{}{
						{values.ConvertTime(time.Date(2018, 4, 17, 0, 0, 0, 0, time.UTC)), "cpu", "A", 42.0},
						{values.ConvertTime(time.Date(2018, 4, 17, 0, 0, 1, 0, time.UTC)), "cpu", "A", 43.0},
					},
				},
				{
					ColMeta: []execute.ColMeta{
						{Label: "_time", Kind: execute.TimeColKind, Type: execute.TTime},
						{Label: "_measurement", Kind: execute.TagColKind, Type: execute.TString},
						{Label: "host", Kind: execute.TagColKind, Type: execute.TString},
						{Label: "_value", Kind: execute.ValueColKind, Type: execute.TFloat},
					},
					Bnds: execute.Bounds{
						Start: values.ConvertTime(time.Date(2018, 4, 17, 0, 5, 0, 0, time.UTC)),
						Stop:  values.ConvertTime(time.Date(2018, 4, 17, 0, 10, 0, 0, time.UTC)),
					},
					Data: [][]interface{}{
						{values.ConvertTime(time.Date(2018, 4, 17, 0, 5, 0, 0, time.UTC)), "cpu", "A", 52.0},
						{values.ConvertTime(time.Date(2018, 4, 17, 0, 5, 1, 0, time.UTC)), "cpu", "A", 53.0},
					},
				},
			},
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			decoder := csv.NewResultDecoder(csv.ResultDecoderConfig{})
			result, err := decoder.Decode(strings.NewReader(tc.data))
			if err != nil {
				t.Fatal(err)
			}
			var got []*executetest.Block
			if err := result.Blocks().Do(func(b execute.Block) error {
				got = append(got, executetest.ConvertBlock(b))
				return nil
			}); err != nil {
				t.Fatal(err)
			}

			if !cmp.Equal(got, tc.want) {
				t.Error("unexpected results -want/+got", cmp.Diff(tc.want, got))
			}
		})
	}

}

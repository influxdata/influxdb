package csv_test

import (
	"bytes"
	"regexp"
	"testing"
	"time"

	"github.com/andreyvit/diff"
	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/platform/query"
	"github.com/influxdata/platform/query/csv"
	"github.com/influxdata/platform/query/execute/executetest"
	"github.com/influxdata/platform/query/values"
	"github.com/pkg/errors"
)

type TestCase struct {
	name          string
	skip          bool
	encoded       []byte
	result        *executetest.Result
	decoderConfig csv.ResultDecoderConfig
	encoderConfig csv.ResultEncoderConfig
}

var symetricalTestCases = []TestCase{
	{
		name:          "single table",
		encoderConfig: csv.DefaultEncoderConfig(),
		encoded: toCRLF(`#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,string,string,double
#group,false,false,true,true,false,true,true,false
#default,_result,,,,,,,
,result,table,_start,_stop,_time,_measurement,host,_value
,,0,2018-04-17T00:00:00Z,2018-04-17T00:05:00Z,2018-04-17T00:00:00Z,cpu,A,42
,,0,2018-04-17T00:00:00Z,2018-04-17T00:05:00Z,2018-04-17T00:00:01Z,cpu,A,43
`),
		result: &executetest.Result{
			Nm: "_result",
			Tbls: []*executetest.Table{{
				KeyCols: []string{"_start", "_stop", "_measurement", "host"},
				ColMeta: []query.ColMeta{
					{Label: "_start", Type: query.TTime},
					{Label: "_stop", Type: query.TTime},
					{Label: "_time", Type: query.TTime},
					{Label: "_measurement", Type: query.TString},
					{Label: "host", Type: query.TString},
					{Label: "_value", Type: query.TFloat},
				},
				Data: [][]interface{}{
					{
						values.ConvertTime(time.Date(2018, 4, 17, 0, 0, 0, 0, time.UTC)),
						values.ConvertTime(time.Date(2018, 4, 17, 0, 5, 0, 0, time.UTC)),
						values.ConvertTime(time.Date(2018, 4, 17, 0, 0, 0, 0, time.UTC)),
						"cpu",
						"A",
						42.0,
					},
					{
						values.ConvertTime(time.Date(2018, 4, 17, 0, 0, 0, 0, time.UTC)),
						values.ConvertTime(time.Date(2018, 4, 17, 0, 5, 0, 0, time.UTC)),
						values.ConvertTime(time.Date(2018, 4, 17, 0, 0, 1, 0, time.UTC)),
						"cpu",
						"A",
						43.0,
					},
				},
			}},
		},
	},
	{
		name:          "single empty table",
		encoderConfig: csv.DefaultEncoderConfig(),
		encoded: toCRLF(`#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,string,string,double
#group,false,false,true,true,false,true,true,false
#default,_result,0,2018-04-17T00:00:00Z,2018-04-17T00:05:00Z,,cpu,A,
,result,table,_start,_stop,_time,_measurement,host,_value
`),
		result: &executetest.Result{
			Nm: "_result",
			Tbls: []*executetest.Table{{
				KeyCols: []string{"_start", "_stop", "_measurement", "host"},
				KeyValues: []interface{}{
					values.ConvertTime(time.Date(2018, 4, 17, 0, 0, 0, 0, time.UTC)),
					values.ConvertTime(time.Date(2018, 4, 17, 0, 5, 0, 0, time.UTC)),
					"cpu",
					"A",
				},
				ColMeta: []query.ColMeta{
					{Label: "_start", Type: query.TTime},
					{Label: "_stop", Type: query.TTime},
					{Label: "_time", Type: query.TTime},
					{Label: "_measurement", Type: query.TString},
					{Label: "host", Type: query.TString},
					{Label: "_value", Type: query.TFloat},
				},
			}},
		},
	},
	{
		name:          "multiple tables",
		encoderConfig: csv.DefaultEncoderConfig(),
		encoded: toCRLF(`#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,string,string,double
#group,false,false,true,true,false,true,true,false
#default,_result,,,,,,,
,result,table,_start,_stop,_time,_measurement,host,_value
,,0,2018-04-17T00:00:00Z,2018-04-17T00:05:00Z,2018-04-17T00:00:00Z,cpu,A,42
,,0,2018-04-17T00:00:00Z,2018-04-17T00:05:00Z,2018-04-17T00:00:01Z,cpu,A,43
,,1,2018-04-17T00:05:00Z,2018-04-17T00:10:00Z,2018-04-17T00:06:00Z,mem,A,52
,,1,2018-04-17T00:05:00Z,2018-04-17T00:10:00Z,2018-04-17T00:07:01Z,mem,A,53
`),
		result: &executetest.Result{
			Nm: "_result",
			Tbls: []*executetest.Table{
				{
					KeyCols: []string{"_start", "_stop", "_measurement", "host"},
					ColMeta: []query.ColMeta{
						{Label: "_start", Type: query.TTime},
						{Label: "_stop", Type: query.TTime},
						{Label: "_time", Type: query.TTime},
						{Label: "_measurement", Type: query.TString},
						{Label: "host", Type: query.TString},
						{Label: "_value", Type: query.TFloat},
					},
					Data: [][]interface{}{
						{
							values.ConvertTime(time.Date(2018, 4, 17, 0, 0, 0, 0, time.UTC)),
							values.ConvertTime(time.Date(2018, 4, 17, 0, 5, 0, 0, time.UTC)),
							values.ConvertTime(time.Date(2018, 4, 17, 0, 0, 0, 0, time.UTC)),
							"cpu",
							"A",
							42.0,
						},
						{
							values.ConvertTime(time.Date(2018, 4, 17, 0, 0, 0, 0, time.UTC)),
							values.ConvertTime(time.Date(2018, 4, 17, 0, 5, 0, 0, time.UTC)),
							values.ConvertTime(time.Date(2018, 4, 17, 0, 0, 1, 0, time.UTC)),
							"cpu",
							"A",
							43.0,
						},
					},
				},
				{
					KeyCols: []string{"_start", "_stop", "_measurement", "host"},
					ColMeta: []query.ColMeta{
						{Label: "_start", Type: query.TTime},
						{Label: "_stop", Type: query.TTime},
						{Label: "_time", Type: query.TTime},
						{Label: "_measurement", Type: query.TString},
						{Label: "host", Type: query.TString},
						{Label: "_value", Type: query.TFloat},
					},
					Data: [][]interface{}{
						{
							values.ConvertTime(time.Date(2018, 4, 17, 0, 5, 0, 0, time.UTC)),
							values.ConvertTime(time.Date(2018, 4, 17, 0, 10, 0, 0, time.UTC)),
							values.ConvertTime(time.Date(2018, 4, 17, 0, 6, 0, 0, time.UTC)),
							"mem",
							"A",
							52.0,
						},
						{
							values.ConvertTime(time.Date(2018, 4, 17, 0, 5, 0, 0, time.UTC)),
							values.ConvertTime(time.Date(2018, 4, 17, 0, 10, 0, 0, time.UTC)),
							values.ConvertTime(time.Date(2018, 4, 17, 0, 7, 1, 0, time.UTC)),
							"mem",
							"A",
							53.0,
						},
					},
				},
			},
		},
	},
	{
		name:          "multiple tables with differing schemas",
		encoderConfig: csv.DefaultEncoderConfig(),
		encoded: toCRLF(`#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,string,string,double
#group,false,false,true,true,false,true,true,false
#default,_result,,,,,,,
,result,table,_start,_stop,_time,_measurement,host,_value
,,0,2018-04-17T00:00:00Z,2018-04-17T00:05:00Z,2018-04-17T00:00:00Z,cpu,A,42
,,0,2018-04-17T00:00:00Z,2018-04-17T00:05:00Z,2018-04-17T00:00:01Z,cpu,A,43
,,1,2018-04-17T00:05:00Z,2018-04-17T00:10:00Z,2018-04-17T00:06:00Z,mem,A,52
,,1,2018-04-17T00:05:00Z,2018-04-17T00:10:00Z,2018-04-17T00:07:01Z,mem,A,53

#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,string,string,double,double
#group,false,false,true,true,false,true,false,false,false
#default,_result,,,,,,,,
,result,table,_start,_stop,_time,location,device,min,max
,,2,2018-04-17T00:00:00Z,2018-04-17T00:05:00Z,2018-04-17T00:00:00Z,USA,1563,42,67.9
,,2,2018-04-17T00:00:00Z,2018-04-17T00:05:00Z,2018-04-17T00:00:01Z,USA,1414,43,44.7
,,3,2018-04-17T00:05:00Z,2018-04-17T00:10:00Z,2018-04-17T00:06:00Z,Europe,4623,52,89.3
,,3,2018-04-17T00:05:00Z,2018-04-17T00:10:00Z,2018-04-17T00:07:01Z,Europe,3163,53,55.6
`),
		result: &executetest.Result{
			Nm: "_result",
			Tbls: []*executetest.Table{
				{
					KeyCols: []string{"_start", "_stop", "_measurement", "host"},
					ColMeta: []query.ColMeta{
						{Label: "_start", Type: query.TTime},
						{Label: "_stop", Type: query.TTime},
						{Label: "_time", Type: query.TTime},
						{Label: "_measurement", Type: query.TString},
						{Label: "host", Type: query.TString},
						{Label: "_value", Type: query.TFloat},
					},
					Data: [][]interface{}{
						{
							values.ConvertTime(time.Date(2018, 4, 17, 0, 0, 0, 0, time.UTC)),
							values.ConvertTime(time.Date(2018, 4, 17, 0, 5, 0, 0, time.UTC)),
							values.ConvertTime(time.Date(2018, 4, 17, 0, 0, 0, 0, time.UTC)),
							"cpu",
							"A",
							42.0,
						},
						{
							values.ConvertTime(time.Date(2018, 4, 17, 0, 0, 0, 0, time.UTC)),
							values.ConvertTime(time.Date(2018, 4, 17, 0, 5, 0, 0, time.UTC)),
							values.ConvertTime(time.Date(2018, 4, 17, 0, 0, 1, 0, time.UTC)),
							"cpu",
							"A",
							43.0,
						},
					},
				},
				{
					KeyCols: []string{"_start", "_stop", "_measurement", "host"},
					ColMeta: []query.ColMeta{
						{Label: "_start", Type: query.TTime},
						{Label: "_stop", Type: query.TTime},
						{Label: "_time", Type: query.TTime},
						{Label: "_measurement", Type: query.TString},
						{Label: "host", Type: query.TString},
						{Label: "_value", Type: query.TFloat},
					},
					Data: [][]interface{}{
						{
							values.ConvertTime(time.Date(2018, 4, 17, 0, 5, 0, 0, time.UTC)),
							values.ConvertTime(time.Date(2018, 4, 17, 0, 10, 0, 0, time.UTC)),
							values.ConvertTime(time.Date(2018, 4, 17, 0, 6, 0, 0, time.UTC)),
							"mem",
							"A",
							52.0,
						},
						{
							values.ConvertTime(time.Date(2018, 4, 17, 0, 5, 0, 0, time.UTC)),
							values.ConvertTime(time.Date(2018, 4, 17, 0, 10, 0, 0, time.UTC)),
							values.ConvertTime(time.Date(2018, 4, 17, 0, 7, 1, 0, time.UTC)),
							"mem",
							"A",
							53.0,
						},
					},
				},
				{
					KeyCols: []string{"_start", "_stop", "location"},
					ColMeta: []query.ColMeta{
						{Label: "_start", Type: query.TTime},
						{Label: "_stop", Type: query.TTime},
						{Label: "_time", Type: query.TTime},
						{Label: "location", Type: query.TString},
						{Label: "device", Type: query.TString},
						{Label: "min", Type: query.TFloat},
						{Label: "max", Type: query.TFloat},
					},
					Data: [][]interface{}{
						{
							values.ConvertTime(time.Date(2018, 4, 17, 0, 0, 0, 0, time.UTC)),
							values.ConvertTime(time.Date(2018, 4, 17, 0, 5, 0, 0, time.UTC)),
							values.ConvertTime(time.Date(2018, 4, 17, 0, 0, 0, 0, time.UTC)),
							"USA",
							"1563",
							42.0,
							67.9,
						},
						{
							values.ConvertTime(time.Date(2018, 4, 17, 0, 0, 0, 0, time.UTC)),
							values.ConvertTime(time.Date(2018, 4, 17, 0, 5, 0, 0, time.UTC)),
							values.ConvertTime(time.Date(2018, 4, 17, 0, 0, 1, 0, time.UTC)),
							"USA",
							"1414",
							43.0,
							44.7,
						},
					},
				},
				{
					KeyCols: []string{"_start", "_stop", "location"},
					ColMeta: []query.ColMeta{
						{Label: "_start", Type: query.TTime},
						{Label: "_stop", Type: query.TTime},
						{Label: "_time", Type: query.TTime},
						{Label: "location", Type: query.TString},
						{Label: "device", Type: query.TString},
						{Label: "min", Type: query.TFloat},
						{Label: "max", Type: query.TFloat},
					},
					Data: [][]interface{}{
						{
							values.ConvertTime(time.Date(2018, 4, 17, 0, 5, 0, 0, time.UTC)),
							values.ConvertTime(time.Date(2018, 4, 17, 0, 10, 0, 0, time.UTC)),
							values.ConvertTime(time.Date(2018, 4, 17, 0, 6, 0, 0, time.UTC)),
							"Europe",
							"4623",
							52.0,
							89.3,
						},
						{
							values.ConvertTime(time.Date(2018, 4, 17, 0, 5, 0, 0, time.UTC)),
							values.ConvertTime(time.Date(2018, 4, 17, 0, 10, 0, 0, time.UTC)),
							values.ConvertTime(time.Date(2018, 4, 17, 0, 7, 1, 0, time.UTC)),
							"Europe",
							"3163",
							53.0,
							55.6,
						},
					},
				},
			},
		},
	},
	{
		name:          "multiple tables with one empty",
		encoderConfig: csv.DefaultEncoderConfig(),
		encoded: toCRLF(`#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,string,string,double
#group,false,false,true,true,false,true,true,false
#default,_result,,,,,,,
,result,table,_start,_stop,_time,_measurement,host,_value
,,0,2018-04-17T00:00:00Z,2018-04-17T00:05:00Z,2018-04-17T00:00:00Z,cpu,A,42
,,0,2018-04-17T00:00:00Z,2018-04-17T00:05:00Z,2018-04-17T00:00:01Z,cpu,A,43
,,1,2018-04-17T00:05:00Z,2018-04-17T00:10:00Z,2018-04-17T00:06:00Z,mem,A,52
,,1,2018-04-17T00:05:00Z,2018-04-17T00:10:00Z,2018-04-17T00:07:01Z,mem,A,53

#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,string,string,double
#group,false,false,true,true,false,true,true,false
#default,_result,2,2018-04-17T00:00:00Z,2018-04-17T00:05:00Z,,cpu,A,
,result,table,_start,_stop,_time,_measurement,host,_value
`),
		result: &executetest.Result{
			Nm: "_result",
			Tbls: []*executetest.Table{
				{
					KeyCols: []string{"_start", "_stop", "_measurement", "host"},
					ColMeta: []query.ColMeta{
						{Label: "_start", Type: query.TTime},
						{Label: "_stop", Type: query.TTime},
						{Label: "_time", Type: query.TTime},
						{Label: "_measurement", Type: query.TString},
						{Label: "host", Type: query.TString},
						{Label: "_value", Type: query.TFloat},
					},
					Data: [][]interface{}{
						{
							values.ConvertTime(time.Date(2018, 4, 17, 0, 0, 0, 0, time.UTC)),
							values.ConvertTime(time.Date(2018, 4, 17, 0, 5, 0, 0, time.UTC)),
							values.ConvertTime(time.Date(2018, 4, 17, 0, 0, 0, 0, time.UTC)),
							"cpu",
							"A",
							42.0,
						},
						{
							values.ConvertTime(time.Date(2018, 4, 17, 0, 0, 0, 0, time.UTC)),
							values.ConvertTime(time.Date(2018, 4, 17, 0, 5, 0, 0, time.UTC)),
							values.ConvertTime(time.Date(2018, 4, 17, 0, 0, 1, 0, time.UTC)),
							"cpu",
							"A",
							43.0,
						},
					},
				},
				{
					KeyCols: []string{"_start", "_stop", "_measurement", "host"},
					ColMeta: []query.ColMeta{
						{Label: "_start", Type: query.TTime},
						{Label: "_stop", Type: query.TTime},
						{Label: "_time", Type: query.TTime},
						{Label: "_measurement", Type: query.TString},
						{Label: "host", Type: query.TString},
						{Label: "_value", Type: query.TFloat},
					},
					Data: [][]interface{}{
						{
							values.ConvertTime(time.Date(2018, 4, 17, 0, 5, 0, 0, time.UTC)),
							values.ConvertTime(time.Date(2018, 4, 17, 0, 10, 0, 0, time.UTC)),
							values.ConvertTime(time.Date(2018, 4, 17, 0, 6, 0, 0, time.UTC)),
							"mem",
							"A",
							52.0,
						},
						{
							values.ConvertTime(time.Date(2018, 4, 17, 0, 5, 0, 0, time.UTC)),
							values.ConvertTime(time.Date(2018, 4, 17, 0, 10, 0, 0, time.UTC)),
							values.ConvertTime(time.Date(2018, 4, 17, 0, 7, 1, 0, time.UTC)),
							"mem",
							"A",
							53.0,
						},
					},
				},
				{
					KeyCols: []string{"_start", "_stop", "_measurement", "host"},
					KeyValues: []interface{}{
						values.ConvertTime(time.Date(2018, 4, 17, 0, 0, 0, 0, time.UTC)),
						values.ConvertTime(time.Date(2018, 4, 17, 0, 5, 0, 0, time.UTC)),
						"cpu",
						"A",
					},
					ColMeta: []query.ColMeta{
						{Label: "_start", Type: query.TTime},
						{Label: "_stop", Type: query.TTime},
						{Label: "_time", Type: query.TTime},
						{Label: "_measurement", Type: query.TString},
						{Label: "host", Type: query.TString},
						{Label: "_value", Type: query.TFloat},
					},
				},
			},
		},
	},
}

func TestResultDecoder(t *testing.T) {
	testCases := []TestCase{
		{
			name:          "single table with defaults",
			encoderConfig: csv.DefaultEncoderConfig(),
			encoded: toCRLF(`#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,string,string,double
#group,false,false,true,true,false,true,true,false
#default,_result,0,2018-04-17T00:00:00Z,2018-04-17T00:05:00Z,,cpu,A,
,result,table,_start,_stop,_time,_measurement,host,_value
,,,,,2018-04-17T00:00:00Z,cpu,A,42.0
,,,,,2018-04-17T00:00:01Z,cpu,A,43.0
`),
			result: &executetest.Result{
				Nm: "_result",
				Tbls: []*executetest.Table{{
					KeyCols: []string{"_start", "_stop", "_measurement", "host"},
					ColMeta: []query.ColMeta{
						{Label: "_start", Type: query.TTime},
						{Label: "_stop", Type: query.TTime},
						{Label: "_time", Type: query.TTime},
						{Label: "_measurement", Type: query.TString},
						{Label: "host", Type: query.TString},
						{Label: "_value", Type: query.TFloat},
					},
					Data: [][]interface{}{
						{
							values.ConvertTime(time.Date(2018, 4, 17, 0, 0, 0, 0, time.UTC)),
							values.ConvertTime(time.Date(2018, 4, 17, 0, 5, 0, 0, time.UTC)),
							values.ConvertTime(time.Date(2018, 4, 17, 0, 0, 0, 0, time.UTC)),
							"cpu",
							"A",
							42.0,
						},
						{
							values.ConvertTime(time.Date(2018, 4, 17, 0, 0, 0, 0, time.UTC)),
							values.ConvertTime(time.Date(2018, 4, 17, 0, 5, 0, 0, time.UTC)),
							values.ConvertTime(time.Date(2018, 4, 17, 0, 0, 1, 0, time.UTC)),
							"cpu",
							"A",
							43.0,
						},
					},
				}},
			},
		},
	}
	testCases = append(testCases, symetricalTestCases...)
	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			if tc.skip {
				t.Skip()
			}
			decoder := csv.NewResultDecoder(tc.decoderConfig)
			result, err := decoder.Decode(bytes.NewReader(tc.encoded))
			if err != nil {
				t.Fatal(err)
			}
			got := &executetest.Result{
				Nm: result.Name(),
			}
			if err := result.Tables().Do(func(b query.Table) error {
				cb, err := executetest.ConvertTable(b)
				if err != nil {
					return err
				}
				got.Tbls = append(got.Tbls, cb)
				return nil
			}); err != nil {
				t.Fatal(err)
			}

			got.Normalize()
			tc.result.Normalize()

			if !cmp.Equal(got, tc.result) {
				t.Error("unexpected results -want/+got", cmp.Diff(tc.result, got))
			}
		})
	}
}

func TestResultEncoder(t *testing.T) {
	testCases := []TestCase{
	// Add tests cases specific to encoding here
	}
	testCases = append(testCases, symetricalTestCases...)
	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			if tc.skip {
				t.Skip()
			}
			encoder := csv.NewResultEncoder(tc.encoderConfig)
			var got bytes.Buffer
			n, err := encoder.Encode(&got, tc.result)
			if err != nil {
				t.Fatal(err)
			}

			if g, w := got.String(), string(tc.encoded); g != w {
				t.Errorf("unexpected encoding -want/+got:\n%s", diff.LineDiff(w, g))
			}
			if g, w := n, int64(len(tc.encoded)); g != w {
				t.Errorf("unexpected encoding count -want/+got:\n%s", cmp.Diff(w, g))
			}
		})
	}
}
func TestMutliResultEncoder(t *testing.T) {
	testCases := []struct {
		name    string
		results query.ResultIterator
		encoded []byte
		config  csv.ResultEncoderConfig
	}{
		{
			name:   "single result",
			config: csv.DefaultEncoderConfig(),
			results: query.NewSliceResultIterator([]query.Result{&executetest.Result{
				Nm: "_result",
				Tbls: []*executetest.Table{{
					KeyCols: []string{"_start", "_stop", "_measurement", "host"},
					ColMeta: []query.ColMeta{
						{Label: "_start", Type: query.TTime},
						{Label: "_stop", Type: query.TTime},
						{Label: "_time", Type: query.TTime},
						{Label: "_measurement", Type: query.TString},
						{Label: "host", Type: query.TString},
						{Label: "_value", Type: query.TFloat},
					},
					Data: [][]interface{}{
						{
							values.ConvertTime(time.Date(2018, 4, 17, 0, 0, 0, 0, time.UTC)),
							values.ConvertTime(time.Date(2018, 4, 17, 0, 5, 0, 0, time.UTC)),
							values.ConvertTime(time.Date(2018, 4, 17, 0, 0, 0, 0, time.UTC)),
							"cpu",
							"A",
							42.0,
						},
						{
							values.ConvertTime(time.Date(2018, 4, 17, 0, 0, 0, 0, time.UTC)),
							values.ConvertTime(time.Date(2018, 4, 17, 0, 5, 0, 0, time.UTC)),
							values.ConvertTime(time.Date(2018, 4, 17, 0, 0, 1, 0, time.UTC)),
							"cpu",
							"A",
							43.0,
						},
					},
				}},
			}}),
			encoded: toCRLF(`#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,string,string,double
#group,false,false,true,true,false,true,true,false
#default,_result,,,,,,,
,result,table,_start,_stop,_time,_measurement,host,_value
,,0,2018-04-17T00:00:00Z,2018-04-17T00:05:00Z,2018-04-17T00:00:00Z,cpu,A,42
,,0,2018-04-17T00:00:00Z,2018-04-17T00:05:00Z,2018-04-17T00:00:01Z,cpu,A,43

`),
		},
		{
			name:   "two results",
			config: csv.DefaultEncoderConfig(),
			results: query.NewSliceResultIterator([]query.Result{
				&executetest.Result{
					Nm: "_result",
					Tbls: []*executetest.Table{{
						KeyCols: []string{"_start", "_stop", "_measurement", "host"},
						ColMeta: []query.ColMeta{
							{Label: "_start", Type: query.TTime},
							{Label: "_stop", Type: query.TTime},
							{Label: "_time", Type: query.TTime},
							{Label: "_measurement", Type: query.TString},
							{Label: "host", Type: query.TString},
							{Label: "_value", Type: query.TFloat},
						},
						Data: [][]interface{}{
							{
								values.ConvertTime(time.Date(2018, 4, 17, 0, 0, 0, 0, time.UTC)),
								values.ConvertTime(time.Date(2018, 4, 17, 0, 5, 0, 0, time.UTC)),
								values.ConvertTime(time.Date(2018, 4, 17, 0, 0, 0, 0, time.UTC)),
								"cpu",
								"A",
								42.0,
							},
							{
								values.ConvertTime(time.Date(2018, 4, 17, 0, 0, 0, 0, time.UTC)),
								values.ConvertTime(time.Date(2018, 4, 17, 0, 5, 0, 0, time.UTC)),
								values.ConvertTime(time.Date(2018, 4, 17, 0, 0, 1, 0, time.UTC)),
								"cpu",
								"A",
								43.0,
							},
						},
					}},
				},
				&executetest.Result{
					Nm: "mean",
					Tbls: []*executetest.Table{{
						KeyCols: []string{"_start", "_stop", "_measurement", "host"},
						ColMeta: []query.ColMeta{
							{Label: "_start", Type: query.TTime},
							{Label: "_stop", Type: query.TTime},
							{Label: "_time", Type: query.TTime},
							{Label: "_measurement", Type: query.TString},
							{Label: "host", Type: query.TString},
							{Label: "_value", Type: query.TFloat},
						},
						Data: [][]interface{}{
							{
								values.ConvertTime(time.Date(2018, 4, 17, 0, 0, 0, 0, time.UTC)),
								values.ConvertTime(time.Date(2018, 4, 17, 0, 5, 0, 0, time.UTC)),
								values.ConvertTime(time.Date(2018, 4, 17, 0, 0, 0, 0, time.UTC)),
								"cpu",
								"A",
								40.0,
							},
							{
								values.ConvertTime(time.Date(2018, 4, 17, 0, 0, 0, 0, time.UTC)),
								values.ConvertTime(time.Date(2018, 4, 17, 0, 5, 0, 0, time.UTC)),
								values.ConvertTime(time.Date(2018, 4, 17, 0, 0, 1, 0, time.UTC)),
								"cpu",
								"A",
								40.1,
							},
						},
					}},
				},
			}),
			encoded: toCRLF(`#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,string,string,double
#group,false,false,true,true,false,true,true,false
#default,_result,,,,,,,
,result,table,_start,_stop,_time,_measurement,host,_value
,,0,2018-04-17T00:00:00Z,2018-04-17T00:05:00Z,2018-04-17T00:00:00Z,cpu,A,42
,,0,2018-04-17T00:00:00Z,2018-04-17T00:05:00Z,2018-04-17T00:00:01Z,cpu,A,43

#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,string,string,double
#group,false,false,true,true,false,true,true,false
#default,mean,,,,,,,
,result,table,_start,_stop,_time,_measurement,host,_value
,,0,2018-04-17T00:00:00Z,2018-04-17T00:05:00Z,2018-04-17T00:00:00Z,cpu,A,40
,,0,2018-04-17T00:00:00Z,2018-04-17T00:05:00Z,2018-04-17T00:00:01Z,cpu,A,40.1

`),
		},
		{
			name:   "error results",
			config: csv.DefaultEncoderConfig(),
			results: errorResultIterator{
				Error: errors.New("test error"),
			},
			encoded: toCRLF(`error,reference
test error,
`),
		},
	}
	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			encoder := csv.NewMultiResultEncoder(tc.config)
			var got bytes.Buffer
			n, err := encoder.Encode(&got, tc.results)
			if err != nil {
				t.Fatal(err)
			}

			if g, w := got.String(), string(tc.encoded); g != w {
				t.Errorf("unexpected encoding -want/+got:\n%s", diff.LineDiff(w, g))
			}
			if g, w := n, int64(len(tc.encoded)); g != w {
				t.Errorf("unexpected encoding count -want/+got:\n%s", cmp.Diff(w, g))
			}
		})
	}
}

var crlfPattern = regexp.MustCompile(`\r?\n`)

func toCRLF(data string) []byte {
	return []byte(crlfPattern.ReplaceAllString(data, "\r\n"))
}

type errorResultIterator struct {
	Error error
}

func (r errorResultIterator) More() bool {
	return false
}

func (r errorResultIterator) Next() query.Result {
	panic("no results")
}

func (r errorResultIterator) Cancel() {
}

func (r errorResultIterator) Err() error {
	return r.Error
}

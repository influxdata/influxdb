package csv_test

import (
	"bytes"
	"regexp"
	"testing"
	"time"

	"github.com/andreyvit/diff"
	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/ifql/query/execute"
	"github.com/influxdata/ifql/query/execute/executetest"
	"github.com/influxdata/ifql/values"
	"github.com/influxdata/platform/query/csv"
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
#partition,false,false,true,true,false,true,true,false
#default,_result,,,,,,,
,result,table,_start,_stop,_time,_measurement,host,_value
,,0,2018-04-17T00:00:00Z,2018-04-17T00:05:00Z,2018-04-17T00:00:00Z,cpu,A,42
,,0,2018-04-17T00:00:00Z,2018-04-17T00:05:00Z,2018-04-17T00:00:01Z,cpu,A,43
`),
		result: &executetest.Result{Blks: []*executetest.Block{{
			KeyCols: []string{"_start", "_stop", "_measurement", "host"},
			ColMeta: []execute.ColMeta{
				{Label: "_start", Type: execute.TTime},
				{Label: "_stop", Type: execute.TTime},
				{Label: "_time", Type: execute.TTime},
				{Label: "_measurement", Type: execute.TString},
				{Label: "host", Type: execute.TString},
				{Label: "_value", Type: execute.TFloat},
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
		}}},
	},
	{
		name: "single empty table",
		// The encoder cannot encode a empty tables, it needs to know ahead of time if it is empty
		skip:          true,
		encoderConfig: csv.DefaultEncoderConfig(),
		encoded: toCRLF(`#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,string,string,double
#partition,false,false,true,true,false,true,true,false
#default,_result,0,2018-04-17T00:00:00Z,2018-04-17T00:05:00Z,,cpu,A,
,result,table,_start,_stop,_time,_measurement,host,_value
`),
		result: &executetest.Result{Blks: []*executetest.Block{{
			KeyCols: []string{"_start", "_stop", "_measurement", "host"},
			KeyValues: []interface{}{
				values.ConvertTime(time.Date(2018, 4, 17, 0, 0, 0, 0, time.UTC)),
				values.ConvertTime(time.Date(2018, 4, 17, 0, 5, 0, 0, time.UTC)),
				"cpu",
				"A",
			},
			ColMeta: []execute.ColMeta{
				{Label: "_start", Type: execute.TTime},
				{Label: "_stop", Type: execute.TTime},
				{Label: "_time", Type: execute.TTime},
				{Label: "_measurement", Type: execute.TString},
				{Label: "host", Type: execute.TString},
				{Label: "_value", Type: execute.TFloat},
			},
		}}},
	},
	{
		name:          "multiple tables",
		encoderConfig: csv.DefaultEncoderConfig(),
		encoded: toCRLF(`#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,string,string,double
#partition,false,false,true,true,false,true,true,false
#default,_result,,,,,,,
,result,table,_start,_stop,_time,_measurement,host,_value
,,0,2018-04-17T00:00:00Z,2018-04-17T00:05:00Z,2018-04-17T00:00:00Z,cpu,A,42
,,0,2018-04-17T00:00:00Z,2018-04-17T00:05:00Z,2018-04-17T00:00:01Z,cpu,A,43
,,1,2018-04-17T00:05:00Z,2018-04-17T00:10:00Z,2018-04-17T00:06:00Z,mem,A,52
,,1,2018-04-17T00:05:00Z,2018-04-17T00:10:00Z,2018-04-17T00:07:01Z,mem,A,53
`),
		result: &executetest.Result{Blks: []*executetest.Block{
			{
				KeyCols: []string{"_start", "_stop", "_measurement", "host"},
				ColMeta: []execute.ColMeta{
					{Label: "_start", Type: execute.TTime},
					{Label: "_stop", Type: execute.TTime},
					{Label: "_time", Type: execute.TTime},
					{Label: "_measurement", Type: execute.TString},
					{Label: "host", Type: execute.TString},
					{Label: "_value", Type: execute.TFloat},
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
				ColMeta: []execute.ColMeta{
					{Label: "_start", Type: execute.TTime},
					{Label: "_stop", Type: execute.TTime},
					{Label: "_time", Type: execute.TTime},
					{Label: "_measurement", Type: execute.TString},
					{Label: "host", Type: execute.TString},
					{Label: "_value", Type: execute.TFloat},
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
		}},
	},
	{
		name:          "multiple tables with differing schemas",
		encoderConfig: csv.DefaultEncoderConfig(),
		encoded: toCRLF(`#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,string,string,double
#partition,false,false,true,true,false,true,true,false
#default,_result,,,,,,,
,result,table,_start,_stop,_time,_measurement,host,_value
,,0,2018-04-17T00:00:00Z,2018-04-17T00:05:00Z,2018-04-17T00:00:00Z,cpu,A,42
,,0,2018-04-17T00:00:00Z,2018-04-17T00:05:00Z,2018-04-17T00:00:01Z,cpu,A,43
,,1,2018-04-17T00:05:00Z,2018-04-17T00:10:00Z,2018-04-17T00:06:00Z,mem,A,52
,,1,2018-04-17T00:05:00Z,2018-04-17T00:10:00Z,2018-04-17T00:07:01Z,mem,A,53

#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,string,string,double,double
#partition,false,false,true,true,false,true,false,false,false
#default,_result,,,,,,,,
,result,table,_start,_stop,_time,location,device,min,max
,,2,2018-04-17T00:00:00Z,2018-04-17T00:05:00Z,2018-04-17T00:00:00Z,USA,1563,42,67.9
,,2,2018-04-17T00:00:00Z,2018-04-17T00:05:00Z,2018-04-17T00:00:01Z,USA,1414,43,44.7
,,3,2018-04-17T00:05:00Z,2018-04-17T00:10:00Z,2018-04-17T00:06:00Z,Europe,4623,52,89.3
,,3,2018-04-17T00:05:00Z,2018-04-17T00:10:00Z,2018-04-17T00:07:01Z,Europe,3163,53,55.6
`),
		result: &executetest.Result{Blks: []*executetest.Block{
			{
				KeyCols: []string{"_start", "_stop", "_measurement", "host"},
				ColMeta: []execute.ColMeta{
					{Label: "_start", Type: execute.TTime},
					{Label: "_stop", Type: execute.TTime},
					{Label: "_time", Type: execute.TTime},
					{Label: "_measurement", Type: execute.TString},
					{Label: "host", Type: execute.TString},
					{Label: "_value", Type: execute.TFloat},
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
				ColMeta: []execute.ColMeta{
					{Label: "_start", Type: execute.TTime},
					{Label: "_stop", Type: execute.TTime},
					{Label: "_time", Type: execute.TTime},
					{Label: "_measurement", Type: execute.TString},
					{Label: "host", Type: execute.TString},
					{Label: "_value", Type: execute.TFloat},
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
				ColMeta: []execute.ColMeta{
					{Label: "_start", Type: execute.TTime},
					{Label: "_stop", Type: execute.TTime},
					{Label: "_time", Type: execute.TTime},
					{Label: "location", Type: execute.TString},
					{Label: "device", Type: execute.TString},
					{Label: "min", Type: execute.TFloat},
					{Label: "max", Type: execute.TFloat},
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
				ColMeta: []execute.ColMeta{
					{Label: "_start", Type: execute.TTime},
					{Label: "_stop", Type: execute.TTime},
					{Label: "_time", Type: execute.TTime},
					{Label: "location", Type: execute.TString},
					{Label: "device", Type: execute.TString},
					{Label: "min", Type: execute.TFloat},
					{Label: "max", Type: execute.TFloat},
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
		}},
	},
	{
		name: "multiple tables with one empty",
		// The encoder cannot encode a empty tables, it needs to know ahead of time if it is empty
		skip:          true,
		encoderConfig: csv.DefaultEncoderConfig(),
		encoded: toCRLF(`#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,string,string,double
#partition,false,false,true,true,false,true,true,false
#default,_result,,,,,,,
,result,table,_start,_stop,_time,_measurement,host,_value
,,0,2018-04-17T00:00:00Z,2018-04-17T00:05:00Z,2018-04-17T00:00:00Z,cpu,A,42
,,0,2018-04-17T00:00:00Z,2018-04-17T00:05:00Z,2018-04-17T00:00:01Z,cpu,A,43
,,1,2018-04-17T00:05:00Z,2018-04-17T00:10:00Z,2018-04-17T00:06:00Z,mem,A,52
,,1,2018-04-17T00:05:00Z,2018-04-17T00:10:00Z,2018-04-17T00:07:01Z,mem,A,53

#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,string,string,double
#partition,false,false,true,true,false,true,true,false
#default,_result,0,2018-04-17T00:00:00Z,2018-04-17T00:05:00Z,,cpu,A,
,result,table,_start,_stop,_time,_measurement,host,_value
`),
		result: &executetest.Result{Blks: []*executetest.Block{
			{
				KeyCols: []string{"_start", "_stop", "_measurement", "host"},
				ColMeta: []execute.ColMeta{
					{Label: "_start", Type: execute.TTime},
					{Label: "_stop", Type: execute.TTime},
					{Label: "_time", Type: execute.TTime},
					{Label: "_measurement", Type: execute.TString},
					{Label: "host", Type: execute.TString},
					{Label: "_value", Type: execute.TFloat},
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
				ColMeta: []execute.ColMeta{
					{Label: "_start", Type: execute.TTime},
					{Label: "_stop", Type: execute.TTime},
					{Label: "_time", Type: execute.TTime},
					{Label: "_measurement", Type: execute.TString},
					{Label: "host", Type: execute.TString},
					{Label: "_value", Type: execute.TFloat},
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
				ColMeta: []execute.ColMeta{
					{Label: "_start", Type: execute.TTime},
					{Label: "_stop", Type: execute.TTime},
					{Label: "_time", Type: execute.TTime},
					{Label: "_measurement", Type: execute.TString},
					{Label: "host", Type: execute.TString},
					{Label: "_value", Type: execute.TFloat},
				},
			},
		}},
	},
}

func TestResultDecoder(t *testing.T) {
	testCases := []TestCase{
		{
			name:          "single table with defaults",
			encoderConfig: csv.DefaultEncoderConfig(),
			encoded: toCRLF(`#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,string,string,double
#partition,false,false,true,true,false,true,true,false
#default,_result,0,2018-04-17T00:00:00Z,2018-04-17T00:05:00Z,,cpu,A,
,result,table,_start,_stop,_time,_measurement,host,_value
,,,,,2018-04-17T00:00:00Z,cpu,A,42.0
,,,,,2018-04-17T00:00:01Z,cpu,A,43.0
`),
			result: &executetest.Result{Blks: []*executetest.Block{{
				KeyCols: []string{"_start", "_stop", "_measurement", "host"},
				ColMeta: []execute.ColMeta{
					{Label: "_start", Type: execute.TTime},
					{Label: "_stop", Type: execute.TTime},
					{Label: "_time", Type: execute.TTime},
					{Label: "_measurement", Type: execute.TString},
					{Label: "host", Type: execute.TString},
					{Label: "_value", Type: execute.TFloat},
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
			}}},
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
			got := new(executetest.Result)
			if err := result.Blocks().Do(func(b execute.Block) error {
				cb, err := executetest.ConvertBlock(b)
				if err != nil {
					return err
				}
				got.Blks = append(got.Blks, cb)
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
			err := encoder.Encode(&got, tc.result)
			if err != nil {
				t.Fatal(err)
			}

			if g, w := got.String(), string(tc.encoded); g != w {
				t.Errorf("unexpected encoding -want/+got:\n%s", diff.LineDiff(w, g))
			}
		})
	}

}

var crlfPattern = regexp.MustCompile(`\r?\n`)

func toCRLF(data string) []byte {
	return []byte(crlfPattern.ReplaceAllString(data, "\r\n"))
}

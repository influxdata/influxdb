package influxql_test

import (
	"bytes"
	"regexp"
	"testing"

	"github.com/andreyvit/diff"
	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/platform/query/csv"
	"github.com/influxdata/platform/query/influxql"
)

var crlfPattern = regexp.MustCompile(`\r?\n`)

func toCRLF(data string) []byte {
	return []byte(crlfPattern.ReplaceAllString(data, "\r\n"))
}

func TestResponse_ResultIterator(t *testing.T) {
	type testCase struct {
		name     string
		encoded  []byte
		response *influxql.Response
		err      error
	}
	tests := []testCase{
		{
			name: "single series",
			encoded: toCRLF(`#datatype,string,long,dateTime:RFC3339,double,long,string,boolean,string,string,string
#group,false,false,false,false,false,false,false,true,true,true
#default,0,,,,,,,,,
,result,table,_time,usage_user,test,mystr,this,cpu,host,_measurement
,,0,2018-08-29T13:08:47Z,10.2,10,yay,true,cpu-total,a,cpu
,,0,2018-08-29T13:08:48Z,12.1,20,nay,false,cpu-total,a,cpu
,,0,2018-08-29T13:08:47Z,112,30,way,false,cpu-total,a,cpu
,,0,2018-08-29T13:08:48Z,123.2,40,pay,true,cpu-total,a,cpu

`),
			response: &influxql.Response{
				Results: []influxql.Result{
					{
						StatementID: 0,
						Series: []*influxql.Row{
							{
								Name: "cpu",
								Tags: map[string]string{
									"cpu":  "cpu-total",
									"host": "a",
								},
								Columns: []string{"time", "usage_user", "test", "mystr", "this"},
								Values: [][]interface{}{
									{int64(1535548127000000000), 10.2, int64(10), "yay", true},
									{int64(1535548128000000000), 12.1, int64(20), "nay", false},
									{int64(1535548127000000000), 112.0, int64(30), "way", false},
									{int64(1535548128000000000), 123.2, int64(40), "pay", true},
								},
							},
						},
					},
				},
			},
		},
		{
			name: "multiple series",
			encoded: toCRLF(`#datatype,string,long,dateTime:RFC3339,double,long,string,string,string,string
#group,false,false,false,false,false,false,true,true,true
#default,0,,,,,,,,
,result,table,_time,usage_user,test,mystr,cpu,host,_measurement
,,0,2018-08-29T13:08:47Z,10.2,10,yay,cpu-total,a,cpu
,,0,2018-08-29T13:08:48Z,12.1,20,nay,cpu-total,a,cpu
,,0,2018-08-29T13:08:47Z,112,30,way,cpu-total,a,cpu
,,0,2018-08-29T13:08:48Z,123.2,40,pay,cpu-total,a,cpu
,,1,2018-08-29T18:27:31Z,10.2,10,yay,cpu-total,b,cpu
,,1,2018-08-29T18:27:31Z,12.1,20,nay,cpu-total,b,cpu
,,1,2018-08-29T18:27:31Z,112,30,way,cpu-total,b,cpu
,,1,2018-08-29T18:27:31Z,123.2,40,pay,cpu-total,b,cpu

`),
			response: &influxql.Response{
				Results: []influxql.Result{
					{
						StatementID: 0,
						Series: []*influxql.Row{
							{
								Name: "cpu",
								Tags: map[string]string{
									"cpu":  "cpu-total",
									"host": "a",
								},
								Columns: []string{"time", "usage_user", "test", "mystr"},
								Values: [][]interface{}{
									{float64(1535548127000000000), 10.2, int64(10), "yay"},
									{float64(1535548128000000000), 12.1, int64(20), "nay"},
									{float64(1535548127000000000), 112.0, int64(30), "way"},
									{float64(1535548128000000000), 123.2, int64(40), "pay"},
								},
							},
							{
								Name: "cpu",
								Tags: map[string]string{
									"cpu":  "cpu-total",
									"host": "b",
								},
								Columns: []string{"time", "usage_user", "test", "mystr"},
								Values: [][]interface{}{
									{"2018-08-29T18:27:31Z", 10.2, int64(10), "yay"},
									{"2018-08-29T18:27:31Z", 12.1, int64(20), "nay"},
									{"2018-08-29T18:27:31Z", 112.0, int64(30), "way"},
									{"2018-08-29T18:27:31Z", 123.2, int64(40), "pay"},
								},
							},
						},
					},
				},
			},
		},
		{
			name: "multiple series with same columns but different types",
			encoded: toCRLF(`#datatype,string,long,dateTime:RFC3339,double,long,string,string,string,string
#group,false,false,false,false,false,false,true,true,true
#default,0,,,,,,,,
,result,table,_time,usage_user,test,mystr,cpu,host,_measurement
,,0,2018-08-29T13:08:47Z,10.2,1,yay,cpu-total,a,cpu
,,0,2018-08-29T13:08:48Z,12.1,2,nay,cpu-total,a,cpu
,,0,2018-08-29T13:08:47Z,112,3,way,cpu-total,a,cpu
,,0,2018-08-29T13:08:48Z,123.2,4,pay,cpu-total,a,cpu

#datatype,string,long,dateTime:RFC3339,double,double,string,string,string,string
#group,false,false,false,false,false,false,true,true,true
#default,0,,,,,,,,
,result,table,_time,usage_user,test,mystr,cpu,host,_measurement
,,1,2018-08-29T13:08:47Z,10.2,10,yay,cpu-total,a,cpu
,,1,2018-08-29T13:08:48Z,12.1,20,nay,cpu-total,a,cpu
,,1,2018-08-29T13:08:47Z,112,30,way,cpu-total,a,cpu
,,1,2018-08-29T13:08:48Z,123.2,40,pay,cpu-total,a,cpu

`),
			response: &influxql.Response{
				Results: []influxql.Result{
					{
						StatementID: 0,
						Series: []*influxql.Row{
							{
								Name: "cpu",
								Tags: map[string]string{
									"cpu":  "cpu-total",
									"host": "a",
								},
								Columns: []string{"time", "usage_user", "test", "mystr"},
								Values: [][]interface{}{
									{int64(1535548127000000000), 10.2, int64(1), "yay"},
									{int64(1535548128000000000), 12.1, int64(2), "nay"},
									{int64(1535548127000000000), 112.0, int64(3), "way"},
									{int64(1535548128000000000), 123.2, int64(4), "pay"},
								},
							},
							{
								Name: "cpu",
								Tags: map[string]string{
									"cpu":  "cpu-total",
									"host": "a",
								},
								Columns: []string{"time", "usage_user", "test", "mystr"},
								Values: [][]interface{}{
									{int64(1535548127000000000), 10.2, float64(10), "yay"},
									{int64(1535548128000000000), 12.1, float64(20), "nay"},
									{int64(1535548127000000000), 112.0, float64(30), "way"},
									{int64(1535548128000000000), 123.2, float64(40), "pay"},
								},
							},
						},
					},
				},
			},
		},
		{
			name: "multiple results",
			encoded: toCRLF(`#datatype,string,long,dateTime:RFC3339,double,long,string,string,string,string
#group,false,false,false,false,false,false,true,true,true
#default,0,,,,,,,,
,result,table,_time,usage_user,test,mystr,cpu,host,_measurement
,,0,2018-08-29T13:08:47Z,10.2,10,yay,cpu-total,a,cpu
,,0,2018-08-29T13:08:48Z,12.1,20,nay,cpu-total,a,cpu
,,0,2018-08-29T13:08:47Z,112,30,way,cpu-total,a,cpu
,,0,2018-08-29T13:08:48Z,123.2,40,pay,cpu-total,a,cpu
,,1,2018-08-29T13:08:47Z,10.2,10,yay,cpu-total,b,cpu
,,1,2018-08-29T13:08:48Z,12.1,20,nay,cpu-total,b,cpu
,,1,2018-08-29T13:08:47Z,112,30,way,cpu-total,b,cpu
,,1,2018-08-29T13:08:48Z,123.2,40,pay,cpu-total,b,cpu

#datatype,string,long,dateTime:RFC3339,double,long,string,string,string,string
#group,false,false,false,false,false,false,true,true,true
#default,1,,,,,,,,
,result,table,_time,usage_user,test,mystr,cpu,host,_measurement
,,0,2018-08-29T13:08:47Z,10.2,10,yay,cpu-total,a,cpu
,,0,2018-08-29T13:08:48Z,12.1,20,nay,cpu-total,a,cpu
,,0,2018-08-29T13:08:47Z,112,30,way,cpu-total,a,cpu
,,0,2018-08-29T13:08:48Z,123.2,40,pay,cpu-total,a,cpu

`),
			response: &influxql.Response{
				Results: []influxql.Result{
					{
						StatementID: 0,
						Series: []*influxql.Row{
							{
								Name: "cpu",
								Tags: map[string]string{
									"cpu":  "cpu-total",
									"host": "a",
								},
								Columns: []string{"time", "usage_user", "test", "mystr"},
								Values: [][]interface{}{
									{int64(1535548127000000000), 10.2, int64(10), "yay"},
									{int64(1535548128000000000), 12.1, int64(20), "nay"},
									{int64(1535548127000000000), 112.0, int64(30), "way"},
									{int64(1535548128000000000), 123.2, int64(40), "pay"},
								},
							},
							{
								Name: "cpu",
								Tags: map[string]string{
									"cpu":  "cpu-total",
									"host": "b",
								},
								Columns: []string{"time", "usage_user", "test", "mystr"},
								Values: [][]interface{}{
									{int64(1535548127000000000), 10.2, int64(10), "yay"},
									{int64(1535548128000000000), 12.1, int64(20), "nay"},
									{int64(1535548127000000000), 112.0, int64(30), "way"},
									{int64(1535548128000000000), 123.2, int64(40), "pay"},
								},
							},
						},
					},
					{
						StatementID: 1,
						Series: []*influxql.Row{
							{
								Name: "cpu",
								Tags: map[string]string{
									"cpu":  "cpu-total",
									"host": "a",
								},
								Columns: []string{"time", "usage_user", "test", "mystr"},
								Values: [][]interface{}{
									{int64(1535548127000000000), 10.2, int64(10), "yay"},
									{int64(1535548128000000000), 12.1, int64(20), "nay"},
									{int64(1535548127000000000), 112.0, int64(30), "way"},
									{int64(1535548128000000000), 123.2, int64(40), "pay"},
								},
							},
						},
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encoderConfig := csv.DefaultEncoderConfig()
			encoder := csv.NewMultiResultEncoder(encoderConfig)
			var got bytes.Buffer
			n, err := encoder.Encode(&got, influxql.NewResponseIterator(tt.response))
			if err != nil && tt.err != nil {
				if err.Error() != tt.err.Error() {
					t.Errorf("unexpected error want: %s\n got: %s\n", tt.err.Error(), err.Error())
				}
			} else if err != nil {
				t.Errorf("unexpected error want: none\n got: %s\n", err.Error())
			} else if tt.err != nil {
				t.Errorf("unexpected error want: %s\n got: none", tt.err.Error())
			}

			if g, w := got.String(), string(tt.encoded); g != w {
				t.Errorf("unexpected encoding -want/+got:\n%s", diff.LineDiff(w, g))
			}
			if g, w := n, int64(len(tt.encoded)); g != w {
				t.Errorf("unexpected encoding count -want/+got:\n%s", cmp.Diff(w, g))
			}
		})
	}
}

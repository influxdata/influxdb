package influxql

import (
	"testing"
	"time"

	"bytes"
	"strings"

	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/platform/query"
	"github.com/influxdata/platform/query/execute"
	"github.com/influxdata/platform/query/execute/executetest"
)

var epoch = time.Unix(0, 0)

func TestMultiResultEncoder_Encode(t *testing.T) {
	testCases := []struct {
		name   string
		query  string
		blocks map[string][]*executetest.Block
		output string
	}{
		{
			query: "",
			name:  "one result one row",
			blocks: map[string][]*executetest.Block{
				"0": {{
					KeyCols: []string{"_measurement", "_field", "host"},
					ColMeta: []query.ColMeta{
						{Label: "_time", Type: query.TTime},
						{Label: "_measurement", Type: query.TString},
						{Label: "_field", Type: query.TString},
						{Label: "host", Type: query.TString},
						{Label: execute.DefaultValueColLabel, Type: query.TFloat},
					},
					Data: [][]interface{}{
						{execute.Time(5), "cpu", "max", "localhost", 98.9},
					},
				}},
			},
			output: `{"results":[{"statement_id":0,"series":[{"name":"cpu","tags":{"host":"localhost"},"columns":["time","max"],"values":[["1970-01-01T00:00:00Z",98.9]]}]}]}`,
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {

			resultsmap := map[string]query.Result{}

			for k, v := range tc.blocks {
				resultsmap[k] = executetest.NewResult(v)
			}

			results := query.NewMapResultIterator(resultsmap)

			var resp bytes.Buffer
			var influxQLEncoder MultiResultEncoder
			err := influxQLEncoder.Encode(&resp, results)

			if err != nil {
				t.Error("error writing to buffer: ", err)
			}
			got := strings.TrimSpace(resp.String())
			if !cmp.Equal(got, tc.output) {
				t.Error("unexpected results -want/+got", cmp.Diff(tc.output, got))
			}
		})
	}
}

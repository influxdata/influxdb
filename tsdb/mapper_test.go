package tsdb

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/influxdb/influxdb/influxql"
)

func TestShardMapper_WriteAndSingleMapperTagSets(t *testing.T) {
	tmpDir, _ := ioutil.TempDir("", "shard_test")
	defer os.RemoveAll(tmpDir)
	shard := mustCreateShard(tmpDir)

	pt1time := time.Unix(1, 0).UTC()
	pt1 := NewPoint(
		"cpu",
		map[string]string{"host": "serverA", "region": "us-east"},
		map[string]interface{}{"value": 42},
		pt1time,
	)
	pt2time := time.Unix(2, 0).UTC()
	pt2 := NewPoint(
		"cpu",
		map[string]string{"host": "serverB", "region": "us-east"},
		map[string]interface{}{"value": 60},
		pt2time,
	)
	err := shard.WritePoints([]Point{pt1, pt2})
	if err != nil {
		t.Fatalf(err.Error())
	}

	var tests = []struct {
		stmt     string
		expected []string
	}{
		{
			stmt:     `SELECT value FROM cpu`,
			expected: []string{"cpu"},
		},
		{
			stmt:     `SELECT value FROM cpu GROUP BY host`,
			expected: []string{"cpuhost|serverA", "cpuhost|serverB"},
		},
		{
			stmt:     `SELECT value FROM cpu GROUP BY region`,
			expected: []string{"cpuregion|us-east"},
		},
		{
			stmt:     `SELECT value FROM cpu WHERE host='serverA'`,
			expected: []string{"cpu"},
		},
		{
			stmt:     `SELECT value FROM cpu WHERE host='serverB'`,
			expected: []string{"cpu"},
		},
		{
			stmt:     `SELECT value FROM cpu WHERE host='serverC'`,
			expected: []string{},
		},
	}

	for _, tt := range tests {
		stmt := mustParseSelectStatement(tt.stmt)
		mapper := openMapperOrFail(t, shard, stmt)
		got := mapper.TagSets()
		if !reflect.DeepEqual(got, tt.expected) {
			t.Errorf("test '%s'\n\tgot      %s\n\texpected %s", tt.stmt, got, tt.expected)
		}
	}
}

func TestShardMapper_WriteAndSingleMapperRawQuery(t *testing.T) {
	tmpDir, _ := ioutil.TempDir("", "shard_test")
	defer os.RemoveAll(tmpDir)
	shard := mustCreateShard(tmpDir)

	pt1time := time.Unix(1, 0).UTC()
	pt1 := NewPoint(
		"cpu",
		map[string]string{"host": "serverA", "region": "us-east"},
		map[string]interface{}{"value": 42},
		pt1time,
	)
	pt2time := time.Unix(2, 0).UTC()
	pt2 := NewPoint(
		"cpu",
		map[string]string{"host": "serverB", "region": "us-east"},
		map[string]interface{}{"value": 60},
		pt2time,
	)
	err := shard.WritePoints([]Point{pt1, pt2})
	if err != nil {
		t.Fatalf(err.Error())
	}

	var tests = []struct {
		stmt      string
		reqTagSet string
		chunkSize int
		expected  []string
	}{
		{
			stmt:      `SELECT value FROM cpu`,
			reqTagSet: "cpu",
			expected:  []string{`{"Name":"cpu","values":[{"time":1000000000,"value":42},{"time":2000000000,"value":60}]}`},
		},
		{
			stmt:      `SELECT value FROM cpu`,
			reqTagSet: "cpu",
			chunkSize: 1,
			expected:  []string{`{"Name":"cpu","values":[{"time":1000000000,"value":42}]}`},
		},
		{
			stmt:      `SELECT value FROM cpu`,
			reqTagSet: "cpu",
			chunkSize: 2,
			expected:  []string{`{"Name":"cpu","values":[{"time":1000000000,"value":42},{"time":2000000000,"value":60}]}`},
		},
		{
			stmt:      `SELECT value FROM cpu`,
			reqTagSet: "cpu",
			chunkSize: 3,
			expected:  []string{`{"Name":"cpu","values":[{"time":1000000000,"value":42},{"time":2000000000,"value":60}]}`},
		},
		{
			stmt:      `SELECT value FROM cpu GROUP BY host`,
			reqTagSet: "cpuhost|serverA",
			expected:  []string{`{"Name":"cpu","tags":{"host":"serverA"},"values":[{"time":1000000000,"value":42}]}`},
		},
		{
			stmt:      `SELECT value FROM cpu GROUP BY host`,
			reqTagSet: "cpuhost|serverB",
			expected: []string{
				`{"Name":"cpu","tags":{"host":"serverB"},"values":[{"time":2000000000,"value":60}]}`,
			},
		},
		{
			stmt:      `SELECT value FROM cpu GROUP BY region`,
			reqTagSet: "cpuregion|us-east",
			expected:  []string{`{"Name":"cpu","tags":{"region":"us-east"},"values":[{"time":1000000000,"value":42},{"time":2000000000,"value":60}]}`},
		},
		{
			stmt:      `SELECT value FROM cpu WHERE host='serverA'`,
			reqTagSet: "cpu",
			expected:  []string{`{"Name":"cpu","values":[{"time":1000000000,"value":42}]}`},
		},
		{
			stmt:      `SELECT value FROM cpu WHERE host='serverB'`,
			reqTagSet: "cpu",
			expected:  []string{`{"Name":"cpu","values":[{"time":2000000000,"value":60}]}`},
		},
		{
			stmt:      `SELECT value FROM cpu WHERE host='serverC'`,
			reqTagSet: "cpu",
			expected:  []string{`null`},
		},
		{
			stmt:      `SELECT value FROM cpu WHERE value = 60`,
			reqTagSet: "cpu",
			expected:  []string{`{"Name":"cpu","values":[{"time":2000000000,"value":60}]}`},
		},
		{
			stmt:      `SELECT value FROM cpu WHERE value != 60`,
			reqTagSet: "cpu",
			expected:  []string{`{"Name":"cpu","values":[{"time":1000000000,"value":42}]}`},
		},
		{
			stmt:      fmt.Sprintf(`SELECT value FROM cpu WHERE time = '%s'`, pt1time.Format(influxql.DateTimeFormat)),
			reqTagSet: "cpu",
			expected:  []string{`{"Name":"cpu","values":[{"time":1000000000,"value":42}]}`},
		},
		{
			stmt:      fmt.Sprintf(`SELECT value FROM cpu WHERE time > '%s'`, pt1time.Format(influxql.DateTimeFormat)),
			reqTagSet: "cpu",
			expected:  []string{`{"Name":"cpu","values":[{"time":2000000000,"value":60}]}`},
		},
		{
			stmt:      fmt.Sprintf(`SELECT value FROM cpu WHERE time > '%s'`, pt2time.Format(influxql.DateTimeFormat)),
			reqTagSet: "cpu",
			expected:  []string{`{"Name":"cpu"}`},
		},
	}

	for _, tt := range tests {
		stmt := mustParseSelectStatement(tt.stmt)
		mapper := openMapperOrFail(t, shard, stmt)

		for _, s := range tt.expected {
			got := nextChunkAsJson(t, mapper, tt.reqTagSet, tt.chunkSize)
			if got != s {
				t.Errorf("test '%s'\n\tgot      %s\n\texpected %s", tt.stmt, got, tt.expected)
				break
			}
		}
	}
}

func TestShardMapper_WriteAndSingleMapperRawQueryMultiValue(t *testing.T) {
	tmpDir, _ := ioutil.TempDir("", "shard_test")
	defer os.RemoveAll(tmpDir)
	shard := mustCreateShard(tmpDir)

	pt1time := time.Unix(1, 0).UTC()
	pt1 := NewPoint(
		"cpu",
		map[string]string{"host": "serverA", "region": "us-east"},
		map[string]interface{}{"foo": 42, "bar": 43},
		pt1time,
	)
	pt2time := time.Unix(2, 0).UTC()
	pt2 := NewPoint(
		"cpu",
		map[string]string{"host": "serverB", "region": "us-east"},
		map[string]interface{}{"foo": 60, "bar": 61},
		pt2time,
	)
	err := shard.WritePoints([]Point{pt1, pt2})
	if err != nil {
		t.Fatalf(err.Error())
	}

	var tests = []struct {
		stmt      string
		reqTagSet string
		chunkSize int
		expected  []string
	}{
		{
			stmt:      `SELECT foo FROM cpu`,
			reqTagSet: "cpu",
			expected:  []string{`{"Name":"cpu","values":[{"time":1000000000,"value":42},{"time":2000000000,"value":60}]}`},
		},
		{
			stmt:      `SELECT foo,bar FROM cpu`,
			reqTagSet: "cpu",
			expected:  []string{`{"Name":"cpu","values":[{"time":1000000000,"value":{"bar":43,"foo":42}},{"time":2000000000,"value":{"bar":61,"foo":60}}]}`},
		},
	}

	for _, tt := range tests {
		stmt := mustParseSelectStatement(tt.stmt)
		mapper := openMapperOrFail(t, shard, stmt)

		for _, s := range tt.expected {
			got := nextChunkAsJson(t, mapper, tt.reqTagSet, tt.chunkSize)
			if got != s {
				t.Errorf("test '%s'\n\tgot      %s\n\texpected %s", tt.stmt, got, tt.expected)
				break
			}
		}
	}
}

func mustCreateShard(dir string) *Shard {
	tmpShard := path.Join(dir, "shard")
	index := NewDatabaseIndex()
	sh := NewShard(index, tmpShard)
	if err := sh.Open(); err != nil {
		panic(fmt.Sprintf("error opening shard: %s", err.Error()))
	}
	return sh
}

// mustParseSelectStatement parses a select statement. Panic on error.
func mustParseSelectStatement(s string) *influxql.SelectStatement {
	stmt, err := influxql.NewParser(strings.NewReader(s)).ParseStatement()
	if err != nil {
		panic(err)
	}
	return stmt.(*influxql.SelectStatement)
}

func openMapperOrFail(t *testing.T, shard *Shard, stmt *influxql.SelectStatement) *RawMapper {
	mapper := NewRawMapper(shard, stmt)

	if err := mapper.Open(); err != nil {
		t.Fatalf("failed to open raw mapper: %s", err.Error())
	}
	return mapper
}

func nextChunkAsJson(t *testing.T, mapper *RawMapper, tagset string, chunkSize int) string {
	r, err := mapper.NextChunk(tagset, chunkSize)
	if err != nil {
		t.Fatalf("failed to get next chunk from mapper: %s", err.Error())
	}
	b, err := json.Marshal(r)
	if err != nil {
		t.Fatalf("failed to marshal chunk as JSON: %s", err.Error())
	}
	return string(b)
}

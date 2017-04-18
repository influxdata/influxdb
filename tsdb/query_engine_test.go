package tsdb

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/influxdb/influxdb/influxql"
)

func TestShardMapper_WriteAndSingleMapperRawQuery(t *testing.T) {
	tmpDir, _ := ioutil.TempDir("", "shard_test")
	defer os.RemoveAll(tmpDir)
	shard := mustCreateShard(tmpDir)

	pt1time := time.Unix(1, 0).UTC()
	pt1 := NewPoint(
		"cpu",
		map[string]string{"host": "serverA"},
		map[string]interface{}{"value": 42},
		pt1time,
	)
	pt2time := time.Unix(2, 0).UTC()
	pt2 := NewPoint(
		"cpu",
		map[string]string{"host": "serverB"},
		map[string]interface{}{"value": 60},
		pt2time,
	)
	err := shard.WritePoints([]Point{pt1, pt2})
	if err != nil {
		t.Fatalf(err.Error())
	}

	var tests = []struct {
		stmt     string
		expected string
	}{
		{
			stmt:     `SELECT value FROM eth`,
			expected: `null`,
		},
		{
			stmt:     `SELECT value FROM cpu`,
			expected: `[{"Time":1000000000,"Values":42},{"Time":2000000000,"Values":60}]`,
		},
		{
			stmt:     `SELECT value FROM cpu WHERE host='serverA'`,
			expected: `[{"Time":1000000000,"Values":42}]`,
		},
		{
			stmt:     `SELECT value FROM cpu WHERE host='serverB'`,
			expected: `[{"Time":2000000000,"Values":60}]`,
		},
		{
			stmt:     `SELECT value FROM cpu WHERE host='serverC'`,
			expected: `null`,
		},
		{
			stmt:     `SELECT value FROM cpu WHERE value = 60`,
			expected: `[{"Time":2000000000,"Values":60}]`,
		},
		{
			stmt:     `SELECT value FROM cpu WHERE value != 60`,
			expected: `[{"Time":1000000000,"Values":42}]`,
		},
		{
			stmt:     fmt.Sprintf(`SELECT value FROM cpu WHERE time = '%s'`, pt1time.Format(influxql.DateTimeFormat)),
			expected: `[{"Time":1000000000,"Values":42}]`,
		},
		{
			stmt:     fmt.Sprintf(`SELECT value FROM cpu WHERE time > '%s'`, pt1time.Format(influxql.DateTimeFormat)),
			expected: `[{"Time":2000000000,"Values":60}]`,
		},
		{
			stmt:     fmt.Sprintf(`SELECT value FROM cpu WHERE time > '%s'`, pt2time.Format(influxql.DateTimeFormat)),
			expected: `null`,
		},
	}

	for _, tt := range tests {
		stmt := mustParseSelectStatement(tt.stmt)
		mapper := beginMapperOrFail(t, shard, stmt)

		got := nextChunkAsJson(t, mapper)
		if got != tt.expected {
			t.Errorf("test '%s'\n\tgot      %s\n\texpected %s", tt.stmt, got, tt.expected)
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

func beginMapperOrFail(t *testing.T, shard *Shard, stmt *influxql.SelectStatement) *ShardMapper {
	mapper := NewShardMapper(shard)
	if err := mapper.Open(); err != nil {
		t.Fatalf("failed to open mapper: %s", err.Error())
	}
	if err := mapper.Begin(stmt, 1); err != nil {
		t.Fatalf("failed to begin mapper: %s", err.Error())
	}
	return mapper
}

func nextChunkAsJson(t *testing.T, mapper *ShardMapper) string {
	_, r, _, err := mapper.NextChunk()
	if err != nil {
		t.Fatalf("failed to get next chunk from mapper: %s", err.Error())
	}
	b, err := json.Marshal(r)
	if err != nil {
		t.Fatalf("failed to marshal chunk as JSON: %s", err.Error())
	}
	return string(b)
}

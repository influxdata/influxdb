package tsdb

import (
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/influxdb/influxdb/influxql"
)

func TestShardMapper_WriteAndSingleMapper(t *testing.T) {
	tmpDir, _ := ioutil.TempDir("", "shard_test")
	defer os.RemoveAll(tmpDir)
	shard := mustCreateShard(tmpDir)

	pt := NewPoint(
		"cpu",
		map[string]string{"host": "server"},
		map[string]interface{}{"value": 42},
		time.Unix(1, 2),
	)
	err := shard.WritePoints([]Point{pt})
	if err != nil {
		t.Fatalf(err.Error())
	}

	stmt := mustParseSelectStatement("SELECT value FROM cpu")
	mapper := NewShardMapper(shard)
	if err := mapper.Open(); err != nil {
		t.Fatalf("failed to open mapper: %s", err.Error())
	}
	if err := mapper.Begin(stmt, 1); err != nil {
		t.Fatalf("failed to begin mapper: %s", err.Error())
	}

	if _, r, _, _ := mapper.NextChunk(); r == nil {
		t.Fatalf("didn't receive result when expected: %v", r)
	}
	if _, r, _, _ := mapper.NextChunk(); r != nil {
		t.Fatal("received result when unexpected")
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

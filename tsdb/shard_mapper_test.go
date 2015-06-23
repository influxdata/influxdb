package tsdb

import (
	"io/ioutil"
	"os"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/influxdb/influxdb/influxql"
)

func TestShardMapper_WriteAndMap(t *testing.T) {
	tmpDir, _ := ioutil.TempDir("", "shard_test")
	defer os.RemoveAll(tmpDir)
	tmpShard := path.Join(tmpDir, "shard")

	index := NewDatabaseIndex()
	sh := NewShard(index, tmpShard)
	if err := sh.Open(); err != nil {
		t.Fatalf("error openeing shard: %s", err.Error())
	}

	pt := NewPoint(
		"cpu",
		map[string]string{"host": "server"},
		map[string]interface{}{"value": 1.0},
		time.Unix(1, 2),
	)
	err := sh.WritePoints([]Point{pt})
	if err != nil {
		t.Fatalf(err.Error())
	}

	stmt := mustParseSelectStatement("SELECT value FROM cpu")
	mappers, err := CreateShardMappers(stmt, sh)
	if err != nil {
		t.Fatalf("error creating shard mappers: %s", err.Error())
	}
	if len(mappers) != 1 {
		t.Fatalf("incorrect number of mappers created, exp 1, got %d", len(mappers))
	}

}

// mustParseSelectStatement parses a select statement. Panic on error.
func mustParseSelectStatement(s string) *influxql.SelectStatement {
	stmt, err := influxql.NewParser(strings.NewReader(s)).ParseStatement()
	if err != nil {
		panic(err)
	}
	return stmt.(*influxql.SelectStatement)
}

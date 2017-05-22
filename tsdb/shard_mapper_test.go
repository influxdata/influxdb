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
	tmpShard := path.Join(tmpDir, "shard")

	index := NewDatabaseIndex()
	sh := NewShard(index, tmpShard)
	if err := sh.Open(); err != nil {
		t.Fatalf("error openeing shard: %s", err.Error())
	}

	pt := NewPoint(
		"cpu",
		map[string]string{"host": "server"},
		map[string]interface{}{"value": 42},
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
	mapper := mappers[0]

	if err := mapper.Open(); err != nil {
		t.Fatalf("error opening shard mapper: %s", err.Error())
	}

	if err := mapper.Begin(nil, 1, 100); err != nil {
		t.Fatalf("error beginning shard mapper: %s", err.Error())
	}

	values, err := mapper.NextInterval()
	if err != nil {
		t.Fatalf("error getting next interval: %s", err.Error())
	}
	fmt.Println(">>>>", values)

}

// mustParseSelectStatement parses a select statement. Panic on error.
func mustParseSelectStatement(s string) *influxql.SelectStatement {
	stmt, err := influxql.NewParser(strings.NewReader(s)).ParseStatement()
	if err != nil {
		panic(err)
	}
	return stmt.(*influxql.SelectStatement)
}

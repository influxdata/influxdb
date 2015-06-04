package tsdb

import (
	"io/ioutil"
	"os"
	"reflect"
	"testing"
	"time"
)

func TestShardWriteAndIndex(t *testing.T) {
	path, _ := ioutil.TempDir("", "")
	defer os.RemoveAll(path)
	path += "/shard"

	index := NewDatabaseIndex()
	sh := NewShard(index, path)
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

	pt.SetTime(time.Unix(2, 3))
	err = sh.WritePoints([]Point{pt})
	if err != nil {
		t.Fatalf(err.Error())
	}

	validateIndex := func() {
		if !reflect.DeepEqual(index.names, []string{"cpu"}) {
			t.Fatalf("measurement names in shard didn't match")
		}
		if len(index.series) != 1 {
			t.Fatalf("series wasn't in index")
		}
		seriesTags := index.series[string(pt.Key())].Tags
		if len(seriesTags) != len(pt.Tags()) || pt.Tags()["host"] != seriesTags["host"] {
			t.Fatalf("tags weren't properly saved to series index: %v, %v", pt.Tags(), index.series[string(pt.Key())].Tags)
		}
		if !reflect.DeepEqual(index.measurements["cpu"].TagKeys(), []string{"host"}) {
			t.Fatalf("tag key wasn't saved to measurement index")
		}
	}

	validateIndex()

	// ensure the index gets loaded after closing and opening the shard
	sh.Close()

	index = NewDatabaseIndex()
	sh = NewShard(index, path)
	if err := sh.Open(); err != nil {
		t.Fatalf("error openeing shard: %s", err.Error())
	}

	validateIndex()

	// and ensure that we can still write data
	pt.SetTime(time.Unix(2, 6))
	err = sh.WritePoints([]Point{pt})
	if err != nil {
		t.Fatalf(err.Error())
	}
}

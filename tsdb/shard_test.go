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

	sh := NewShard()
	if err := sh.Open(path); err != nil {
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
		if !reflect.DeepEqual(sh.names, []string{"cpu"}) {
			t.Fatalf("measurement names in shard din't match")
		}
		if len(sh.series) != 1 {
			t.Fatalf("series wasn't in index")
		}
		seriesTags := sh.series[pt.Key()].Tags
		if len(seriesTags) != len(pt.Tags()) || pt.Tags()["host"] != seriesTags["host"] {
			t.Fatalf("tags weren't properly saved to series index: %v, %v", pt.Tags(), sh.series[pt.Key()].Tags)
		}
		if !reflect.DeepEqual(sh.measurements["cpu"].tagKeys(), []string{"host"}) {
			t.Fatalf("tag key wasn't saved to measurement index")
		}
	}

	validateIndex()

	// ensure the index gets loaded after closing and opening the shard
	sh.Close()

	sh = NewShard()
	if err := sh.Open(path); err != nil {
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

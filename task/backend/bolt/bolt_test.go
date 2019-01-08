package bolt_test

import (
	"io/ioutil"
	"os"
	"testing"

	bolt "github.com/coreos/bbolt"
	_ "github.com/influxdata/influxdb/query/builtin"
	"github.com/influxdata/influxdb/task/backend"
	boltstore "github.com/influxdata/influxdb/task/backend/bolt"
	"github.com/influxdata/influxdb/task/backend/storetest"
	"github.com/influxdata/influxdb/task/options"
)

func init() {
	// TODO(mr): remove as part of https://github.com/influxdata/platform/issues/484.
	options.EnableScriptCacheForTest()
}

func TestBoltStore(t *testing.T) {
	var f *os.File
	storetest.NewStoreTest(
		"boltstore",
		func(t *testing.T) backend.Store {
			var err error
			f, err = ioutil.TempFile("", "influx_bolt_task_store_test")
			if err != nil {
				t.Fatalf("failed to create tempfile for test db %v\n", err)
			}
			db, err := bolt.Open(f.Name(), os.ModeTemporary, nil)
			if err != nil {
				t.Fatalf("failed to open bolt db for test db %v\n", err)
			}
			s, err := boltstore.New(db, "testbucket")
			if err != nil {
				t.Fatalf("failed to create new bolt store %v\n", err)
			}
			return s
		},
		func(t *testing.T, s backend.Store) {
			if err := s.Close(); err != nil {
				t.Error(err)
			}
			err := os.Remove(f.Name())
			if err != nil {
				t.Error(err)
			}
		},
	)(t)
}

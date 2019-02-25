package bolt_test

import (
	"context"
	"io/ioutil"
	"os"
	"testing"
	"time"

	bolt "github.com/coreos/bbolt"
	"github.com/influxdata/influxdb"
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

func TestSkip(t *testing.T) {
	f, err := ioutil.TempFile("", "influx_bolt_task_store_test")
	if err != nil {
		t.Fatalf("failed to create tempfile for test db %v\n", err)
	}
	defer f.Close()
	defer os.Remove(f.Name())

	db, err := bolt.Open(f.Name(), os.ModeTemporary, nil)
	if err != nil {
		t.Fatalf("failed to open bolt db for test db %v\n", err)
	}
	s, err := boltstore.New(db, "testbucket")
	if err != nil {
		t.Fatalf("failed to create new bolt store %v\n", err)
	}

	schedAfter := time.Now().Add(-time.Minute)
	tskID, err := s.CreateTask(context.Background(), backend.CreateTaskRequest{
		Org:             influxdb.ID(1),
		AuthorizationID: influxdb.ID(2),
		Script:          `option task = {name:"x", every:1s} from(bucket:"b-src") |> range(start:-1m) |> to(bucket:"b-dst", org:"o")`,
		ScheduleAfter:   schedAfter.Unix(),
		Status:          backend.TaskActive,
	})
	if err != nil {
		t.Fatalf("failed to create new task %v\n", err)
	}

	rc, err := s.CreateNextRun(context.Background(), tskID, schedAfter.Add(10*time.Second).Unix())
	if err != nil {
		t.Fatalf("failed to create new run %v\n", err)
	}

	if err := s.FinishRun(context.Background(), tskID, rc.Created.RunID); err != nil {
		t.Fatalf("failed to finish run %v\n", err)
	}

	meta, err := s.FindTaskMetaByID(context.Background(), tskID)
	if err != nil {
		t.Fatalf("failed to pull meta %v\n", err)
	}
	if meta.LatestCompleted <= schedAfter.Unix() {
		t.Fatal("failed to update latestCompleted")
	}

	latestCompleted := meta.LatestCompleted

	s.Close()

	db, err = bolt.Open(f.Name(), os.ModeTemporary, nil)
	if err != nil {
		t.Fatalf("failed to open bolt db for test db %v\n", err)
	}
	s, err = boltstore.New(db, "testbucket", boltstore.NoCatchUp)
	if err != nil {
		t.Fatalf("failed to create new bolt store %v\n", err)
	}
	defer s.Close()

	meta, err = s.FindTaskMetaByID(context.Background(), tskID)
	if err != nil {
		t.Fatalf("failed to pull meta %v\n", err)
	}

	if meta.LatestCompleted == latestCompleted {
		t.Fatal("failed to overwrite latest completed on new meta pull")
	}
	latestCompleted = meta.LatestCompleted

	rc, err = s.CreateNextRun(context.Background(), tskID, time.Now().Add(10*time.Second).Unix())
	if err != nil {
		t.Fatalf("failed to create new run %v\n", err)
	}

	if err := s.FinishRun(context.Background(), tskID, rc.Created.RunID); err != nil {
		t.Fatalf("failed to finish run %v\n", err)
	}

	tasks, err := s.ListTasks(context.Background(), backend.TaskSearchParams{})
	if err != nil {
		t.Fatalf("failed to pull meta %v\n", err)
	}

	if len(tasks) != 1 {
		t.Fatal("task not found")
	}

	if tasks[0].Meta.LatestCompleted == latestCompleted {
		t.Fatal("failed to run after an override")
	}
}

package task_test

import (
	"context"
	"io/ioutil"
	"os"
	"testing"

	bolt "github.com/coreos/bbolt"
	_ "github.com/influxdata/platform/query/builtin"
	"github.com/influxdata/platform/task/adaptertest"
	"github.com/influxdata/platform/task/backend"
	boltstore "github.com/influxdata/platform/task/backend/bolt"
)

func inMemFactory(t *testing.T) (*adaptertest.System, context.CancelFunc) {
	st := backend.NewInMemStore()
	lrw := backend.NewInMemRunReaderWriter()

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		<-ctx.Done()
		st.Close()
	}()

	return &adaptertest.System{S: st, LR: lrw, LW: lrw, Ctx: ctx}, cancel
}

func boltFactory(t *testing.T) (*adaptertest.System, context.CancelFunc) {
	lrw := backend.NewInMemRunReaderWriter()

	f, err := ioutil.TempFile("", "platform_adapter_test_bolt")
	if err != nil {
		t.Fatal(err)
	}
	db, err := bolt.Open(f.Name(), 0644, nil)
	if err != nil {
		t.Fatal(err)
	}
	st, err := boltstore.New(db, "testbucket")
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		<-ctx.Done()
		if err := st.Close(); err != nil {
			t.Logf("error closing bolt: %v", err)
		}
		if err := os.Remove(f.Name()); err != nil {
			t.Logf("error removing bolt tempfile: %v", err)
		}
	}()

	return &adaptertest.System{S: st, LR: lrw, LW: lrw, Ctx: ctx}, cancel
}

func TestPlatformAdapter(t *testing.T) {
	t.Run("in-mem", func(t *testing.T) {
		t.Parallel()
		adaptertest.TestTaskService(t, inMemFactory)
	})

	t.Run("bolt", func(t *testing.T) {
		t.Parallel()
		adaptertest.TestTaskService(t, boltFactory)
	})
}

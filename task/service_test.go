package task_test

import (
	"context"
	"io/ioutil"
	"os"
	"testing"

	bolt "github.com/coreos/bbolt"
	"github.com/influxdata/influxdb/inmem"
	_ "github.com/influxdata/influxdb/query/builtin"
	"github.com/influxdata/influxdb/task/backend"
	boltstore "github.com/influxdata/influxdb/task/backend/bolt"
	"github.com/influxdata/influxdb/task/mock"
	"github.com/influxdata/influxdb/task/servicetest"
)

func inMemFactory(t *testing.T) (*servicetest.System, context.CancelFunc) {
	st := backend.NewInMemStore()
	lrw := backend.NewInMemRunReaderWriter()

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		<-ctx.Done()
		st.Close()
	}()

	i := inmem.NewService()
	return &servicetest.System{
		TaskControlService: servicetest.TaskControlAdaptor(st, lrw, lrw),
		Ctx:                ctx,
		I:                  i,
		TaskService:        servicetest.UsePlatformAdaptor(st, lrw, mock.NewScheduler(), i),
	}, cancel
}

func boltFactory(t *testing.T) (*servicetest.System, context.CancelFunc) {
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

	i := inmem.NewService()
	return &servicetest.System{
		TaskControlService: servicetest.TaskControlAdaptor(st, lrw, lrw),
		TaskService:        servicetest.UsePlatformAdaptor(st, lrw, mock.NewScheduler(), i),
		Ctx:                ctx,
		I:                  i,
	}, cancel
}

func TestTaskService(t *testing.T) {
	t.Run("in-mem", func(t *testing.T) {
		t.Parallel()
		servicetest.TestTaskService(t, inMemFactory)
	})

	t.Run("bolt", func(t *testing.T) {
		t.Parallel()
		servicetest.TestTaskService(t, boltFactory)
	})
}

package backend_test

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/influxdata/flux/control"
	"github.com/influxdata/flux/execute"
	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/inmem"
	"github.com/influxdata/influxdb/query"
	pcontrol "github.com/influxdata/influxdb/query/control"
	"github.com/influxdata/influxdb/storage"
	"github.com/influxdata/influxdb/storage/readservice"
	"github.com/influxdata/influxdb/task/backend"
	"github.com/influxdata/influxdb/task/backend/storetest"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"
)

func TestInMemRunStore(t *testing.T) {
	storetest.NewRunStoreTest(
		"inmem",
		func(t *testing.T) (backend.LogWriter, backend.LogReader, storetest.MakeNewAuthorizationFunc) {
			rw := backend.NewInMemRunReaderWriter()
			return rw, rw, nil
		},
		func(t *testing.T, w backend.LogWriter, r backend.LogReader) {})(t)
}

func TestQueryStorageRunStore(t *testing.T) {
	storetest.NewRunStoreTest(
		"PointLogWriter and QueryLogReader",
		func(t *testing.T) (backend.LogWriter, backend.LogReader, storetest.MakeNewAuthorizationFunc) {
			lrw := newFullStackAwareLogReaderWriter(t)
			return lrw, lrw, lrw.makeNewAuthorization
		},
		func(t *testing.T, w backend.LogWriter, r backend.LogReader) {
			if w.(*fullStackAwareLogReaderWriter) != r.(*fullStackAwareLogReaderWriter) {
				panic("impossible cleanup values")
			}
			w.(*fullStackAwareLogReaderWriter).Close(t)
		},
	)(t)
}

type fullStackAwareLogReaderWriter struct {
	*backend.PointLogWriter
	*backend.QueryLogReader

	queryController *pcontrol.Controller

	rootDir       string
	storageEngine *storage.Engine

	i *inmem.Service
}

func (lrw *fullStackAwareLogReaderWriter) Close(t *testing.T) {
	if err := lrw.queryController.Shutdown(context.Background()); err != nil {
		t.Error(err)
	}
	if err := lrw.storageEngine.Close(); err != nil {
		t.Error(err)
	}
	if err := os.RemoveAll(lrw.rootDir); err != nil {
		t.Error(err)
	}
}

func (lrw *fullStackAwareLogReaderWriter) makeNewAuthorization(ctx context.Context, t *testing.T) *influxdb.Authorization {
	o := &influxdb.Organization{Name: fmt.Sprintf("org-%s-%d", t.Name(), time.Now().UnixNano())}
	if err := lrw.i.CreateOrganization(ctx, o); err != nil {
		t.Fatal(err)
	}

	u := &influxdb.User{Name: fmt.Sprintf("user-%s-%d", t.Name(), time.Now().UnixNano())}
	if err := lrw.i.CreateUser(ctx, u); err != nil {
		t.Fatal(err)
	}

	a := &influxdb.Authorization{
		UserID:      u.ID,
		OrgID:       o.ID,
		Permissions: influxdb.OperPermissions(),
	}
	if err := lrw.i.CreateAuthorization(ctx, a); err != nil {
		t.Fatal(err)
	}

	return a
}

func newFullStackAwareLogReaderWriter(t *testing.T) *fullStackAwareLogReaderWriter {
	// Mostly copied out of cmd/influxd/main.go.
	logger := zaptest.NewLogger(t)

	rootDir, err := ioutil.TempDir("", "task-logreaderwriter-")
	if err != nil {
		t.Fatal(err)
	}

	engine := storage.NewEngine(rootDir, storage.NewConfig())
	engine.WithLogger(logger)

	if err := engine.Open(context.Background()); err != nil {
		t.Fatal(err)
	}

	defer func() {
		if t.Failed() {
			engine.Close()
		}
	}()

	svc := inmem.NewService()

	const (
		concurrencyQuota = 10
		memoryBytesQuota = 1e6
	)

	cc := control.Config{
		ExecutorDependencies: make(execute.Dependencies),
		ConcurrencyQuota:     concurrencyQuota,
		MemoryBytesQuota:     int64(memoryBytesQuota),
		Logger:               logger.With(zap.String("service", "storage-reads")),
	}

	if err := readservice.AddControllerConfigDependencies(
		&cc, engine, svc, svc,
	); err != nil {
		t.Fatal(err)
	}

	queryController := pcontrol.New(cc)

	return &fullStackAwareLogReaderWriter{
		PointLogWriter: backend.NewPointLogWriter(engine),
		QueryLogReader: backend.NewQueryLogReader(query.QueryServiceBridge{AsyncQueryService: queryController}),

		queryController: queryController,

		rootDir:       rootDir,
		storageEngine: engine,

		i: svc,
	}
}

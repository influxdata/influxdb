package backend_test

import (
	"context"
	"io/ioutil"
	"os"
	"testing"

	"github.com/influxdata/flux/control"
	"github.com/influxdata/flux/execute"
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
		func(t *testing.T) (backend.LogWriter, backend.LogReader) {
			rw := backend.NewInMemRunReaderWriter()
			return rw, rw
		},
		func(t *testing.T, w backend.LogWriter, r backend.LogReader) {})(t)
}

func TestQueryStorageRunStore(t *testing.T) {
	storetest.NewRunStoreTest(
		"PointLogWriter and QueryLogReader",
		func(t *testing.T) (backend.LogWriter, backend.LogReader) {
			lrw := newFullStackAwareLogReaderWriter(t)
			return lrw, lrw
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

func newFullStackAwareLogReaderWriter(t *testing.T) *fullStackAwareLogReaderWriter {
	// Mostly copied out of cmd/influxd/main.go.
	logger := zaptest.NewLogger(t)

	rootDir, err := ioutil.TempDir("", "task-logreaderwriter-")
	if err != nil {
		t.Fatal(err)
	}

	engine := storage.NewEngine(rootDir, storage.NewConfig())
	engine.WithLogger(logger)

	if err := engine.Open(); err != nil {
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
	}
}

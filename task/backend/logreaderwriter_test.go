package backend_test

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/influxdata/platform/inmem"
	"github.com/influxdata/platform/query"
	"github.com/influxdata/platform/storage"
	"github.com/influxdata/platform/storage/readservice"
	"github.com/influxdata/platform/task/backend"
	"github.com/influxdata/platform/task/backend/storetest"
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

	rootDir       string
	storageEngine *storage.Engine
}

func (lrw *fullStackAwareLogReaderWriter) Close(t *testing.T) {
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

	storageQueryService, err := readservice.NewProxyQueryService(
		engine,
		svc,
		svc,
		logger.With(zap.String("service", "storage-reads")),
	)
	if err != nil {
		t.Fatal(err)
	}

	var queryService query.QueryService = storageQueryService.(query.ProxyQueryServiceBridge).QueryService

	return &fullStackAwareLogReaderWriter{
		PointLogWriter: backend.NewPointLogWriter(engine),
		QueryLogReader: backend.NewQueryLogReader(queryService),

		rootDir:       rootDir,
		storageEngine: engine,
	}
}

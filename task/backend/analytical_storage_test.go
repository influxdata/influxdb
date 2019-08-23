package backend_test

import (
	"context"
	"io/ioutil"
	"os"
	"testing"

	"github.com/influxdata/flux/execute"
	"github.com/influxdata/influxdb"
	icontext "github.com/influxdata/influxdb/context"
	"github.com/influxdata/influxdb/inmem"
	"github.com/influxdata/influxdb/kv"
	"github.com/influxdata/influxdb/query"
	_ "github.com/influxdata/influxdb/query/builtin"
	"github.com/influxdata/influxdb/query/control"
	"github.com/influxdata/influxdb/storage"
	"github.com/influxdata/influxdb/storage/readservice"
	"github.com/influxdata/influxdb/task/backend"
	"github.com/influxdata/influxdb/task/servicetest"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"
)

func TestAnalyticalStore(t *testing.T) {
	servicetest.TestTaskService(
		t,
		func(t *testing.T) (*servicetest.System, context.CancelFunc) {
			ctx, cancelFunc := context.WithCancel(context.Background())
			svc := kv.NewService(inmem.NewKVStore())
			if err := svc.Initialize(ctx); err != nil {
				t.Fatalf("error initializing urm service: %v", err)
			}

			ab := newAnalyticalBackend(t, svc, svc)
			svcStack := backend.NewAnalyticalStorage(zaptest.NewLogger(t), svc, svc, ab.PointsWriter(), ab.QueryService())

			go func() {
				<-ctx.Done()
				ab.Close(t)
			}()

			authCtx := icontext.SetAuthorizer(ctx, &influxdb.Authorization{
				Permissions: influxdb.OperPermissions(),
			})

			return &servicetest.System{
				TaskControlService: svcStack,
				TaskService:        svcStack,
				I:                  svc,
				Ctx:                authCtx,
			}, cancelFunc
		},
	)
}

type analyticalBackend struct {
	queryController *control.Controller
	rootDir         string
	storageEngine   *storage.Engine
}

func (ab *analyticalBackend) PointsWriter() storage.PointsWriter {
	return ab.storageEngine
}

func (ab *analyticalBackend) QueryService() query.QueryService {
	return query.QueryServiceBridge{AsyncQueryService: ab.queryController}
}

func (lrw *analyticalBackend) Close(t *testing.T) {
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

func newAnalyticalBackend(t *testing.T, orgSvc influxdb.OrganizationService, bucketSvc influxdb.BucketService) *analyticalBackend {
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
			os.RemoveAll(rootDir)
		}
	}()

	const (
		concurrencyQuota         = 10
		memoryBytesQuotaPerQuery = 1e6
		queueSize                = 10
	)

	cc := control.Config{
		ExecutorDependencies:     make(execute.Dependencies),
		ConcurrencyQuota:         concurrencyQuota,
		MemoryBytesQuotaPerQuery: int64(memoryBytesQuotaPerQuery),
		QueueSize:                queueSize,
		Logger:                   logger.With(zap.String("service", "storage-reads")),
	}

	// TODO(adam): do we need a proper secret service here?
	if err := readservice.AddControllerConfigDependencies(
		&cc, engine, bucketSvc, orgSvc, nil,
	); err != nil {
		t.Fatal(err)
	}

	queryController, err := control.New(cc)
	if err != nil {
		t.Fatal(err)
	}

	return &analyticalBackend{
		queryController: queryController,
		rootDir:         rootDir,
		storageEngine:   engine,
	}
}

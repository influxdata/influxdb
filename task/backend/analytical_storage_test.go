package backend_test

import (
	"context"
	"errors"
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/influxdata/flux"
	"github.com/influxdata/influxdb"
	icontext "github.com/influxdata/influxdb/context"
	"github.com/influxdata/influxdb/inmem"
	"github.com/influxdata/influxdb/kv"
	"github.com/influxdata/influxdb/mock"
	"github.com/influxdata/influxdb/query"
	_ "github.com/influxdata/influxdb/query/builtin"
	"github.com/influxdata/influxdb/query/control"
	stdlib "github.com/influxdata/influxdb/query/stdlib/influxdata/influxdb"
	"github.com/influxdata/influxdb/storage"
	"github.com/influxdata/influxdb/storage/reads"
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

			var (
				ab       = newAnalyticalBackend(t, svc, svc)
				logger   = zaptest.NewLogger(t)
				rr       = backend.NewStoragePointsWriterRecorder(ab.PointsWriter(), logger)
				svcStack = backend.NewAnalyticalRunStorage(logger, svc, svc, svc, rr, ab.QueryService())
			)

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

func TestDeduplicateRuns(t *testing.T) {
	svc := kv.NewService(inmem.NewKVStore())
	if err := svc.Initialize(context.Background()); err != nil {
		t.Fatalf("error initializing kv service: %v", err)
	}

	ab := newAnalyticalBackend(t, svc, svc)
	defer ab.Close(t)

	mockTS := &mock.TaskService{
		FindTaskByIDFn: func(context.Context, influxdb.ID) (*influxdb.Task, error) {
			return &influxdb.Task{ID: 1, OrganizationID: 20}, nil
		},
		FindRunsFn: func(context.Context, influxdb.RunFilter) ([]*influxdb.Run, int, error) {
			return []*influxdb.Run{
				&influxdb.Run{ID: 2, Status: "started"},
			}, 1, nil
		},
	}
	mockTCS := &mock.TaskControlService{
		FinishRunFn: func(ctx context.Context, taskID, runID influxdb.ID) (*influxdb.Run, error) {
			return &influxdb.Run{ID: 2, TaskID: 1, Status: "success", ScheduledFor: time.Now(), StartedAt: time.Now().Add(1), FinishedAt: time.Now().Add(2)}, nil
		},
	}
	mockBS := mock.NewBucketService()

	svcStack := backend.NewAnalyticalStorage(zaptest.NewLogger(t), mockTS, mockBS, mockTCS, ab.PointsWriter(), ab.QueryService())

	_, err := svcStack.FinishRun(context.Background(), 1, 2)
	if err != nil {
		t.Fatal(err)
	}

	runs, _, err := svcStack.FindRuns(context.Background(), influxdb.RunFilter{Task: 1})
	if err != nil {
		t.Fatal(err)
	}

	if len(runs) != 1 {
		t.Fatalf("expected 1 run but got %d", len(runs))
	}

	if runs[0].Status != "success" {
		t.Fatalf("expected the deduped run to be 'success', got: %s", runs[0].Status)
	}
}

func Test_AnalyticalStorage_FinishRun(t *testing.T) {
	run := &influxdb.Run{
		ID: influxdb.ID(1),
	}

	for _, test := range []struct {
		name string
		// inputs
		orgID  influxdb.ID
		taskID influxdb.ID
		run    *influxdb.Run
		// expectations
		call recordCall
	}{
		{
			name:   "happy path legacy system bucket",
			orgID:  influxdb.ID(10),
			taskID: influxdb.ID(5),
			run:    run,
			call: recordCall{
				orgID:    influxdb.ID(10),
				org:      "influxdata",
				bucketID: influxdb.TasksSystemBucketID,
				bucket:   "_tasks",
				run:      run,
			},
		},
		{
			name:   "happy path new system bucket",
			orgID:  influxdb.ID(11),
			taskID: influxdb.ID(5),
			run:    run,
			call: recordCall{
				orgID:    influxdb.ID(11),
				org:      "influxdata",
				bucketID: influxdb.ID(7),
				bucket:   "_tasks",
				run:      run,
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			var (
				task = &influxdb.Task{
					ID:             test.taskID,
					OrganizationID: test.orgID,
					Organization:   "influxdata",
				}
				recorder = &runRecorder{
					isLegacyOrg: func(id influxdb.ID) bool {
						return id == influxdb.ID(10)
					},
				}
				ts = &mock.TaskService{
					FindTaskByIDFn: func(_ context.Context, id influxdb.ID) (*influxdb.Task, error) {
						if id != test.taskID {
							t.Fatalf("unexpected task ID %v", id)
						}

						return task, nil
					},
				}
				bs = &mock.BucketService{
					FindBucketByNameFn: func(_ context.Context, orgID influxdb.ID, name string) (*influxdb.Bucket, error) {
						if name != influxdb.TasksSystemBucketName {
							return nil, errors.New("bucket not found")
						}

						switch orgID {
						case influxdb.ID(10):
							return &influxdb.Bucket{
								ID:              influxdb.TasksSystemBucketID,
								Type:            influxdb.BucketTypeSystem,
								Name:            "_tasks",
								RetentionPeriod: time.Hour * 24 * 3,
								Description:     "System bucket for task logs",
							}, nil
						case influxdb.ID(11):
							return &influxdb.Bucket{
								ID:              influxdb.ID(7),
								OrgID:           influxdb.ID(11),
								Type:            influxdb.BucketTypeSystem,
								Name:            "_tasks",
								RetentionPeriod: time.Hour * 24 * 3,
								Description:     "System bucket for task logs",
							}, nil
						}

						return nil, errors.New("bucket not found")
					},
				}
				tcs = &mock.TaskControlService{
					FinishRunFn: func(_ context.Context, taskID, runID influxdb.ID) (*influxdb.Run, error) {
						if taskID != test.taskID {
							t.Fatalf("unexpected task ID %v", taskID)
						}

						if runID != test.run.ID {
							t.Fatalf("unexpected run ID %v", runID)
						}

						return test.run, nil
					},
				}
				store = backend.NewAnalyticalRunStorage(zap.NewNop(), ts, bs, tcs, recorder, nil)
			)

			run, err := store.FinishRun(context.Background(), test.taskID, test.run.ID)
			if err != nil {
				t.Fatalf("unexpected error from finish run %q", err)
			}

			if run != test.run {
				t.Errorf("expected run %v to be %v", run, test.run)
			}

			if recorder.call != test.call {
				t.Errorf("expected run recorder to have call %v, instead has %v", test.call, recorder.call)
			}
		})
	}
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

func (ab *analyticalBackend) Close(t *testing.T) {
	if err := ab.queryController.Shutdown(context.Background()); err != nil {
		t.Error(err)
	}
	if err := ab.storageEngine.Close(); err != nil {
		t.Error(err)
	}
	if err := os.RemoveAll(ab.rootDir); err != nil {
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

	// TODO(adam): do we need a proper secret service here?
	reader := reads.NewReader(readservice.NewStore(engine))
	deps, err := stdlib.NewDependencies(reader, engine, bucketSvc, orgSvc, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	cc := control.Config{
		ExecutorDependencies:     []flux.Dependency{deps},
		ConcurrencyQuota:         concurrencyQuota,
		MemoryBytesQuotaPerQuery: int64(memoryBytesQuotaPerQuery),
		QueueSize:                queueSize,
		Logger:                   logger.With(zap.String("service", "storage-reads")),
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

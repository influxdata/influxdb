package launcher

import (
	"context"
	"fmt"
	"io"
	"net"
	nethttp "net/http"
	_ "net/http/pprof" // needed to add pprof to our binary.
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/influxdata/flux/control"
	"github.com/influxdata/flux/execute"
	platform "github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/bolt"
	"github.com/influxdata/influxdb/chronograf/server"
	protofs "github.com/influxdata/influxdb/fs"
	"github.com/influxdata/influxdb/gather"
	"github.com/influxdata/influxdb/http"
	"github.com/influxdata/influxdb/inmem"
	"github.com/influxdata/influxdb/internal/fs"
	"github.com/influxdata/influxdb/kit/cli"
	"github.com/influxdata/influxdb/kit/prom"
	"github.com/influxdata/influxdb/kv"
	influxlogger "github.com/influxdata/influxdb/logger"
	"github.com/influxdata/influxdb/nats"
	infprom "github.com/influxdata/influxdb/prometheus"
	"github.com/influxdata/influxdb/proto"
	"github.com/influxdata/influxdb/query"
	pcontrol "github.com/influxdata/influxdb/query/control"
	"github.com/influxdata/influxdb/snowflake"
	"github.com/influxdata/influxdb/source"
	"github.com/influxdata/influxdb/storage"
	"github.com/influxdata/influxdb/storage/readservice"
	"github.com/influxdata/influxdb/task"
	taskbackend "github.com/influxdata/influxdb/task/backend"
	taskbolt "github.com/influxdata/influxdb/task/backend/bolt"
	"github.com/influxdata/influxdb/task/backend/coordinator"
	taskexecutor "github.com/influxdata/influxdb/task/backend/executor"
	_ "github.com/influxdata/influxdb/tsdb/tsi1" // needed for tsi1
	_ "github.com/influxdata/influxdb/tsdb/tsm1" // needed for tsm1
	"github.com/influxdata/influxdb/vault"
	pzap "github.com/influxdata/influxdb/zap"
	"github.com/opentracing/opentracing-go"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

const (
	// BoltStore stores all REST resources in boltdb.
	BoltStore = "bolt"
	// MemoryStore stores all REST resources in memory (useful for testing).
	MemoryStore = "memory"

	// LogTracing enables tracing via zap logs
	LogTracing = "log"
)

// Launcher represents the main program execution.
type Launcher struct {
	wg      sync.WaitGroup
	cancel  func()
	running bool

	storeType  string
	assetsPath string
	testing    bool

	logLevel          string
	tracingType       string
	reportingDisabled bool

	httpBindAddress string
	boltPath        string
	enginePath      string
	protosPath      string
	secretStore     string

	boltClient *bolt.Client
	kvService  *kv.Service
	engine     *storage.Engine

	queryController *pcontrol.Controller

	httpPort   int
	httpServer *nethttp.Server

	natsServer *nats.Server

	scheduler *taskbackend.TickScheduler
	taskStore taskbackend.Store

	logger *zap.Logger
	reg    *prom.Registry

	// BuildInfo contains commit, version and such of influxdb.
	BuildInfo platform.BuildInfo

	Stdin      io.Reader
	Stdout     io.Writer
	Stderr     io.Writer
	apibackend *http.APIBackend
}

// NewLauncher returns a new instance of Launcher connected to standard in/out/err.
func NewLauncher() *Launcher {
	return &Launcher{
		Stdin:  os.Stdin,
		Stdout: os.Stdout,
		Stderr: os.Stderr,
	}
}

// Running returns true if the main Launcher has started running.
func (m *Launcher) Running() bool {
	return m.running
}

// ReportingDisabled is true if opted out of usage stats.
func (m *Launcher) ReportingDisabled() bool {
	return m.reportingDisabled
}

// Registry returns the prometheus metrics registry.
func (m *Launcher) Registry() *prom.Registry {
	return m.reg
}

// Logger returns the launchers logger.
func (m *Launcher) Logger() *zap.Logger {
	return m.logger
}

// SetBuild adds version, commit, and date to prometheus metrics.
func (m *Launcher) SetBuild(version, commit, date string) {
	m.BuildInfo.Version = version
	m.BuildInfo.Commit = commit
	m.BuildInfo.Date = date
}

// URL returns the URL to connect to the HTTP server.
func (m *Launcher) URL() string {
	return fmt.Sprintf("http://127.0.0.1:%d", m.httpPort)
}

// Engine returns a reference to the storage engine. It should only be called
// for end-to-end testing purposes.
func (m *Launcher) Engine() *storage.Engine {
	return m.engine
}

// Shutdown shuts down the HTTP server and waits for all services to clean up.
func (m *Launcher) Shutdown(ctx context.Context) {
	m.httpServer.Shutdown(ctx)

	m.logger.Info("Stopping", zap.String("service", "task"))
	m.scheduler.Stop()

	m.logger.Info("Stopping", zap.String("service", "nats"))
	m.natsServer.Close()

	m.logger.Info("Stopping", zap.String("service", "bolt"))
	if err := m.boltClient.Close(); err != nil {
		m.logger.Info("failed closing bolt", zap.Error(err))
	}

	m.logger.Info("Stopping", zap.String("service", "query"))
	if err := m.queryController.Shutdown(ctx); err != nil && err != context.Canceled {
		m.logger.Info("Failed closing query service", zap.Error(err))
	}

	m.logger.Info("Stopping", zap.String("service", "storage-engine"))
	if err := m.engine.Close(); err != nil {
		m.logger.Error("failed to close engine", zap.Error(err))
	}

	m.wg.Wait()

	m.logger.Sync()
}

// Cancel executes the context cancel on the program. Used for testing.
func (m *Launcher) Cancel() { m.cancel() }

// Run executes the program with the given CLI arguments.
func (m *Launcher) Run(ctx context.Context, args ...string) error {
	dir, err := fs.InfluxDir()
	if err != nil {
		return fmt.Errorf("failed to determine influx directory: %v", err)
	}

	prog := &cli.Program{
		Name: "influxd",
		Run:  func() error { return m.run(ctx) },
		Opts: []cli.Opt{
			{
				DestP:   &m.logLevel,
				Flag:    "log-level",
				Default: "info",
				Desc:    "supported log levels are debug, info, and error",
			},
			{
				DestP:   &m.tracingType,
				Flag:    "tracing-type",
				Default: LogTracing,
				Desc:    fmt.Sprintf("supported tracing types are %s", LogTracing),
			},
			{
				DestP:   &m.httpBindAddress,
				Flag:    "http-bind-address",
				Default: ":9999",
				Desc:    "bind address for the REST HTTP API",
			},
			{
				DestP:   &m.boltPath,
				Flag:    "bolt-path",
				Default: filepath.Join(dir, "influxd.bolt"),
				Desc:    "path to boltdb database",
			},
			{
				DestP: &m.assetsPath,
				Flag:  "assets-path",
				Desc:  "override default assets by serving from a specific directory (developer mode)",
			},
			{
				DestP:   &m.storeType,
				Flag:    "store",
				Default: "bolt",
				Desc:    "backing store for REST resources (bolt or memory)",
			},
			{
				DestP:   &m.testing,
				Flag:    "e2e-testing",
				Default: false,
				Desc:    "add /debug/flush endpoint to clear stores; used for end-to-end tests",
			},
			{
				DestP:   &m.enginePath,
				Flag:    "engine-path",
				Default: filepath.Join(dir, "engine"),
				Desc:    "path to persistent engine files",
			},
			{
				DestP:   &m.secretStore,
				Flag:    "secret-store",
				Default: "bolt",
				Desc:    "data store for secrets (bolt or vault)",
			},
			{
				DestP:   &m.protosPath,
				Flag:    "protos-path",
				Default: filepath.Join(dir, "protos"),
				Desc:    "path to protos on the filesystem",
			},
			{
				DestP:   &m.reportingDisabled,
				Flag:    "reporting-disabled",
				Default: false,
				Desc:    "disable sending telemetry data to https://telemetry.influxdata.com every 8 hours",
			},
		},
	}

	cmd := cli.NewCommand(prog)
	cmd.SetArgs(args)
	return cmd.Execute()
}

func (m *Launcher) run(ctx context.Context) (err error) {
	m.running = true
	ctx, m.cancel = context.WithCancel(ctx)

	var lvl zapcore.Level
	if err := lvl.Set(m.logLevel); err != nil {
		return fmt.Errorf("unknown log level; supported levels are debug, info, and error")
	}

	// Create top level logger
	logconf := &influxlogger.Config{
		Format: "auto",
		Level:  lvl,
	}
	m.logger, err = logconf.New(m.Stdout)
	if err != nil {
		return err
	}

	m.logger.Info("Welcome to InfluxDB",
		zap.String("version", m.BuildInfo.Version),
		zap.String("commit", m.BuildInfo.Commit),
		zap.String("build_date", m.BuildInfo.Date),
	)

	switch m.tracingType {
	case LogTracing:
		m.logger.Info("tracing via zap logging")
		tracer := new(pzap.Tracer)
		tracer.Logger = m.logger
		tracer.IDGenerator = snowflake.NewIDGenerator()
		opentracing.SetGlobalTracer(tracer)
	}

	m.boltClient = bolt.NewClient()
	m.boltClient.Path = m.boltPath
	m.boltClient.WithLogger(m.logger.With(zap.String("service", "bolt")))

	if err := m.boltClient.Open(ctx); err != nil {
		m.logger.Error("failed opening bolt", zap.Error(err))
		return err
	}

	var flusher http.Flusher
	switch m.storeType {
	case BoltStore:
		store := bolt.NewKVStore(m.boltPath)
		store.WithDB(m.boltClient.DB())
		m.kvService = kv.NewService(store)
		if m.testing {
			flusher = store
		}
	case MemoryStore:
		store := inmem.NewKVStore()
		m.kvService = kv.NewService(store)
		if m.testing {
			flusher = store
		}
	default:
		err := fmt.Errorf("unknown store type %s; expected bolt or memory", m.storeType)
		m.logger.Error("failed opening bolt", zap.Error(err))
		return err
	}

	m.kvService.Logger = m.logger.With(zap.String("store", "kv"))
	if err := m.kvService.Initialize(ctx); err != nil {
		m.logger.Error("failed to initialize kv service", zap.Error(err))
		return err
	}

	m.reg = prom.NewRegistry()
	m.reg.MustRegister(
		prometheus.NewGoCollector(),
		infprom.NewInfluxCollector(m.boltClient, m.BuildInfo),
	)
	m.reg.WithLogger(m.logger)
	m.reg.MustRegister(m.boltClient)

	var (
		orgSvc           platform.OrganizationService             = m.kvService
		authSvc          platform.AuthorizationService            = m.kvService
		userSvc          platform.UserService                     = m.kvService
		variableSvc      platform.VariableService                 = m.kvService
		bucketSvc        platform.BucketService                   = m.kvService
		sourceSvc        platform.SourceService                   = m.kvService
		sessionSvc       platform.SessionService                  = m.kvService
		passwdsSvc       platform.PasswordsService                = m.kvService
		dashboardSvc     platform.DashboardService                = m.kvService
		dashboardLogSvc  platform.DashboardOperationLogService    = m.kvService
		userLogSvc       platform.UserOperationLogService         = m.kvService
		bucketLogSvc     platform.BucketOperationLogService       = m.kvService
		orgLogSvc        platform.OrganizationOperationLogService = m.kvService
		onboardingSvc    platform.OnboardingService               = m.kvService
		scraperTargetSvc platform.ScraperTargetStoreService       = m.kvService
		telegrafSvc      platform.TelegrafConfigStore             = m.kvService
		userResourceSvc  platform.UserResourceMappingService      = m.kvService
		labelSvc         platform.LabelService                    = m.kvService
		secretSvc        platform.SecretService                   = m.kvService
		lookupSvc        platform.LookupService                   = m.kvService
	)

	switch m.secretStore {
	case "bolt":
		// If it is bolt, then we already set it above.
	case "vault":
		// The vault secret service is configured using the standard vault environment variables.
		// https://www.vaultproject.io/docs/commands/index.html#environment-variables
		svc, err := vault.NewSecretService()
		if err != nil {
			m.logger.Error("failed initalizing vault secret service", zap.Error(err))
			return err
		}
		secretSvc = svc
	default:
		err := fmt.Errorf("unknown secret service %q, expected \"bolt\" or \"vault\"", m.secretStore)
		m.logger.Error("failed setting secret service", zap.Error(err))
		return err
	}

	// Load proto examples from the user data.
	protoSvc := protofs.NewProtoService(m.protosPath, m.logger, dashboardSvc)
	if err := protoSvc.Open(ctx); err != nil {
		m.logger.Error("failed to read protos from the filesystem", zap.Error(err))
		return err
	}

	// ... now, load the proto examples that are build with release.
	protoData, err := proto.Load(m.logger)
	if err != nil {
		return err
	}

	// join the release proto examples with the user data examples.
	protoSvc.WithProtos(protoData)

	chronografSvc, err := server.NewServiceV2(ctx, m.boltClient.DB())
	if err != nil {
		m.logger.Error("failed creating chronograf service", zap.Error(err))
		return err
	}

	var pointsWriter storage.PointsWriter
	{
		m.engine = storage.NewEngine(m.enginePath, storage.NewConfig(), storage.WithRetentionEnforcer(bucketSvc))
		m.engine.WithLogger(m.logger)

		if err := m.engine.Open(); err != nil {
			m.logger.Error("failed to open engine", zap.Error(err))
			return err
		}
		// The Engine's metrics must be registered after it opens.
		m.reg.MustRegister(m.engine.PrometheusCollectors()...)

		pointsWriter = m.engine

		const (
			concurrencyQuota = 10
			memoryBytesQuota = 1e6
		)

		cc := control.Config{
			ExecutorDependencies: make(execute.Dependencies),
			ConcurrencyQuota:     concurrencyQuota,
			MemoryBytesQuota:     int64(memoryBytesQuota),
			Logger:               m.logger.With(zap.String("service", "storage-reads")),
		}

		if err := readservice.AddControllerConfigDependencies(
			&cc, m.engine, bucketSvc, orgSvc,
		); err != nil {
			m.logger.Error("Failed to configure query controller dependencies", zap.Error(err))
			return err
		}

		m.queryController = pcontrol.New(cc)
		m.reg.MustRegister(m.queryController.PrometheusCollectors()...)
	}

	var storageQueryService = readservice.NewProxyQueryService(m.queryController)
	var taskSvc platform.TaskService
	{
		var (
			store taskbackend.Store
			err   error
		)
		store, err = taskbolt.New(m.boltClient.DB(), "tasks", taskbolt.NoCatchUp)
		if err != nil {
			m.logger.Error("failed opening task bolt", zap.Error(err))
			return err
		}

		if m.storeType == "memory" {
			store = taskbackend.NewInMemStore()
		}

		executor := taskexecutor.NewAsyncQueryServiceExecutor(m.logger.With(zap.String("service", "task-executor")), m.queryController, authSvc, store)

		lw := taskbackend.NewPointLogWriter(pointsWriter)
		m.scheduler = taskbackend.NewScheduler(store, executor, lw, time.Now().UTC().Unix(), taskbackend.WithTicker(ctx, 100*time.Millisecond), taskbackend.WithLogger(m.logger))
		m.scheduler.Start(ctx)
		m.reg.MustRegister(m.scheduler.PrometheusCollectors()...)

		queryService := query.QueryServiceBridge{AsyncQueryService: m.queryController}
		lr := taskbackend.NewQueryLogReader(queryService)
		taskSvc = task.PlatformAdapter(coordinator.New(m.logger.With(zap.String("service", "task-coordinator")), m.scheduler, store), lr, m.scheduler, authSvc, userResourceSvc, orgSvc)
		taskSvc = task.NewValidator(taskSvc, bucketSvc)
		m.taskStore = store
	}

	// NATS streaming server
	m.natsServer = nats.NewServer()
	if err := m.natsServer.Open(); err != nil {
		m.logger.Error("failed to start nats streaming server", zap.Error(err))
		return err
	}

	publisher := nats.NewAsyncPublisher("nats-publisher")
	if err := publisher.Open(); err != nil {
		m.logger.Error("failed to connect to streaming server", zap.Error(err))
		return err
	}

	// TODO(jm): this is an example of using a subscriber to consume from the channel. It should be removed.
	subscriber := nats.NewQueueSubscriber("nats-subscriber")
	if err := subscriber.Open(); err != nil {
		m.logger.Error("failed to connect to streaming server", zap.Error(err))
		return err
	}

	subscriber.Subscribe(gather.MetricsSubject, "metrics", &gather.RecorderHandler{
		Logger: m.logger,
		Recorder: gather.PointWriter{
			Writer: pointsWriter,
		},
	})
	scraperScheduler, err := gather.NewScheduler(10, m.logger, scraperTargetSvc, publisher, subscriber, 10*time.Second, 30*time.Second)
	if err != nil {
		m.logger.Error("failed to create scraper subscriber", zap.Error(err))
		return err
	}

	m.wg.Add(1)
	go func(logger *zap.Logger) {
		defer m.wg.Done()
		logger = logger.With(zap.String("service", "scraper"))
		if err := scraperScheduler.Run(ctx); err != nil {
			logger.Error("failed scraper service", zap.Error(err))
		}
		logger.Info("Stopping")
	}(m.logger)

	m.httpServer = &nethttp.Server{
		Addr: m.httpBindAddress,
	}

	m.apibackend = &http.APIBackend{
		AssetsPath:           m.assetsPath,
		Logger:               m.logger,
		NewBucketService:     source.NewBucketService,
		NewQueryService:      source.NewQueryService,
		PointsWriter:         pointsWriter,
		AuthorizationService: authSvc,
		// Wrap the BucketService in a storage backed one that will ensure deleted buckets are removed from the storage engine.
		BucketService:                   storage.NewBucketService(bucketSvc, m.engine),
		SessionService:                  sessionSvc,
		UserService:                     userSvc,
		OrganizationService:             orgSvc,
		UserResourceMappingService:      userResourceSvc,
		LabelService:                    labelSvc,
		DashboardService:                dashboardSvc,
		DashboardOperationLogService:    dashboardLogSvc,
		BucketOperationLogService:       bucketLogSvc,
		UserOperationLogService:         userLogSvc,
		OrganizationOperationLogService: orgLogSvc,
		SourceService:                   sourceSvc,
		VariableService:                 variableSvc,
		PasswordsService:                passwdsSvc,
		OnboardingService:               onboardingSvc,
		InfluxQLService:                 nil, // No InfluxQL support
		FluxService:                     storageQueryService,
		TaskService:                     taskSvc,
		TelegrafService:                 telegrafSvc,
		ScraperTargetStoreService:       scraperTargetSvc,
		ChronografService:               chronografSvc,
		SecretService:                   secretSvc,
		LookupService:                   lookupSvc,
		ProtoService:                    protoSvc,
		OrgLookupService:                m.kvService,
	}

	// HTTP server
	httpLogger := m.logger.With(zap.String("service", "http"))
	platformHandler := http.NewPlatformHandler(m.apibackend)
	m.reg.MustRegister(platformHandler.PrometheusCollectors()...)

	h := http.NewHandlerFromRegistry("platform", m.reg)
	h.Handler = platformHandler
	h.Logger = httpLogger
	h.Tracer = opentracing.GlobalTracer()

	m.httpServer.Handler = h
	// If we are in testing mode we allow all data to be flushed and removed.
	if m.testing {
		m.httpServer.Handler = http.DebugFlush(h, flusher)
	}

	ln, err := net.Listen("tcp", m.httpBindAddress)
	if err != nil {
		httpLogger.Error("failed http listener", zap.Error(err))
		httpLogger.Info("Stopping")
		return err
	}

	if addr, ok := ln.Addr().(*net.TCPAddr); ok {
		m.httpPort = addr.Port
	}

	m.wg.Add(1)
	go func(logger *zap.Logger) {
		defer m.wg.Done()
		logger.Info("Listening", zap.String("transport", "http"), zap.String("addr", m.httpBindAddress), zap.Int("port", m.httpPort))

		if err := m.httpServer.Serve(ln); err != nethttp.ErrServerClosed {
			logger.Error("failed http service", zap.Error(err))
		}
		logger.Info("Stopping")
	}(httpLogger)

	return nil
}

// OrganizationService returns the internal organization service.
func (m *Launcher) OrganizationService() platform.OrganizationService {
	return m.apibackend.OrganizationService
}

// QueryController returns the internal query service.
func (m *Launcher) QueryController() *pcontrol.Controller {
	return m.queryController
}

// BucketService returns the internal bucket service.
func (m *Launcher) BucketService() platform.BucketService {
	return m.apibackend.BucketService
}

// UserService returns the internal suser service.
func (m *Launcher) UserService() platform.UserService {
	return m.apibackend.UserService
}

// AuthorizationService returns the internal authorization service.
func (m *Launcher) AuthorizationService() platform.AuthorizationService {
	return m.apibackend.AuthorizationService
}

// TaskService returns the internal task service.
func (m *Launcher) TaskService() platform.TaskService {
	return m.apibackend.TaskService
}

// TaskStore returns the internal store service.
func (m *Launcher) TaskStore() taskbackend.Store {
	return m.taskStore
}

// TaskScheduler returns the internal scheduler service.
func (m *Launcher) TaskScheduler() taskbackend.Scheduler {
	return m.scheduler
}

// KeyValueService returns the internal key-value service.
func (m *Launcher) KeyValueService() *kv.Service {
	return m.kvService
}

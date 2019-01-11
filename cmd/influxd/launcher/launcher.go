package launcher

import (
	"context"
	"fmt"
	"io"
	"net"
	nethttp "net/http"
	_ "net/http/pprof"
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
	"github.com/influxdata/influxdb/internal/fs"
	"github.com/influxdata/influxdb/kit/cli"
	"github.com/influxdata/influxdb/kit/prom"
	influxlogger "github.com/influxdata/influxdb/logger"
	"github.com/influxdata/influxdb/nats"
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
	_ "github.com/influxdata/influxdb/tsdb/tsi1"
	_ "github.com/influxdata/influxdb/tsdb/tsm1"
	"github.com/influxdata/influxdb/vault"
	pzap "github.com/influxdata/influxdb/zap"
	opentracing "github.com/opentracing/opentracing-go"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// Launcher represents the main program execution.
type Launcher struct {
	wg      sync.WaitGroup
	cancel  func()
	running bool

	logLevel        string
	httpBindAddress string
	boltPath        string
	natsPath        string
	developerMode   bool
	enginePath      string
	protosPath      string

	secretStore string

	boltClient *bolt.Client
	engine     *storage.Engine

	queryController *pcontrol.Controller

	httpPort   int
	httpServer *nethttp.Server

	natsServer *nats.Server

	scheduler *taskbackend.TickScheduler

	logger *zap.Logger

	Stdin  io.Reader
	Stdout io.Writer
	Stderr io.Writer
}

// NewLauncher returns a new instance of Launcher connected to standard in/out/err.
func NewLauncher() *Launcher {
	return &Launcher{
		Stdin:  os.Stdin,
		Stdout: os.Stdout,
		Stderr: os.Stderr,
	}
}

func (m *Launcher) Running() bool {
	return m.running
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
	if err := m.queryController.Shutdown(ctx); err != nil {
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
				DestP:   &m.developerMode,
				Flag:    "developer-mode",
				Default: false,
				Desc:    "serve assets from the local filesystem in developer mode",
			},
			{
				DestP:   &m.natsPath,
				Flag:    "nats-path",
				Default: filepath.Join(dir, "nats"),
				Desc:    "path to NATS queue for scraping tasks",
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

	// set tracing
	tracer := new(pzap.Tracer)
	tracer.Logger = m.logger
	tracer.IDGenerator = snowflake.NewIDGenerator()
	opentracing.SetGlobalTracer(tracer)

	reg := prom.NewRegistry()
	reg.MustRegister(prometheus.NewGoCollector())
	reg.WithLogger(m.logger)

	m.boltClient = bolt.NewClient()
	m.boltClient.Path = m.boltPath
	m.boltClient.WithLogger(m.logger.With(zap.String("service", "bolt")))

	if err := m.boltClient.Open(ctx); err != nil {
		m.logger.Error("failed opening bolt", zap.Error(err))
		return err
	}

	var (
		orgSvc           platform.OrganizationService             = m.boltClient
		authSvc          platform.AuthorizationService            = m.boltClient
		userSvc          platform.UserService                     = m.boltClient
		macroSvc         platform.MacroService                    = m.boltClient
		bucketSvc        platform.BucketService                   = m.boltClient
		sourceSvc        platform.SourceService                   = m.boltClient
		sessionSvc       platform.SessionService                  = m.boltClient
		basicAuthSvc     platform.BasicAuthService                = m.boltClient
		dashboardSvc     platform.DashboardService                = m.boltClient
		dashboardLogSvc  platform.DashboardOperationLogService    = m.boltClient
		userLogSvc       platform.UserOperationLogService         = m.boltClient
		bucketLogSvc     platform.BucketOperationLogService       = m.boltClient
		orgLogSvc        platform.OrganizationOperationLogService = m.boltClient
		onboardingSvc    platform.OnboardingService               = m.boltClient
		scraperTargetSvc platform.ScraperTargetStoreService       = m.boltClient
		telegrafSvc      platform.TelegrafConfigStore             = m.boltClient
		userResourceSvc  platform.UserResourceMappingService      = m.boltClient
		labelSvc         platform.LabelService                    = m.boltClient
		secretSvc        platform.SecretService                   = m.boltClient
		lookupSvc        platform.LookupService                   = m.boltClient
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

	protoSvc := protofs.NewProtoService(m.protosPath, m.logger, dashboardSvc)
	if err := protoSvc.Open(ctx); err != nil {
		m.logger.Error("failed to read protos from the filesystem", zap.Error(err))
		return err
	}

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
		reg.MustRegister(m.engine.PrometheusCollectors()...)

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
		reg.MustRegister(m.queryController.PrometheusCollectors()...)
	}

	var storageQueryService query.ProxyQueryService = readservice.NewProxyQueryService(m.queryController)
	var taskSvc platform.TaskService
	{
		boltStore, err := taskbolt.New(m.boltClient.DB(), "tasks")
		if err != nil {
			m.logger.Error("failed opening task bolt", zap.Error(err))
			return err
		}

		executor := taskexecutor.NewAsyncQueryServiceExecutor(m.logger.With(zap.String("service", "task-executor")), m.queryController, boltStore)

		lw := taskbackend.NewPointLogWriter(pointsWriter)
		m.scheduler = taskbackend.NewScheduler(boltStore, executor, lw, time.Now().UTC().Unix(), taskbackend.WithTicker(ctx, 100*time.Millisecond), taskbackend.WithLogger(m.logger))
		m.scheduler.Start(ctx)
		reg.MustRegister(m.scheduler.PrometheusCollectors()...)

		queryService := query.QueryServiceBridge{AsyncQueryService: m.queryController}
		lr := taskbackend.NewQueryLogReader(queryService)
		taskSvc = task.PlatformAdapter(coordinator.New(m.logger.With(zap.String("service", "task-coordinator")), m.scheduler, boltStore), lr, m.scheduler)
		taskSvc = task.NewValidator(taskSvc, bucketSvc)
	}

	// NATS streaming server
	m.natsServer = nats.NewServer(nats.Config{FilestoreDir: m.natsPath})
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

	subscriber.Subscribe(gather.MetricsSubject, "", &gather.RecorderHandler{
		Logger: m.logger,
		Recorder: gather.PointWriter{
			Writer: pointsWriter,
		},
	})
	scraperScheduler, err := gather.NewScheduler(10, m.logger, scraperTargetSvc, publisher, subscriber, 0, 0)
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

	handlerConfig := &http.APIBackend{
		DeveloperMode:        m.developerMode,
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
		MacroService:                    macroSvc,
		BasicAuthService:                basicAuthSvc,
		OnboardingService:               onboardingSvc,
		ProxyQueryService:               storageQueryService,
		TaskService:                     taskSvc,
		TelegrafService:                 telegrafSvc,
		ScraperTargetStoreService:       scraperTargetSvc,
		ChronografService:               chronografSvc,
		SecretService:                   secretSvc,
		LookupService:                   lookupSvc,
		ProtoService:                    protoSvc,
	}

	// HTTP server
	httpLogger := m.logger.With(zap.String("service", "http"))
	platformHandler := http.NewPlatformHandler(handlerConfig)
	reg.MustRegister(platformHandler.PrometheusCollectors()...)

	h := http.NewHandlerFromRegistry("platform", reg)
	h.Handler = platformHandler
	h.Logger = httpLogger
	h.Tracer = opentracing.GlobalTracer()

	m.httpServer.Handler = h

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

package main

import (
	"context"
	"fmt"
	nethttp "net/http"
	_ "net/http/pprof"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/influxdata/platform/snowflake"
	"github.com/opentracing/opentracing-go"

	"github.com/influxdata/platform"
	"github.com/influxdata/platform/bolt"
	"github.com/influxdata/platform/chronograf/server"
	"github.com/influxdata/platform/gather"
	"github.com/influxdata/platform/http"
	"github.com/influxdata/platform/internal/fs"
	"github.com/influxdata/platform/kit/cli"
	"github.com/influxdata/platform/kit/prom"
	"github.com/influxdata/platform/kit/signals"
	influxlogger "github.com/influxdata/platform/logger"
	"github.com/influxdata/platform/nats"
	"github.com/influxdata/platform/query"
	_ "github.com/influxdata/platform/query/builtin"
	"github.com/influxdata/platform/source"
	"github.com/influxdata/platform/storage"
	"github.com/influxdata/platform/storage/readservice"
	"github.com/influxdata/platform/task"
	taskbackend "github.com/influxdata/platform/task/backend"
	taskbolt "github.com/influxdata/platform/task/backend/bolt"
	"github.com/influxdata/platform/task/backend/coordinator"
	taskexecutor "github.com/influxdata/platform/task/backend/executor"
	_ "github.com/influxdata/platform/tsdb/tsi1"
	_ "github.com/influxdata/platform/tsdb/tsm1"
	pzap "github.com/influxdata/platform/zap"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func main() {
	dir, err := fs.InfluxDir()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to determine influx directory: %v", err)
		os.Exit(1)
	}

	prog := &cli.Program{
		Name: "influxd",
		Run:  run,
		Opts: []cli.Opt{
			{
				DestP:   &logLevel,
				Flag:    "log-level",
				Default: "info",
				Desc:    "supported log levels are debug, info, and error",
			},
			{
				DestP:   &httpBindAddress,
				Flag:    "http-bind-address",
				Default: ":9999",
				Desc:    "bind address for the REST HTTP API",
			},
			{
				DestP:   &boltPath,
				Flag:    "bolt-path",
				Default: filepath.Join(dir, "influxd.bolt"),
				Desc:    "path to boltdb database",
			},
			{
				DestP:   &developerMode,
				Flag:    "developer-mode",
				Default: false,
				Desc:    "serve assets from the local filesystem in developer mode",
			},
			{
				DestP:   &natsPath,
				Flag:    "nats-path",
				Default: filepath.Join(dir, "nats"),
				Desc:    "path to NATS queue for scraping tasks",
			},
			{
				DestP:   &enginePath,
				Flag:    "engine-path",
				Default: filepath.Join(dir, "engine"),
				Desc:    "path to persistent engine files",
			},
		},
	}

	cmd := cli.NewCommand(prog)
	if err := cmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

var (
	logLevel        string
	httpBindAddress string
	boltPath        string
	natsPath        string
	developerMode   bool
	enginePath      string
)

func run() error {
	ctx := context.Background()
	// exit with SIGINT and SIGTERM
	ctx = signals.WithStandardSignals(ctx)

	var lvl zapcore.Level
	if err := lvl.Set(logLevel); err != nil {
		return fmt.Errorf("unknown log level; supported levels are debug, info, and error")
	}

	// Create top level logger
	logconf := &influxlogger.Config{
		Format: "auto",
		Level:  lvl,
	}
	logger, err := logconf.New(os.Stdout)
	if err != nil {
		return err
	}
	defer logger.Sync()

	// set tracing
	tracer := new(pzap.Tracer)
	tracer.Logger = logger
	tracer.IDGenerator = snowflake.NewIDGenerator()
	opentracing.SetGlobalTracer(tracer)

	reg := prom.NewRegistry()
	reg.MustRegister(prometheus.NewGoCollector())
	reg.WithLogger(logger)

	c := bolt.NewClient()
	c.Path = boltPath
	c.WithLogger(logger.With(zap.String("service", "bolt")))

	if err := c.Open(ctx); err != nil {
		logger.Error("failed opening bolt", zap.Error(err))
		return err
	}
	defer func(logger *zap.Logger) {
		logger = logger.With(zap.String("service", "bolt"))
		logger.Info("Stopping")
		if err := c.Close(); err != nil {
			logger.Info("failed closing bolt", zap.String("service", "bolt"))
		}
	}(logger)

	var (
		orgSvc           platform.OrganizationService             = c
		authSvc          platform.AuthorizationService            = c
		userSvc          platform.UserService                     = c
		viewSvc          platform.ViewService                     = c
		macroSvc         platform.MacroService                    = c
		bucketSvc        platform.BucketService                   = c
		sourceSvc        platform.SourceService                   = c
		sessionSvc       platform.SessionService                  = c
		basicAuthSvc     platform.BasicAuthService                = c
		dashboardSvc     platform.DashboardService                = c
		dashboardLogSvc  platform.DashboardOperationLogService    = c
		userLogSvc       platform.UserOperationLogService         = c
		bucketLogSvc     platform.BucketOperationLogService       = c
		orgLogSvc        platform.OrganizationOperationLogService = c
		onboardingSvc    platform.OnboardingService               = c
		userResourceSvc  platform.UserResourceMappingService      = c
		scraperTargetSvc platform.ScraperTargetStoreService       = c
	)

	chronografSvc, err := server.NewServiceV2(ctx, c.DB())
	if err != nil {
		logger.Error("failed creating chronograf service", zap.Error(err))
		return err
	}

	var storageQueryService query.ProxyQueryService

	var telegrafSvc platform.TelegrafConfigStore = c

	var pointsWriter storage.PointsWriter
	{
		config := storage.NewConfig()
		config.EngineOptions.WALEnabled = true // Enable a disk-based WAL.
		config.EngineOptions.Config = config.Config

		engine := storage.NewEngine(enginePath, config, storage.WithRetentionEnforcer(bucketSvc))
		engine.WithLogger(logger)
		reg.MustRegister(engine.PrometheusCollectors()...)

		if err := engine.Open(); err != nil {
			logger.Error("failed to open engine", zap.Error(err))
			return err
		}
		defer func() {
			logger.Info("Stopping", zap.String("service", "storage-engine"))
			if err := engine.Close(); err != nil {
				logger.Error("failed to close engine", zap.Error(err))
			}
		}()

		pointsWriter = engine

		service, err := readservice.NewProxyQueryService(
			engine, bucketSvc, orgSvc, logger.With(zap.String("service", "storage-reads")))
		if err != nil {
			logger.Error("failed to create query service", zap.Error(err))
			return err
		}

		storageQueryService = service
	}

	var queryService query.QueryService = storageQueryService.(query.ProxyQueryServiceBridge).QueryService
	var taskSvc platform.TaskService
	{
		boltStore, err := taskbolt.New(c.DB(), "tasks")
		if err != nil {
			logger.Error("failed opening task bolt", zap.Error(err))
			return err
		}

		executor := taskexecutor.NewQueryServiceExecutor(logger.With(zap.String("service", "task-executor")), queryService, boltStore)

		// TODO(lh): Replace NopLogWriter with real log writer
		scheduler := taskbackend.NewScheduler(boltStore, executor, taskbackend.NopLogWriter{}, time.Now().UTC().Unix(), taskbackend.WithTicker(ctx, time.Second), taskbackend.WithLogger(logger))
		scheduler.Start(ctx)
		defer func() {
			logger.Info("Stopping", zap.String("service", "task"))
			scheduler.Stop()
		}()
		reg.MustRegister(scheduler.PrometheusCollectors()...)

		// TODO(lh): Replace NopLogReader with real log reader
		taskSvc = task.PlatformAdapter(coordinator.New(logger.With(zap.String("service", "task-coordinator")), scheduler, boltStore), taskbackend.NopLogReader{}, scheduler)
		// TODO(lh): Add in `taskSvc = task.NewValidator(taskSvc)` once we have Authentication coming in the context.
		// see issue #563
	}

	// NATS streaming server
	natsServer := nats.NewServer(nats.Config{FilestoreDir: natsPath})
	if err := natsServer.Open(); err != nil {
		logger.Error("failed to start nats streaming server", zap.Error(err))
		return err
	}
	defer func() {
		logger.Info("Stopping", zap.String("service", "nats"))
		natsServer.Close()
	}()

	publisher := nats.NewAsyncPublisher("nats-publisher")
	if err := publisher.Open(); err != nil {
		logger.Error("failed to connect to streaming server", zap.Error(err))
		return err
	}

	// TODO(jm): this is an example of using a subscriber to consume from the channel. It should be removed.
	subscriber := nats.NewQueueSubscriber("nats-subscriber")
	if err := subscriber.Open(); err != nil {
		logger.Error("failed to connect to streaming server", zap.Error(err))
		return err
	}

	scraperScheduler, err := gather.NewScheduler(10, logger, scraperTargetSvc, publisher, subscriber, 0, 0)
	if err != nil {
		logger.Error("failed to create scraper subscriber", zap.Error(err))
		return err
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func(logger *zap.Logger) {
		defer wg.Done()
		logger = logger.With(zap.String("service", "scraper"))
		if err := scraperScheduler.Run(ctx); err != nil {
			logger.Error("failed scraper service", zap.Error(err))
		}
		logger.Info("Stopping")
	}(logger)

	httpServer := &nethttp.Server{
		Addr: httpBindAddress,
	}

	handlerConfig := &http.APIBackend{
		Logger:                          logger,
		NewBucketService:                source.NewBucketService,
		NewQueryService:                 source.NewQueryService,
		PointsWriter:                    pointsWriter,
		AuthorizationService:            authSvc,
		BucketService:                   bucketSvc,
		SessionService:                  sessionSvc,
		UserService:                     userSvc,
		OrganizationService:             orgSvc,
		UserResourceMappingService:      userResourceSvc,
		DashboardService:                dashboardSvc,
		DashboardOperationLogService:    dashboardLogSvc,
		BucketOperationLogService:       bucketLogSvc,
		UserOperationLogService:         userLogSvc,
		OrganizationOperationLogService: orgLogSvc,
		ViewService:                     viewSvc,
		SourceService:                   sourceSvc,
		MacroService:                    macroSvc,
		BasicAuthService:                basicAuthSvc,
		OnboardingService:               onboardingSvc,
		ProxyQueryService:               storageQueryService,
		TaskService:                     taskSvc,
		TelegrafService:                 telegrafSvc,
		ScraperTargetStoreService:       scraperTargetSvc,
		ChronografService:               chronografSvc,
	}

	// HTTP server
	wg.Add(1)
	go func(logger *zap.Logger) {
		defer wg.Done()
		logger = logger.With(zap.String("service", "http"))
		platformHandler := http.NewPlatformHandler(handlerConfig)
		reg.MustRegister(platformHandler.PrometheusCollectors()...)

		h := http.NewHandlerFromRegistry("platform", reg)
		h.Handler = platformHandler
		h.Logger = logger
		h.Tracer = opentracing.GlobalTracer()

		httpServer.Handler = h
		logger.Info("Listening", zap.String("transport", "http"), zap.String("addr", httpBindAddress))
		if err := httpServer.ListenAndServe(); err != nethttp.ErrServerClosed {
			logger.Error("failed http service", zap.Error(err))
		}
		logger.Info("Stopping")
	}(logger)

	<-ctx.Done()
	cctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	httpServer.Shutdown(cctx)
	wg.Wait()
	return nil
}

package main

import (
	"context"
	"fmt"
	nethttp "net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"os/user"
	"path/filepath"
	"syscall"
	"time"

	"github.com/influxdata/platform"
	"github.com/influxdata/platform/bolt"
	"github.com/influxdata/platform/chronograf/server"
	"github.com/influxdata/platform/gather"
	"github.com/influxdata/platform/http"
	"github.com/influxdata/platform/kit/prom"
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
	"github.com/prometheus/client_golang/prometheus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"go.uber.org/zap"
)

func main() {
	Execute()
}

const (
	// IngressSubject is the subject that subscribers and publishers use for writing and consuming line protocol
	IngressSubject = "ingress"
	// IngressGroup is the Nats Streaming Subscriber group, allowing multiple subscribers to distribute work
	IngressGroup = "ingress"
)

var (
	httpBindAddress   string
	authorizationPath string
	boltPath          string
	natsPath          string
	developerMode     bool
	enginePath        string
)

func influxDir() (string, error) {
	var dir string
	// By default, store meta and data files in current users home directory
	u, err := user.Current()
	if err == nil {
		dir = u.HomeDir
	} else if home := os.Getenv("HOME"); home != "" {
		dir = home
	} else {
		wd, err := os.Getwd()
		if err != nil {
			return "", err
		}
		dir = wd
	}
	dir = filepath.Join(dir, ".influxdbv2")

	return dir, nil
}

func init() {
	dir, err := influxDir()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to determine influx directory: %v", err)
		os.Exit(1)
	}

	viper.SetEnvPrefix("INFLUX")

	platformCmd.Flags().StringVar(&httpBindAddress, "http-bind-address", ":9999", "bind address for the rest http api")
	viper.BindEnv("HTTP_BIND_ADDRESS")
	if h := viper.GetString("HTTP_BIND_ADDRESS"); h != "" {
		httpBindAddress = h
	}

	platformCmd.Flags().StringVar(&authorizationPath, "authorization-path", "", "path to a bootstrap token")
	viper.BindEnv("TOKEN_PATH")
	if h := viper.GetString("TOKEN_PATH"); h != "" {
		authorizationPath = h
	}

	platformCmd.Flags().StringVar(&boltPath, "bolt-path", filepath.Join(dir, "influxd.bolt"), "path to boltdb database")
	viper.BindEnv("BOLT_PATH")
	if h := viper.GetString("BOLT_PATH"); h != "" {
		boltPath = h
	}

	platformCmd.Flags().BoolVar(&developerMode, "developer-mode", false, "serve assets from the local filesystem in developer mode")
	viper.BindEnv("DEV_MODE")
	if h := viper.GetBool("DEV_MODE"); h {
		developerMode = h
	}

	// TODO(edd): do we need NATS for anything?
	platformCmd.Flags().StringVar(&natsPath, "nats-path", filepath.Join(dir, "nats"), "path to persistent NATS files")
	viper.BindEnv("NATS_PATH")
	if h := viper.GetString("NATS_PATH"); h != "" {
		natsPath = h
	}

	platformCmd.Flags().StringVar(&enginePath, "engine-path", filepath.Join(dir, "engine"), "path to persistent engine files")
	viper.BindEnv("ENGINE_PATH")
	if h := viper.GetString("ENGINE_PATH"); h != "" {
		enginePath = h
	}
}

var platformCmd = &cobra.Command{
	Use:   "influxd",
	Short: "influxdata platform",
	Run:   platformF,
}

func platformF(cmd *cobra.Command, args []string) {
	ctx := context.Background()
	// Create top level logger
	logger := influxlogger.New(os.Stdout)

	reg := prom.NewRegistry()
	reg.MustRegister(prometheus.NewGoCollector())
	reg.WithLogger(logger)

	c := bolt.NewClient()
	c.Path = boltPath
	c.WithLogger(logger)

	if err := c.Open(ctx); err != nil {
		logger.Error("failed opening bolt", zap.Error(err))
		os.Exit(1)
	}
	defer c.Close()

	var authSvc platform.AuthorizationService
	{
		authSvc = c
	}

	var bucketSvc platform.BucketService
	{
		bucketSvc = c
	}

	var orgSvc platform.OrganizationService
	{
		orgSvc = c
	}

	var userSvc platform.UserService
	{
		userSvc = c
	}

	var userResourceSvc platform.UserResourceMappingService
	{
		userResourceSvc = c
	}

	var dashboardSvc platform.DashboardService
	{
		dashboardSvc = c
	}

	var viewSvc platform.ViewService
	{
		viewSvc = c
	}

	var sourceSvc platform.SourceService
	{
		sourceSvc = c
	}

	var macroSvc platform.MacroService
	{
		macroSvc = c
	}

	var basicAuthSvc platform.BasicAuthService
	{
		basicAuthSvc = c
	}

	var sessionSvc platform.SessionService
	{
		sessionSvc = c
	}

	var onboardingSvc platform.OnboardingService = c

	var storageQueryService query.ProxyQueryService
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
			os.Exit(1)
		}

		pointsWriter = engine

		service, err := readservice.NewProxyQueryService(
			engine, bucketSvc, orgSvc, logger.With(zap.String("service", "storage-reads")))
		if err != nil {
			logger.Error("failed to create query service", zap.Error(err))
			os.Exit(1)
		}

		storageQueryService = service
	}

	var queryService query.QueryService = storageQueryService.(query.ProxyQueryServiceBridge).QueryService
	var taskSvc platform.TaskService
	{
		boltStore, err := taskbolt.New(c.DB(), "tasks")
		if err != nil {
			logger.Fatal("failed opening task bolt", zap.Error(err))
		}

		executor := taskexecutor.NewQueryServiceExecutor(logger.With(zap.String("svc", "task-executor")), queryService, boltStore)

		// TODO(lh): Replace NopLogWriter with real log writer
		scheduler := taskbackend.NewScheduler(boltStore, executor, taskbackend.NopLogWriter{}, time.Now().UTC().Unix(), taskbackend.WithTicker(ctx, time.Second), taskbackend.WithLogger(logger))
		scheduler.Start(ctx)

		// TODO(lh): Replace NopLogReader with real log reader
		taskSvc = task.PlatformAdapter(coordinator.New(logger.With(zap.String("svc", "task-coordinator")), scheduler, boltStore), taskbackend.NopLogReader{})
		// TODO(lh): Add in `taskSvc = task.NewValidator(taskSvc)` once we have Authentication coming in the context.
		// see issue #563
	}

	var scraperTargetSvc platform.ScraperTargetStoreService = c

	chronografSvc, err := server.NewServiceV2(ctx, c.DB())
	if err != nil {
		logger.Error("failed creating chronograf service", zap.Error(err))
		os.Exit(1)
	}

	errc := make(chan error)

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGTERM, os.Interrupt)

	// NATS streaming server
	natsServer := nats.NewServer(nats.Config{FilestoreDir: natsPath})
	if err := natsServer.Open(); err != nil {
		logger.Error("failed to start nats streaming server", zap.Error(err))
		os.Exit(1)
	}

	publisher := nats.NewAsyncPublisher("nats-publisher")
	if err := publisher.Open(); err != nil {
		logger.Error("failed to connect to streaming server", zap.Error(err))
		os.Exit(1)
	}

	// TODO(jm): this is an example of using a subscriber to consume from the channel. It should be removed.
	subscriber := nats.NewQueueSubscriber("nats-subscriber")
	if err := subscriber.Open(); err != nil {
		logger.Error("failed to connect to streaming server", zap.Error(err))
		os.Exit(1)
	}

	scraperScheduler, err := gather.NewScheduler(10, logger, scraperTargetSvc, publisher, subscriber, 0, 0)
	if err != nil {
		logger.Error("failed to create scraper subscriber", zap.Error(err))
		os.Exit(1)
	}
	go func() {
		errc <- scraperScheduler.Run(ctx)
	}()

	httpServer := &nethttp.Server{
		Addr: httpBindAddress,
	}

	handlerConfig := &http.APIBackend{
		Logger:                     logger,
		NewBucketService:           source.NewBucketService,
		NewQueryService:            source.NewQueryService,
		PointsWriter:               pointsWriter,
		AuthorizationService:       authSvc,
		BucketService:              bucketSvc,
		SessionService:             sessionSvc,
		UserService:                userSvc,
		OrganizationService:        orgSvc,
		UserResourceMappingService: userResourceSvc,
		DashboardService:           dashboardSvc,
		ViewService:                viewSvc,
		SourceService:              sourceSvc,
		MacroService:               macroSvc,
		BasicAuthService:           basicAuthSvc,
		OnboardingService:          onboardingSvc,
		ProxyQueryService:          storageQueryService,
		TaskService:                taskSvc,
		ScraperTargetStoreService:  scraperTargetSvc,
		ChronografService:          chronografSvc,
	}

	// HTTP server
	go func() {
		platformHandler := http.NewPlatformHandler(handlerConfig)
		reg.MustRegister(platformHandler.PrometheusCollectors()...)

		h := http.NewHandlerFromRegistry("platform", reg)
		h.Handler = platformHandler

		httpServer.Handler = h
		logger.Info("Listening", zap.String("transport", "http"), zap.String("addr", httpBindAddress))
		errc <- httpServer.ListenAndServe()
	}()

	select {
	case <-sigs:
	case err := <-errc:
		logger.Fatal("unable to start platform", zap.Error(err))
	}

	cctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	httpServer.Shutdown(cctx)
}

// Execute executes the idped command
func Execute() {
	if err := platformCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

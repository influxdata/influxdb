package main

import (
	"context"
	"fmt"
	"io"
	nethttp "net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"os/user"
	"path/filepath"
	"runtime"
	"syscall"
	"time"

	"go.uber.org/zap"

	"github.com/influxdata/flux/control"
	"github.com/influxdata/flux/execute"
	influxlogger "github.com/influxdata/influxdb/logger"
	"github.com/influxdata/platform"
	"github.com/influxdata/platform/bolt"
	"github.com/influxdata/platform/chronograf/server"
	"github.com/influxdata/platform/gather"
	"github.com/influxdata/platform/http"
	"github.com/influxdata/platform/kit/prom"
	"github.com/influxdata/platform/nats"
	"github.com/influxdata/platform/query"
	_ "github.com/influxdata/platform/query/builtin"
	pcontrol "github.com/influxdata/platform/query/control"
	"github.com/influxdata/platform/source"
	"github.com/influxdata/platform/task"
	taskbackend "github.com/influxdata/platform/task/backend"
	taskbolt "github.com/influxdata/platform/task/backend/bolt"
	"github.com/influxdata/platform/task/backend/coordinator"
	taskexecutor "github.com/influxdata/platform/task/backend/executor"
	pzap "github.com/influxdata/platform/zap"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
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
	walPath           string
	developerMode     bool
)

func influxDir() (string, error) {
	var dir string
	// By default, store meta and data files in current users home directory
	u, err := user.Current()
	if err == nil {
		dir = u.HomeDir
	} else if os.Getenv("HOME") != "" {
		dir = os.Getenv("HOME")
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

	platformCmd.Flags().StringVar(&boltPath, "bolt-path", "influxd.bolt", "path to boltdb database")
	viper.BindEnv("BOLT_PATH")
	if h := viper.GetString("BOLT_PATH"); h != "" {
		boltPath = h
	}

	platformCmd.Flags().BoolVar(&developerMode, "developer-mode", false, "serve assets from the local filesystem in developer mode")
	viper.BindEnv("DEV_MODE")
	if h := viper.GetBool("DEV_MODE"); h {
		developerMode = h
	}

	dir, err := influxDir()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to determine influx directory: %v", err)
		os.Exit(1)
	}

	platformCmd.Flags().StringVar(&walPath, "wal-path", filepath.Join(dir, "wal"), "path to persistent WAL files")
	viper.BindEnv("WAL_PATH")
	if h := viper.GetString("WAL_PATH"); h != "" {
		walPath = h
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

	var cellSvc platform.ViewService
	{
		cellSvc = c
	}

	var sourceSvc platform.SourceService
	{
		sourceSvc = c
	}

	var macroSvc platform.MacroService
	{
		macroSvc = c
	}

	var onboardingSvc platform.OnboardingService = c

	var queryService query.QueryService
	{
		// TODO(lh): this is temporary until query endpoint is added here.
		config := control.Config{
			ExecutorDependencies: make(execute.Dependencies),
			ConcurrencyQuota:     runtime.NumCPU() * 2,
			MemoryBytesQuota:     0,
			Verbose:              false,
		}

		queryService = query.QueryServiceBridge{
			AsyncQueryService: pcontrol.New(config),
		}
	}

	var taskSvc platform.TaskService
	{
		boltStore, err := taskbolt.New(c.DB(), "tasks")
		if err != nil {
			logger.Fatal("failed opening task bolt", zap.Error(err))
		}

		executor := taskexecutor.NewQueryServiceExecutor(logger, queryService, boltStore)

		// TODO(lh): Replace NopLogWriter with real log writer
		scheduler := taskbackend.NewScheduler(boltStore, executor, taskbackend.NopLogWriter{}, time.Now().UTC().Unix())
		scheduler.Start(context.Background())

		// TODO(lh): Replace NopLogReader with real log reader
		taskSvc = task.PlatformAdapter(coordinator.New(scheduler, boltStore), taskbackend.NopLogReader{})
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
	natsServer := nats.NewServer(nats.Config{FilestoreDir: walPath})
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

	if err := subscriber.Subscribe(IngressSubject, IngressGroup, &nats.LogHandler{Logger: logger}); err != nil {
		logger.Error("failed to create nats subscriber", zap.Error(err))
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

	// HTTP server
	go func() {
		bucketHandler := http.NewBucketHandler()
		bucketHandler.BucketService = bucketSvc

		orgHandler := http.NewOrgHandler()
		orgHandler.OrganizationService = orgSvc
		orgHandler.BucketService = bucketSvc
		orgHandler.UserResourceMappingService = userResourceSvc

		userHandler := http.NewUserHandler()
		userHandler.UserService = userSvc

		dashboardHandler := http.NewDashboardHandler()
		dashboardHandler.DashboardService = dashboardSvc

		cellHandler := http.NewViewHandler()
		cellHandler.ViewService = cellSvc

		macroHandler := http.NewMacroHandler()
		macroHandler.MacroService = macroSvc

		authHandler := http.NewAuthorizationHandler()
		authHandler.AuthorizationService = authSvc
		authHandler.Logger = logger.With(zap.String("handler", "auth"))

		assetHandler := http.NewAssetHandler()
		assetHandler.Develop = developerMode
		fluxLangHandler := http.NewFluxLangHandler()

		sourceHandler := http.NewSourceHandler()
		sourceHandler.SourceService = sourceSvc
		sourceHandler.NewBucketService = source.NewBucketService
		sourceHandler.NewQueryService = source.NewQueryService

		setupHandler := http.NewSetupHandler()
		setupHandler.OnboardingService = onboardingSvc

		taskHandler := http.NewTaskHandler(logger)
		taskHandler.TaskService = taskSvc

		publishFn := func(r io.Reader) error {
			return publisher.Publish(IngressSubject, r)
		}

		writeHandler := http.NewWriteHandler(publishFn)
		writeHandler.AuthorizationService = authSvc
		writeHandler.OrganizationService = orgSvc
		writeHandler.BucketService = bucketSvc
		writeHandler.Logger = logger.With(zap.String("handler", "write"))

		queryHandler := http.NewFluxHandler()
		queryHandler.AuthorizationService = authSvc
		queryHandler.OrganizationService = orgSvc
		queryHandler.Logger = logger.With(zap.String("handler", "query"))
		queryHandler.ProxyQueryService = pzap.NewProxyQueryService(queryHandler.Logger)

		// TODO(desa): what to do about idpe.
		chronografHandler := http.NewChronografHandler(chronografSvc)

		platformHandler := &http.PlatformHandler{
			BucketHandler:        bucketHandler,
			OrgHandler:           orgHandler,
			UserHandler:          userHandler,
			AuthorizationHandler: authHandler,
			DashboardHandler:     dashboardHandler,
			AssetHandler:         assetHandler,
			FluxLangHandler:      fluxLangHandler,
			ChronografHandler:    chronografHandler,
			SourceHandler:        sourceHandler,
			TaskHandler:          taskHandler,
			ViewHandler:          cellHandler,
			MacroHandler:         macroHandler,
			QueryHandler:         queryHandler,
			WriteHandler:         writeHandler,
			SetupHandler:         setupHandler,
		}
		reg.MustRegister(platformHandler.PrometheusCollectors()...)

		h := http.NewHandlerFromRegistry("platform", reg)
		h.Handler = platformHandler

		httpServer.Handler = h
		logger.Info("listening", zap.String("transport", "http"), zap.String("addr", httpBindAddress))
		errc <- httpServer.ListenAndServe()
	}()

	select {
	case <-sigs:
	case err := <-errc:
		logger.Fatal("unable to start platform", zap.Error(err))
	}

	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	httpServer.Shutdown(ctx)
}

// Execute executes the idped command
func Execute() {
	if err := platformCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

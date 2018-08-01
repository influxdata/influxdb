package main

import (
	"context"
	"fmt"
	nethttp "net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	"go.uber.org/zap"

	influxlogger "github.com/influxdata/influxdb/logger"
	"github.com/influxdata/platform"
	"github.com/influxdata/platform/bolt"
	"github.com/influxdata/platform/chronograf/server"
	"github.com/influxdata/platform/http"
	"github.com/influxdata/platform/kit/prom"
	"github.com/influxdata/platform/query"
	_ "github.com/influxdata/platform/query/builtin"
	"github.com/influxdata/platform/query/control"
	"github.com/influxdata/platform/query/execute"
	"github.com/influxdata/platform/task"
	taskbackend "github.com/influxdata/platform/task/backend"
	taskbolt "github.com/influxdata/platform/task/backend/bolt"
	"github.com/influxdata/platform/task/backend/coordinator"
	taskexecutor "github.com/influxdata/platform/task/backend/executor"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

func main() {
	Execute()
}

var (
	httpBindAddress   string
	authorizationPath string
	boltPath          string
)

func init() {
	viper.SetEnvPrefix("INFLUX")

	platformCmd.Flags().StringVar(&httpBindAddress, "http-bind-address", ":9999", "bind address for the rest http api")
	viper.BindEnv("HTTP_BIND_ADDRESS")
	if h := viper.GetString("HTTP_BIND_ADDRESS"); h != "" {
		httpBindAddress = h
	}

	platformCmd.Flags().StringVar(&authorizationPath, "authorizationPath", "", "path to a bootstrap token")
	viper.BindEnv("TOKEN_PATH")
	if h := viper.GetString("TOKEN_PATH"); h != "" {
		authorizationPath = h
	}

	platformCmd.Flags().StringVar(&boltPath, "bolt-path", "idpdb.bolt", "path to boltdb database")
	viper.BindEnv("BOLT_PATH")
	if h := viper.GetString("BOLT_PATH"); h != "" {
		boltPath = h
	}
}

var platformCmd = &cobra.Command{
	Use:   "idpd",
	Short: "influxdata platform",
	Run:   platformF,
}

func platformF(cmd *cobra.Command, args []string) {
	// Create top level logger
	logger := influxlogger.New(os.Stdout)

	reg := prom.NewRegistry()
	reg.MustRegister(prometheus.NewGoCollector())
	reg.WithLogger(logger)

	c := bolt.NewClient()
	c.Path = boltPath

	if err := c.Open(context.TODO()); err != nil {
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

	var dashboardSvc platform.DashboardService
	{
		dashboardSvc = c
	}

	var sourceSvc platform.SourceService
	{
		sourceSvc = c
	}

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
			AsyncQueryService: control.New(config),
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

		// TODO(lh): Replace NopLogReader with real log reader
		taskSvc = task.PlatformAdapter(coordinator.New(scheduler, boltStore), taskbackend.NopLogReader{})
	}

	chronografSvc, err := server.NewServiceV2(context.TODO(), c.DB())
	if err != nil {
		logger.Error("failed creating chronograf service", zap.Error(err))
		os.Exit(1)
	}

	errc := make(chan error)

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGTERM)

	httpServer := &nethttp.Server{
		Addr: httpBindAddress,
	}

	// HTTP server
	go func() {
		bucketHandler := http.NewBucketHandler()
		bucketHandler.BucketService = bucketSvc

		orgHandler := http.NewOrgHandler()
		orgHandler.OrganizationService = orgSvc

		userHandler := http.NewUserHandler()
		userHandler.UserService = userSvc

		dashboardHandler := http.NewDashboardHandler()
		dashboardHandler.DashboardService = dashboardSvc

		authHandler := http.NewAuthorizationHandler()
		authHandler.AuthorizationService = authSvc
		authHandler.Logger = logger.With(zap.String("handler", "auth"))

		assetHandler := http.NewAssetHandler()
		fluxLangHandler := http.NewFluxLangHandler()

		sourceHandler := http.NewSourceHandler()
		sourceHandler.SourceService = sourceSvc

		taskHandler := http.NewTaskHandler()
		taskHandler.TaskService = taskSvc

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

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
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

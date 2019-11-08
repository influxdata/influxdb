package launcher

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	nethttp "net/http"
	_ "net/http/pprof" // needed to add pprof to our binary.
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/influxdata/flux"
	platform "github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/authorizer"
	"github.com/influxdata/influxdb/bolt"
	"github.com/influxdata/influxdb/chronograf/server"
	"github.com/influxdata/influxdb/cmd/influxd/inspect"
	"github.com/influxdata/influxdb/gather"
	"github.com/influxdata/influxdb/http"
	"github.com/influxdata/influxdb/inmem"
	"github.com/influxdata/influxdb/internal/fs"
	"github.com/influxdata/influxdb/kit/cli"
	"github.com/influxdata/influxdb/kit/prom"
	"github.com/influxdata/influxdb/kit/signals"
	"github.com/influxdata/influxdb/kit/tracing"
	"github.com/influxdata/influxdb/kv"
	influxlogger "github.com/influxdata/influxdb/logger"
	"github.com/influxdata/influxdb/nats"
	"github.com/influxdata/influxdb/pkger"
	infprom "github.com/influxdata/influxdb/prometheus"
	"github.com/influxdata/influxdb/query"
	"github.com/influxdata/influxdb/query/control"
	"github.com/influxdata/influxdb/query/stdlib/influxdata/influxdb"
	"github.com/influxdata/influxdb/snowflake"
	"github.com/influxdata/influxdb/source"
	"github.com/influxdata/influxdb/storage"
	"github.com/influxdata/influxdb/storage/reads"
	"github.com/influxdata/influxdb/storage/readservice"
	taskbackend "github.com/influxdata/influxdb/task/backend"
	"github.com/influxdata/influxdb/task/backend/coordinator"
	taskexecutor "github.com/influxdata/influxdb/task/backend/executor"
	"github.com/influxdata/influxdb/task/backend/middleware"
	"github.com/influxdata/influxdb/task/backend/scheduler"
	"github.com/influxdata/influxdb/telemetry"
	_ "github.com/influxdata/influxdb/tsdb/tsi1" // needed for tsi1
	_ "github.com/influxdata/influxdb/tsdb/tsm1" // needed for tsm1
	"github.com/influxdata/influxdb/vault"
	pzap "github.com/influxdata/influxdb/zap"
	opentracing "github.com/opentracing/opentracing-go"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/spf13/cobra"
	jaegerconfig "github.com/uber/jaeger-client-go/config"
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
	// JaegerTracing enables tracing via the Jaeger client library
	JaegerTracing = "jaeger"
)

func NewCommand() *cobra.Command {
	l := NewLauncher()
	cmd := &cobra.Command{
		Use:   "run",
		Short: "Start the influxd server (default)",
		Run: func(cmd *cobra.Command, args []string) {
			// exit with SIGINT and SIGTERM
			ctx := context.Background()
			ctx = signals.WithStandardSignals(ctx)

			if err := l.run(ctx); err != nil {
				fmt.Fprintln(os.Stderr, err)
				os.Exit(1)
			} else if !l.Running() {
				os.Exit(1)
			}

			var wg sync.WaitGroup
			if !l.ReportingDisabled() {
				reporter := telemetry.NewReporter(l.Registry())
				reporter.Interval = 8 * time.Hour
				reporter.Logger = l.Logger()
				wg.Add(1)
				go func() {
					defer wg.Done()
					reporter.Report(ctx)
				}()
			}

			<-ctx.Done()

			// Attempt clean shutdown.
			ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
			defer cancel()
			l.Shutdown(ctx)
			wg.Wait()
		},
	}

	buildLauncherCommand(l, cmd)

	return cmd
}

var vaultConfig vault.Config

func buildLauncherCommand(l *Launcher, cmd *cobra.Command) {
	dir, err := fs.InfluxDir()
	if err != nil {
		panic(fmt.Errorf("failed to determine influx directory: %v", err))
	}

	opts := []cli.Opt{
		{
			DestP:   &l.logLevel,
			Flag:    "log-level",
			Default: zapcore.InfoLevel.String(),
			Desc:    "supported log levels are debug, info, and error",
		},
		{
			DestP:   &l.tracingType,
			Flag:    "tracing-type",
			Default: "",
			Desc:    fmt.Sprintf("supported tracing types are %s, %s", LogTracing, JaegerTracing),
		},
		{
			DestP:   &l.httpBindAddress,
			Flag:    "http-bind-address",
			Default: ":9999",
			Desc:    "bind address for the REST HTTP API",
		},
		{
			DestP:   &l.boltPath,
			Flag:    "bolt-path",
			Default: filepath.Join(dir, "influxd.bolt"),
			Desc:    "path to boltdb database",
		},
		{
			DestP: &l.assetsPath,
			Flag:  "assets-path",
			Desc:  "override default assets by serving from a specific directory (developer mode)",
		},
		{
			DestP:   &l.storeType,
			Flag:    "store",
			Default: "bolt",
			Desc:    "backing store for REST resources (bolt or memory)",
		},
		{
			DestP:   &l.testing,
			Flag:    "e2e-testing",
			Default: false,
			Desc:    "add /debug/flush endpoint to clear stores; used for end-to-end tests",
		},
		{
			DestP:   &l.enginePath,
			Flag:    "engine-path",
			Default: filepath.Join(dir, "engine"),
			Desc:    "path to persistent engine files",
		},
		{
			DestP:   &l.secretStore,
			Flag:    "secret-store",
			Default: "bolt",
			Desc:    "data store for secrets (bolt or vault)",
		},
		{
			DestP:   &l.reportingDisabled,
			Flag:    "reporting-disabled",
			Default: false,
			Desc:    "disable sending telemetry data to https://telemetry.influxdata.com every 8 hours",
		},
		{
			DestP:   &l.sessionLength,
			Flag:    "session-length",
			Default: 60, // 60 minutes
			Desc:    "ttl in minutes for newly created sessions",
		},
		{
			DestP:   &l.sessionRenewDisabled,
			Flag:    "session-renew-disabled",
			Default: false,
			Desc:    "disables automatically extending session ttl on request",
		},
		{
			DestP: &vaultConfig.Address,
			Flag:  "vault-addr",
			Desc:  "address of the Vault server expressed as a URL and port, for example: https://127.0.0.1:8200/.",
		},
		{
			DestP: &vaultConfig.ClientTimeout,
			Flag:  "vault-client-timeout",
			Desc:  "timeout variable. The default value is 60s.",
		},
		{
			DestP: &vaultConfig.MaxRetries,
			Flag:  "vault-max-retries",
			Desc:  "maximum number of retries when a 5xx error code is encountered. The default is 2, for three total attempts. Set this to 0 or less to disable retrying.",
		},
		{
			DestP: &vaultConfig.CACert,
			Flag:  "vault-cacert",
			Desc:  "path to a PEM-encoded CA certificate file on the local disk. This file is used to verify the Vault server's SSL certificate. This environment variable takes precedence over VAULT_CAPATH.",
		},
		{
			DestP: &vaultConfig.CAPath,
			Flag:  "vault-capath",
			Desc:  "path to a directory of PEM-encoded CA certificate files on the local disk. These certificates are used to verify the Vault server's SSL certificate.",
		},
		{
			DestP: &vaultConfig.ClientCert,
			Flag:  "vault-client-cert",
			Desc:  "path to a PEM-encoded client certificate on the local disk. This file is used for TLS communication with the Vault server.",
		},
		{
			DestP: &vaultConfig.ClientKey,
			Flag:  "vault-client-key",
			Desc:  "path to an unencrypted, PEM-encoded private key on disk which corresponds to the matching client certificate.",
		},
		{
			DestP: &vaultConfig.InsecureSkipVerify,
			Flag:  "vault-skip-verify",
			Desc:  "do not verify Vault's presented certificate before communicating with it. Setting this variable is not recommended and voids Vault's security model.",
		},
		{
			DestP: &vaultConfig.TLSServerName,
			Flag:  "vault-tls-server-name",
			Desc:  "name to use as the SNI host when connecting via TLS.",
		},
		{
			DestP: &vaultConfig.Token,
			Flag:  "vault-token",
			Desc:  "vault authentication token",
		},
		{
			DestP:   &l.httpTlsCert,
			Flag:    "tls-cert",
			Default: "",
			Desc:    "TLS certificate for HTTPs",
		},
		{
			DestP:   &l.httpTlsKey,
			Flag:    "tls-key",
			Default: "",
			Desc:    "TLS key for HTTPs",
		},
		{
			DestP:   &l.EnableNewScheduler,
			Flag:    "feature-enable-new-scheduler",
			Default: false,
			Desc:    "feature flag that enables using the new treescheduler",
		},
	}

	cli.BindOptions(cmd, opts)
	cmd.AddCommand(inspect.NewCommand())

}

// Launcher represents the main program execution.
type Launcher struct {
	wg      sync.WaitGroup
	cancel  func()
	running bool

	storeType            string
	assetsPath           string
	testing              bool
	sessionLength        int // in minutes
	sessionRenewDisabled bool

	logLevel          string
	tracingType       string
	reportingDisabled bool

	httpBindAddress string
	boltPath        string
	enginePath      string
	secretStore     string

	boltClient    *bolt.Client
	kvService     *kv.Service
	engine        *storage.Engine
	StorageConfig storage.Config

	queryController *control.Controller

	httpPort    int
	httpServer  *nethttp.Server
	httpTlsCert string
	httpTlsKey  string

	natsServer *nats.Server
	natsPort   int

	EnableNewScheduler bool
	scheduler          *taskbackend.TickScheduler
	treeScheduler      *scheduler.TreeScheduler
	taskControlService taskbackend.TaskControlService

	jaegerTracerCloser io.Closer
	logger             *zap.Logger
	reg                *prom.Registry

	Stdin      io.Reader
	Stdout     io.Writer
	Stderr     io.Writer
	apibackend *http.APIBackend
}

// NewLauncher returns a new instance of Launcher connected to standard in/out/err.
func NewLauncher() *Launcher {
	return &Launcher{
		Stdin:         os.Stdin,
		Stdout:        os.Stdout,
		Stderr:        os.Stderr,
		StorageConfig: storage.NewConfig(),
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

// URL returns the URL to connect to the HTTP server.
func (m *Launcher) URL() string {
	return fmt.Sprintf("http://127.0.0.1:%d", m.httpPort)
}

// NatsURL returns the URL to connection to the NATS server.
func (m *Launcher) NatsURL() string {
	return fmt.Sprintf("http://127.0.0.1:%d", m.natsPort)
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
	if m.EnableNewScheduler {
		m.treeScheduler.Stop()
	} else {
		m.scheduler.Stop()
	}

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

	if m.jaegerTracerCloser != nil {
		if err := m.jaegerTracerCloser.Close(); err != nil {
			m.logger.Warn("failed to closer Jaeger tracer", zap.Error(err))
		}
	}

	m.logger.Sync()
}

// Cancel executes the context cancel on the program. Used for testing.
func (m *Launcher) Cancel() { m.cancel() }

// Run executes the program with the given CLI arguments.
func (m *Launcher) Run(ctx context.Context, args ...string) error {
	cmd := &cobra.Command{
		Use:   "run",
		Short: "Start the influxd server (default)",
		RunE: func(cmd *cobra.Command, args []string) error {
			return m.run(ctx)
		},
	}

	buildLauncherCommand(m, cmd)

	cmd.SetArgs(args)
	return cmd.Execute()
}

func (m *Launcher) run(ctx context.Context) (err error) {
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

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

	info := platform.GetBuildInfo()
	m.logger.Info("Welcome to InfluxDB",
		zap.String("version", info.Version),
		zap.String("commit", info.Commit),
		zap.String("build_date", info.Date),
	)

	switch m.tracingType {
	case LogTracing:
		m.logger.Info("tracing via zap logging")
		tracer := new(pzap.Tracer)
		tracer.Logger = m.logger
		tracer.IDGenerator = snowflake.NewIDGenerator()
		opentracing.SetGlobalTracer(tracer)

	case JaegerTracing:
		m.logger.Info("tracing via Jaeger")
		cfg, err := jaegerconfig.FromEnv()
		if err != nil {
			m.logger.Error("failed to get Jaeger client config from environment variables", zap.Error(err))
			break
		}
		tracer, closer, err := cfg.NewTracer()
		if err != nil {
			m.logger.Error("failed to instantiate Jaeger tracer", zap.Error(err))
			break
		}
		opentracing.SetGlobalTracer(tracer)
		m.jaegerTracerCloser = closer
	}

	m.boltClient = bolt.NewClient()
	m.boltClient.Path = m.boltPath
	m.boltClient.WithLogger(m.logger.With(zap.String("service", "bolt")))

	if err := m.boltClient.Open(ctx); err != nil {
		m.logger.Error("failed opening bolt", zap.Error(err))
		return err
	}

	serviceConfig := kv.ServiceConfig{
		SessionLength: time.Duration(m.sessionLength) * time.Minute,
	}

	var flusher http.Flusher
	switch m.storeType {
	case BoltStore:
		store := bolt.NewKVStore(m.boltPath)
		store.WithDB(m.boltClient.DB())
		m.kvService = kv.NewService(store, serviceConfig)
		if m.testing {
			flusher = store
		}
	case MemoryStore:
		store := inmem.NewKVStore()
		m.kvService = kv.NewService(store, serviceConfig)
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
		infprom.NewInfluxCollector(m.boltClient, info),
	)
	m.reg.WithLogger(m.logger)
	m.reg.MustRegister(m.boltClient)

	var (
		orgSvc                  platform.OrganizationService             = m.kvService
		authSvc                 platform.AuthorizationService            = m.kvService
		userSvc                 platform.UserService                     = m.kvService
		variableSvc             platform.VariableService                 = m.kvService
		bucketSvc               platform.BucketService                   = m.kvService
		sourceSvc               platform.SourceService                   = m.kvService
		sessionSvc              platform.SessionService                  = m.kvService
		passwdsSvc              platform.PasswordsService                = m.kvService
		dashboardSvc            platform.DashboardService                = m.kvService
		dashboardLogSvc         platform.DashboardOperationLogService    = m.kvService
		userLogSvc              platform.UserOperationLogService         = m.kvService
		bucketLogSvc            platform.BucketOperationLogService       = m.kvService
		orgLogSvc               platform.OrganizationOperationLogService = m.kvService
		onboardingSvc           platform.OnboardingService               = m.kvService
		scraperTargetSvc        platform.ScraperTargetStoreService       = m.kvService
		telegrafSvc             platform.TelegrafConfigStore             = m.kvService
		userResourceSvc         platform.UserResourceMappingService      = m.kvService
		labelSvc                platform.LabelService                    = m.kvService
		secretSvc               platform.SecretService                   = m.kvService
		lookupSvc               platform.LookupService                   = m.kvService
		notificationEndpointSvc platform.NotificationEndpointService     = m.kvService
	)

	switch m.secretStore {
	case "bolt":
		// If it is bolt, then we already set it above.
	case "vault":
		// The vault secret service is configured using the standard vault environment variables.
		// https://www.vaultproject.io/docs/commands/index.html#environment-variables
		svc, err := vault.NewSecretService(vault.WithConfig(vaultConfig))
		if err != nil {
			m.logger.Error("failed initializing vault secret service", zap.Error(err))
			return err
		}
		secretSvc = svc
	default:
		err := fmt.Errorf("unknown secret service %q, expected \"bolt\" or \"vault\"", m.secretStore)
		m.logger.Error("failed setting secret service", zap.Error(err))
		return err
	}

	chronografSvc, err := server.NewServiceV2(ctx, m.boltClient.DB())
	if err != nil {
		m.logger.Error("failed creating chronograf service", zap.Error(err))
		return err
	}

	var deleteService platform.DeleteService
	var pointsWriter storage.PointsWriter
	{
		m.engine = storage.NewEngine(m.enginePath, m.StorageConfig, storage.WithRetentionEnforcer(bucketSvc))
		m.engine.WithLogger(m.logger)

		if err := m.engine.Open(ctx); err != nil {
			m.logger.Error("failed to open engine", zap.Error(err))
			return err
		}
		// The Engine's metrics must be registered after it opens.
		m.reg.MustRegister(m.engine.PrometheusCollectors()...)

		pointsWriter = m.engine
		deleteService = m.engine

		// TODO(cwolff): Figure out a good default per-query memory limit:
		//   https://github.com/influxdata/influxdb/issues/13642
		const (
			concurrencyQuota         = 10
			memoryBytesQuotaPerQuery = math.MaxInt64
			QueueSize                = 10
		)

		cc := control.Config{
			ConcurrencyQuota:         concurrencyQuota,
			MemoryBytesQuotaPerQuery: int64(memoryBytesQuotaPerQuery),
			QueueSize:                QueueSize,
			Logger:                   m.logger.With(zap.String("service", "storage-reads")),
		}

		authBucketSvc := authorizer.NewBucketService(bucketSvc)
		authOrgSvc := authorizer.NewOrgService(orgSvc)
		authSecretSvc := authorizer.NewSecretService(secretSvc)
		reader := reads.NewReader(readservice.NewStore(m.engine))
		deps, err := influxdb.NewDependencies(reader, m.engine, authBucketSvc, authOrgSvc, authSecretSvc, cc.MetricLabelKeys)
		if err != nil {
			m.logger.Error("Failed to get query controller dependencies", zap.Error(err))
			return err
		}
		cc.ExecutorDependencies = []flux.Dependency{deps}

		c, err := control.New(cc)
		if err != nil {
			m.logger.Error("Failed to create query controller", zap.Error(err))
			return err
		}
		m.queryController = c
		m.reg.MustRegister(m.queryController.PrometheusCollectors()...)
	}

	var storageQueryService = readservice.NewProxyQueryService(m.queryController)
	var taskSvc platform.TaskService
	{
		// create the task stack:
		// validation(coordinator(analyticalstore(kv.Service)))
		combinedTaskService := taskbackend.NewAnalyticalStorage(m.logger.With(zap.String("service", "task-analytical-store")), m.kvService, m.kvService, m.kvService, pointsWriter, query.QueryServiceBridge{AsyncQueryService: m.queryController})
		if m.EnableNewScheduler {
			executor, executorMetrics := taskexecutor.NewExecutor(
				m.logger.With(zap.String("service", "task-executor")),
				query.QueryServiceBridge{AsyncQueryService: m.queryController},
				authSvc,
				combinedTaskService,
				combinedTaskService,
			)
			m.reg.MustRegister(executorMetrics.PrometheusCollectors()...)
			schLogger := m.logger.With(zap.String("service", "task-scheduler"))

			sch, sm, err := scheduler.NewScheduler(
				executor,
				taskbackend.NewSchedulableTaskService(m.kvService),
				scheduler.WithOnErrorFn(func(ctx context.Context, taskID scheduler.ID, scheduledAt time.Time, err error) {
					schLogger.Info(
						"error in scheduler run",
						zap.String("taskID", platform.ID(taskID).String()),
						zap.Time("scheduledAt", scheduledAt),
						zap.Error(err))
				}),
			)
			if err != nil {
				m.logger.Fatal("could not start task scheduler", zap.Error(err))
			}
			m.treeScheduler = sch
			m.reg.MustRegister(sm.PrometheusCollectors()...)
			coordLogger := m.logger.With(zap.String("service", "task-coordinator"))
			taskCoord := coordinator.NewCoordinator(
				coordLogger,
				sch,
				executor)

			taskSvc = middleware.New(combinedTaskService, taskCoord)
			m.taskControlService = combinedTaskService
			if err := taskbackend.TaskNotifyCoordinatorOfExisting(
				ctx,
				taskSvc,
				combinedTaskService,
				taskCoord,
				func(ctx context.Context, taskID platform.ID, runID platform.ID) error {
					_, err := executor.ResumeCurrentRun(ctx, taskID, runID)
					return err
				},
				coordLogger); err != nil {
				m.logger.Error("failed to resume existing tasks", zap.Error(err))
			}
		} else {

			// define the executor and build analytical storage middleware
			executor := taskexecutor.NewAsyncQueryServiceExecutor(m.logger.With(zap.String("service", "task-executor")), m.queryController, authSvc, combinedTaskService)

			// create the scheduler
			m.scheduler = taskbackend.NewScheduler(combinedTaskService, executor, time.Now().UTC().Unix(), taskbackend.WithTicker(ctx, 100*time.Millisecond), taskbackend.WithLogger(m.logger))
			m.scheduler.Start(ctx)
			m.reg.MustRegister(m.scheduler.PrometheusCollectors()...)

			logger := m.logger.With(zap.String("service", "task-coordinator"))
			coordinator := coordinator.New(logger, m.scheduler)

			// resume existing task claims from task service
			if err := taskbackend.NotifyCoordinatorOfExisting(ctx, combinedTaskService, coordinator, logger); err != nil {
				logger.Error("failed to resume existing tasks", zap.Error(err))
			}

			taskSvc = middleware.New(combinedTaskService, coordinator)
			taskSvc = authorizer.NewTaskService(m.logger.With(zap.String("service", "task-authz-validator")), taskSvc)
			m.taskControlService = combinedTaskService
		}

	}

	var checkSvc platform.CheckService
	{
		coordinator := coordinator.New(m.logger, m.scheduler)
		checkSvc = middleware.NewCheckService(m.kvService, m.kvService, coordinator)
	}

	var notificationRuleSvc platform.NotificationRuleStore
	{
		coordinator := coordinator.New(m.logger, m.scheduler)
		notificationRuleSvc = middleware.NewNotificationRuleStore(m.kvService, m.kvService, coordinator)
	}

	// NATS streaming server
	natsOpts := nats.NewDefaultServerOptions()
	nextPort := int64(4222)

	// Welcome to ghetto land. It doesn't seem possible to tell NATS to initialise
	// a random port. In some integration-style tests, this launcher gets initialised
	// multiple times, and sometimes the port from the previous instantiation is
	// still open.
	//
	// This atrocity checks if the port is free, and if it's not, moves on to the
	// next one.
	var total int
	for {
		l, err := net.Listen("tcp", fmt.Sprintf(":%d", nextPort))
		if err == nil {
			if err := l.Close(); err != nil {
				return err
			}
			break
		}
		time.Sleep(time.Second)
		nextPort++
		total++
		if total > 50 {
			return errors.New("unable to find free port for Nats server")
		}
	}
	natsOpts.Port = int(nextPort)
	m.natsServer = nats.NewServer(&natsOpts)
	m.natsPort = int(nextPort)

	if err := m.natsServer.Open(); err != nil {
		m.logger.Error("failed to start nats streaming server", zap.Error(err))
		return err
	}

	publisher := nats.NewAsyncPublisher(fmt.Sprintf("nats-publisher-%d", m.natsPort), m.NatsURL())
	if err := publisher.Open(); err != nil {
		m.logger.Error("failed to connect to streaming server", zap.Error(err))
		return err
	}

	// TODO(jm): this is an example of using a subscriber to consume from the channel. It should be removed.
	subscriber := nats.NewQueueSubscriber(fmt.Sprintf("nats-subscriber-%d", m.natsPort), m.NatsURL())
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
		HTTPErrorHandler:     http.ErrorHandler(0),
		Logger:               m.logger,
		SessionRenewDisabled: m.sessionRenewDisabled,
		NewBucketService:     source.NewBucketService,
		NewQueryService:      source.NewQueryService,
		PointsWriter:         pointsWriter,
		DeleteService:        deleteService,
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
		NotificationRuleStore:           notificationRuleSvc,
		NotificationEndpointService:     notificationEndpointSvc,
		CheckService:                    checkSvc,
		ScraperTargetStoreService:       scraperTargetSvc,
		ChronografService:               chronografSvc,
		SecretService:                   secretSvc,
		LookupService:                   lookupSvc,
		DocumentService:                 m.kvService,
		OrgLookupService:                m.kvService,
		WriteEventRecorder:              infprom.NewEventRecorder("write"),
		QueryEventRecorder:              infprom.NewEventRecorder("query"),
	}

	m.reg.MustRegister(m.apibackend.PrometheusCollectors()...)

	var pkgSVC pkger.SVC
	{
		b := m.apibackend
		pkgSVC = pkger.NewService(
			pkger.WithLogger(m.logger.With(zap.String("service", "pkger"))),
			pkger.WithBucketSVC(b.BucketService),
			pkger.WithDashboardSVC(b.DashboardService),
			pkger.WithLabelSVC(b.LabelService),
			pkger.WithVariableSVC(b.VariableService),
		)
	}

	var pkgHTTPServer *http.HandlerPkg
	{
		pkgHTTPServer = http.NewHandlerPkg(m.apibackend.HTTPErrorHandler, pkgSVC)
	}

	// HTTP server
	platformHandler := http.NewPlatformHandler(m.apibackend, http.WithResourceHandler(pkgHTTPServer))
	m.reg.MustRegister(platformHandler.PrometheusCollectors()...)

	h := http.NewHandlerFromRegistry("platform", m.reg)
	h.Handler = platformHandler
	httpLogger := m.logger.With(zap.String("service", "http"))
	if logconf.Level == zap.DebugLevel {
		h.Handler = http.LoggingMW(httpLogger)(h.Handler)
	}
	h.Logger = httpLogger

	m.httpServer.Handler = h
	// If we are in testing mode we allow all data to be flushed and removed.
	if m.testing {
		m.httpServer.Handler = http.DebugFlush(ctx, h, flusher)
	}

	ln, err := net.Listen("tcp", m.httpBindAddress)
	if err != nil {
		httpLogger.Error("failed http listener", zap.Error(err))
		httpLogger.Info("Stopping")
		return err
	}

	var cer tls.Certificate
	transport := "http"

	if m.httpTlsCert != "" && m.httpTlsKey != "" {
		var err error
		cer, err = tls.LoadX509KeyPair(m.httpTlsCert, m.httpTlsKey)

		if err != nil {
			httpLogger.Error("failed to load x509 key pair", zap.Error(err))
			httpLogger.Info("Stopping")
			return err
		}
		transport = "https"

		m.httpServer.TLSConfig = &tls.Config{}
	}

	if addr, ok := ln.Addr().(*net.TCPAddr); ok {
		m.httpPort = addr.Port
	}

	m.wg.Add(1)
	go func(logger *zap.Logger) {
		defer m.wg.Done()
		logger.Info("Listening", zap.String("transport", transport), zap.String("addr", m.httpBindAddress), zap.Int("port", m.httpPort))

		if cer.Certificate != nil {
			if err := m.httpServer.ServeTLS(ln, m.httpTlsCert, m.httpTlsKey); err != nethttp.ErrServerClosed {
				logger.Error("failed https service", zap.Error(err))
			}
		} else {
			if err := m.httpServer.Serve(ln); err != nethttp.ErrServerClosed {
				logger.Error("failed http service", zap.Error(err))
			}
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
func (m *Launcher) QueryController() *control.Controller {
	return m.queryController
}

// BucketService returns the internal bucket service.
func (m *Launcher) BucketService() platform.BucketService {
	return m.apibackend.BucketService
}

// UserService returns the internal user service.
func (m *Launcher) UserService() platform.UserService {
	return m.apibackend.UserService
}

// AuthorizationService returns the internal authorization service.
func (m *Launcher) AuthorizationService() platform.AuthorizationService {
	return m.apibackend.AuthorizationService
}

// SecretService returns the internal secret service.
func (m *Launcher) SecretService() platform.SecretService {
	return m.apibackend.SecretService
}

// TaskService returns the internal task service.
func (m *Launcher) TaskService() platform.TaskService {
	return m.apibackend.TaskService
}

// TaskControlService returns the internal store service.
func (m *Launcher) TaskControlService() taskbackend.TaskControlService {
	return m.taskControlService
}

// TaskScheduler returns the internal scheduler service.
// TODO(docmerlin): remove this when we delete the old scheduler
func (m *Launcher) TaskScheduler() taskbackend.Scheduler {
	return m.scheduler
}

// KeyValueService returns the internal key-value service.
func (m *Launcher) KeyValueService() *kv.Service {
	return m.kvService
}

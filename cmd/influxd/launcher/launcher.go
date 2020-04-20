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
	platform "github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/authorization"
	"github.com/influxdata/influxdb/v2/authorizer"
	"github.com/influxdata/influxdb/v2/bolt"
	"github.com/influxdata/influxdb/v2/chronograf/server"
	"github.com/influxdata/influxdb/v2/cmd/influxd/inspect"
	"github.com/influxdata/influxdb/v2/dbrp"
	"github.com/influxdata/influxdb/v2/endpoints"
	"github.com/influxdata/influxdb/v2/gather"
	"github.com/influxdata/influxdb/v2/http"
	"github.com/influxdata/influxdb/v2/inmem"
	"github.com/influxdata/influxdb/v2/internal/fs"
	"github.com/influxdata/influxdb/v2/kit/cli"
	"github.com/influxdata/influxdb/v2/kit/feature"
	overrideflagger "github.com/influxdata/influxdb/v2/kit/feature/override"
	"github.com/influxdata/influxdb/v2/kit/prom"
	"github.com/influxdata/influxdb/v2/kit/signals"
	"github.com/influxdata/influxdb/v2/kit/tracing"
	kithttp "github.com/influxdata/influxdb/v2/kit/transport/http"
	"github.com/influxdata/influxdb/v2/kv"
	influxlogger "github.com/influxdata/influxdb/v2/logger"
	"github.com/influxdata/influxdb/v2/nats"
	"github.com/influxdata/influxdb/v2/pkger"
	infprom "github.com/influxdata/influxdb/v2/prometheus"
	"github.com/influxdata/influxdb/v2/query"
	"github.com/influxdata/influxdb/v2/query/control"
	"github.com/influxdata/influxdb/v2/query/stdlib/influxdata/influxdb"
	"github.com/influxdata/influxdb/v2/snowflake"
	"github.com/influxdata/influxdb/v2/source"
	"github.com/influxdata/influxdb/v2/storage"
	storageflux "github.com/influxdata/influxdb/v2/storage/flux"
	"github.com/influxdata/influxdb/v2/storage/readservice"
	taskbackend "github.com/influxdata/influxdb/v2/task/backend"
	"github.com/influxdata/influxdb/v2/task/backend/coordinator"
	"github.com/influxdata/influxdb/v2/task/backend/executor"
	"github.com/influxdata/influxdb/v2/task/backend/middleware"
	"github.com/influxdata/influxdb/v2/task/backend/scheduler"
	"github.com/influxdata/influxdb/v2/telemetry"
	"github.com/influxdata/influxdb/v2/tenant"
	_ "github.com/influxdata/influxdb/v2/tsdb/tsi1" // needed for tsi1
	_ "github.com/influxdata/influxdb/v2/tsdb/tsm1" // needed for tsm1
	"github.com/influxdata/influxdb/v2/vault"
	pzap "github.com/influxdata/influxdb/v2/zap"
	"github.com/opentracing/opentracing-go"
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

// NewCommand creates the command to run influxdb.
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
				reporter := telemetry.NewReporter(l.Log(), l.Registry())
				reporter.Interval = 8 * time.Hour
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
			Default: filepath.Join(dir, bolt.DefaultFilename),
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
			DestP:   &l.httpTLSCert,
			Flag:    "tls-cert",
			Default: "",
			Desc:    "TLS certificate for HTTPs",
		},
		{
			DestP:   &l.httpTLSKey,
			Flag:    "tls-key",
			Default: "",
			Desc:    "TLS key for HTTPs",
		},
		{
			DestP:   &l.enableNewMetaStore,
			Flag:    "new-meta-store",
			Default: true,
			Desc:    "enables the new meta store",
		},
		{
			DestP:   &l.noTasks,
			Flag:    "no-tasks",
			Default: false,
			Desc:    "disables the task scheduler",
		},
		{
			DestP:   &l.concurrencyQuota,
			Flag:    "query-concurrency",
			Default: 10,
			Desc:    "the number of queries that are allowed to execute concurrently",
		},
		{
			DestP:   &l.initialMemoryBytesQuotaPerQuery,
			Flag:    "query-initial-memory-bytes",
			Default: 0,
			Desc:    "the initial number of bytes allocated for a query when it is started. If this is unset, then query-memory-bytes will be used",
		},
		{
			DestP:   &l.memoryBytesQuotaPerQuery,
			Flag:    "query-memory-bytes",
			Default: math.MaxInt64,
			Desc:    "maximum number of bytes a query is allowed to use at any given time. This must be greater or equal to query-initial-memory-bytes",
		},
		{
			DestP:   &l.maxMemoryBytes,
			Flag:    "query-max-memory-bytes",
			Default: 0,
			Desc:    "the maximum amount of memory used for queries. If this is unset, then this number is query-concurrency * query-memory-bytes",
		},
		{
			DestP:   &l.queueSize,
			Flag:    "query-queue-size",
			Default: 10,
			Desc:    "the number of queries that are allowed to be awaiting execution before new queries are rejected",
		},
		{
			DestP: &l.featureFlags,
			Flag:  "feature-flags",
			Desc:  "feature flag overrides",
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

	enableNewMetaStore bool

	featureFlags map[string]string

	// Query options.
	concurrencyQuota                int
	initialMemoryBytesQuotaPerQuery int
	memoryBytesQuotaPerQuery        int
	maxMemoryBytes                  int
	queueSize                       int

	boltClient    *bolt.Client
	kvStore       kv.Store
	kvService     *kv.Service
	engine        Engine
	StorageConfig storage.Config

	queryController *control.Controller

	httpPort    int
	httpServer  *nethttp.Server
	httpTLSCert string
	httpTLSKey  string

	natsServer *nats.Server
	natsPort   int

	noTasks            bool
	scheduler          stoppingScheduler
	executor           *executor.Executor
	taskControlService taskbackend.TaskControlService

	jaegerTracerCloser io.Closer
	log                *zap.Logger
	reg                *prom.Registry

	Stdin      io.Reader
	Stdout     io.Writer
	Stderr     io.Writer
	apibackend *http.APIBackend
}

type stoppingScheduler interface {
	scheduler.Scheduler
	Stop()
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

// Log returns the launchers logger.
func (m *Launcher) Log() *zap.Logger {
	return m.log
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
func (m *Launcher) Engine() Engine {
	return m.engine
}

// Shutdown shuts down the HTTP server and waits for all services to clean up.
func (m *Launcher) Shutdown(ctx context.Context) {
	m.httpServer.Shutdown(ctx)

	m.log.Info("Stopping", zap.String("service", "task"))

	m.scheduler.Stop()

	m.log.Info("Stopping", zap.String("service", "nats"))
	m.natsServer.Close()

	m.log.Info("Stopping", zap.String("service", "bolt"))
	if err := m.boltClient.Close(); err != nil {
		m.log.Info("Failed closing bolt", zap.Error(err))
	}

	m.log.Info("Stopping", zap.String("service", "query"))
	if err := m.queryController.Shutdown(ctx); err != nil && err != context.Canceled {
		m.log.Info("Failed closing query service", zap.Error(err))
	}

	m.log.Info("Stopping", zap.String("service", "storage-engine"))
	if err := m.engine.Close(); err != nil {
		m.log.Error("Failed to close engine", zap.Error(err))
	}

	m.wg.Wait()

	if m.jaegerTracerCloser != nil {
		if err := m.jaegerTracerCloser.Close(); err != nil {
			m.log.Warn("Failed to closer Jaeger tracer", zap.Error(err))
		}
	}

	m.log.Sync()
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
	m.log, err = logconf.New(m.Stdout)
	if err != nil {
		return err
	}

	info := platform.GetBuildInfo()
	m.log.Info("Welcome to InfluxDB",
		zap.String("version", info.Version),
		zap.String("commit", info.Commit),
		zap.String("build_date", info.Date),
	)

	switch m.tracingType {
	case LogTracing:
		m.log.Info("Tracing via zap logging")
		tracer := pzap.NewTracer(m.log, snowflake.NewIDGenerator())
		opentracing.SetGlobalTracer(tracer)

	case JaegerTracing:
		m.log.Info("Tracing via Jaeger")
		cfg, err := jaegerconfig.FromEnv()
		if err != nil {
			m.log.Error("Failed to get Jaeger client config from environment variables", zap.Error(err))
			break
		}
		tracer, closer, err := cfg.NewTracer()
		if err != nil {
			m.log.Error("Failed to instantiate Jaeger tracer", zap.Error(err))
			break
		}
		opentracing.SetGlobalTracer(tracer)
		m.jaegerTracerCloser = closer
	}

	m.boltClient = bolt.NewClient(m.log.With(zap.String("service", "bolt")))
	m.boltClient.Path = m.boltPath

	if err := m.boltClient.Open(ctx); err != nil {
		m.log.Error("Failed opening bolt", zap.Error(err))
		return err
	}

	serviceConfig := kv.ServiceConfig{
		SessionLength: time.Duration(m.sessionLength) * time.Minute,
	}

	flushers := flushers{}
	switch m.storeType {
	case BoltStore:
		store := bolt.NewKVStore(m.log.With(zap.String("service", "kvstore-bolt")), m.boltPath)
		store.WithDB(m.boltClient.DB())
		m.kvStore = store
		m.kvService = kv.NewService(m.log.With(zap.String("store", "kv")), store, serviceConfig)
		if m.testing {
			flushers = append(flushers, store)
		}
	case MemoryStore:
		store := inmem.NewKVStore()
		m.kvStore = store
		m.kvService = kv.NewService(m.log.With(zap.String("store", "kv")), store, serviceConfig)
		if m.testing {
			flushers = append(flushers, store)
		}
	default:
		err := fmt.Errorf("unknown store type %s; expected bolt or memory", m.storeType)
		m.log.Error("Failed opening bolt", zap.Error(err))
		return err
	}

	if err := m.kvService.Initialize(ctx); err != nil {
		m.log.Error("Failed to initialize kv service", zap.Error(err))
		return err
	}

	m.reg = prom.NewRegistry(m.log.With(zap.String("service", "prom_registry")))
	m.reg.MustRegister(
		prometheus.NewGoCollector(),
		infprom.NewInfluxCollector(m.boltClient, info),
	)
	m.reg.MustRegister(m.boltClient)

	var (
		userSvc         platform.UserService                = m.kvService
		orgSvc          platform.OrganizationService        = m.kvService
		userResourceSvc platform.UserResourceMappingService = m.kvService
		bucketSvc       platform.BucketService              = m.kvService
		passwdsSvc      platform.PasswordsService           = m.kvService

		authSvc                   platform.AuthorizationService            = m.kvService
		variableSvc               platform.VariableService                 = m.kvService
		sourceSvc                 platform.SourceService                   = m.kvService
		sessionSvc                platform.SessionService                  = m.kvService
		dashboardSvc              platform.DashboardService                = m.kvService
		dashboardLogSvc           platform.DashboardOperationLogService    = m.kvService
		userLogSvc                platform.UserOperationLogService         = m.kvService
		bucketLogSvc              platform.BucketOperationLogService       = m.kvService
		orgLogSvc                 platform.OrganizationOperationLogService = m.kvService
		scraperTargetSvc          platform.ScraperTargetStoreService       = m.kvService
		telegrafSvc               platform.TelegrafConfigStore             = m.kvService
		labelSvc                  platform.LabelService                    = m.kvService
		secretSvc                 platform.SecretService                   = m.kvService
		lookupSvc                 platform.LookupService                   = m.kvService
		notificationEndpointStore platform.NotificationEndpointService     = m.kvService
	)

	store, err := tenant.NewStore(m.kvStore)
	if err != nil {
		m.log.Error("Failed creating new meta store", zap.Error(err))
		return err
	}

	if m.enableNewMetaStore {
		ts := tenant.NewService(store)
		userSvc = tenant.NewUserLogger(m.log.With(zap.String("store", "new")), tenant.NewUserMetrics(m.reg, ts, tenant.WithSuffix("new")))
		orgSvc = tenant.NewOrgLogger(m.log.With(zap.String("store", "new")), tenant.NewOrgMetrics(m.reg, ts, tenant.WithSuffix("new")))
		userResourceSvc = tenant.NewURMLogger(m.log.With(zap.String("store", "new")), tenant.NewUrmMetrics(m.reg, ts, tenant.WithSuffix("new")))
		bucketSvc = tenant.NewBucketLogger(m.log.With(zap.String("store", "new")), tenant.NewBucketMetrics(m.reg, ts, tenant.WithSuffix("new")))
		passwdsSvc = tenant.NewPasswordLogger(m.log.With(zap.String("store", "new")), tenant.NewPasswordMetrics(m.reg, ts, tenant.WithSuffix("new")))
	}

	switch m.secretStore {
	case "bolt":
		// If it is bolt, then we already set it above.
	case "vault":
		// The vault secret service is configured using the standard vault environment variables.
		// https://www.vaultproject.io/docs/commands/index.html#environment-variables
		svc, err := vault.NewSecretService(vault.WithConfig(vaultConfig))
		if err != nil {
			m.log.Error("Failed initializing vault secret service", zap.Error(err))
			return err
		}
		secretSvc = svc
	default:
		err := fmt.Errorf("unknown secret service %q, expected \"bolt\" or \"vault\"", m.secretStore)
		m.log.Error("Failed setting secret service", zap.Error(err))
		return err
	}

	chronografSvc, err := server.NewServiceV2(ctx, m.boltClient.DB())
	if err != nil {
		m.log.Error("Failed creating chronograf service", zap.Error(err))
		return err
	}

	if m.testing {
		// the testing engine will write/read into a temporary directory
		engine := NewTemporaryEngine(m.StorageConfig, storage.WithRetentionEnforcer(bucketSvc))
		flushers = append(flushers, engine)
		m.engine = engine
	} else {
		m.engine = storage.NewEngine(m.enginePath, m.StorageConfig, storage.WithRetentionEnforcer(bucketSvc))
	}
	m.engine.WithLogger(m.log)
	if err := m.engine.Open(ctx); err != nil {
		m.log.Error("Failed to open engine", zap.Error(err))
		return err
	}
	// The Engine's metrics must be registered after it opens.
	m.reg.MustRegister(m.engine.PrometheusCollectors()...)

	var (
		deleteService platform.DeleteService = m.engine
		pointsWriter  storage.PointsWriter   = m.engine
		backupService platform.BackupService = m.engine
	)

	deps, err := influxdb.NewDependencies(
		storageflux.NewReader(readservice.NewStore(m.engine)),
		m.engine,
		authorizer.NewBucketService(bucketSvc, userResourceSvc),
		authorizer.NewOrgService(orgSvc),
		authorizer.NewSecretService(secretSvc),
		nil,
	)
	if err != nil {
		m.log.Error("Failed to get query controller dependencies", zap.Error(err))
		return err
	}

	m.queryController, err = control.New(control.Config{
		ConcurrencyQuota:                m.concurrencyQuota,
		InitialMemoryBytesQuotaPerQuery: int64(m.initialMemoryBytesQuotaPerQuery),
		MemoryBytesQuotaPerQuery:        int64(m.memoryBytesQuotaPerQuery),
		MaxMemoryBytes:                  int64(m.maxMemoryBytes),
		QueueSize:                       m.queueSize,
		Logger:                          m.log.With(zap.String("service", "storage-reads")),
		ExecutorDependencies:            []flux.Dependency{deps},
	})
	if err != nil {
		m.log.Error("Failed to create query controller", zap.Error(err))
		return err
	}

	m.reg.MustRegister(m.queryController.PrometheusCollectors()...)

	var storageQueryService = readservice.NewProxyQueryService(m.queryController)
	var taskSvc platform.TaskService
	{
		// create the task stack
		combinedTaskService := taskbackend.NewAnalyticalStorage(m.log.With(zap.String("service", "task-analytical-store")), m.kvService, m.kvService, m.kvService, pointsWriter, query.QueryServiceBridge{AsyncQueryService: m.queryController})

		executor, executorMetrics := executor.NewExecutor(
			m.log.With(zap.String("service", "task-executor")),
			query.QueryServiceBridge{AsyncQueryService: m.queryController},
			authSvc,
			combinedTaskService,
			combinedTaskService,
		)
		m.executor = executor
		m.reg.MustRegister(executorMetrics.PrometheusCollectors()...)
		schLogger := m.log.With(zap.String("service", "task-scheduler"))

		var sch stoppingScheduler = &scheduler.NoopScheduler{}
		if !m.noTasks {
			var (
				sm  *scheduler.SchedulerMetrics
				err error
			)
			sch, sm, err = scheduler.NewScheduler(
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
				m.log.Fatal("could not start task scheduler", zap.Error(err))
			}
			m.reg.MustRegister(sm.PrometheusCollectors()...)
		}

		m.scheduler = sch

		coordLogger := m.log.With(zap.String("service", "task-coordinator"))
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
			m.log.Error("Failed to resume existing tasks", zap.Error(err))
		}
	}

	dbrpSvc, err := dbrp.NewService(ctx, authorizer.NewBucketService(bucketSvc, userResourceSvc), m.kvStore)
	if err != nil {
		return err
	}

	dbrpSvc = dbrp.NewAuthorizedService(dbrpSvc)

	var checkSvc platform.CheckService
	{
		coordinator := coordinator.NewCoordinator(m.log, m.scheduler, m.executor)
		checkSvc = middleware.NewCheckService(m.kvService, m.kvService, coordinator)
	}

	var notificationRuleSvc platform.NotificationRuleStore
	{
		coordinator := coordinator.NewCoordinator(m.log, m.scheduler, m.executor)
		notificationRuleSvc = middleware.NewNotificationRuleStore(m.kvService, m.kvService, coordinator)
	}

	// NATS streaming server
	natsOpts := nats.NewDefaultServerOptions()

	// Welcome to ghetto land. It doesn't seem possible to tell NATS to initialise
	// a random port. In some integration-style tests, this launcher gets initialised
	// multiple times, and sometimes the port from the previous instantiation is
	// still open.
	//
	// This atrocity checks if the port is free, and if it's not, moves on to the
	// next one. This best-effort approach may still fail occasionally when, for example,
	// two tests race on isAddressPortAvailable.
	var total int
	for {
		portAvailable, err := isAddressPortAvailable(natsOpts.Host, natsOpts.Port)
		if err != nil {
			return err
		}
		if portAvailable && natsOpts.Host == "" {
			// Double-check localhost to accommodate tests
			time.Sleep(100 * time.Millisecond)
			portAvailable, err = isAddressPortAvailable("localhost", natsOpts.Port)
			if err != nil {
				return err
			}
		}
		if portAvailable {
			break
		}

		time.Sleep(100 * time.Millisecond)
		natsOpts.Port++
		total++
		if total > 50 {
			return errors.New("unable to find free port for Nats server")
		}
	}
	m.natsServer = nats.NewServer(&natsOpts)
	m.natsPort = natsOpts.Port

	if err := m.natsServer.Open(); err != nil {
		m.log.Error("Failed to start nats streaming server", zap.Error(err))
		return err
	}

	publisher := nats.NewAsyncPublisher(m.log, fmt.Sprintf("nats-publisher-%d", m.natsPort), m.NatsURL())
	if err := publisher.Open(); err != nil {
		m.log.Error("Failed to connect to streaming server", zap.Error(err))
		return err
	}

	// TODO(jm): this is an example of using a subscriber to consume from the channel. It should be removed.
	subscriber := nats.NewQueueSubscriber(fmt.Sprintf("nats-subscriber-%d", m.natsPort), m.NatsURL())
	if err := subscriber.Open(); err != nil {
		m.log.Error("Failed to connect to streaming server", zap.Error(err))
		return err
	}

	subscriber.Subscribe(gather.MetricsSubject, "metrics", gather.NewRecorderHandler(m.log, gather.PointWriter{Writer: pointsWriter}))
	scraperScheduler, err := gather.NewScheduler(m.log, 10, scraperTargetSvc, publisher, subscriber, 10*time.Second, 30*time.Second)
	if err != nil {
		m.log.Error("Failed to create scraper subscriber", zap.Error(err))
		return err
	}

	m.wg.Add(1)
	go func(log *zap.Logger) {
		defer m.wg.Done()
		log = log.With(zap.String("service", "scraper"))
		if err := scraperScheduler.Run(ctx); err != nil {
			log.Error("Failed scraper service", zap.Error(err))
		}
		log.Info("Stopping")
	}(m.log)

	m.httpServer = &nethttp.Server{
		Addr: m.httpBindAddress,
	}

	flagger := feature.DefaultFlagger()
	if len(m.featureFlags) > 0 {
		f, err := overrideflagger.Make(m.featureFlags, feature.ByKey)
		if err != nil {
			m.log.Error("Failed to configure feature flag overrides",
				zap.Error(err), zap.Any("overrides", m.featureFlags))
			return err
		}
		m.log.Info("Running with feature flag overrides", zap.Any("overrides", m.featureFlags))
		flagger = f
	}

	m.apibackend = &http.APIBackend{
		AssetsPath:           m.assetsPath,
		HTTPErrorHandler:     kithttp.ErrorHandler(0),
		Logger:               m.log,
		SessionRenewDisabled: m.sessionRenewDisabled,
		NewBucketService:     source.NewBucketService,
		NewQueryService:      source.NewQueryService,
		PointsWriter:         pointsWriter,
		DeleteService:        deleteService,
		BackupService:        backupService,
		KVBackupService:      m.kvService,
		AuthorizationService: authSvc,
		AlgoWProxy:           &http.NoopProxyHandler{},
		// Wrap the BucketService in a storage backed one that will ensure deleted buckets are removed from the storage engine.
		BucketService:                   storage.NewBucketService(bucketSvc, m.engine),
		SessionService:                  sessionSvc,
		UserService:                     userSvc,
		DBRPService:                     dbrpSvc,
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
		InfluxQLService:                 storageQueryService,
		FluxService:                     storageQueryService,
		TaskService:                     taskSvc,
		TelegrafService:                 telegrafSvc,
		NotificationRuleStore:           notificationRuleSvc,
		NotificationEndpointService:     endpoints.NewService(notificationEndpointStore, secretSvc, userResourceSvc, orgSvc),
		CheckService:                    checkSvc,
		ScraperTargetStoreService:       scraperTargetSvc,
		ChronografService:               chronografSvc,
		SecretService:                   secretSvc,
		LookupService:                   lookupSvc,
		DocumentService:                 m.kvService,
		OrgLookupService:                m.kvService,
		WriteEventRecorder:              infprom.NewEventRecorder("write"),
		QueryEventRecorder:              infprom.NewEventRecorder("query"),
		Flagger:                         flagger,
		FlagsHandler:                    feature.NewFlagsHandler(kithttp.ErrorHandler(0), feature.ByKey),
	}

	m.reg.MustRegister(m.apibackend.PrometheusCollectors()...)

	authAgent := new(authorizer.AuthAgent)

	var pkgSVC pkger.SVC
	{
		b := m.apibackend
		authedOrgSVC := authorizer.NewOrgService(b.OrganizationService)
		authedURMSVC := authorizer.NewURMService(b.OrgLookupService, b.UserResourceMappingService)
		pkgerLogger := m.log.With(zap.String("service", "pkger"))
		pkgSVC = pkger.NewService(
			pkger.WithLogger(pkgerLogger),
			pkger.WithStore(pkger.NewStoreKV(m.kvStore)),
			pkger.WithBucketSVC(authorizer.NewBucketService(b.BucketService, b.UserResourceMappingService)),
			pkger.WithCheckSVC(authorizer.NewCheckService(b.CheckService, authedURMSVC, authedOrgSVC)),
			pkger.WithDashboardSVC(authorizer.NewDashboardService(b.DashboardService)),
			pkger.WithLabelSVC(authorizer.NewLabelServiceWithOrg(b.LabelService, b.OrgLookupService)),
			pkger.WithNotificationEndpointSVC(authorizer.NewNotificationEndpointService(b.NotificationEndpointService, authedURMSVC, authedOrgSVC)),
			pkger.WithNotificationRuleSVC(authorizer.NewNotificationRuleStore(b.NotificationRuleStore, authedURMSVC, authedOrgSVC)),
			pkger.WithOrganizationService(authorizer.NewOrgService(b.OrganizationService)),
			pkger.WithSecretSVC(authorizer.NewSecretService(b.SecretService)),
			pkger.WithTaskSVC(authorizer.NewTaskService(pkgerLogger, b.TaskService)),
			pkger.WithTelegrafSVC(authorizer.NewTelegrafConfigService(b.TelegrafService, b.UserResourceMappingService)),
			pkger.WithVariableSVC(authorizer.NewVariableService(b.VariableService)),
		)
		pkgSVC = pkger.MWTracing()(pkgSVC)
		pkgSVC = pkger.MWMetrics(m.reg)(pkgSVC)
		pkgSVC = pkger.MWLogging(pkgerLogger)(pkgSVC)
		pkgSVC = pkger.MWAuth(authAgent)(pkgSVC)
	}

	var pkgHTTPServer *pkger.HTTPServer
	{
		pkgServerLogger := m.log.With(zap.String("handler", "pkger"))
		pkgHTTPServer = pkger.NewHTTPServer(pkgServerLogger, pkgSVC)
	}

	var onboardHTTPServer *tenant.OnboardHandler
	{
		onboardSvc := tenant.NewOnboardService(store, authSvc)                                            // basic service
		onboardSvc = tenant.NewAuthedOnboardSvc(onboardSvc)                                               // with auth
		onboardSvc = tenant.NewOnboardingMetrics(m.reg, onboardSvc, tenant.WithSuffix("new"))             // with metrics
		onboardSvc = tenant.NewOnboardingLogger(m.log.With(zap.String("handler", "onboard")), onboardSvc) // with logging

		onboardHTTPServer = tenant.NewHTTPOnboardHandler(m.log, onboardSvc)
	}

	// feature flagging for new authorization service
	var authHTTPServer *kithttp.FeatureHandler
	{
		ts := tenant.NewService(store) // todo (al): remove when tenant is un-flagged
		authLogger := m.log.With(zap.String("handler", "authorization"))

		oldBackend := http.NewAuthorizationBackend(authLogger, m.apibackend)
		oldBackend.AuthorizationService = authorizer.NewAuthorizationService(authSvc)
		oldHandler := http.NewAuthorizationHandler(authLogger, oldBackend)

		authStore, err := authorization.NewStore(m.kvStore)
		if err != nil {
			m.log.Error("Failed creating new authorization store", zap.Error(err))
			return err
		}
		authService := authorization.NewService(authStore, ts)
		authService = authorization.NewAuthedAuthorizationService(authService, ts)
		authService = authorization.NewAuthMetrics(m.reg, authService)
		authService = authorization.NewAuthLogger(authLogger, authService)

		newHandler := authorization.NewHTTPAuthHandler(m.log, authService, ts, lookupSvc)
		authHTTPServer = kithttp.NewFeatureHandler(feature.NewAuthPackage(), flagger, oldHandler, newHandler, newHandler.Prefix())
	}

	{
		platformHandler := http.NewPlatformHandler(m.apibackend, http.WithResourceHandler(pkgHTTPServer), http.WithResourceHandler(onboardHTTPServer), http.WithResourceHandler(authHTTPServer))

		httpLogger := m.log.With(zap.String("service", "http"))
		m.httpServer.Handler = http.NewHandlerFromRegistry(
			"platform",
			m.reg,
			http.WithLog(httpLogger),
			http.WithAPIHandler(platformHandler),
		)

		if logconf.Level == zap.DebugLevel {
			m.httpServer.Handler = http.LoggingMW(httpLogger)(m.httpServer.Handler)
		}
		// If we are in testing mode we allow all data to be flushed and removed.
		if m.testing {
			m.httpServer.Handler = http.DebugFlush(ctx, m.httpServer.Handler, flushers)
		}
	}

	ln, err := net.Listen("tcp", m.httpBindAddress)
	if err != nil {
		m.log.Error("failed http listener", zap.Error(err))
		m.log.Info("Stopping")
		return err
	}

	var cer tls.Certificate
	transport := "http"

	if m.httpTLSCert != "" && m.httpTLSKey != "" {
		var err error
		cer, err = tls.LoadX509KeyPair(m.httpTLSCert, m.httpTLSKey)

		if err != nil {
			m.log.Error("failed to load x509 key pair", zap.Error(err))
			m.log.Info("Stopping")
			return err
		}
		transport = "https"

		m.httpServer.TLSConfig = &tls.Config{}
	}

	if addr, ok := ln.Addr().(*net.TCPAddr); ok {
		m.httpPort = addr.Port
	}

	m.wg.Add(1)
	go func(log *zap.Logger) {
		defer m.wg.Done()
		log.Info("Listening", zap.String("transport", transport), zap.String("addr", m.httpBindAddress), zap.Int("port", m.httpPort))

		if cer.Certificate != nil {
			if err := m.httpServer.ServeTLS(ln, m.httpTLSCert, m.httpTLSKey); err != nethttp.ErrServerClosed {
				log.Error("Failed https service", zap.Error(err))
			}
		} else {
			if err := m.httpServer.Serve(ln); err != nethttp.ErrServerClosed {
				log.Error("Failed http service", zap.Error(err))
			}
		}
		log.Info("Stopping")
	}(m.log)

	return nil
}

// isAddressPortAvailable checks whether the address:port is available to listen,
// by using net.Listen to verify that the port opens successfully, then closes the listener.
func isAddressPortAvailable(address string, port int) (bool, error) {
	if l, err := net.Listen("tcp", fmt.Sprintf("%s:%d", address, port)); err == nil {
		if err := l.Close(); err != nil {
			return false, err
		}
		return true, nil
	}
	return false, nil
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

// UserResourceMappingService returns the internal user resource mapping service.
func (m *Launcher) UserResourceMappingService() platform.UserResourceMappingService {
	return m.apibackend.UserResourceMappingService
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

// KeyValueService returns the internal key-value service.
func (m *Launcher) KeyValueService() *kv.Service {
	return m.kvService
}

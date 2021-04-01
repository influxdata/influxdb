package launcher

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	nethttp "net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	platform2 "github.com/influxdata/influxdb/v2/kit/platform"

	"github.com/influxdata/flux"
	"github.com/influxdata/flux/dependencies/testing"
	platform "github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/authorization"
	"github.com/influxdata/influxdb/v2/authorizer"
	"github.com/influxdata/influxdb/v2/bolt"
	"github.com/influxdata/influxdb/v2/checks"
	"github.com/influxdata/influxdb/v2/chronograf/server"
	"github.com/influxdata/influxdb/v2/dashboards"
	dashboardTransport "github.com/influxdata/influxdb/v2/dashboards/transport"
	"github.com/influxdata/influxdb/v2/dbrp"
	"github.com/influxdata/influxdb/v2/gather"
	"github.com/influxdata/influxdb/v2/http"
	iqlcontrol "github.com/influxdata/influxdb/v2/influxql/control"
	iqlquery "github.com/influxdata/influxdb/v2/influxql/query"
	"github.com/influxdata/influxdb/v2/inmem"
	"github.com/influxdata/influxdb/v2/internal/resource"
	"github.com/influxdata/influxdb/v2/kit/feature"
	overrideflagger "github.com/influxdata/influxdb/v2/kit/feature/override"
	"github.com/influxdata/influxdb/v2/kit/metric"
	"github.com/influxdata/influxdb/v2/kit/prom"
	"github.com/influxdata/influxdb/v2/kit/tracing"
	kithttp "github.com/influxdata/influxdb/v2/kit/transport/http"
	"github.com/influxdata/influxdb/v2/kv"
	"github.com/influxdata/influxdb/v2/kv/migration"
	"github.com/influxdata/influxdb/v2/kv/migration/all"
	"github.com/influxdata/influxdb/v2/label"
	"github.com/influxdata/influxdb/v2/nats"
	endpointservice "github.com/influxdata/influxdb/v2/notification/endpoint/service"
	ruleservice "github.com/influxdata/influxdb/v2/notification/rule/service"
	"github.com/influxdata/influxdb/v2/pkger"
	infprom "github.com/influxdata/influxdb/v2/prometheus"
	"github.com/influxdata/influxdb/v2/query"
	"github.com/influxdata/influxdb/v2/query/control"
	"github.com/influxdata/influxdb/v2/query/fluxlang"
	"github.com/influxdata/influxdb/v2/query/stdlib/influxdata/influxdb"
	"github.com/influxdata/influxdb/v2/secret"
	"github.com/influxdata/influxdb/v2/session"
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
	telegrafservice "github.com/influxdata/influxdb/v2/telegraf/service"
	"github.com/influxdata/influxdb/v2/telemetry"
	"github.com/influxdata/influxdb/v2/tenant"
	_ "github.com/influxdata/influxdb/v2/tsdb/engine/tsm1" // needed for tsm1
	_ "github.com/influxdata/influxdb/v2/tsdb/index/tsi1"  // needed for tsi1
	authv1 "github.com/influxdata/influxdb/v2/v1/authorization"
	iqlcoordinator "github.com/influxdata/influxdb/v2/v1/coordinator"
	"github.com/influxdata/influxdb/v2/v1/services/meta"
	storage2 "github.com/influxdata/influxdb/v2/v1/services/storage"
	"github.com/influxdata/influxdb/v2/vault"
	pzap "github.com/influxdata/influxdb/v2/zap"
	"github.com/opentracing/opentracing-go"
	"github.com/prometheus/client_golang/prometheus"
	jaegerconfig "github.com/uber/jaeger-client-go/config"
	"go.uber.org/zap"
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

// Launcher represents the main program execution.
type Launcher struct {
	wg       sync.WaitGroup
	cancel   func()
	doneChan <-chan struct{}

	flagger feature.Flagger

	boltClient *bolt.Client
	kvStore    kv.SchemaStore
	kvService  *kv.Service

	// storage engine
	engine Engine

	// InfluxQL query engine
	queryController *control.Controller

	httpPort   int
	httpServer *nethttp.Server
	tlsEnabled bool

	natsServer *nats.Server
	natsPort   int

	scheduler          stoppingScheduler
	executor           *executor.Executor
	taskControlService taskbackend.TaskControlService

	jaegerTracerCloser io.Closer
	log                *zap.Logger
	reg                *prom.Registry

	apibackend *http.APIBackend
}

type stoppingScheduler interface {
	scheduler.Scheduler
	Stop()
}

// NewLauncher returns a new instance of Launcher with a no-op logger.
func NewLauncher() *Launcher {
	return &Launcher{
		log: zap.NewNop(),
	}
}

// Registry returns the prometheus metrics registry.
func (m *Launcher) Registry() *prom.Registry {
	return m.reg
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
func (m *Launcher) Shutdown(ctx context.Context) error {
	var errs []string

	if err := m.httpServer.Shutdown(ctx); err != nil {
		m.log.Error("Failed to close HTTP server", zap.Error(err))
		errs = append(errs, err.Error())
	}

	m.log.Info("Stopping", zap.String("service", "task"))

	m.scheduler.Stop()

	m.log.Info("Stopping", zap.String("service", "nats"))
	m.natsServer.Close()

	m.log.Info("Stopping", zap.String("service", "bolt"))
	if err := m.boltClient.Close(); err != nil {
		m.log.Error("Failed closing bolt", zap.Error(err))
		errs = append(errs, err.Error())
	}

	m.log.Info("Stopping", zap.String("service", "query"))
	if err := m.queryController.Shutdown(ctx); err != nil && err != context.Canceled {
		m.log.Error("Failed closing query service", zap.Error(err))
		errs = append(errs, err.Error())
	}

	m.log.Info("Stopping", zap.String("service", "storage-engine"))
	if err := m.engine.Close(); err != nil {
		m.log.Error("Failed to close engine", zap.Error(err))
		errs = append(errs, err.Error())
	}

	m.wg.Wait()

	if m.jaegerTracerCloser != nil {
		if err := m.jaegerTracerCloser.Close(); err != nil {
			m.log.Error("Failed to closer Jaeger tracer", zap.Error(err))
			errs = append(errs, err.Error())
		}
	}

	// N.B. We ignore any errors here because Sync is known to fail with EINVAL
	// when logging to Stdout on certain OS's.
	//
	// Uber made the same change within the core of the logger implementation.
	// See: https://github.com/uber-go/zap/issues/328
	_ = m.log.Sync()

	if len(errs) > 0 {
		return fmt.Errorf("failed to shut down server: [%s]", strings.Join(errs, ","))
	}
	return nil
}

func (m *Launcher) Done() <-chan struct{} {
	return m.doneChan
}

func (m *Launcher) run(ctx context.Context, opts *InfluxdOpts) (err error) {
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	ctx, m.cancel = context.WithCancel(ctx)
	m.doneChan = ctx.Done()

	info := platform.GetBuildInfo()
	m.log.Info("Welcome to InfluxDB",
		zap.String("version", info.Version),
		zap.String("commit", info.Commit),
		zap.String("build_date", info.Date),
	)

	switch opts.TracingType {
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
	m.boltClient.Path = opts.BoltPath

	if err := m.boltClient.Open(ctx); err != nil {
		m.log.Error("Failed opening bolt", zap.Error(err))
		return err
	}

	var flushers flushers
	switch opts.StoreType {
	case BoltStore:
		store := bolt.NewKVStore(m.log.With(zap.String("service", "kvstore-bolt")), opts.BoltPath)
		store.WithDB(m.boltClient.DB())
		m.kvStore = store
		if opts.Testing {
			flushers = append(flushers, store)
		}

	case MemoryStore:
		store := inmem.NewKVStore()
		m.kvStore = store
		if opts.Testing {
			flushers = append(flushers, store)
		}

	default:
		err := fmt.Errorf("unknown store type %s; expected bolt or memory", opts.StoreType)
		m.log.Error("Failed opening bolt", zap.Error(err))
		return err
	}

	migrator, err := migration.NewMigrator(
		m.log.With(zap.String("service", "migrations")),
		m.kvStore,
		all.Migrations[:]...,
	)
	if err != nil {
		m.log.Error("Failed to initialize kv migrator", zap.Error(err))
		return err
	}

	// apply migrations to metadata store
	if err := migrator.Up(ctx); err != nil {
		m.log.Error("Failed to apply migrations", zap.Error(err))
		return err
	}

	m.reg = prom.NewRegistry(m.log.With(zap.String("service", "prom_registry")))
	m.reg.MustRegister(
		prometheus.NewGoCollector(),
		infprom.NewInfluxCollector(m.boltClient, info),
	)
	m.reg.MustRegister(m.boltClient)

	tenantStore := tenant.NewStore(m.kvStore)
	ts := tenant.NewSystem(tenantStore, m.log.With(zap.String("store", "new")), m.reg, metric.WithSuffix("new"))

	serviceConfig := kv.ServiceConfig{
		FluxLanguageService: fluxlang.DefaultService,
	}

	m.kvService = kv.NewService(m.log.With(zap.String("store", "kv")), m.kvStore, ts, serviceConfig)

	var (
		opLogSvc                                              = tenant.NewOpLogService(m.kvStore, m.kvService)
		userLogSvc   platform.UserOperationLogService         = opLogSvc
		bucketLogSvc platform.BucketOperationLogService       = opLogSvc
		orgLogSvc    platform.OrganizationOperationLogService = opLogSvc
	)
	var (
		variableSvc      platform.VariableService           = m.kvService
		sourceSvc        platform.SourceService             = m.kvService
		scraperTargetSvc platform.ScraperTargetStoreService = m.kvService
	)

	var authSvc platform.AuthorizationService
	{
		authStore, err := authorization.NewStore(m.kvStore)
		if err != nil {
			m.log.Error("Failed creating new authorization store", zap.Error(err))
			return err
		}
		authSvc = authorization.NewService(authStore, ts)
	}

	secretStore, err := secret.NewStore(m.kvStore)
	if err != nil {
		m.log.Error("Failed creating new meta store", zap.Error(err))
		return err
	}

	var secretSvc platform.SecretService = secret.NewMetricService(m.reg, secret.NewLogger(m.log.With(zap.String("service", "secret")), secret.NewService(secretStore)))

	switch opts.SecretStore {
	case "bolt":
		// If it is bolt, then we already set it above.
	case "vault":
		// The vault secret service is configured using the standard vault environment variables.
		// https://www.vaultproject.io/docs/commands/index.html#environment-variables
		svc, err := vault.NewSecretService(vault.WithConfig(opts.VaultConfig))
		if err != nil {
			m.log.Error("Failed initializing vault secret service", zap.Error(err))
			return err
		}
		secretSvc = svc
	default:
		err := fmt.Errorf("unknown secret service %q, expected \"bolt\" or \"vault\"", opts.SecretStore)
		m.log.Error("Failed setting secret service", zap.Error(err))
		return err
	}

	chronografSvc, err := server.NewServiceV2(ctx, m.boltClient.DB())
	if err != nil {
		m.log.Error("Failed creating chronograf service", zap.Error(err))
		return err
	}

	metaClient := meta.NewClient(meta.NewConfig(), m.kvStore)
	if err := metaClient.Open(); err != nil {
		m.log.Error("Failed to open meta client", zap.Error(err))
		return err
	}

	if opts.Testing {
		// the testing engine will write/read into a temporary directory
		engine := NewTemporaryEngine(
			opts.StorageConfig,
			storage.WithMetaClient(metaClient),
		)
		flushers = append(flushers, engine)
		m.engine = engine
	} else {
		// check for 2.x data / state from a prior 2.x
		if err := checkForPriorVersion(ctx, m.log, opts.BoltPath, opts.EnginePath, ts.BucketService, metaClient); err != nil {
			os.Exit(1)
		}

		m.engine = storage.NewEngine(
			opts.EnginePath,
			opts.StorageConfig,
			storage.WithMetaClient(metaClient),
		)
	}
	m.engine.WithLogger(m.log)
	if err := m.engine.Open(ctx); err != nil {
		m.log.Error("Failed to open engine", zap.Error(err))
		return err
	}
	// The Engine's metrics must be registered after it opens.
	m.reg.MustRegister(m.engine.PrometheusCollectors()...)

	var (
		deleteService  platform.DeleteService  = m.engine
		pointsWriter   storage.PointsWriter    = m.engine
		backupService  platform.BackupService  = m.engine
		restoreService platform.RestoreService = m.engine
	)

	deps, err := influxdb.NewDependencies(
		storageflux.NewReader(storage2.NewStore(m.engine.TSDBStore(), m.engine.MetaClient())),
		m.engine,
		authorizer.NewBucketService(ts.BucketService),
		authorizer.NewOrgService(ts.OrganizationService),
		authorizer.NewSecretService(secretSvc),
		nil,
	)
	if err != nil {
		m.log.Error("Failed to get query controller dependencies", zap.Error(err))
		return err
	}

	dependencyList := []flux.Dependency{deps}
	if opts.Testing {
		dependencyList = append(dependencyList, testing.FrameworkConfig{})
	}

	m.queryController, err = control.New(control.Config{
		ConcurrencyQuota:                opts.ConcurrencyQuota,
		InitialMemoryBytesQuotaPerQuery: opts.InitialMemoryBytesQuotaPerQuery,
		MemoryBytesQuotaPerQuery:        opts.MemoryBytesQuotaPerQuery,
		MaxMemoryBytes:                  opts.MaxMemoryBytes,
		QueueSize:                       opts.QueueSize,
		ExecutorDependencies:            dependencyList,
	}, m.log.With(zap.String("service", "storage-reads")))
	if err != nil {
		m.log.Error("Failed to create query controller", zap.Error(err))
		return err
	}

	m.reg.MustRegister(m.queryController.PrometheusCollectors()...)

	var storageQueryService = readservice.NewProxyQueryService(m.queryController)
	var taskSvc platform.TaskService
	{
		// create the task stack
		combinedTaskService := taskbackend.NewAnalyticalStorage(
			m.log.With(zap.String("service", "task-analytical-store")),
			m.kvService,
			ts.BucketService,
			m.kvService,
			pointsWriter,
			query.QueryServiceBridge{AsyncQueryService: m.queryController},
		)

		executor, executorMetrics := executor.NewExecutor(
			m.log.With(zap.String("service", "task-executor")),
			query.QueryServiceBridge{AsyncQueryService: m.queryController},
			ts.UserService,
			combinedTaskService,
			combinedTaskService,
			executor.WithFlagger(m.flagger),
		)
		m.executor = executor
		m.reg.MustRegister(executorMetrics.PrometheusCollectors()...)
		schLogger := m.log.With(zap.String("service", "task-scheduler"))

		var sch stoppingScheduler = &scheduler.NoopScheduler{}
		if !opts.NoTasks {
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
						zap.String("taskID", platform2.ID(taskID).String()),
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
			func(ctx context.Context, taskID platform2.ID, runID platform2.ID) error {
				_, err := executor.ResumeCurrentRun(ctx, taskID, runID)
				return err
			},
			coordLogger); err != nil {
			m.log.Error("Failed to resume existing tasks", zap.Error(err))
		}
	}

	dbrpSvc := dbrp.NewAuthorizedService(dbrp.NewService(ctx, authorizer.NewBucketService(ts.BucketService), m.kvStore))

	cm := iqlcontrol.NewControllerMetrics([]string{})
	m.reg.MustRegister(cm.PrometheusCollectors()...)

	mapper := &iqlcoordinator.LocalShardMapper{
		MetaClient: metaClient,
		TSDBStore:  m.engine.TSDBStore(),
		DBRP:       dbrpSvc,
	}

	m.log.Info("Configuring InfluxQL statement executor (zeros indicate unlimited).",
		zap.Int("max_select_point", opts.CoordinatorConfig.MaxSelectPointN),
		zap.Int("max_select_series", opts.CoordinatorConfig.MaxSelectSeriesN),
		zap.Int("max_select_buckets", opts.CoordinatorConfig.MaxSelectBucketsN))

	qe := iqlquery.NewExecutor(m.log, cm)
	se := &iqlcoordinator.StatementExecutor{
		MetaClient:        metaClient,
		TSDBStore:         m.engine.TSDBStore(),
		ShardMapper:       mapper,
		DBRP:              dbrpSvc,
		MaxSelectPointN:   opts.CoordinatorConfig.MaxSelectPointN,
		MaxSelectSeriesN:  opts.CoordinatorConfig.MaxSelectSeriesN,
		MaxSelectBucketsN: opts.CoordinatorConfig.MaxSelectBucketsN,
	}
	qe.StatementExecutor = se
	qe.StatementNormalizer = se

	var checkSvc platform.CheckService
	{
		coordinator := coordinator.NewCoordinator(m.log, m.scheduler, m.executor)
		checkSvc = checks.NewService(m.log.With(zap.String("svc", "checks")), m.kvStore, ts.OrganizationService, m.kvService)
		checkSvc = middleware.NewCheckService(checkSvc, m.kvService, coordinator)
	}

	var notificationEndpointSvc platform.NotificationEndpointService
	{
		notificationEndpointSvc = endpointservice.New(endpointservice.NewStore(m.kvStore), secretSvc)
	}

	var notificationRuleSvc platform.NotificationRuleStore
	{
		coordinator := coordinator.NewCoordinator(m.log, m.scheduler, m.executor)
		notificationRuleSvc, err = ruleservice.New(m.log, m.kvStore, m.kvService, ts.OrganizationService, notificationEndpointSvc)
		if err != nil {
			return err
		}

		// tasks service notification middleware which keeps task service up to date
		// with persisted changes to notification rules.
		notificationRuleSvc = middleware.NewNotificationRuleStore(notificationRuleSvc, m.kvService, coordinator)
	}

	var telegrafSvc platform.TelegrafConfigStore
	{
		telegrafSvc = telegrafservice.New(m.kvStore)
	}

	// NATS streaming server
	natsOpts := nats.NewDefaultServerOptions()
	natsOpts.Port = opts.NatsPort
	natsOpts.MaxPayload = opts.NatsMaxPayloadBytes
	m.natsServer = nats.NewServer(&natsOpts)
	if err := m.natsServer.Open(); err != nil {
		m.log.Error("Failed to start nats streaming server", zap.Error(err))
		return err
	}
	// If a random port was used, the opts will be updated to include the selected value.
	m.natsPort = natsOpts.Port
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

	if m.flagger == nil {
		m.flagger = feature.DefaultFlagger()
		if len(opts.FeatureFlags) > 0 {
			f, err := overrideflagger.Make(opts.FeatureFlags, feature.ByKey)
			if err != nil {
				m.log.Error("Failed to configure feature flag overrides",
					zap.Error(err), zap.Any("overrides", opts.FeatureFlags))
				return err
			}
			m.log.Info("Running with feature flag overrides", zap.Any("overrides", opts.FeatureFlags))
			m.flagger = f
		}
	}

	var sessionSvc platform.SessionService
	{
		sessionSvc = session.NewService(
			session.NewStorage(inmem.NewSessionStore()),
			ts.UserService,
			ts.UserResourceMappingService,
			authSvc,
			session.WithSessionLength(time.Duration(opts.SessionLength)*time.Minute),
		)
		sessionSvc = session.NewSessionMetrics(m.reg, sessionSvc)
		sessionSvc = session.NewSessionLogger(m.log.With(zap.String("service", "session")), sessionSvc)
	}

	var labelSvc platform.LabelService
	{
		labelsStore, err := label.NewStore(m.kvStore)
		if err != nil {
			m.log.Error("Failed creating new labels store", zap.Error(err))
			return err
		}
		labelSvc = label.NewService(labelsStore)
	}

	ts.BucketService = storage.NewBucketService(m.log, ts.BucketService, m.engine)
	ts.BucketService = dbrp.NewBucketService(m.log, ts.BucketService, dbrpSvc)

	onboardingLogger := m.log.With(zap.String("handler", "onboard"))
	onboardOpts := []tenant.OnboardServiceOptionFn{tenant.WithOnboardingLogger(onboardingLogger)}
	if opts.TestingAlwaysAllowSetup {
		onboardOpts = append(onboardOpts, tenant.WithAlwaysAllowInitialUser())
	}

	onboardSvc := tenant.NewOnboardService(ts, authSvc, onboardOpts...)                   // basic service
	onboardSvc = tenant.NewAuthedOnboardSvc(onboardSvc)                                   // with auth
	onboardSvc = tenant.NewOnboardingMetrics(m.reg, onboardSvc, metric.WithSuffix("new")) // with metrics
	onboardSvc = tenant.NewOnboardingLogger(onboardingLogger, onboardSvc)                 // with logging

	var (
		authorizerV1 platform.AuthorizerV1
		passwordV1   platform.PasswordsService
		authSvcV1    *authv1.Service
	)
	{
		authStore, err := authv1.NewStore(m.kvStore)
		if err != nil {
			m.log.Error("Failed creating new authorization store", zap.Error(err))
			return err
		}

		authSvcV1 = authv1.NewService(authStore, ts)
		passwordV1 = authv1.NewCachingPasswordsService(authSvcV1)

		authorizerV1 = &authv1.Authorizer{
			AuthV1:   authSvcV1,
			AuthV2:   authSvc,
			Comparer: passwordV1,
			User:     ts,
		}
	}

	var (
		dashboardSvc    platform.DashboardService
		dashboardLogSvc platform.DashboardOperationLogService
	)
	{
		dashboardService := dashboards.NewService(m.kvStore, m.kvService)
		dashboardSvc = dashboardService
		dashboardLogSvc = dashboardService
	}

	// resourceResolver is a deprecated type which combines the lookups
	// of multiple resources into one type, used to resolve the resources
	// associated org ID or name . It is a stop-gap while we move this
	// behaviour off of *kv.Service to aid in reducing the coupling on this type.
	resourceResolver := &resource.Resolver{
		AuthorizationFinder:        authSvc,
		BucketFinder:               ts.BucketService,
		OrganizationFinder:         ts.OrganizationService,
		DashboardFinder:            dashboardSvc,
		SourceFinder:               sourceSvc,
		TaskFinder:                 taskSvc,
		TelegrafConfigFinder:       telegrafSvc,
		VariableFinder:             variableSvc,
		TargetFinder:               scraperTargetSvc,
		CheckFinder:                checkSvc,
		NotificationEndpointFinder: notificationEndpointSvc,
		NotificationRuleFinder:     notificationRuleSvc,
	}

	m.apibackend = &http.APIBackend{
		AssetsPath:           opts.AssetsPath,
		HTTPErrorHandler:     kithttp.ErrorHandler(0),
		Logger:               m.log,
		SessionRenewDisabled: opts.SessionRenewDisabled,
		NewBucketService:     source.NewBucketService,
		NewQueryService:      source.NewQueryService,
		PointsWriter: &storage.LoggingPointsWriter{
			Underlying:    pointsWriter,
			BucketFinder:  ts.BucketService,
			LogBucketName: platform.MonitoringSystemBucketName,
		},
		DeleteService:        deleteService,
		BackupService:        backupService,
		RestoreService:       restoreService,
		AuthorizationService: authSvc,
		AuthorizerV1:         authorizerV1,
		AlgoWProxy:           &http.NoopProxyHandler{},
		// Wrap the BucketService in a storage backed one that will ensure deleted buckets are removed from the storage engine.
		BucketService:                   ts.BucketService,
		SessionService:                  sessionSvc,
		UserService:                     ts.UserService,
		OnboardingService:               onboardSvc,
		DBRPService:                     dbrpSvc,
		OrganizationService:             ts.OrganizationService,
		UserResourceMappingService:      ts.UserResourceMappingService,
		LabelService:                    labelSvc,
		DashboardService:                dashboardSvc,
		DashboardOperationLogService:    dashboardLogSvc,
		BucketOperationLogService:       bucketLogSvc,
		UserOperationLogService:         userLogSvc,
		OrganizationOperationLogService: orgLogSvc,
		SourceService:                   sourceSvc,
		VariableService:                 variableSvc,
		PasswordsService:                ts.PasswordsService,
		InfluxQLService:                 storageQueryService,
		InfluxqldService:                iqlquery.NewProxyExecutor(m.log, qe),
		FluxService:                     storageQueryService,
		FluxLanguageService:             fluxlang.DefaultService,
		TaskService:                     taskSvc,
		TelegrafService:                 telegrafSvc,
		NotificationRuleStore:           notificationRuleSvc,
		NotificationEndpointService:     notificationEndpointSvc,
		CheckService:                    checkSvc,
		ScraperTargetStoreService:       scraperTargetSvc,
		ChronografService:               chronografSvc,
		SecretService:                   secretSvc,
		LookupService:                   resourceResolver,
		DocumentService:                 m.kvService,
		OrgLookupService:                resourceResolver,
		WriteEventRecorder:              infprom.NewEventRecorder("write"),
		QueryEventRecorder:              infprom.NewEventRecorder("query"),
		Flagger:                         m.flagger,
		FlagsHandler:                    feature.NewFlagsHandler(kithttp.ErrorHandler(0), feature.ByKey),
	}

	m.reg.MustRegister(m.apibackend.PrometheusCollectors()...)

	authAgent := new(authorizer.AuthAgent)

	var pkgSVC pkger.SVC
	{
		b := m.apibackend
		authedOrgSVC := authorizer.NewOrgService(b.OrganizationService)
		authedUrmSVC := authorizer.NewURMService(b.OrgLookupService, b.UserResourceMappingService)
		pkgerLogger := m.log.With(zap.String("service", "pkger"))
		pkgSVC = pkger.NewService(
			pkger.WithLogger(pkgerLogger),
			pkger.WithStore(pkger.NewStoreKV(m.kvStore)),
			pkger.WithBucketSVC(authorizer.NewBucketService(b.BucketService)),
			pkger.WithCheckSVC(authorizer.NewCheckService(b.CheckService, authedUrmSVC, authedOrgSVC)),
			pkger.WithDashboardSVC(authorizer.NewDashboardService(b.DashboardService)),
			pkger.WithLabelSVC(label.NewAuthedLabelService(labelSvc, b.OrgLookupService)),
			pkger.WithNotificationEndpointSVC(authorizer.NewNotificationEndpointService(b.NotificationEndpointService, authedUrmSVC, authedOrgSVC)),
			pkger.WithNotificationRuleSVC(authorizer.NewNotificationRuleStore(b.NotificationRuleStore, authedUrmSVC, authedOrgSVC)),
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

	var stacksHTTPServer *pkger.HTTPServerStacks
	{
		tLogger := m.log.With(zap.String("handler", "stacks"))
		stacksHTTPServer = pkger.NewHTTPServerStacks(tLogger, pkgSVC)
	}

	var templatesHTTPServer *pkger.HTTPServerTemplates
	{
		tLogger := m.log.With(zap.String("handler", "templates"))
		templatesHTTPServer = pkger.NewHTTPServerTemplates(tLogger, pkgSVC)
	}

	userHTTPServer := ts.NewUserHTTPHandler(m.log)
	onboardHTTPServer := tenant.NewHTTPOnboardHandler(m.log, onboardSvc)

	// feature flagging for new labels service
	var labelHandler *label.LabelHandler
	{
		b := m.apibackend

		labelSvc = label.NewAuthedLabelService(labelSvc, b.OrgLookupService)
		labelSvc = label.NewLabelLogger(m.log.With(zap.String("handler", "labels")), labelSvc)
		labelSvc = label.NewLabelMetrics(m.reg, labelSvc)
		labelHandler = label.NewHTTPLabelHandler(m.log, labelSvc)
	}

	// feature flagging for new authorization service
	var authHTTPServer *authorization.AuthHandler
	{
		authLogger := m.log.With(zap.String("handler", "authorization"))

		var authService platform.AuthorizationService
		authService = authorization.NewAuthedAuthorizationService(authSvc, ts)
		authService = authorization.NewAuthMetrics(m.reg, authService)
		authService = authorization.NewAuthLogger(authLogger, authService)

		authHTTPServer = authorization.NewHTTPAuthHandler(m.log, authService, ts)
	}

	var v1AuthHTTPServer *authv1.AuthHandler
	{
		authLogger := m.log.With(zap.String("handler", "v1_authorization"))

		var authService platform.AuthorizationService
		authService = authorization.NewAuthedAuthorizationService(authSvcV1, ts)
		authService = authorization.NewAuthLogger(authLogger, authService)

		passService := authv1.NewAuthedPasswordService(authv1.AuthFinder(authSvcV1), passwordV1)
		v1AuthHTTPServer = authv1.NewHTTPAuthHandler(m.log, authService, passService, ts)
	}

	var sessionHTTPServer *session.SessionHandler
	{
		sessionHTTPServer = session.NewSessionHandler(m.log.With(zap.String("handler", "session")), sessionSvc, ts.UserService, ts.PasswordsService)
	}

	orgHTTPServer := ts.NewOrgHTTPHandler(m.log, secret.NewAuthedService(secretSvc))

	bucketHTTPServer := ts.NewBucketHTTPHandler(m.log, labelSvc)

	var dashboardServer *dashboardTransport.DashboardHandler
	{
		urmHandler := tenant.NewURMHandler(
			m.log.With(zap.String("handler", "urm")),
			platform.DashboardsResourceType,
			"id",
			ts.UserService,
			tenant.NewAuthedURMService(ts.OrganizationService, ts.UserResourceMappingService),
		)

		labelHandler := label.NewHTTPEmbeddedHandler(
			m.log.With(zap.String("handler", "label")),
			platform.DashboardsResourceType,
			labelSvc,
		)

		dashboardServer = dashboardTransport.NewDashboardHandler(
			m.log.With(zap.String("handler", "dashboards")),
			authorizer.NewDashboardService(dashboardSvc),
			labelSvc,
			ts.UserService,
			ts.OrganizationService,
			urmHandler,
			labelHandler,
		)
	}

	platformHandler := http.NewPlatformHandler(
		m.apibackend,
		http.WithResourceHandler(stacksHTTPServer),
		http.WithResourceHandler(templatesHTTPServer),
		http.WithResourceHandler(onboardHTTPServer),
		http.WithResourceHandler(authHTTPServer),
		http.WithResourceHandler(labelHandler),
		http.WithResourceHandler(sessionHTTPServer.SignInResourceHandler()),
		http.WithResourceHandler(sessionHTTPServer.SignOutResourceHandler()),
		http.WithResourceHandler(userHTTPServer.MeResourceHandler()),
		http.WithResourceHandler(userHTTPServer.UserResourceHandler()),
		http.WithResourceHandler(orgHTTPServer),
		http.WithResourceHandler(bucketHTTPServer),
		http.WithResourceHandler(v1AuthHTTPServer),
		http.WithResourceHandler(dashboardServer),
	)

	httpLogger := m.log.With(zap.String("service", "http"))
	var httpHandler nethttp.Handler = http.NewRootHandler(
		"platform",
		http.WithLog(httpLogger),
		http.WithAPIHandler(platformHandler),
		http.WithPprofEnabled(!opts.ProfilingDisabled),
		http.WithMetrics(m.reg, !opts.MetricsDisabled),
	)

	if opts.LogLevel == zap.DebugLevel {
		httpHandler = http.LoggingMW(httpLogger)(httpHandler)
	}
	// If we are in testing mode we allow all data to be flushed and removed.
	if opts.Testing {
		httpHandler = http.DebugFlush(ctx, httpHandler, flushers)
	}

	if !opts.ReportingDisabled {
		m.runReporter(ctx)
	}
	if err := m.runHTTP(opts, httpHandler); err != nil {
		return err
	}

	return nil
}

// runHTTP configures and launches a listener for incoming HTTP(S) requests.
// The listener is run in a separate goroutine. If it fails to start up, it
// will cancel the launcher.
func (m *Launcher) runHTTP(opts *InfluxdOpts, handler nethttp.Handler) error {
	log := m.log.With(zap.String("service", "tcp-listener"))

	m.httpServer = &nethttp.Server{
		Addr:              opts.HttpBindAddress,
		Handler:           handler,
		ReadHeaderTimeout: opts.HttpReadHeaderTimeout,
		ReadTimeout:       opts.HttpReadTimeout,
		WriteTimeout:      opts.HttpWriteTimeout,
		IdleTimeout:       opts.HttpIdleTimeout,
	}

	ln, err := net.Listen("tcp", opts.HttpBindAddress)
	if err != nil {
		log.Error("Failed to set up TCP listener", zap.String("addr", opts.HttpBindAddress), zap.Error(err))
		return err
	}
	if addr, ok := ln.Addr().(*net.TCPAddr); ok {
		m.httpPort = addr.Port
	}
	m.wg.Add(1)

	m.tlsEnabled = opts.HttpTLSCert != "" && opts.HttpTLSKey != ""
	if !m.tlsEnabled {
		if opts.HttpTLSCert != "" || opts.HttpTLSKey != "" {
			log.Warn("TLS requires specifying both cert and key, falling back to HTTP")
		}

		go func(log *zap.Logger) {
			defer m.wg.Done()
			log.Info("Listening", zap.String("transport", "http"), zap.String("addr", opts.HttpBindAddress), zap.Int("port", m.httpPort))

			if err := m.httpServer.Serve(ln); err != nethttp.ErrServerClosed {
				log.Error("Failed to serve HTTP", zap.Error(err))
				m.cancel()
			}
			log.Info("Stopping")
		}(log)

		return nil
	}

	if _, err = tls.LoadX509KeyPair(opts.HttpTLSCert, opts.HttpTLSKey); err != nil {
		log.Error("Failed to load x509 key pair", zap.String("cert-path", opts.HttpTLSCert), zap.String("key-path", opts.HttpTLSKey))
		return err
	}

	var tlsMinVersion uint16
	var useStrictCiphers = opts.HttpTLSStrictCiphers
	switch opts.HttpTLSMinVersion {
	case "1.0":
		log.Warn("Setting the minimum version of TLS to 1.0 - this is discouraged. Please use 1.2 or 1.3")
		tlsMinVersion = tls.VersionTLS10
	case "1.1":
		log.Warn("Setting the minimum version of TLS to 1.1 - this is discouraged. Please use 1.2 or 1.3")
		tlsMinVersion = tls.VersionTLS11
	case "1.2":
		tlsMinVersion = tls.VersionTLS12
	case "1.3":
		if useStrictCiphers {
			log.Warn("TLS version 1.3 does not support configuring strict ciphers")
			useStrictCiphers = false
		}
		tlsMinVersion = tls.VersionTLS13
	default:
		return fmt.Errorf("unsupported TLS version: %s", opts.HttpTLSMinVersion)
	}

	// nil uses the default cipher suite
	var cipherConfig []uint16 = nil
	if useStrictCiphers {
		// See https://ssl-config.mozilla.org/#server=go&version=1.14.4&config=intermediate&guideline=5.6
		cipherConfig = []uint16{
			tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
			tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
			tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
			tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
			tls.TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305,
			tls.TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305,
		}
	}

	m.httpServer.TLSConfig = &tls.Config{
		CurvePreferences:         []tls.CurveID{tls.CurveP521, tls.CurveP384, tls.CurveP256},
		PreferServerCipherSuites: !useStrictCiphers,
		MinVersion:               tlsMinVersion,
		CipherSuites:             cipherConfig,
	}

	go func(log *zap.Logger) {
		defer m.wg.Done()
		log.Info("Listening", zap.String("transport", "https"), zap.String("addr", opts.HttpBindAddress), zap.Int("port", m.httpPort))

		if err := m.httpServer.ServeTLS(ln, opts.HttpTLSCert, opts.HttpTLSKey); err != nethttp.ErrServerClosed {
			log.Error("Failed to serve HTTPS", zap.Error(err))
			m.cancel()
		}
		log.Info("Stopping")
	}(log)

	return nil
}

// runReporter configures and launches a periodic telemetry report for the server.
func (m *Launcher) runReporter(ctx context.Context) {
	reporter := telemetry.NewReporter(m.log, m.reg)
	reporter.Interval = 8 * time.Hour
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		reporter.Report(ctx)
	}()
}

func checkForPriorVersion(ctx context.Context, log *zap.Logger, boltPath string, enginePath string, bs platform.BucketService, metaClient *meta.Client) error {
	buckets, _, err := bs.FindBuckets(ctx, platform.BucketFilter{})
	if err != nil {
		log.Error("Failed to retrieve buckets", zap.Error(err))
		return err
	}

	hasErrors := false

	// if there are no buckets, we will be fine
	if len(buckets) > 0 {
		log.Info("Checking InfluxDB metadata for prior version.", zap.String("bolt_path", boltPath))

		for i := range buckets {
			bucket := buckets[i]
			if dbi := metaClient.Database(bucket.ID.String()); dbi == nil {
				log.Error("Missing metadata for bucket.", zap.String("bucket", bucket.Name), zap.Stringer("bucket_id", bucket.ID))
				hasErrors = true
			}
		}

		if hasErrors {
			log.Error("Incompatible InfluxDB 2.0 metadata found. File must be moved before influxd will start.", zap.String("path", boltPath))
		}
	}

	// see if there are existing files which match the old directory structure
	{
		for _, name := range []string{"_series", "index"} {
			dir := filepath.Join(enginePath, name)
			if fi, err := os.Stat(dir); err == nil {
				if fi.IsDir() {
					log.Error("Found directory that is incompatible with this version of InfluxDB.", zap.String("path", dir))
					hasErrors = true
				}
			}
		}
	}

	if hasErrors {
		log.Error("Incompatible InfluxDB 2.0 version found. Move all files outside of engine_path before influxd will start.", zap.String("engine_path", enginePath))
		return errors.New("incompatible InfluxDB version")
	}

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

// CheckService returns the internal check service.
func (m *Launcher) CheckService() platform.CheckService {
	return m.apibackend.CheckService
}

// KeyValueService returns the internal key-value service.
func (m *Launcher) KeyValueService() *kv.Service {
	return m.kvService
}

func (m *Launcher) DBRPMappingServiceV2() platform.DBRPMappingServiceV2 {
	return m.apibackend.DBRPService
}

func (m *Launcher) SessionService() platform.SessionService {
	return m.apibackend.SessionService
}

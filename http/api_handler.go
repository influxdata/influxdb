package http

import (
	"context"
	"net/http"

	"github.com/go-chi/chi"
	"github.com/influxdata/httprouter"
	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/authorizer"
	"github.com/influxdata/influxdb/v2/chronograf/server"
	"github.com/influxdata/influxdb/v2/dbrp"
	"github.com/influxdata/influxdb/v2/http/metric"
	"github.com/influxdata/influxdb/v2/influxql"
	"github.com/influxdata/influxdb/v2/kit/feature"
	"github.com/influxdata/influxdb/v2/kit/prom"
	kithttp "github.com/influxdata/influxdb/v2/kit/transport/http"
	"github.com/influxdata/influxdb/v2/query"
	"github.com/influxdata/influxdb/v2/storage"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

// APIHandler is a collection of all the service handlers.
type APIHandler struct {
	chi.Router
}

// APIBackend is all services and associated parameters required to construct
// an APIHandler.
type APIBackend struct {
	AssetsPath string // if empty then assets are served from bindata.
	Logger     *zap.Logger
	influxdb.HTTPErrorHandler
	SessionRenewDisabled bool
	// MaxBatchSizeBytes is the maximum number of bytes which can be written
	// in a single points batch
	MaxBatchSizeBytes int64

	// WriteParserMaxBytes specifies the maximum number of bytes that may be allocated when processing a single
	// write request. A value of zero specifies there is no limit.
	WriteParserMaxBytes int

	// WriteParserMaxLines specifies the maximum number of lines that may be parsed when processing a single
	// write request. A value of zero specifies there is no limit.
	WriteParserMaxLines int

	// WriteParserMaxValues specifies the maximum number of values that may be parsed when processing a single
	// write request. A value of zero specifies there is no limit.
	WriteParserMaxValues int

	NewBucketService func(*influxdb.Source) (influxdb.BucketService, error)
	NewQueryService  func(*influxdb.Source) (query.ProxyQueryService, error)

	WriteEventRecorder metric.EventRecorder
	QueryEventRecorder metric.EventRecorder

	AlgoWProxy FeatureProxyHandler

	PointsWriter                    storage.PointsWriter
	DeleteService                   influxdb.DeleteService
	BackupService                   influxdb.BackupService
	RestoreService                  influxdb.RestoreService
	AuthorizationService            influxdb.AuthorizationService
	AuthorizerV1                    influxdb.AuthorizerV1
	OnboardingService               influxdb.OnboardingService
	DBRPService                     influxdb.DBRPMappingServiceV2
	BucketService                   influxdb.BucketService
	SessionService                  influxdb.SessionService
	UserService                     influxdb.UserService
	OrganizationService             influxdb.OrganizationService
	UserResourceMappingService      influxdb.UserResourceMappingService
	LabelService                    influxdb.LabelService
	DashboardService                influxdb.DashboardService
	DashboardOperationLogService    influxdb.DashboardOperationLogService
	BucketOperationLogService       influxdb.BucketOperationLogService
	UserOperationLogService         influxdb.UserOperationLogService
	OrganizationOperationLogService influxdb.OrganizationOperationLogService
	SourceService                   influxdb.SourceService
	VariableService                 influxdb.VariableService
	PasswordsService                influxdb.PasswordsService
	InfluxQLService                 query.ProxyQueryService
	InfluxqldService                influxql.ProxyQueryService
	FluxService                     query.ProxyQueryService
	FluxLanguageService             influxdb.FluxLanguageService
	TaskService                     influxdb.TaskService
	CheckService                    influxdb.CheckService
	TelegrafService                 influxdb.TelegrafConfigStore
	ScraperTargetStoreService       influxdb.ScraperTargetStoreService
	SecretService                   influxdb.SecretService
	LookupService                   influxdb.LookupService
	ChronografService               *server.Service
	OrgLookupService                authorizer.OrgIDResolver
	DocumentService                 influxdb.DocumentService
	NotificationRuleStore           influxdb.NotificationRuleStore
	NotificationEndpointService     influxdb.NotificationEndpointService
	Flagger                         feature.Flagger
	FlagsHandler                    http.Handler
}

// PrometheusCollectors exposes the prometheus collectors associated with an APIBackend.
func (b *APIBackend) PrometheusCollectors() []prometheus.Collector {
	var cs []prometheus.Collector

	if pc, ok := b.WriteEventRecorder.(prom.PrometheusCollector); ok {
		cs = append(cs, pc.PrometheusCollectors()...)
	}

	if pc, ok := b.QueryEventRecorder.(prom.PrometheusCollector); ok {
		cs = append(cs, pc.PrometheusCollectors()...)
	}

	return cs
}

// APIHandlerOptFn is a functional input param to set parameters on
// the APIHandler.
type APIHandlerOptFn func(chi.Router)

// WithResourceHandler registers a resource handler on the APIHandler.
func WithResourceHandler(resHandler kithttp.ResourceHandler) APIHandlerOptFn {
	return func(h chi.Router) {
		h.Mount(resHandler.Prefix(), resHandler)
	}
}

// NewAPIHandler constructs all api handlers beneath it and returns an APIHandler
func NewAPIHandler(b *APIBackend, opts ...APIHandlerOptFn) *APIHandler {
	h := &APIHandler{
		Router: NewBaseChiRouter(kithttp.NewAPI(kithttp.WithLog(b.Logger))),
	}

	b.UserResourceMappingService = authorizer.NewURMService(b.OrgLookupService, b.UserResourceMappingService)

	h.Mount("/api/v2", serveLinksHandler(b.HTTPErrorHandler))

	checkBackend := NewCheckBackend(b.Logger.With(zap.String("handler", "check")), b)
	checkBackend.CheckService = authorizer.NewCheckService(b.CheckService,
		b.UserResourceMappingService, b.OrganizationService)
	h.Mount(prefixChecks, NewCheckHandler(b.Logger, checkBackend))

	h.Mount(prefixChronograf, NewChronografHandler(b.ChronografService, b.HTTPErrorHandler))

	deleteBackend := NewDeleteBackend(b.Logger.With(zap.String("handler", "delete")), b)
	h.Mount(prefixDelete, NewDeleteHandler(b.Logger, deleteBackend))

	documentBackend := NewDocumentBackend(b.Logger.With(zap.String("handler", "document")), b)
	documentBackend.DocumentService = authorizer.NewDocumentService(b.DocumentService)
	h.Mount(prefixDocuments, NewDocumentHandler(documentBackend))

	fluxBackend := NewFluxBackend(b.Logger.With(zap.String("handler", "query")), b)
	h.Mount(prefixQuery, NewFluxHandler(b.Logger, fluxBackend))

	notificationEndpointBackend := NewNotificationEndpointBackend(b.Logger.With(zap.String("handler", "notificationEndpoint")), b)
	notificationEndpointBackend.NotificationEndpointService = authorizer.NewNotificationEndpointService(b.NotificationEndpointService,
		b.UserResourceMappingService, b.OrganizationService)
	h.Mount(prefixNotificationEndpoints, NewNotificationEndpointHandler(notificationEndpointBackend.Logger(), notificationEndpointBackend))

	notificationRuleBackend := NewNotificationRuleBackend(b.Logger.With(zap.String("handler", "notification_rule")), b)
	notificationRuleBackend.NotificationRuleStore = authorizer.NewNotificationRuleStore(b.NotificationRuleStore,
		b.UserResourceMappingService, b.OrganizationService)
	h.Mount(prefixNotificationRules, NewNotificationRuleHandler(b.Logger, notificationRuleBackend))

	scraperBackend := NewScraperBackend(b.Logger.With(zap.String("handler", "scraper")), b)
	scraperBackend.ScraperStorageService = authorizer.NewScraperTargetStoreService(b.ScraperTargetStoreService,
		b.UserResourceMappingService,
		b.OrganizationService)
	h.Mount(prefixTargets, NewScraperHandler(b.Logger, scraperBackend))

	sourceBackend := NewSourceBackend(b.Logger.With(zap.String("handler", "source")), b)
	sourceBackend.SourceService = authorizer.NewSourceService(b.SourceService)
	sourceBackend.BucketService = authorizer.NewBucketService(b.BucketService)
	h.Mount(prefixSources, NewSourceHandler(b.Logger, sourceBackend))

	h.Mount("/api/v2/swagger.json", newSwaggerLoader(b.Logger.With(zap.String("service", "swagger-loader")), b.HTTPErrorHandler))

	taskLogger := b.Logger.With(zap.String("handler", "bucket"))
	taskBackend := NewTaskBackend(taskLogger, b)
	taskBackend.TaskService = authorizer.NewTaskService(taskLogger, b.TaskService)
	taskHandler := NewTaskHandler(b.Logger, taskBackend)
	h.Mount(prefixTasks, taskHandler)

	telegrafBackend := NewTelegrafBackend(b.Logger.With(zap.String("handler", "telegraf")), b)
	telegrafBackend.TelegrafService = authorizer.NewTelegrafConfigService(b.TelegrafService, b.UserResourceMappingService)
	h.Mount(prefixTelegrafPlugins, NewTelegrafHandler(b.Logger, telegrafBackend))
	h.Mount(prefixTelegraf, NewTelegrafHandler(b.Logger, telegrafBackend))

	h.Mount("/api/v2/flags", b.FlagsHandler)

	variableBackend := NewVariableBackend(b.Logger.With(zap.String("handler", "variable")), b)
	variableBackend.VariableService = authorizer.NewVariableService(b.VariableService)
	h.Mount(prefixVariables, NewVariableHandler(b.Logger, variableBackend))

	backupBackend := NewBackupBackend(b)
	backupBackend.BackupService = authorizer.NewBackupService(backupBackend.BackupService)
	h.Mount(prefixBackup, NewBackupHandler(backupBackend))

	restoreBackend := NewRestoreBackend(b)
	restoreBackend.RestoreService = authorizer.NewRestoreService(restoreBackend.RestoreService)
	h.Mount(prefixRestore, NewRestoreHandler(restoreBackend))

	h.Mount(dbrp.PrefixDBRP, dbrp.NewHTTPHandler(b.Logger, b.DBRPService, b.OrganizationService))

	writeBackend := NewWriteBackend(b.Logger.With(zap.String("handler", "write")), b)
	h.Mount(prefixWrite, NewWriteHandler(b.Logger, writeBackend,
		WithMaxBatchSizeBytes(b.MaxBatchSizeBytes),
		//WithParserOptions(
		//	models.WithParserMaxBytes(b.WriteParserMaxBytes),
		//	models.WithParserMaxLines(b.WriteParserMaxLines),
		//	models.WithParserMaxValues(b.WriteParserMaxValues),
		//),
	))

	for _, o := range opts {
		o(h)
	}
	return h
}

var apiLinks = map[string]interface{}{
	// when adding new links, please take care to keep this list alphabetical
	// as this makes it easier to verify values against the swagger document.
	"authorizations": "/api/v2/authorizations",
	"backup":         "/api/v2/backup",
	"buckets":        "/api/v2/buckets",
	"dashboards":     "/api/v2/dashboards",
	"external": map[string]string{
		"statusFeed": "https://www.influxdata.com/feed/json",
	},
	"flags":                 "/api/v2/flags",
	"labels":                "/api/v2/labels",
	"variables":             "/api/v2/variables",
	"me":                    "/api/v2/me",
	"notificationRules":     "/api/v2/notificationRules",
	"notificationEndpoints": "/api/v2/notificationEndpoints",
	"orgs":                  "/api/v2/orgs",
	"query": map[string]string{
		"self":        "/api/v2/query",
		"ast":         "/api/v2/query/ast",
		"analyze":     "/api/v2/query/analyze",
		"suggestions": "/api/v2/query/suggestions",
	},
	"restore":  "/api/v2/restore",
	"setup":    "/api/v2/setup",
	"signin":   "/api/v2/signin",
	"signout":  "/api/v2/signout",
	"sources":  "/api/v2/sources",
	"scrapers": "/api/v2/scrapers",
	"swagger":  "/api/v2/swagger.json",
	"system": map[string]string{
		"metrics": "/metrics",
		"debug":   "/debug/pprof",
		"health":  "/health",
	},
	"tasks":     "/api/v2/tasks",
	"checks":    "/api/v2/checks",
	"telegrafs": "/api/v2/telegrafs",
	"plugins":   "/api/v2/telegraf/plugins",
	"users":     "/api/v2/users",
	"write":     "/api/v2/write",
	"delete":    "/api/v2/delete",
}

func serveLinksHandler(errorHandler influxdb.HTTPErrorHandler) http.Handler {
	fn := func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		if err := encodeResponse(ctx, w, http.StatusOK, apiLinks); err != nil {
			errorHandler.HandleHTTPError(ctx, err, w)
		}
	}
	return http.HandlerFunc(fn)
}

func decodeIDFromCtx(ctx context.Context, name string) (influxdb.ID, error) {
	params := httprouter.ParamsFromContext(ctx)
	idStr := params.ByName(name)

	if idStr == "" {
		return 0, &influxdb.Error{
			Code: influxdb.EInvalid,
			Msg:  "url missing " + name,
		}
	}

	var i influxdb.ID
	if err := i.DecodeFromString(idStr); err != nil {
		return 0, &influxdb.Error{
			Code: influxdb.EInvalid,
			Err:  err,
		}
	}

	return i, nil
}

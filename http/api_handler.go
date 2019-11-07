package http

import (
	"net/http"
	"strings"

	"github.com/go-chi/chi"
	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/authorizer"
	"github.com/influxdata/influxdb/chronograf/server"
	"github.com/influxdata/influxdb/http/metric"
	"github.com/influxdata/influxdb/kit/prom"
	"github.com/influxdata/influxdb/query"
	"github.com/influxdata/influxdb/storage"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

// APIHandler is a collection of all the service handlers.
type APIHandler struct {
	influxdb.HTTPErrorHandler
	AssetHandler                *AssetHandler
	AuthorizationHandler        *AuthorizationHandler
	BucketHandler               *BucketHandler
	CheckHandler                *CheckHandler
	ChronografHandler           *ChronografHandler
	DashboardHandler            *DashboardHandler
	DeleteHandler               *DeleteHandler
	DocumentHandler             *DocumentHandler
	LabelHandler                *LabelHandler
	NotificationEndpointHandler *NotificationEndpointHandler
	NotificationRuleHandler     *NotificationRuleHandler
	OrgHandler                  *OrgHandler
	QueryHandler                *FluxHandler
	ScraperHandler              *ScraperHandler
	SessionHandler              *SessionHandler
	SetupHandler                *SetupHandler
	SourceHandler               *SourceHandler
	SwaggerHandler              http.Handler
	TaskHandler                 *TaskHandler
	TelegrafHandler             *TelegrafHandler
	UserHandler                 *UserHandler
	VariableHandler             *VariableHandler
	WriteHandler                *WriteHandler

	Gateway chi.Router
}

// APIBackend is all services and associated parameters required to construct
// an APIHandler.
type APIBackend struct {
	AssetsPath string // if empty then assets are served from bindata.
	Logger     *zap.Logger
	influxdb.HTTPErrorHandler
	SessionRenewDisabled bool

	NewBucketService func(*influxdb.Source) (influxdb.BucketService, error)
	NewQueryService  func(*influxdb.Source) (query.ProxyQueryService, error)

	WriteEventRecorder metric.EventRecorder
	QueryEventRecorder metric.EventRecorder

	PointsWriter                    storage.PointsWriter
	DeleteService                   influxdb.DeleteService
	AuthorizationService            influxdb.AuthorizationService
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
	OnboardingService               influxdb.OnboardingService
	InfluxQLService                 query.ProxyQueryService
	FluxService                     query.ProxyQueryService
	TaskService                     influxdb.TaskService
	CheckService                    influxdb.CheckService
	TelegrafService                 influxdb.TelegrafConfigStore
	ScraperTargetStoreService       influxdb.ScraperTargetStoreService
	SecretService                   influxdb.SecretService
	LookupService                   influxdb.LookupService
	ChronografService               *server.Service
	OrgLookupService                authorizer.OrganizationService
	DocumentService                 influxdb.DocumentService
	NotificationRuleStore           influxdb.NotificationRuleStore
	NotificationEndpointService     influxdb.NotificationEndpointService
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

// ResourceHandler is an HTTP handler for a resource. The prefix
// describes the url path prefix that relates to the handler
// endpoints.
type ResourceHandler interface {
	Prefix() string
	http.Handler
}

// APIHandlerOptFn is a functional input param to set parameters on
// the APIHandler.
type APIHandlerOptFn func(*APIHandler)

// WithResourceHandler registers a resource handler on the APIHandler.
func WithResourceHandler(resHandler ResourceHandler) APIHandlerOptFn {
	return func(h *APIHandler) {
		h.Gateway.Mount(resHandler.Prefix(), resHandler)
	}
}

// NewAPIHandler constructs all api handlers beneath it and returns an APIHandler
func NewAPIHandler(b *APIBackend, opts ...APIHandlerOptFn) *APIHandler {
	h := &APIHandler{
		HTTPErrorHandler: b.HTTPErrorHandler,
		Gateway:          newBaseChiRouter(b.HTTPErrorHandler),
	}
	for _, o := range opts {
		o(h)
	}

	internalURM := b.UserResourceMappingService
	b.UserResourceMappingService = authorizer.NewURMService(b.OrgLookupService, b.UserResourceMappingService)

	documentBackend := NewDocumentBackend(b)
	h.DocumentHandler = NewDocumentHandler(documentBackend)

	sessionBackend := NewSessionBackend(b)
	h.SessionHandler = NewSessionHandler(sessionBackend)

	bucketBackend := NewBucketBackend(b)
	bucketBackend.BucketService = authorizer.NewBucketService(b.BucketService)
	h.BucketHandler = NewBucketHandler(bucketBackend)

	orgBackend := NewOrgBackend(b)
	orgBackend.OrganizationService = authorizer.NewOrgService(b.OrganizationService)
	h.OrgHandler = NewOrgHandler(orgBackend)

	userBackend := NewUserBackend(b)
	userBackend.UserService = authorizer.NewUserService(b.UserService)
	h.UserHandler = NewUserHandler(userBackend)

	dashboardBackend := NewDashboardBackend(b)
	dashboardBackend.DashboardService = authorizer.NewDashboardService(b.DashboardService)
	h.DashboardHandler = NewDashboardHandler(dashboardBackend)

	variableBackend := NewVariableBackend(b)
	variableBackend.VariableService = authorizer.NewVariableService(b.VariableService)
	h.VariableHandler = NewVariableHandler(variableBackend)

	authorizationBackend := NewAuthorizationBackend(b)
	authorizationBackend.AuthorizationService = authorizer.NewAuthorizationService(b.AuthorizationService)
	h.AuthorizationHandler = NewAuthorizationHandler(authorizationBackend)

	scraperBackend := NewScraperBackend(b)
	scraperBackend.ScraperStorageService = authorizer.NewScraperTargetStoreService(b.ScraperTargetStoreService,
		b.UserResourceMappingService,
		b.OrganizationService)
	h.ScraperHandler = NewScraperHandler(scraperBackend)

	sourceBackend := NewSourceBackend(b)
	sourceBackend.SourceService = authorizer.NewSourceService(b.SourceService)
	sourceBackend.BucketService = authorizer.NewBucketService(b.BucketService)
	h.SourceHandler = NewSourceHandler(sourceBackend)

	setupBackend := NewSetupBackend(b)
	h.SetupHandler = NewSetupHandler(setupBackend)

	taskBackend := NewTaskBackend(b)
	h.TaskHandler = NewTaskHandler(taskBackend)
	h.TaskHandler.UserResourceMappingService = internalURM

	telegrafBackend := NewTelegrafBackend(b)
	telegrafBackend.TelegrafService = authorizer.NewTelegrafConfigService(b.TelegrafService, b.UserResourceMappingService)
	h.TelegrafHandler = NewTelegrafHandler(telegrafBackend)

	notificationRuleBackend := NewNotificationRuleBackend(b)
	notificationRuleBackend.NotificationRuleStore = authorizer.NewNotificationRuleStore(b.NotificationRuleStore,
		b.UserResourceMappingService, b.OrganizationService)
	h.NotificationRuleHandler = NewNotificationRuleHandler(notificationRuleBackend)

	notificationEndpointBackend := NewNotificationEndpointBackend(b)
	notificationEndpointBackend.NotificationEndpointService = authorizer.NewNotificationEndpointService(b.NotificationEndpointService,
		b.UserResourceMappingService, b.OrganizationService)
	h.NotificationEndpointHandler = NewNotificationEndpointHandler(notificationEndpointBackend)

	checkBackend := NewCheckBackend(b)
	checkBackend.CheckService = authorizer.NewCheckService(b.CheckService,
		b.UserResourceMappingService, b.OrganizationService)
	h.CheckHandler = NewCheckHandler(checkBackend)

	writeBackend := NewWriteBackend(b)
	h.WriteHandler = NewWriteHandler(writeBackend)

	deleteBackend := NewDeleteBackend(b)
	h.DeleteHandler = NewDeleteHandler(deleteBackend)

	fluxBackend := NewFluxBackend(b)
	h.QueryHandler = NewFluxHandler(fluxBackend)

	h.ChronografHandler = NewChronografHandler(b.ChronografService, b.HTTPErrorHandler)
	h.SwaggerHandler = newSwaggerLoader(b.Logger.With(zap.String("service", "swagger-loader")), b.HTTPErrorHandler)
	h.LabelHandler = NewLabelHandler(authorizer.NewLabelService(b.LabelService), b.HTTPErrorHandler)

	return h
}

var apiLinks = map[string]interface{}{
	// when adding new links, please take care to keep this list alphabetical
	// as this makes it easier to verify values against the swagger document.
	"authorizations": "/api/v2/authorizations",
	"buckets":        "/api/v2/buckets",
	"dashboards":     "/api/v2/dashboards",
	"external": map[string]string{
		"statusFeed": "https://www.influxdata.com/feed/json",
	},
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
	"users":     "/api/v2/users",
	"write":     "/api/v2/write",
	"delete":    "/api/v2/delete",
}

func (h *APIHandler) serveLinks(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	if err := encodeResponse(ctx, w, http.StatusOK, apiLinks); err != nil {
		h.HandleHTTPError(ctx, err, w)
	}
}

// ServeHTTP delegates a request to the appropriate subhandler.
func (h *APIHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	setCORSResponseHeaders(w, r)
	if r.Method == "OPTIONS" {
		return
	}

	// Serve the links base links for the API.
	if r.URL.Path == "/api/v2/" || r.URL.Path == "/api/v2" {
		h.serveLinks(w, r)
		return
	}

	if r.URL.Path == "/api/v2/signin" || r.URL.Path == "/api/v2/signout" {
		h.SessionHandler.ServeHTTP(w, r)
		return
	}

	if strings.HasPrefix(r.URL.Path, "/api/v2/setup") {
		h.SetupHandler.ServeHTTP(w, r)
		return
	}

	if strings.HasPrefix(r.URL.Path, "/api/v2/write") {
		h.WriteHandler.ServeHTTP(w, r)
		return
	}

	if strings.HasPrefix(r.URL.Path, "/api/v2/delete") {
		h.DeleteHandler.ServeHTTP(w, r)
		return
	}

	if strings.HasPrefix(r.URL.Path, "/api/v2/query") {
		h.QueryHandler.ServeHTTP(w, r)
		return
	}

	if strings.HasPrefix(r.URL.Path, "/api/v2/buckets") {
		h.BucketHandler.ServeHTTP(w, r)
		return
	}

	if strings.HasPrefix(r.URL.Path, "/api/v2/labels") {
		h.LabelHandler.ServeHTTP(w, r)
		return
	}

	if strings.HasPrefix(r.URL.Path, "/api/v2/users") {
		h.UserHandler.ServeHTTP(w, r)
		return
	}

	if strings.HasPrefix(r.URL.Path, "/api/v2/me") {
		h.UserHandler.ServeHTTP(w, r)
		return
	}

	if strings.HasPrefix(r.URL.Path, "/api/v2/orgs") {
		h.OrgHandler.ServeHTTP(w, r)
		return
	}

	if strings.HasPrefix(r.URL.Path, "/api/v2/authorizations") {
		h.AuthorizationHandler.ServeHTTP(w, r)
		return
	}

	if strings.HasPrefix(r.URL.Path, "/api/v2/dashboards") {
		h.DashboardHandler.ServeHTTP(w, r)
		return
	}

	if strings.HasPrefix(r.URL.Path, "/api/v2/sources") {
		h.SourceHandler.ServeHTTP(w, r)
		return
	}

	if strings.HasPrefix(r.URL.Path, "/api/v2/scrapers") {
		h.ScraperHandler.ServeHTTP(w, r)
		return
	}

	if strings.HasPrefix(r.URL.Path, "/api/v2/tasks") {
		h.TaskHandler.ServeHTTP(w, r)
		return
	}

	if strings.HasPrefix(r.URL.Path, "/api/v2/checks") {
		h.CheckHandler.ServeHTTP(w, r)
		return
	}

	if strings.HasPrefix(r.URL.Path, "/api/v2/telegrafs") {
		h.TelegrafHandler.ServeHTTP(w, r)
		return
	}

	if strings.HasPrefix(r.URL.Path, "/api/v2/notificationRules") {
		h.NotificationRuleHandler.ServeHTTP(w, r)
		return
	}

	if strings.HasPrefix(r.URL.Path, "/api/v2/notificationEndpoints") {
		h.NotificationEndpointHandler.ServeHTTP(w, r)
		return
	}

	if strings.HasPrefix(r.URL.Path, "/api/v2/variables") {
		h.VariableHandler.ServeHTTP(w, r)
		return
	}

	if strings.HasPrefix(r.URL.Path, "/api/v2/documents") {
		h.DocumentHandler.ServeHTTP(w, r)
		return
	}

	if strings.HasPrefix(r.URL.Path, "/chronograf/") {
		h.ChronografHandler.ServeHTTP(w, r)
		return
	}

	if r.URL.Path == "/api/v2/swagger.json" {
		h.SwaggerHandler.ServeHTTP(w, r)
		return
	}

	// router has not found route registered on it directly
	// if a route slips through, then the same 404 as before
	// if a route matches on the gateway router, it will use
	// whatever handler that matches the router in question.
	h.Gateway.ServeHTTP(w, r)
}

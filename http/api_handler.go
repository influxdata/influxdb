package http

import (
	http "net/http"
	"strings"

	influxdb "github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/authorizer"
	"github.com/influxdata/influxdb/chronograf/server"
	"github.com/influxdata/influxdb/query"
	"github.com/influxdata/influxdb/storage"
	"go.uber.org/zap"
)

// APIHandler is a collection of all the service handlers.
type APIHandler struct {
	BucketHandler        *BucketHandler
	UserHandler          *UserHandler
	OrgHandler           *OrgHandler
	AuthorizationHandler *AuthorizationHandler
	DashboardHandler     *DashboardHandler
	LabelHandler         *LabelHandler
	AssetHandler         *AssetHandler
	ChronografHandler    *ChronografHandler
	ScraperHandler       *ScraperHandler
	SourceHandler        *SourceHandler
	VariableHandler      *VariableHandler
	TaskHandler          *TaskHandler
	TelegrafHandler      *TelegrafHandler
	QueryHandler         *FluxHandler
	ProtoHandler         *ProtoHandler
	WriteHandler         *WriteHandler
	DocumentHandler      *DocumentHandler
	SetupHandler         *SetupHandler
	SessionHandler       *SessionHandler
	SwaggerHandler       http.HandlerFunc
}

// APIBackend is all services and associated parameters required to construct
// an APIHandler.
type APIBackend struct {
	AssetsPath string // if empty then assets are served from bindata.
	Logger     *zap.Logger

	NewBucketService func(*influxdb.Source) (influxdb.BucketService, error)
	NewQueryService  func(*influxdb.Source) (query.ProxyQueryService, error)

	PointsWriter                    storage.PointsWriter
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
	TelegrafService                 influxdb.TelegrafConfigStore
	ScraperTargetStoreService       influxdb.ScraperTargetStoreService
	SecretService                   influxdb.SecretService
	LookupService                   influxdb.LookupService
	ChronografService               *server.Service
	ProtoService                    influxdb.ProtoService
	OrgLookupService                authorizer.OrganizationService
	ViewService                     influxdb.ViewService
	DocumentService                 influxdb.DocumentService
}

// NewAPIHandler constructs all api handlers beneath it and returns an APIHandler
func NewAPIHandler(b *APIBackend) *APIHandler {
	h := &APIHandler{}

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
	scraperBackend.ScraperStorageService = authorizer.NewScraperTargetStoreService(b.ScraperTargetStoreService, b.UserResourceMappingService)
	h.ScraperHandler = NewScraperHandler(scraperBackend)

	sourceBackend := NewSourceBackend(b)
	sourceBackend.SourceService = authorizer.NewSourceService(b.SourceService)
	sourceBackend.NewBucketService = b.NewBucketService
	sourceBackend.NewQueryService = b.NewQueryService
	h.SourceHandler = NewSourceHandler(sourceBackend)

	setupBackend := NewSetupBackend(b)
	h.SetupHandler = NewSetupHandler(setupBackend)

	taskBackend := NewTaskBackend(b)
	h.TaskHandler = NewTaskHandler(taskBackend)
	h.TaskHandler.UserResourceMappingService = internalURM

	telegrafBackend := NewTelegrafBackend(b)
	telegrafBackend.TelegrafService = authorizer.NewTelegrafConfigService(b.TelegrafService, b.UserResourceMappingService)
	h.TelegrafHandler = NewTelegrafHandler(telegrafBackend)

	writeBackend := NewWriteBackend(b)
	h.WriteHandler = NewWriteHandler(writeBackend)

	fluxBackend := NewFluxBackend(b)
	h.QueryHandler = NewFluxHandler(fluxBackend)

	h.ProtoHandler = NewProtoHandler(NewProtoBackend(b))
	h.ChronografHandler = NewChronografHandler(b.ChronografService)
	h.SwaggerHandler = SwaggerHandler()
	h.LabelHandler = NewLabelHandler(b.LabelService)

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
	"labels":    "/api/v2/labels",
	"variables": "/api/v2/variables",
	"me":        "/api/v2/me",
	"orgs":      "/api/v2/orgs",
	"protos":    "/api/v2/protos",
	"query": map[string]string{
		"self":        "/api/v2/query",
		"ast":         "/api/v2/query/ast",
		"analyze":     "/api/v2/query/analyze",
		"spec":        "/api/v2/query/spec",
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
	"telegrafs": "/api/v2/telegrafs",
	"users":     "/api/v2/users",
	"write":     "/api/v2/write",
}

func (h *APIHandler) serveLinks(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	if err := encodeResponse(ctx, w, http.StatusOK, apiLinks); err != nil {
		EncodeError(ctx, err, w)
		return
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

	if strings.HasPrefix(r.URL.Path, "/api/v2/telegrafs") {
		h.TelegrafHandler.ServeHTTP(w, r)
		return
	}

	if strings.HasPrefix(r.URL.Path, "/api/v2/variables") {
		h.VariableHandler.ServeHTTP(w, r)
		return
	}

	if strings.HasPrefix(r.URL.Path, "/api/v2/protos") {
		h.ProtoHandler.ServeHTTP(w, r)
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

	notFoundHandler(w, r)
}

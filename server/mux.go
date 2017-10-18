package server

import (
	"encoding/json"
	"fmt"
	"net/http"
	"path"
	"strconv"
	"strings"

	"github.com/NYTimes/gziphandler"
	"github.com/bouk/httprouter"
	"github.com/influxdata/chronograf" // When julienschmidt/httprouter v2 w/ context is out, switch
	"github.com/influxdata/chronograf/oauth2"
)

const (
	// JSONType the mimetype for a json request
	JSONType = "application/json"
)

// MuxOpts are the options for the router.  Mostly related to auth.
type MuxOpts struct {
	Logger        chronograf.Logger
	Develop       bool                 // Develop loads assets from filesystem instead of bindata
	Basepath      string               // URL path prefix under which all chronograf routes will be mounted
	PrefixRoutes  bool                 // Mounts all backend routes under route specified by the Basepath
	UseAuth       bool                 // UseAuth turns on Github OAuth and JWT
	Auth          oauth2.Authenticator // Auth is used to authenticate and authorize
	ProviderFuncs []func(func(oauth2.Provider, oauth2.Mux))
	StatusFeedURL string            // JSON Feed URL for the client Status page News Feed
	CustomLinks   map[string]string // Any custom external links for client's User menu
}

// NewMux attaches all the route handlers; handler returned servers chronograf.
func NewMux(opts MuxOpts, service Service) http.Handler {
	hr := httprouter.New()

	/* React Application */
	assets := Assets(AssetsOpts{
		Develop: opts.Develop,
		Logger:  opts.Logger,
	})

	// Prefix any URLs found in the React assets with any configured basepath
	prefixedAssets := NewDefaultURLPrefixer(basepath, assets, opts.Logger)

	// Compress the assets with gzip if an accepted encoding
	compressed := gziphandler.GzipHandler(prefixedAssets)

	// The react application handles all the routing if the server does not
	// know about the route.  This means that we never have unknown routes on
	// the server.
	hr.NotFound = compressed

	var router chronograf.Router = hr

	// Set route prefix for all routes if basepath is present
	if opts.PrefixRoutes {
		router = &MountableRouter{
			Prefix:   opts.Basepath,
			Delegate: hr,
		}

		//The assets handler is always unaware of basepaths, so the
		// basepath needs to always be removed before sending requests to it
		hr.NotFound = http.StripPrefix(opts.Basepath, hr.NotFound)
	}

	EnsureViewer := func(next http.HandlerFunc) http.HandlerFunc {
		return AuthorizedUser(service.UsersStore, opts.UseAuth, ViewerRoleName, opts.Logger, next)
	}
	EnsureEditor := func(next http.HandlerFunc) http.HandlerFunc {
		return AuthorizedUser(service.UsersStore, opts.UseAuth, EditorRoleName, opts.Logger, next)
	}
	EnsureAdmin := func(next http.HandlerFunc) http.HandlerFunc {
		return AuthorizedUser(service.UsersStore, opts.UseAuth, AdminRoleName, opts.Logger, next)
	}

	/* Documentation */
	router.GET("/swagger.json", Spec())
	router.GET("/docs", Redoc("/swagger.json"))

	/* API */
	// Sources
	router.GET("/chronograf/v1/sources", EnsureViewer(service.Sources))
	router.POST("/chronograf/v1/sources", EnsureEditor(service.NewSource))

	router.GET("/chronograf/v1/sources/:id", EnsureViewer(service.SourcesID))
	router.PATCH("/chronograf/v1/sources/:id", EnsureEditor(service.UpdateSource))
	router.DELETE("/chronograf/v1/sources/:id", EnsureEditor(service.RemoveSource))

	// Source Proxy to Influx; Has gzip compression around the handler
	influx := gziphandler.GzipHandler(http.HandlerFunc(EnsureViewer(service.Influx)))
	router.Handler("POST", "/chronograf/v1/sources/:id/proxy", influx)

	// Write proxies line protocol write requests to InfluxDB
	router.POST("/chronograf/v1/sources/:id/write", EnsureEditor(service.Write))

	// Queries is used to analyze a specific queries
	router.POST("/chronograf/v1/sources/:id/queries", EnsureEditor(service.Queries))

	// All possible permissions for users in this source
	router.GET("/chronograf/v1/sources/:id/permissions", EnsureViewer(service.Permissions))

	// Users associated with the data source
	router.GET("/chronograf/v1/sources/:id/users", EnsureViewer(service.SourceUsers))
	router.POST("/chronograf/v1/sources/:id/users", EnsureEditor(service.NewSourceUser))

	router.GET("/chronograf/v1/sources/:id/users/:uid", EnsureViewer(service.SourceUserID))
	router.DELETE("/chronograf/v1/sources/:id/users/:uid", EnsureEditor(service.RemoveSourceUser))
	router.PATCH("/chronograf/v1/sources/:id/users/:uid", EnsureEditor(service.UpdateSourceUser))

	// Roles associated with the data source
	router.GET("/chronograf/v1/sources/:id/roles", EnsureViewer(service.SourceRoles))
	router.POST("/chronograf/v1/sources/:id/roles", EnsureEditor(service.NewSourceRole))

	router.GET("/chronograf/v1/sources/:id/roles/:rid", EnsureViewer(service.SourceRoleID))
	router.DELETE("/chronograf/v1/sources/:id/roles/:rid", EnsureEditor(service.RemoveSourceRole))
	router.PATCH("/chronograf/v1/sources/:id/roles/:rid", EnsureEditor(service.UpdateSourceRole))

	// Kapacitor
	router.GET("/chronograf/v1/sources/:id/kapacitors", EnsureViewer(service.Kapacitors))
	router.POST("/chronograf/v1/sources/:id/kapacitors", EnsureEditor(service.NewKapacitor))

	router.GET("/chronograf/v1/sources/:id/kapacitors/:kid", EnsureViewer(service.KapacitorsID))
	router.PATCH("/chronograf/v1/sources/:id/kapacitors/:kid", EnsureEditor(service.UpdateKapacitor))
	router.DELETE("/chronograf/v1/sources/:id/kapacitors/:kid", EnsureEditor(service.RemoveKapacitor))

	// Kapacitor rules
	router.GET("/chronograf/v1/sources/:id/kapacitors/:kid/rules", EnsureViewer(service.KapacitorRulesGet))
	router.POST("/chronograf/v1/sources/:id/kapacitors/:kid/rules", EnsureEditor(service.KapacitorRulesPost))

	router.GET("/chronograf/v1/sources/:id/kapacitors/:kid/rules/:tid", EnsureViewer(service.KapacitorRulesID))
	router.PUT("/chronograf/v1/sources/:id/kapacitors/:kid/rules/:tid", EnsureEditor(service.KapacitorRulesPut))
	router.PATCH("/chronograf/v1/sources/:id/kapacitors/:kid/rules/:tid", EnsureEditor(service.KapacitorRulesStatus))
	router.DELETE("/chronograf/v1/sources/:id/kapacitors/:kid/rules/:tid", EnsureEditor(service.KapacitorRulesDelete))

	// Kapacitor Proxy
	router.GET("/chronograf/v1/sources/:id/kapacitors/:kid/proxy", EnsureViewer(service.KapacitorProxyGet))
	router.POST("/chronograf/v1/sources/:id/kapacitors/:kid/proxy", EnsureEditor(service.KapacitorProxyPost))
	router.PATCH("/chronograf/v1/sources/:id/kapacitors/:kid/proxy", EnsureEditor(service.KapacitorProxyPatch))
	router.DELETE("/chronograf/v1/sources/:id/kapacitors/:kid/proxy", EnsureEditor(service.KapacitorProxyDelete))

	// Mappings
	router.GET("/chronograf/v1/mappings", EnsureViewer(service.GetMappings))

	// Layouts
	router.GET("/chronograf/v1/layouts", EnsureViewer(service.Layouts))
	router.POST("/chronograf/v1/layouts", EnsureEditor(service.NewLayout))

	router.GET("/chronograf/v1/layouts/:id", EnsureViewer(service.LayoutsID))
	router.PUT("/chronograf/v1/layouts/:id", EnsureEditor(service.UpdateLayout))
	router.DELETE("/chronograf/v1/layouts/:id", EnsureEditor(service.RemoveLayout))

	// Users associated with Chronograf
	router.GET("/chronograf/v1/me", EnsureViewer(service.Me))

	router.GET("/chronograf/v1/users", EnsureAdmin(service.Users))
	router.POST("/chronograf/v1/users", EnsureAdmin(service.NewUser))

	router.GET("/chronograf/v1/users/:id", EnsureAdmin(service.UserID))
	router.DELETE("/chronograf/v1/users/:id", EnsureAdmin(service.RemoveUser))
	router.PATCH("/chronograf/v1/users/:id", EnsureAdmin(service.UpdateUser))

	// Dashboards
	router.GET("/chronograf/v1/dashboards", EnsureViewer(service.Dashboards))
	router.POST("/chronograf/v1/dashboards", EnsureEditor(service.NewDashboard))

	router.GET("/chronograf/v1/dashboards/:id", EnsureViewer(service.DashboardID))
	router.DELETE("/chronograf/v1/dashboards/:id", EnsureEditor(service.RemoveDashboard))
	router.PUT("/chronograf/v1/dashboards/:id", EnsureEditor(service.ReplaceDashboard))
	router.PATCH("/chronograf/v1/dashboards/:id", EnsureEditor(service.UpdateDashboard))
	// Dashboard Cells
	router.GET("/chronograf/v1/dashboards/:id/cells", EnsureViewer(service.DashboardCells))
	router.POST("/chronograf/v1/dashboards/:id/cells", EnsureEditor(service.NewDashboardCell))

	router.GET("/chronograf/v1/dashboards/:id/cells/:cid", EnsureViewer(service.DashboardCellID))
	router.DELETE("/chronograf/v1/dashboards/:id/cells/:cid", EnsureEditor(service.RemoveDashboardCell))
	router.PUT("/chronograf/v1/dashboards/:id/cells/:cid", EnsureEditor(service.ReplaceDashboardCell))
	// Dashboard Templates
	router.GET("/chronograf/v1/dashboards/:id/templates", EnsureViewer(service.Templates))
	router.POST("/chronograf/v1/dashboards/:id/templates", EnsureEditor(service.NewTemplate))

	router.GET("/chronograf/v1/dashboards/:id/templates/:tid", EnsureViewer(service.TemplateID))
	router.DELETE("/chronograf/v1/dashboards/:id/templates/:tid", EnsureEditor(service.RemoveTemplate))
	router.PUT("/chronograf/v1/dashboards/:id/templates/:tid", EnsureEditor(service.ReplaceTemplate))

	// Databases
	router.GET("/chronograf/v1/sources/:id/dbs", EnsureViewer(service.GetDatabases))
	router.POST("/chronograf/v1/sources/:id/dbs", EnsureEditor(service.NewDatabase))

	router.DELETE("/chronograf/v1/sources/:id/dbs/:dbid", EnsureAdmin(service.DropDatabase))

	// Retention Policies
	router.GET("/chronograf/v1/sources/:id/dbs/:dbid/rps", EnsureViewer(service.RetentionPolicies))
	router.POST("/chronograf/v1/sources/:id/dbs/:dbid/rps", EnsureEditor(service.NewRetentionPolicy))

	router.PUT("/chronograf/v1/sources/:id/dbs/:dbid/rps/:rpid", EnsureEditor(service.UpdateRetentionPolicy))
	router.DELETE("/chronograf/v1/sources/:id/dbs/:dbid/rps/:rpid", EnsureAdmin(service.DropRetentionPolicy))

	allRoutes := &AllRoutes{
		Logger:      opts.Logger,
		StatusFeed:  opts.StatusFeedURL,
		CustomLinks: opts.CustomLinks,
	}

	router.Handler("GET", "/chronograf/v1/", allRoutes)

	var out http.Handler

	basepath := ""
	if opts.PrefixRoutes {
		basepath = opts.Basepath
	}

	/* Authentication */
	if opts.UseAuth {
		// Encapsulate the router with OAuth2
		var auth http.Handler
		auth, allRoutes.AuthRoutes = AuthAPI(opts, router)
		allRoutes.LogoutLink = "/oauth/logout"

		// Create middleware that redirects to the appropriate provider logout
		router.GET(allRoutes.LogoutLink, Logout("/", basepath, allRoutes.AuthRoutes))
		out = Logger(opts.Logger, PrefixedRedirect(opts.Basepath, auth))
	} else {
		out = Logger(opts.Logger, PrefixedRedirect(opts.Basepath, router))
	}

	return out
}

// AuthAPI adds the OAuth routes if auth is enabled.
func AuthAPI(opts MuxOpts, router chronograf.Router) (http.Handler, AuthRoutes) {
	routes := AuthRoutes{}
	for _, pf := range opts.ProviderFuncs {
		pf(func(p oauth2.Provider, m oauth2.Mux) {
			urlName := PathEscape(strings.ToLower(p.Name()))

			loginPath := path.Join("/oauth", urlName, "login")
			logoutPath := path.Join("/oauth", urlName, "logout")
			callbackPath := path.Join("/oauth", urlName, "callback")

			router.Handler("GET", loginPath, m.Login())
			router.Handler("GET", logoutPath, m.Logout())
			router.Handler("GET", callbackPath, m.Callback())
			routes = append(routes, AuthRoute{
				Name:  p.Name(),
				Label: strings.Title(p.Name()),
				// AuthRoutes are content served to the page. When Basepath is set, it
				// says that all content served to the page will be prefixed with the
				// basepath. Since these routes are consumed by JS, it will need the
				// basepath set to traverse a proxy correctly
				Login:    path.Join(opts.Basepath, loginPath),
				Logout:   path.Join(opts.Basepath, logoutPath),
				Callback: path.Join(opts.Basepath, callbackPath),
			})
		})
	}

	rootPath := "/chronograf/v1"
	logoutPath := "/oauth/logout"

	if opts.PrefixRoutes {
		rootPath = path.Join(opts.Basepath, rootPath)
		logoutPath = path.Join(opts.Basepath, logoutPath)
	}

	tokenMiddleware := AuthorizedToken(opts.Auth, opts.Logger, router)
	// Wrap the API with token validation middleware.
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cleanPath := path.Clean(r.URL.Path) // compare ignoring path garbage, trailing slashes, etc.
		if (strings.HasPrefix(cleanPath, rootPath) && len(cleanPath) > len(rootPath)) || cleanPath == logoutPath {
			tokenMiddleware.ServeHTTP(w, r)
			return
		}
		router.ServeHTTP(w, r)
	}), routes
}

func encodeJSON(w http.ResponseWriter, status int, v interface{}, logger chronograf.Logger) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		unknownErrorWithMessage(w, err, logger)
	}
}

// Error writes an JSON message
func Error(w http.ResponseWriter, code int, msg string, logger chronograf.Logger) {
	e := ErrorMessage{
		Code:    code,
		Message: msg,
	}
	b, err := json.Marshal(e)
	if err != nil {
		code = http.StatusInternalServerError
		b = []byte(`{"code": 500, "message":"server_error"}`)
	}

	logger.
		WithField("component", "server").
		WithField("http_status ", code).
		Error("Error message ", msg)
	w.Header().Set("Content-Type", JSONType)
	w.WriteHeader(code)
	_, _ = w.Write(b)
}

func invalidData(w http.ResponseWriter, err error, logger chronograf.Logger) {
	Error(w, http.StatusUnprocessableEntity, fmt.Sprintf("%v", err), logger)
}

func invalidJSON(w http.ResponseWriter, logger chronograf.Logger) {
	Error(w, http.StatusBadRequest, "Unparsable JSON", logger)
}

func unknownErrorWithMessage(w http.ResponseWriter, err error, logger chronograf.Logger) {
	Error(w, http.StatusInternalServerError, fmt.Sprintf("Unknown error: %v", err), logger)
}

func notFound(w http.ResponseWriter, id int, logger chronograf.Logger) {
	Error(w, http.StatusNotFound, fmt.Sprintf("ID %d not found", id), logger)
}

func paramID(key string, r *http.Request) (int, error) {
	ctx := r.Context()
	param := httprouter.GetParamFromContext(ctx, key)
	id, err := strconv.Atoi(param)
	if err != nil {
		return -1, fmt.Errorf("Error converting ID %s", param)
	}
	return id, nil
}

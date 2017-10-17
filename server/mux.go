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

	/* Documentation */
	router.GET("/swagger.json", Spec())
	router.GET("/docs", Redoc("/swagger.json"))

	/* API */
	// Sources
	router.GET("/chronograf/v1/sources", service.Sources)
	router.POST("/chronograf/v1/sources", service.NewSource)

	router.GET("/chronograf/v1/sources/:id", service.SourcesID)
	router.PATCH("/chronograf/v1/sources/:id", service.UpdateSource)
	router.DELETE("/chronograf/v1/sources/:id", service.RemoveSource)

	// Source Proxy to Influx; Has gzip compression around the handler
	influx := gziphandler.GzipHandler(http.HandlerFunc(service.Influx))
	router.Handler("POST", "/chronograf/v1/sources/:id/proxy", influx)

	// Write proxies line protocol write requests to InfluxDB
	router.POST("/chronograf/v1/sources/:id/write", service.Write)

	// Queries is used to analyze a specific queries
	router.POST("/chronograf/v1/sources/:id/queries", service.Queries)

	// All possible permissions for users in this source
	router.GET("/chronograf/v1/sources/:id/permissions", service.Permissions)

	// Users associated with the data source
	router.GET("/chronograf/v1/sources/:id/users", service.SourceUsers)
	router.POST("/chronograf/v1/sources/:id/users", service.NewSourceUser)

	router.GET("/chronograf/v1/sources/:id/users/:uid", service.SourceUserID)
	router.DELETE("/chronograf/v1/sources/:id/users/:uid", service.RemoveSourceUser)
	router.PATCH("/chronograf/v1/sources/:id/users/:uid", service.UpdateSourceUser)

	// Roles associated with the data source
	router.GET("/chronograf/v1/sources/:id/roles", service.SourceRoles)
	router.POST("/chronograf/v1/sources/:id/roles", service.NewSourceRole)

	router.GET("/chronograf/v1/sources/:id/roles/:rid", service.SourceRoleID)
	router.DELETE("/chronograf/v1/sources/:id/roles/:rid", service.RemoveSourceRole)
	router.PATCH("/chronograf/v1/sources/:id/roles/:rid", service.UpdateSourceRole)

	// Kapacitor
	router.GET("/chronograf/v1/sources/:id/kapacitors", service.Kapacitors)
	router.POST("/chronograf/v1/sources/:id/kapacitors", service.NewKapacitor)

	router.GET("/chronograf/v1/sources/:id/kapacitors/:kid", service.KapacitorsID)
	router.PATCH("/chronograf/v1/sources/:id/kapacitors/:kid", service.UpdateKapacitor)
	router.DELETE("/chronograf/v1/sources/:id/kapacitors/:kid", service.RemoveKapacitor)

	// Kapacitor rules
	router.GET("/chronograf/v1/sources/:id/kapacitors/:kid/rules", service.KapacitorRulesGet)
	router.POST("/chronograf/v1/sources/:id/kapacitors/:kid/rules", service.KapacitorRulesPost)

	router.GET("/chronograf/v1/sources/:id/kapacitors/:kid/rules/:tid", service.KapacitorRulesID)
	router.PUT("/chronograf/v1/sources/:id/kapacitors/:kid/rules/:tid", service.KapacitorRulesPut)
	router.PATCH("/chronograf/v1/sources/:id/kapacitors/:kid/rules/:tid", service.KapacitorRulesStatus)
	router.DELETE("/chronograf/v1/sources/:id/kapacitors/:kid/rules/:tid", service.KapacitorRulesDelete)

	// Kapacitor Proxy
	router.GET("/chronograf/v1/sources/:id/kapacitors/:kid/proxy", service.KapacitorProxyGet)
	router.POST("/chronograf/v1/sources/:id/kapacitors/:kid/proxy", service.KapacitorProxyPost)
	router.PATCH("/chronograf/v1/sources/:id/kapacitors/:kid/proxy", service.KapacitorProxyPatch)
	router.DELETE("/chronograf/v1/sources/:id/kapacitors/:kid/proxy", service.KapacitorProxyDelete)

	// Mappings
	router.GET("/chronograf/v1/mappings", service.GetMappings)

	// Layouts
	router.GET("/chronograf/v1/layouts", service.Layouts)
	router.POST("/chronograf/v1/layouts", service.NewLayout)

	router.GET("/chronograf/v1/layouts/:id", service.LayoutsID)
	router.PUT("/chronograf/v1/layouts/:id", service.UpdateLayout)
	router.DELETE("/chronograf/v1/layouts/:id", service.RemoveLayout)

	// Users associated with Chronograf
	router.GET("/chronograf/v1/me", service.Me)

	router.GET("/chronograf/v1/users", service.Users)
	router.POST("/chronograf/v1/users", service.NewUser)

	router.GET("/chronograf/v1/users/:id", service.UserID)
	router.DELETE("/chronograf/v1/users/:id", service.RemoveUser)
	router.PATCH("/chronograf/v1/users/:id", service.UpdateUser)

	// Dashboards
	router.GET("/chronograf/v1/dashboards", service.Dashboards)
	router.POST("/chronograf/v1/dashboards", service.NewDashboard)

	router.GET("/chronograf/v1/dashboards/:id", service.DashboardID)
	router.DELETE("/chronograf/v1/dashboards/:id", service.RemoveDashboard)
	router.PUT("/chronograf/v1/dashboards/:id", service.ReplaceDashboard)
	router.PATCH("/chronograf/v1/dashboards/:id", service.UpdateDashboard)
	// Dashboard Cells
	router.GET("/chronograf/v1/dashboards/:id/cells", service.DashboardCells)
	router.POST("/chronograf/v1/dashboards/:id/cells", service.NewDashboardCell)

	router.GET("/chronograf/v1/dashboards/:id/cells/:cid", service.DashboardCellID)
	router.DELETE("/chronograf/v1/dashboards/:id/cells/:cid", service.RemoveDashboardCell)
	router.PUT("/chronograf/v1/dashboards/:id/cells/:cid", service.ReplaceDashboardCell)
	// Dashboard Templates
	router.GET("/chronograf/v1/dashboards/:id/templates", service.Templates)
	router.POST("/chronograf/v1/dashboards/:id/templates", service.NewTemplate)

	router.GET("/chronograf/v1/dashboards/:id/templates/:tid", service.TemplateID)
	router.DELETE("/chronograf/v1/dashboards/:id/templates/:tid", service.RemoveTemplate)
	router.PUT("/chronograf/v1/dashboards/:id/templates/:tid", service.ReplaceTemplate)

	// Databases
	router.GET("/chronograf/v1/sources/:id/dbs", service.GetDatabases)
	router.POST("/chronograf/v1/sources/:id/dbs", service.NewDatabase)

	router.DELETE("/chronograf/v1/sources/:id/dbs/:dbid", service.DropDatabase)

	// Retention Policies
	router.GET("/chronograf/v1/sources/:id/dbs/:dbid/rps", service.RetentionPolicies)
	router.POST("/chronograf/v1/sources/:id/dbs/:dbid/rps", service.NewRetentionPolicy)

	router.PUT("/chronograf/v1/sources/:id/dbs/:dbid/rps/:rpid", service.UpdateRetentionPolicy)
	router.DELETE("/chronograf/v1/sources/:id/dbs/:dbid/rps/:rpid", service.DropRetentionPolicy)

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

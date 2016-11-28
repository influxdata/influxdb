package server

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/bouk/httprouter"
	"github.com/influxdata/chronograf" // When julienschmidt/httprouter v2 w/ context is out, switch
	"github.com/influxdata/chronograf/jwt"
)

const (
	// JSONType the mimetype for a json request
	JSONType = "application/json"
)

// MuxOpts are the options for the router.  Mostly related to auth.
type MuxOpts struct {
	Logger             chronograf.Logger
	Develop            bool   // Develop loads assets from filesystem instead of bindata
	UseAuth            bool   // UseAuth turns on Github OAuth and JWT
	TokenSecret        string // TokenSecret is the JWT secret
	GithubClientID     string // GithubClientID is the GH OAuth id
	GithubClientSecret string // GithubClientSecret is the GH OAuth secret
}

// NewMux attaches all the route handlers; handler returned servers chronograf.
func NewMux(opts MuxOpts, service Service) http.Handler {
	router := httprouter.New()

	/* React Application */
	assets := Assets(AssetsOpts{
		Develop: opts.Develop,
		Logger:  opts.Logger,
	})
	// The react application handles all the routing if the server does not
	// know about the route.  This means that we never have unknown
	// routes on the server.
	router.NotFound = assets

	/* Documentation */
	router.GET("/swagger.json", Spec())
	router.GET("/docs", Redoc("/swagger.json"))

	/* API */
	// Root Routes returns all top-level routes in the API
	router.GET("/chronograf/v1/", AllRoutes(opts.Logger))

	// Sources
	router.GET("/chronograf/v1/sources", service.Sources)
	router.POST("/chronograf/v1/sources", service.NewSource)

	router.GET("/chronograf/v1/sources/:id", service.SourcesID)
	router.PATCH("/chronograf/v1/sources/:id", service.UpdateSource)
	router.DELETE("/chronograf/v1/sources/:id", service.RemoveSource)

	// Source Proxy
	router.POST("/chronograf/v1/sources/:id/proxy", service.Proxy)

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

	// Users
	router.GET("/chronograf/v1/me", service.Me)
	router.POST("/chronograf/v1/users", service.NewUser)

	router.GET("/chronograf/v1/users/:id", service.UserID)
	router.PATCH("/chronograf/v1/users/:id", service.UpdateUser)
	router.DELETE("/chronograf/v1/users/:id", service.RemoveUser)

	// Explorations
	router.GET("/chronograf/v1/users/:id/explorations", service.Explorations)
	router.POST("/chronograf/v1/users/:id/explorations", service.NewExploration)

	router.GET("/chronograf/v1/users/:id/explorations/:eid", service.ExplorationsID)
	router.PATCH("/chronograf/v1/users/:id/explorations/:eid", service.UpdateExploration)
	router.DELETE("/chronograf/v1/users/:id/explorations/:eid", service.RemoveExploration)

	/* Authentication */
	if opts.UseAuth {
		auth := AuthAPI(opts, router)
		return Logger(opts.Logger, auth)
	}
	return Logger(opts.Logger, router)
}

// AuthAPI adds the OAuth routes if auth is enabled.
func AuthAPI(opts MuxOpts, router *httprouter.Router) http.Handler {
	auth := jwt.NewJWT(opts.TokenSecret)

	successURL := "/"
	failureURL := "/login"
	gh := NewGithub(
		opts.GithubClientID,
		opts.GithubClientSecret,
		successURL,
		failureURL,
		&auth,
		opts.Logger,
	)

	router.GET("/oauth/github", gh.Login())
	router.GET("/oauth/logout", gh.Logout())
	router.GET("/oauth/github/callback", gh.Callback())

	tokenMiddleware := AuthorizedToken(&auth, &CookieExtractor{Name: "session"}, opts.Logger, router)
	// Wrap the API with token validation middleware.
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/chronograf/v1/") {
			tokenMiddleware.ServeHTTP(w, r)
			return
		}
		router.ServeHTTP(w, r)
	})
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
	w.Write(b)
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

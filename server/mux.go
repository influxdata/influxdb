package server

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/bouk/httprouter"
	"github.com/influxdata/chronograf" // When julienschmidt/httprouter v2 w/ context is out, switch "github.com/influxdata/chronograf
	"github.com/influxdata/chronograf/jwt"
)

const (
	// JSONType the mimetype for a json request
	JSONType = "application/json"
)

var (
	httpAPIRoot               = "/chronograf/v1/"
	httpAPILayouts            = "/chronograf/v1/layouts"
	httpAPILayoutsID          = "/chronograf/v1/layouts/:id"
	httpAPIMappings           = "/chronograf/v1/mappings"
	httpAPISrcs               = "/chronograf/v1/sources"
	httpAPISrcsID             = "/chronograf/v1/sources/:id"
	httpAPISrcsIDProxy        = "/chronograf/v1/sources/:id/proxy"
	httpAPISrcsIDKapas        = "/chronograf/v1/sources/:id/kapacitors"
	httpAPISrcsIDKapasID      = "/chronograf/v1/sources/:id/kapacitors/:kid"
	httpAPISrcsIDKapasIDProxy = "/chronograf/v1/sources/:id/kapacitors/:kid/proxy"
	httpAPIUsrs               = "/chronograf/v1/users"
	httpAPIUsrsID             = "/chronograf/v1/users/:id"
	httpAPIUsrsIDExps         = "/chronograf/v1/users/:id/explorations"
	httpAPIUsrsIDExpsID       = "/chronograf/v1/users/:id/explorations/:eid"

	httpOAuth           = "/oauth"
	httpOAuthLogout     = "/oauth/logout"
	httpOAuthGHCallback = "/oauth/github/callback"

	httpSwagger = "/swagger.json"
	httpDocs    = "/docs"
)

type MuxOpts struct {
	Develop            bool
	UseAuth            bool
	TokenSecret        string
	GithubClientID     string
	GithubClientSecret string
	Logger             chronograf.Logger
}

func NewMux(opts MuxOpts, store Store, proxy InfluxProxy) http.Handler {
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
	router.GET(httpSwagger, Spec())
	router.GET(httpDocs, Redoc(httpSwagger))

	/* API */
	// Root Routes returns all top-level routes in the API
	router.GET(httpAPIRoot, AllRoutes(opts.Logger))

	// Sources
	router.GET(httpAPISrcs, store.Sources)
	router.POST(httpAPISrcs, store.NewSource)

	router.GET(httpAPISrcsID, store.SourcesID)
	router.PATCH(httpAPISrcsID, store.UpdateSource)
	router.DELETE(httpAPISrcsID, store.RemoveSource)

	// Source Proxy
	router.POST(httpAPISrcsIDProxy, proxy.Proxy)

	// Kapacitor
	router.GET(httpAPISrcsIDKapas, store.Kapacitors)
	router.POST(httpAPISrcsIDKapas, store.NewKapacitor)

	router.GET(httpAPISrcsIDKapasID, store.KapacitorsID)
	router.PATCH(httpAPISrcsIDKapasID, store.UpdateKapacitor)
	router.DELETE(httpAPISrcsIDKapasID, store.RemoveKapacitor)

	// Kapacitor Proxy
	router.GET(httpAPISrcsIDKapasIDProxy, proxy.KapacitorProxyGet)
	router.POST(httpAPISrcsIDKapasIDProxy, proxy.KapacitorProxyPost)
	router.PATCH(httpAPISrcsIDKapasIDProxy, proxy.KapacitorProxyPatch)
	router.DELETE(httpAPISrcsIDKapasIDProxy, proxy.KapacitorProxyDelete)

	// Mappings
	router.GET(httpAPIMappings, store.GetMappings)

	// Layouts
	router.GET(httpAPILayouts, store.Layouts)
	router.POST(httpAPILayouts, store.NewLayout)

	router.GET(httpAPILayoutsID, store.LayoutsID)
	router.PUT(httpAPILayoutsID, store.UpdateLayout)
	router.DELETE(httpAPILayoutsID, store.RemoveLayout)

	// Users
	/*
		router.GET(httpAPIUsrs, Users)
		router.POST(httpAPIUsrs, NewUser)

		router.GET(httpAPIUsrsID, UsersID)
		router.PATCH(httpAPIUsrsID, UpdateUser)
		router.DELETE(httpAPIUsrsID, RemoveUser)
	*/
	// Explorations
	router.GET(httpAPIUsrsIDExps, store.Explorations)
	router.POST(httpAPIUsrsIDExps, store.NewExploration)

	router.GET(httpAPIUsrsIDExpsID, store.ExplorationsID)
	router.PATCH(httpAPIUsrsIDExpsID, store.UpdateExploration)
	router.DELETE(httpAPIUsrsIDExpsID, store.RemoveExploration)

	/* Authentication */
	if opts.UseAuth {
		auth := AuthAPI(opts, router)
		return Logger(opts.Logger, auth)
	}
	return Logger(opts.Logger, router)
}

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

	router.GET(httpOAuth, gh.Login())
	router.GET(httpOAuthLogout, gh.Logout())
	router.GET(httpOAuthGHCallback, gh.Callback())

	tokenMiddleware := AuthorizedToken(&auth, &CookieExtractor{Name: "session"}, opts.Logger, router)
	// Wrap the API with token validation middleware.
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, httpAPIRoot) {
			tokenMiddleware.ServeHTTP(w, r)
			return
		} else {
			router.ServeHTTP(w, r)
		}
	})
}

func encodeJSON(w http.ResponseWriter, status int, v interface{}, logger chronograf.Logger) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		unknownErrorWithMessage(w, err)
	}
}

func Error(w http.ResponseWriter, code int, msg string) {
	e := struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
	}{
		Code:    code,
		Message: msg,
	}
	b, err := json.Marshal(e)
	if err != nil {
		//log.Print("go-oidc: failed to marshal %#v: %v", e, err)
		code = http.StatusInternalServerError
		b = []byte(`{"code": 500, "message":"server_error"}`)
	}
	w.Header().Set("Content-Type", JSONType)
	w.WriteHeader(code)
	w.Write(b)
}

func invalidData(w http.ResponseWriter, err error) {
	Error(w, http.StatusUnprocessableEntity, fmt.Sprintf("%v", err))
}

func invalidJSON(w http.ResponseWriter) {
	Error(w, http.StatusBadRequest, "Unparsable JSON")
}

func unknownErrorWithMessage(w http.ResponseWriter, err error) {
	Error(w, http.StatusInternalServerError, fmt.Sprintf("Unknown error: %v", err))
}

func notFound(w http.ResponseWriter, id int) {
	Error(w, http.StatusNotFound, fmt.Sprintf("ID %d not found", id))
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

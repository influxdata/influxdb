package httpd

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"expvar"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	httppprof "net/http/pprof"
	"os"
	"runtime/debug"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/bmizerany/pat"
	"github.com/golang-jwt/jwt"
	"github.com/golang/snappy"
	"github.com/influxdata/flux"
	"github.com/influxdata/flux/lang"
	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/logger"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/monitor"
	"github.com/influxdata/influxdb/monitor/diagnostics"
	"github.com/influxdata/influxdb/prometheus"
	"github.com/influxdata/influxdb/query"
	"github.com/influxdata/influxdb/services/meta"
	"github.com/influxdata/influxdb/services/storage"
	"github.com/influxdata/influxdb/storage/reads"
	"github.com/influxdata/influxdb/storage/reads/datatypes"
	"github.com/influxdata/influxdb/tsdb"
	"github.com/influxdata/influxdb/uuid"
	"github.com/influxdata/influxql"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/prometheus/prompb"
	"go.uber.org/zap"
)

var ErrDiagnosticsValueMissing = errors.New("expected diagnostic value missing")

const (
	// DefaultChunkSize specifies the maximum number of points that will
	// be read before sending results back to the engine.
	//
	// This has no relation to the number of bytes that are returned.
	DefaultChunkSize = 10000

	DefaultDebugRequestsInterval = 10 * time.Second

	MaxDebugRequestsInterval = 6 * time.Hour
)

// AuthenticationMethod defines the type of authentication used.
type AuthenticationMethod int

// Supported authentication methods.
const (
	// Authenticate using basic authentication.
	UserAuthentication AuthenticationMethod = iota

	// Authenticate with jwt.
	BearerAuthentication
)

// TODO: Check HTTP response codes: 400, 401, 403, 409.

// Route specifies how to handle a HTTP verb for a given endpoint.
type Route struct {
	Name           string
	Method         string
	Pattern        string
	Gzipped        bool
	LoggingEnabled bool
	HandlerFunc    interface{}
}

type QueryAuthorizer interface {
	AuthorizeQuery(u meta.User, query *influxql.Query, database string) (query.FineAuthorizer, error)
	AuthorizeDatabase(u meta.User, priv influxql.Privilege, database string) error
	AuthorizeCreateDatabase(u meta.User) error
	AuthorizeCreateRetentionPolicy(u meta.User, db string) error
	AuthorizeDeleteRetentionPolicy(u meta.User, db string) error
}

// userQueryAuthorizer binds the QueryAuthorizer with a specific user for consumption by the query engine.
type userQueryAuthorizer struct {
	auth QueryAuthorizer
	user meta.User
}

func (a *userQueryAuthorizer) AuthorizeDatabase(p influxql.Privilege, name string) bool {
	return a.auth.AuthorizeDatabase(a.user, p, name) == nil
}

// Handler represents an HTTP handler for the InfluxDB server.
type Handler struct {
	mux       *pat.PatternServeMux
	Version   string
	BuildType string

	MetaClient interface {
		Database(name string) *meta.DatabaseInfo
		Databases() []meta.DatabaseInfo
		Authenticate(username, password string) (ui meta.User, err error)
		User(username string) (meta.User, error)
		AdminUserExists() bool
		CreateDatabaseWithRetentionPolicy(name string, spec *meta.RetentionPolicySpec) (*meta.DatabaseInfo, error)
		DropRetentionPolicy(database, name string) error
		CreateRetentionPolicy(database string, spec *meta.RetentionPolicySpec, makeDefault bool) (*meta.RetentionPolicyInfo, error)
		UpdateRetentionPolicy(database, name string, rpu *meta.RetentionPolicyUpdate, makeDefault bool) error
	}

	QueryAuthorizer QueryAuthorizer

	WriteAuthorizer interface {
		AuthorizeWrite(username, database string) error
	}

	QueryExecutor *query.Executor

	Monitor interface {
		Statistics(tags map[string]string) ([]*monitor.Statistic, error)
		Diagnostics() (map[string]*diagnostics.Diagnostics, error)
	}

	PointsWriter interface {
		WritePoints(database, retentionPolicy string, consistencyLevel models.ConsistencyLevel, user meta.User, points []models.Point) error
	}

	Store Store

	// Flux services
	Controller       Controller
	CompilerMappings flux.CompilerMappings

	Config           *Config
	Logger           *zap.Logger
	CLFLogger        *log.Logger
	accessLog        *os.File
	accessLogFilters StatusFilters
	stats            *Statistics

	requestTracker *RequestTracker
	writeThrottler *Throttler
}

// NewHandler returns a new instance of handler with routes.
func NewHandler(c Config) *Handler {
	h := &Handler{
		mux:            pat.New(),
		Config:         &c,
		Logger:         zap.NewNop(),
		CLFLogger:      log.New(os.Stderr, "[httpd] ", 0),
		stats:          &Statistics{},
		requestTracker: NewRequestTracker(),
	}

	// Limit the number of concurrent & enqueued write requests.
	h.writeThrottler = NewThrottler(c.MaxConcurrentWriteLimit, c.MaxEnqueuedWriteLimit)
	h.writeThrottler.EnqueueTimeout = c.EnqueuedWriteTimeout

	// Disable the write log if they have been suppressed.
	writeLogEnabled := c.LogEnabled
	if c.SuppressWriteLog {
		writeLogEnabled = false
	}

	var authWrapper func(handler func(http.ResponseWriter, *http.Request)) interface{}
	if h.Config.AuthEnabled && h.Config.PingAuthEnabled {
		authWrapper = func(handler func(http.ResponseWriter, *http.Request)) interface{} {
			return func(w http.ResponseWriter, r *http.Request, user meta.User) {
				handler(w, r)
			}
		}
	} else {
		authWrapper = func(handler func(http.ResponseWriter, *http.Request)) interface{} {
			return handler
		}
	}

	h.AddRoutes([]Route{
		Route{
			"query-options", // Satisfy CORS checks.
			"OPTIONS", "/query", false, true, h.serveOptions,
		},
		Route{
			"query", // Query serving route.
			"GET", "/query", true, true, h.serveQuery,
		},
		Route{
			"query", // Query serving route.
			"POST", "/query", true, true, h.serveQuery,
		},
		Route{
			"write-options", // Satisfy CORS checks.
			"OPTIONS", "/write", false, true, h.serveOptions,
		},
		Route{
			"write", // Data-ingest route.
			"POST", "/write", true, writeLogEnabled, h.serveWriteV1,
		},
		Route{
			"delete",
			"POST", "/api/v2/delete", false, true, h.serveDeleteV2,
		},
		Route{
			"create-bucket",
			"POST", "/api/v2/buckets", false, true, h.servePostCreateBucketV2,
		},
		Route{
			"delete-bucket",
			"DELETE", "/api/v2/buckets/:dbrp", false, true, h.serveDeleteBucketV2,
		},
		Route{
			"retrieve-bucket",
			"GET", "/api/v2/buckets/:dbrp", false, true, h.serveRetrieveBucketV2,
		},
		Route{
			"list-buckets",
			"GET", "/api/v2/buckets", false, true, h.serveListBucketsV2,
		},
		Route{
			"update-bucket",
			"PATCH", "/api/v2/buckets/:dbrp", false, true, h.serveUpdateBucketV2,
		},
		Route{
			"bucket-labels",
			"GET", "/api/v2/buckets/:dbrp/labels", false, true, h.serveLabelsNotAllowedV2,
		},
		Route{
			"add-bucket-label",
			"POST", "/api/v2/buckets/:dbrp/labels", false, true, h.serveLabelsNotAllowedV2,
		},
		Route{
			"delete-bucket-label",
			"DELETE", "/api/v2/buckets/:dbrp/labels/:labelID", false, true, h.serveLabelsNotAllowedV2,
		},
		Route{
			"bucket-members",
			"GET", "/api/v2/buckets/:dbrp/members", false, true, h.serveBucketMembersNotAllowedV2,
		},
		Route{
			"add-bucket-member",
			"POST", "/api/v2/buckets/:dbrp/members", false, true, h.serveBucketMembersNotAllowedV2,
		},
		Route{
			"delete-bucket-member",
			"DELETE", "/api/v2/buckets/:dbrp/members/:userID", false, true, h.serveBucketMembersNotAllowedV2,
		},
		Route{
			"bucket-owners",
			"GET", "/api/v2/buckets/:dbrp/owners", false, true, h.serveBucketOwnersNotAllowedV2,
		},
		Route{
			"add-bucket-owner",
			"POST", "/api/v2/buckets/:dbrp/owners", false, true, h.serveBucketOwnersNotAllowedV2,
		},
		Route{
			"delete-bucket-owner",
			"DELETE", "/api/v2/buckets/:dbrp/owners/:userID", false, true, h.serveBucketOwnersNotAllowedV2,
		},
		Route{
			"write", // Data-ingest route.
			"POST", "/api/v2/write", true, writeLogEnabled, h.serveWriteV2,
		},
		Route{ // Enable CORS
			"write-options",
			"OPTIONS", "/api/v2/write", false, true, h.serveOptions,
		},
		Route{
			"prometheus-write", // Prometheus remote write
			"POST", "/api/v1/prom/write", false, true, h.servePromWrite,
		},
		Route{
			"prometheus-read", // Prometheus remote read
			"POST", "/api/v1/prom/read", true, true, h.servePromRead,
		},
		Route{ // Ping
			"ping",
			"GET", "/ping", false, true, authWrapper(h.servePing),
		},
		Route{ // Ping
			"ping-head",
			"HEAD", "/ping", false, true, authWrapper(h.servePing),
		},
		Route{ // Ping w/ status
			"status",
			"GET", "/status", false, true, authWrapper(h.serveStatus),
		},
		Route{ // Ping w/ status
			"status-head",
			"HEAD", "/status", false, true, authWrapper(h.serveStatus),
		},
		Route{ // Health
			"health",
			"GET", "/health", false, true, authWrapper(h.serveHealth),
		},
		Route{ // Enable CORS
			"health-options",
			"OPTIONS", "/health", false, true, h.serveOptions,
		},
		Route{
			"prometheus-metrics",
			"GET", "/metrics", false, true, authWrapper(promhttp.Handler().ServeHTTP),
		},
	}...)

	// When PprofAuthEnabled is enabled, create debug/pprof endpoints with the
	// same authentication handlers as other endpoints.
	if h.Config.AuthEnabled && h.Config.PprofEnabled && h.Config.PprofAuthEnabled {
		authWrapper = func(handler func(http.ResponseWriter, *http.Request)) interface{} {
			return func(w http.ResponseWriter, r *http.Request, user meta.User) {
				// TODO: This is the only place we use AuthorizeUnrestricted. It would be better to use an explicit permission
				if user == nil || !user.AuthorizeUnrestricted() {
					h.Logger.Info("Unauthorized request", zap.String("user", user.ID()), zap.String("path", r.URL.Path))
					h.httpError(w, "error authorizing admin access", http.StatusForbidden)
					return
				}
				handler(w, r)
			}
		}
		h.AddRoutes([]Route{
			Route{
				"pprof-cmdline",
				"GET", "/debug/pprof/cmdline", true, true, authWrapper(httppprof.Cmdline),
			},
			Route{
				"pprof-profile",
				"GET", "/debug/pprof/profile", true, true, authWrapper(httppprof.Profile),
			},
			Route{
				"pprof-symbol",
				"GET", "/debug/pprof/symbol", true, true, authWrapper(httppprof.Symbol),
			},
			Route{
				"pprof-all",
				"GET", "/debug/pprof/all", true, true, authWrapper(h.archiveProfilesAndQueries),
			},
			Route{
				"debug-expvar",
				"GET", "/debug/vars", true, true, authWrapper(h.serveExpvar),
			},
			Route{
				"debug-requests",
				"GET", "/debug/requests", true, true, authWrapper(h.serveDebugRequests),
			},
		}...)
	}

	fluxRoute := Route{
		"flux-read",
		"POST", "/api/v2/query", true, true, nil,
	}
	fluxRouteCors := Route{
		"flux-read-options",
		"OPTIONS", "/api/v2/query", false, true, h.serveOptions,
	}

	if !c.FluxEnabled {
		fluxRoute.HandlerFunc = h.serveFluxQueryDisabled
	} else {
		fluxRoute.HandlerFunc = h.serveFluxQuery
	}
	h.AddRoutes(fluxRoute, fluxRouteCors)

	return h
}

func (h *Handler) Open() {
	if h.Config.LogEnabled {
		path := "stderr"

		if h.Config.AccessLogPath != "" {
			f, err := os.OpenFile(h.Config.AccessLogPath, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0666)
			if err != nil {
				h.Logger.Error("unable to open access log, falling back to stderr", zap.Error(err), zap.String("path", h.Config.AccessLogPath))
				return
			}
			h.CLFLogger = log.New(f, "", 0) // [httpd] prefix stripped when logging to a file
			h.accessLog = f
			path = h.Config.AccessLogPath
		}
		h.Logger.Info("opened HTTP access log", zap.String("path", path))
	}
	h.accessLogFilters = StatusFilters(h.Config.AccessLogStatusFilters)

	if h.Config.AuthEnabled && h.Config.SharedSecret == "" {
		h.Logger.Info("Auth is enabled but shared-secret is blank. BearerAuthentication is disabled.")
	}
}

func (h *Handler) Close() {

	// lets gracefully shut down http connections.  we'll give them 10 seconds
	// before we shut them down "with extreme predjudice".

	if h.accessLog != nil {
		h.accessLog.Close()
		h.accessLog = nil
		h.accessLogFilters = nil
	}
}

// Statistics maintains statistics for the httpd service.
type Statistics struct {
	Requests                         int64
	CQRequests                       int64
	QueryRequests                    int64
	WriteRequests                    int64
	PingRequests                     int64
	StatusRequests                   int64
	WriteRequestBytesReceived        int64
	QueryRequestBytesTransmitted     int64
	PointsWrittenOK                  int64
	PointsWrittenDropped             int64
	PointsWrittenFail                int64
	AuthenticationFailures           int64
	RequestDuration                  int64
	QueryRequestDuration             int64
	WriteRequestDuration             int64
	ActiveRequests                   int64
	ActiveWriteRequests              int64
	ClientErrors                     int64
	ServerErrors                     int64
	RecoveredPanics                  int64
	PromWriteRequests                int64
	PromReadRequests                 int64
	FluxQueryRequests                int64
	FluxQueryRequestDuration         int64
	FluxQueryRequestBytesTransmitted int64
}

// Statistics returns statistics for periodic monitoring.
func (h *Handler) Statistics(tags map[string]string) []models.Statistic {
	return []models.Statistic{{
		Name: "httpd",
		Tags: tags,
		Values: map[string]interface{}{
			statRequest:                          atomic.LoadInt64(&h.stats.Requests),
			statQueryRequest:                     atomic.LoadInt64(&h.stats.QueryRequests),
			statWriteRequest:                     atomic.LoadInt64(&h.stats.WriteRequests),
			statPingRequest:                      atomic.LoadInt64(&h.stats.PingRequests),
			statStatusRequest:                    atomic.LoadInt64(&h.stats.StatusRequests),
			statWriteRequestBytesReceived:        atomic.LoadInt64(&h.stats.WriteRequestBytesReceived),
			statQueryRequestBytesTransmitted:     atomic.LoadInt64(&h.stats.QueryRequestBytesTransmitted),
			statPointsWrittenOK:                  atomic.LoadInt64(&h.stats.PointsWrittenOK),
			statPointsWrittenDropped:             atomic.LoadInt64(&h.stats.PointsWrittenDropped),
			statPointsWrittenFail:                atomic.LoadInt64(&h.stats.PointsWrittenFail),
			statAuthFail:                         atomic.LoadInt64(&h.stats.AuthenticationFailures),
			statRequestDuration:                  atomic.LoadInt64(&h.stats.RequestDuration),
			statQueryRequestDuration:             atomic.LoadInt64(&h.stats.QueryRequestDuration),
			statWriteRequestDuration:             atomic.LoadInt64(&h.stats.WriteRequestDuration),
			statRequestsActive:                   atomic.LoadInt64(&h.stats.ActiveRequests),
			statWriteRequestsActive:              atomic.LoadInt64(&h.stats.ActiveWriteRequests),
			statClientError:                      atomic.LoadInt64(&h.stats.ClientErrors),
			statServerError:                      atomic.LoadInt64(&h.stats.ServerErrors),
			statRecoveredPanics:                  atomic.LoadInt64(&h.stats.RecoveredPanics),
			statPromWriteRequest:                 atomic.LoadInt64(&h.stats.PromWriteRequests),
			statPromReadRequest:                  atomic.LoadInt64(&h.stats.PromReadRequests),
			statFluxQueryRequests:                atomic.LoadInt64(&h.stats.FluxQueryRequests),
			statFluxQueryRequestDuration:         atomic.LoadInt64(&h.stats.FluxQueryRequestDuration),
			statFluxQueryRequestBytesTransmitted: atomic.LoadInt64(&h.stats.FluxQueryRequestBytesTransmitted),
		},
	}}
}

// AddRoutes sets the provided routes on the handler.
func (h *Handler) AddRoutes(routes ...Route) {
	for _, r := range routes {
		var handler http.Handler

		// If it's a handler func that requires authorization, wrap it in authentication
		if hf, ok := r.HandlerFunc.(func(http.ResponseWriter, *http.Request, meta.User)); ok {
			handler = authenticate(hf, h, h.Config.AuthEnabled)
		}

		// This is a normal handler signature and does not require authentication
		if hf, ok := r.HandlerFunc.(func(http.ResponseWriter, *http.Request)); ok {
			handler = http.HandlerFunc(hf)
		}

		// Throttle route if this is a write endpoint.
		if r.Method == http.MethodPost {
			switch r.Pattern {
			case "/write", "/api/v1/prom/write":
				handler = h.writeThrottler.Handler(handler)
			default:
			}
		}

		handler = h.responseWriter(handler)
		if r.Gzipped {
			handler = gzipFilter(handler)
		}

		handler = h.SetHeadersHandler(handler)
		handler = cors(handler)
		handler = requestID(handler)
		if h.Config.LogEnabled && r.LoggingEnabled {
			handler = h.logging(handler, r.Name)
		}
		handler = h.recovery(handler, r.Name) // make sure recovery is always last

		h.mux.Add(r.Method, r.Pattern, handler)
	}
}

// ServeHTTP responds to HTTP request to the handler.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	atomic.AddInt64(&h.stats.Requests, 1)
	atomic.AddInt64(&h.stats.ActiveRequests, 1)
	defer atomic.AddInt64(&h.stats.ActiveRequests, -1)
	start := time.Now()

	// Add version and build header to all InfluxDB requests.
	w.Header().Add("X-Influxdb-Version", h.Version)
	w.Header().Add("X-Influxdb-Build", h.BuildType)

	// Maintain backwards compatibility by using unwrapped pprof/debug handlers
	// when PprofAuthEnabled is false.
	if h.Config.AuthEnabled && h.Config.PprofEnabled && h.Config.PprofAuthEnabled {
		h.mux.ServeHTTP(w, r)
	} else if strings.HasPrefix(r.URL.Path, "/debug/pprof") && h.Config.PprofEnabled {
		h.handleProfiles(w, r)
	} else if strings.HasPrefix(r.URL.Path, "/debug/vars") {
		h.serveExpvar(w, r)
	} else if strings.HasPrefix(r.URL.Path, "/debug/requests") {
		h.serveDebugRequests(w, r)
	} else {
		h.mux.ServeHTTP(w, r)
	}

	atomic.AddInt64(&h.stats.RequestDuration, time.Since(start).Nanoseconds())
}

// writeHeader writes the provided status code in the response, and
// updates relevant http error statistics.
func (h *Handler) writeHeader(w http.ResponseWriter, code int) {
	switch code / 100 {
	case 4:
		atomic.AddInt64(&h.stats.ClientErrors, 1)
	case 5:
		atomic.AddInt64(&h.stats.ServerErrors, 1)
	}
	w.WriteHeader(code)
}

// serveQuery parses an incoming query and, if valid, executes the query.
func (h *Handler) serveQuery(w http.ResponseWriter, r *http.Request, user meta.User) {
	atomic.AddInt64(&h.stats.QueryRequests, 1)
	defer func(start time.Time) {
		atomic.AddInt64(&h.stats.QueryRequestDuration, time.Since(start).Nanoseconds())
	}(time.Now())
	h.requestTracker.Add(r, user)

	// Retrieve the underlying ResponseWriter or initialize our own.
	rw, ok := w.(ResponseWriter)
	if !ok {
		rw = NewResponseWriter(w, r)
	}

	// Retrieve the node id the query should be executed on.
	nodeID, _ := strconv.ParseUint(r.FormValue("node_id"), 10, 64)

	var qr io.Reader
	// Attempt to read the form value from the "q" form value.
	if qp := strings.TrimSpace(r.FormValue("q")); qp != "" {
		qr = strings.NewReader(qp)
	} else if r.MultipartForm != nil && r.MultipartForm.File != nil {
		// If we have a multipart/form-data, try to retrieve a file from 'q'.
		if fhs := r.MultipartForm.File["q"]; len(fhs) > 0 {
			f, err := fhs[0].Open()
			if err != nil {
				h.httpError(rw, err.Error(), http.StatusBadRequest)
				return
			}
			defer f.Close()
			qr = f
		}
	}

	if qr == nil {
		h.httpError(rw, `missing required parameter "q"`, http.StatusBadRequest)
		return
	}

	epoch := strings.TrimSpace(r.FormValue("epoch"))

	p := influxql.NewParser(qr)
	db := r.FormValue("db")

	// Sanitize the request query params so it doesn't show up in the response logger.
	// Do this before anything else so a parsing error doesn't leak passwords.
	sanitize(r)

	// Parse the parameters
	rawParams := r.FormValue("params")
	if rawParams != "" {
		var params map[string]interface{}
		decoder := json.NewDecoder(strings.NewReader(rawParams))
		decoder.UseNumber()
		if err := decoder.Decode(&params); err != nil {
			h.httpError(rw, "error parsing query parameters: "+err.Error(), http.StatusBadRequest)
			return
		}

		// Convert json.Number into int64 and float64 values
		for k, v := range params {
			if v, ok := v.(json.Number); ok {
				var err error
				if strings.Contains(string(v), ".") {
					params[k], err = v.Float64()
				} else {
					params[k], err = v.Int64()
				}

				if err != nil {
					h.httpError(rw, "error parsing json value: "+err.Error(), http.StatusBadRequest)
					return
				}
			}
		}
		p.SetParams(params)
	}

	// Parse query from query string.
	q, err := p.ParseQuery()
	if err != nil {
		h.httpError(rw, "error parsing query: "+err.Error(), http.StatusBadRequest)
		return
	}

	// Check authorization.
	var fineAuthorizer query.FineAuthorizer
	if h.Config.AuthEnabled {
		var err error
		if fineAuthorizer, err = h.QueryAuthorizer.AuthorizeQuery(user, q, db); err != nil {
			if authErr, ok := err.(meta.ErrAuthorize); ok {
				h.Logger.Info("Unauthorized request",
					zap.String("user", authErr.User),
					zap.Stringer("query", authErr.Query),
					logger.Database(authErr.Database))
			} else {
				h.Logger.Info("Error authorizing query", zap.Error(err))
			}
			h.httpError(rw, "error authorizing query: "+err.Error(), http.StatusForbidden)
			return
		}
	} else {
		fineAuthorizer = query.OpenAuthorizer
	}

	// Parse chunk size. Use default if not provided or unparsable.
	chunked := r.FormValue("chunked") == "true"
	chunkSize := DefaultChunkSize
	if chunked {
		if n, err := strconv.ParseInt(r.FormValue("chunk_size"), 10, 64); err == nil && int(n) > 0 {
			chunkSize = int(n)
		}
	}

	// Parse whether this is an async command.
	async := r.FormValue("async") == "true"

	opts := query.ExecutionOptions{
		Database:        db,
		RetentionPolicy: r.FormValue("rp"),
		ChunkSize:       chunkSize,
		ReadOnly:        r.Method == "GET",
		NodeID:          nodeID,
		Authorizer:      fineAuthorizer,
	}

	if h.Config.AuthEnabled {
		// The current user determines the authorized actions.
		opts.CoarseAuthorizer = &userQueryAuthorizer{
			auth: h.QueryAuthorizer,
			user: user,
		}
	} else {
		opts.CoarseAuthorizer = query.OpenCoarseAuthorizer
	}

	// Make sure if the client disconnects we signal the query to abort
	var closing chan struct{}
	if !async {
		closing = make(chan struct{})
		if notifier, ok := w.(http.CloseNotifier); ok {
			// CloseNotify() is not guaranteed to send a notification when the query
			// is closed. Use this channel to signal that the query is finished to
			// prevent lingering goroutines that may be stuck.
			done := make(chan struct{})
			defer close(done)

			notify := notifier.CloseNotify()
			go func() {
				// Wait for either the request to finish
				// or for the client to disconnect
				select {
				case <-done:
				case <-notify:
					close(closing)
				}
			}()
			opts.AbortCh = done
		} else {
			defer close(closing)
		}
	}

	// Execute query.
	results := h.QueryExecutor.ExecuteQuery(q, opts, closing)

	// If we are running in async mode, open a goroutine to drain the results
	// and return with a StatusNoContent.
	if async {
		go h.async(q, results)
		h.writeHeader(w, http.StatusNoContent)
		return
	}

	// if we're not chunking, this will be the in memory buffer for all results before sending to client
	resp := Response{Results: make([]*query.Result, 0)}

	// Status header is OK once this point is reached.
	// Attempt to flush the header immediately so the client gets the header information
	// and knows the query was accepted.
	h.writeHeader(rw, http.StatusOK)
	if w, ok := w.(http.Flusher); ok {
		w.Flush()
	}

	// pull all results from the channel
	rows := 0
	for r := range results {
		// Ignore nil results.
		if r == nil {
			continue
		}

		// if requested, convert result timestamps to epoch
		if epoch != "" {
			convertToEpoch(r, epoch)
		}

		// Write out result immediately if chunked.
		if chunked {
			n, _ := rw.WriteResponse(Response{
				Results: []*query.Result{r},
			})
			atomic.AddInt64(&h.stats.QueryRequestBytesTransmitted, int64(n))
			w.(http.Flusher).Flush()
			continue
		}

		// Limit the number of rows that can be returned in a non-chunked
		// response.  This is to prevent the server from going OOM when
		// returning a large response.  If you want to return more than the
		// default chunk size, then use chunking to process multiple blobs.
		// Iterate through the series in this result to count the rows and
		// truncate any rows we shouldn't return.
		if h.Config.MaxRowLimit > 0 {
			for i, series := range r.Series {
				n := h.Config.MaxRowLimit - rows
				if n < len(series.Values) {
					// We have reached the maximum number of values. Truncate
					// the values within this row.
					series.Values = series.Values[:n]
					// Since this was truncated, it will always be a partial return.
					// Add this so the client knows we truncated the response.
					series.Partial = true
				}
				rows += len(series.Values)

				if rows >= h.Config.MaxRowLimit {
					// Drop any remaining series since we have already reached the row limit.
					if i < len(r.Series) {
						r.Series = r.Series[:i+1]
					}
					break
				}
			}
		}

		// It's not chunked so buffer results in memory.
		// Results for statements need to be combined together.
		// We need to check if this new result is for the same statement as
		// the last result, or for the next statement
		l := len(resp.Results)
		if l == 0 {
			resp.Results = append(resp.Results, r)
		} else if resp.Results[l-1].StatementID == r.StatementID {
			if r.Err != nil {
				resp.Results[l-1] = r
				continue
			}

			cr := resp.Results[l-1]
			rowsMerged := 0
			if len(cr.Series) > 0 {
				lastSeries := cr.Series[len(cr.Series)-1]

				for _, row := range r.Series {
					if !lastSeries.SameSeries(row) {
						// Next row is for a different series than last.
						break
					}
					// Values are for the same series, so append them.
					lastSeries.Values = append(lastSeries.Values, row.Values...)
					lastSeries.Partial = row.Partial
					rowsMerged++
				}
			}

			// Append remaining rows as new rows.
			r.Series = r.Series[rowsMerged:]
			cr.Series = append(cr.Series, r.Series...)
			cr.Messages = append(cr.Messages, r.Messages...)
			cr.Partial = r.Partial
		} else {
			resp.Results = append(resp.Results, r)
		}

		// Drop out of this loop and do not process further results when we hit the row limit.
		if h.Config.MaxRowLimit > 0 && rows >= h.Config.MaxRowLimit {
			// If the result is marked as partial, remove that partial marking
			// here. While the series is partial and we would normally have
			// tried to return the rest in the next chunk, we are not using
			// chunking and are truncating the series so we don't want to
			// signal to the client that we plan on sending another JSON blob
			// with another result.  The series, on the other hand, still
			// returns partial true if it was truncated or had more data to
			// send in a future chunk.
			r.Partial = false
			break
		}
	}

	// If it's not chunked we buffered everything in memory, so write it out
	if !chunked {
		n, _ := rw.WriteResponse(resp)
		atomic.AddInt64(&h.stats.QueryRequestBytesTransmitted, int64(n))
	}
}

// async drains the results from an async query and logs a message if it fails.
func (h *Handler) async(q *influxql.Query, results <-chan *query.Result) {
	for r := range results {
		// Drain the results and do nothing with them.
		// If it fails, log the failure so there is at least a record of it.
		if r.Err != nil {
			// Do not log when a statement was not executed since there would
			// have been an earlier error that was already logged.
			if r.Err == query.ErrNotExecuted {
				continue
			}
			h.Logger.Info("Error while running async query",
				zap.Stringer("query", q),
				zap.Error(r.Err))
		}
	}
}

// bucket2drbp extracts a bucket and retention policy from a properly formatted
// string.
//
// The 2.x compatible endpoints encode the database and retention policy names
// in the database URL query value.  It is encoded using a forward slash like
// "database/retentionpolicy" and we should be able to simply split that string
// on the forward slash.
func bucket2dbrp(bucket string) (string, string, error) {
	// test for a slash in our bucket name.
	switch idx := strings.IndexByte(bucket, '/'); idx {
	case -1:
		// if there is no slash, we're mapping bucket to the database.
		switch db := bucket; db {
		case "":
			// if our "database" is an empty string, this is an error.
			return "", "", fmt.Errorf(`bucket name %q is missing a slash; not in "database/retention-policy" format`, bucket)
		default:
			return db, "", nil
		}
	default:
		// there is a slash
		switch db, rp := bucket[:idx], bucket[idx+1:]; {
		case db == "":
			// empty database is unrecoverable
			return "", "", fmt.Errorf(`bucket name %q is in db/rp form but has an empty database`, bucket)
		default:
			return db, rp, nil
		}
	}
}

type DeleteBody struct {
	Start     string `json:"start"`
	Stop      string `json:"stop"`
	Predicate string `json:"predicate"`
}

// serveDeleteV2 maps v2 write parameters to a v1 style handler.  the concepts
// of an "org" and "bucket" are mapped to v1 "database" and "retention
// policies".
func (h *Handler) serveDeleteV2(w http.ResponseWriter, r *http.Request, user meta.User) {
	db, rp, err := bucket2dbrp(r.URL.Query().Get("bucket"))

	if err != nil {
		h.httpError(w, fmt.Sprintf("delete - bucket: %s", err.Error()), http.StatusNotFound)
		return
	}

	if di := h.MetaClient.Database(db); di == nil {
		h.httpError(w, fmt.Sprintf("delete - database not found: %q", db), http.StatusNotFound)
		return
	} else if nil == di.RetentionPolicy(rp) {
		h.httpError(w, fmt.Sprintf("delete - retention policy not found in %q: %q", db, rp), http.StatusNotFound)
		return
	}

	if h.Config.AuthEnabled {
		if user == nil {
			h.httpError(w, fmt.Sprintf("delete - user is required to delete from database %q", db), http.StatusForbidden)
			return
		}

		// DeleteSeries requires write permission to the database
		if err := h.WriteAuthorizer.AuthorizeWrite(user.ID(), db); err != nil {
			h.httpError(w, fmt.Sprintf("delete - %q is not authorized to delete from %q: %s", user.ID(), db, err.Error()), http.StatusForbidden)
			return
		}
	}

	var bs []byte
	if r.ContentLength > 0 {
		if h.Config.MaxBodySize > 0 && r.ContentLength > int64(h.Config.MaxBodySize) {
			h.httpError(w, http.StatusText(http.StatusRequestEntityTooLarge), http.StatusRequestEntityTooLarge)
			return
		}

		// This will just be an initial hint for the reader, as the
		// bytes.Buffer will grow as needed when ReadFrom is called
		bs = make([]byte, 0, r.ContentLength)
	}
	buf := bytes.NewBuffer(bs)

	_, err = buf.ReadFrom(r.Body)
	if err != nil {
		h.httpError(w, fmt.Sprintf("delete - cannot read request body: %s", err.Error()), http.StatusBadRequest)
		return
	}

	var drd DeleteBody
	if err := json.Unmarshal(buf.Bytes(), &drd); err != nil {
		h.httpError(w, fmt.Sprintf("delete - cannot parse request body: %s", err.Error()), http.StatusBadRequest)
		return
	}

	if len(drd.Start) <= 0 {
		h.httpError(w, "delete - start field in RFC3339Nano format required", http.StatusBadRequest)
		return
	}

	if len(drd.Stop) <= 0 {
		h.httpError(w, "delete - stop field in RFC3339Nano format required", http.StatusBadRequest)
		return
	}
	// Avoid injection errors by converting and back-converting time.
	start, err := time.Parse(time.RFC3339Nano, drd.Start)
	if err != nil {
		h.httpError(w, fmt.Sprintf("delete - invalid format for start field %q, please use RFC3339Nano: %s",
			drd.Start, err.Error()), http.StatusBadRequest)
		return
	}

	// Avoid injection errors by converting and back-converting time.
	stop, err := time.Parse(time.RFC3339Nano, drd.Stop)
	if err != nil {
		h.httpError(w, fmt.Sprintf("delete - invalid format for stop field %q, please use RFC3339Nano: %s",
			drd.Stop, err.Error()), http.StatusBadRequest)
		return
	}

	var timePredicate string
	timeRange := fmt.Sprintf("time >= '%s' AND time < '%s'", start.Format(time.RFC3339Nano), stop.Format(time.RFC3339Nano))

	if drd.Predicate != "" {
		timePredicate = fmt.Sprintf("%s AND %s", drd.Predicate, timeRange)
	} else {
		timePredicate = timeRange
	}

	cond, err := influxql.ParseExpr(timePredicate)
	if err != nil {
		h.httpError(w, fmt.Sprintf("delete - cannot parse predicate %q: %s", timePredicate, err.Error()), http.StatusBadRequest)
		return
	}

	const measurement = "_measurement"

	// This has to be nil if there are no sources;
	// an empty slice causes the Statement.String()
	// function into adding an empty WHERE clause.
	// And that breaks Enterprise remote query execution.
	var srcs []influxql.Source = nil
	// take out the _measurement = 'mymeasurement' clause to pass separately
	// Also check for illegal operands.
	_, remainingExpr, err := influxql.PartitionExpr(influxql.CloneExpr(cond), func(e influxql.Expr) (bool, error) {
		switch e := e.(type) {
		case *influxql.BinaryExpr:
			switch e.Op {
			case influxql.EQ:
				tag, ok := e.LHS.(*influxql.VarRef)
				if ok && tag.Val == measurement {
					srcs = append(srcs, &influxql.Measurement{Name: strings.Trim(e.RHS.String(), `"`)})
					return true, nil
				}
			// Not permitted in V2 API DELETE predicates
			case influxql.NEQ, influxql.REGEX, influxql.NEQREGEX, influxql.OR:
				return true,
					fmt.Errorf("delete - predicate only supports equality operators and conjunctions. database: %q, retention policy: %q, start: %q, stop: %q, predicate: %q",
						db, rp, drd.Start, drd.Stop, drd.Predicate)
			}
		}
		return false, nil
	})

	if err != nil {
		h.httpError(w, err.Error(), http.StatusBadRequest)
		return
	}

	influxql.WalkFunc(remainingExpr, fixLiterals)

	if err = h.Store.Delete(db, srcs, remainingExpr); err != nil {
		h.httpError(w,
			fmt.Sprintf("delete - database: %q, retention policy: %q, start: %q, stop: %q, predicate: %q, error: %s",
				db, rp, drd.Start, drd.Stop, drd.Predicate, err.Error()), http.StatusBadRequest)
		return
	}
}

// fixLiterals - If we have an equality comparison between two VarRefs of Unknown type,
// assume it is a tag key (LHS) and a tag value (RHS) and convert the tag value to a
// StringLiteral. This lets us consume V2 DELETE double-quoted syntax.
func fixLiterals(node influxql.Node) {
	if bn, ok := node.(*influxql.BinaryExpr); ok && (bn.Op == influxql.EQ) {
		if lhs, ok := bn.LHS.(*influxql.VarRef); ok && (lhs.Type == influxql.Unknown) {
			if rhs, ok := bn.RHS.(*influxql.VarRef); ok && (rhs.Type == influxql.Unknown) {
				bn.RHS = &influxql.StringLiteral{Val: rhs.Val}
			}
		}
	}
}

type RetentionRule struct {
	Type                      string `json:"type"`
	EverySeconds              int64  `json:"everySeconds"`
	ShardGroupDurationSeconds int64  `json:"shardGroupDurationSeconds"`
}

type BucketUpdate struct {
	Description    string          `json:"description"`
	Name           string          `json:"name"`
	RetentionRules []RetentionRule `json:"retentionRules"`
}

// BucketsBody and RetentionRule should match the 2.0 API definition.
type BucketsBody struct {
	BucketUpdate
	OrgID      string `json:"orgId"`
	Rp         string `json:"rp"`
	SchemaType string `json:"schemaType"`
}

type Bucket struct {
	BucketsBody
	ID                  string    `json:"id,omitempty"`
	Type                string    `json:"type"`
	RetentionPolicyName string    `json:"rp,omitempty"` // This to support v1 sources
	CreatedAt           time.Time `json:"createdAt"`
	UpdatedAt           time.Time `json:"updatedAt"`
}

type Buckets struct {
	Buckets []Bucket `json:"buckets"`
}

func (h *Handler) servePostCreateBucketV2(w http.ResponseWriter, r *http.Request, user meta.User) {
	var bs []byte
	if r.ContentLength > 0 {
		if h.Config.MaxBodySize > 0 && r.ContentLength > int64(h.Config.MaxBodySize) {
			h.httpError(w, http.StatusText(http.StatusRequestEntityTooLarge), http.StatusRequestEntityTooLarge)
			return
		}

		// This will just be an initial hint for the reader, as the
		// bytes.Buffer will grow as needed when ReadFrom is called
		bs = make([]byte, 0, r.ContentLength)
	}
	buf := bytes.NewBuffer(bs)

	_, err := buf.ReadFrom(r.Body)
	if err != nil {
		h.httpError(w, fmt.Sprintf("buckets - cannot read request body: %s", err.Error()), http.StatusBadRequest)
		return
	}

	brd := &BucketsBody{}
	if err = json.Unmarshal(buf.Bytes(), brd); err != nil {
		h.httpError(w, fmt.Sprintf("buckets - cannot parse request body: %s", err.Error()), http.StatusBadRequest)
		return
	}
	db, rp, err := bucket2dbrp(brd.Name)
	if err != nil {
		h.httpError(w, fmt.Sprintf("buckets - illegal bucket name: %s", err.Error()), http.StatusBadRequest)
		return
	} else if rp == "" {
		h.httpError(w, fmt.Sprintf("buckets - illegal bucket name: %q", brd.Name), http.StatusBadRequest)
		return
	} else if !((brd.Rp == rp) || (brd.Rp == "")) {
		h.httpError(w, fmt.Sprintf("buckets - two retention policies specified: %q differs from %q", rp, brd.Rp), http.StatusBadRequest)
		return
	}
	// We only need to validate non-empty
	// retention policy names
	if rp != "" && !meta.ValidName(rp) {
		h.httpError(w, fmt.Sprintf("buckets - retention policy %q: %s", rp, meta.ErrInvalidName.Error()), http.StatusBadRequest)
		return
	}

	var dur, sgDur time.Duration
	if len(brd.RetentionRules) > 0 {
		dur = time.Second * time.Duration(brd.RetentionRules[0].EverySeconds)
		sgDur = time.Duration(brd.RetentionRules[0].ShardGroupDurationSeconds) * time.Second
	} else {
		dur = meta.DefaultRetentionPolicyDuration
		// This will get set to default in normalisedShardDuration()
		// called by CreateRetentionPolicy
		sgDur = 0
	}
	rf := meta.DefaultRetentionPolicyReplicaN
	spec := meta.RetentionPolicySpec{
		Name:               rp,
		Duration:           &dur,
		ReplicaN:           &rf,
		ShardGroupDuration: sgDur,
	}
	dbi := h.MetaClient.Database(db)

	if h.Config.AuthEnabled {
		if user == nil {
			h.httpError(w, fmt.Sprintf("buckets - user is required to to create bucket %q", brd.Name), http.StatusForbidden)
			return
		}

		if dbi == nil {
			if err := h.QueryAuthorizer.AuthorizeCreateDatabase(user); err != nil {
				h.httpError(w, fmt.Sprintf("buckets - %q is not authorized to create %q: %s", user.ID(), brd.Name, err.Error()), http.StatusForbidden)
				return
			}
		} else {
			if err := h.QueryAuthorizer.AuthorizeCreateRetentionPolicy(user, db); err != nil {
				h.httpError(w, fmt.Sprintf("buckets - %q is not authorized to create %q: %s", user.ID(), brd.Name, err.Error()), http.StatusForbidden)
				return
			}
		}
	}
	var rpi *meta.RetentionPolicyInfo
	if dbi == nil {
		if dbi, err = h.MetaClient.CreateDatabaseWithRetentionPolicy(db, &spec); err != nil {
			h.httpError(w, fmt.Sprintf("buckets - cannot create bucket %q: %s", brd.Name, err.Error()), http.StatusBadRequest)
			return
		} else {
			rpi = &dbi.RetentionPolicies[0]
		}

	} else {
		if rpi, err = h.MetaClient.CreateRetentionPolicy(db, &spec, false); err != nil {
			h.httpError(w, fmt.Sprintf("buckets - cannot create bucket %q: %s", brd.Name, err.Error()), http.StatusBadRequest)
			return
		}
	}
	bucket := makeBucket(rpi, db)
	b, err := json.Marshal(bucket)
	if err != nil {
		h.httpError(w, fmt.Sprintf("buckets - cannot marshal bucket %q: %s", brd.Name, err.Error()), http.StatusBadRequest)
		return
	}
	w.WriteHeader(http.StatusCreated)
	w.Write(b)
}

// checkDbRp checks id for database/retention-policy format
func (h *Handler) checkDbRp(w http.ResponseWriter, id string) (string, string, bool) {
	db, rp, err := bucket2dbrp(id)
	if err != nil {
		h.httpError(w, fmt.Sprintf("bucket %q: %s", id, err.Error()), http.StatusBadRequest)
		return "", "", false
	} else if rp == "" {
		h.httpError(w, fmt.Sprintf("bucket %q: illegal bucket id, empty retention policy", id), http.StatusBadRequest)
		return "", "", false
	}
	return db, rp, true
}

// serveDeleteBucketV2 should do the same thing as coordinator/statement_executor.go/executeDropRetentionPolicyStatement
// i.e., Store.DeleteRetentionPolicy and MetaClient.DropRetentionPolicy
func (h *Handler) serveDeleteBucketV2(w http.ResponseWriter, r *http.Request, user meta.User) {
	id := r.URL.Query().Get(":dbrp")
	db, rp, ok := h.checkDbRp(w, id)
	if !ok {
		return
	}
	if h.Config.AuthEnabled {
		if user == nil {
			h.httpError(w, fmt.Sprintf("delete bucket - user is required to delete from database %q", db), http.StatusForbidden)
			return
		}
		if err := h.QueryAuthorizer.AuthorizeDeleteRetentionPolicy(user, db); err != nil {
			h.httpError(w, fmt.Sprintf("buckets - %q is not authorized to modify %q: %s", user.ID(), id, err.Error()), http.StatusForbidden)
			return
		}
	}

	if dbi := h.MetaClient.Database(db); dbi == nil {
		h.httpError(w, fmt.Sprintf("delete bucket - not found: %q", id), http.StatusNotFound)
		return
	} else if rpi := dbi.RetentionPolicy(rp); rpi == nil {
		h.httpError(w, fmt.Sprintf("delete bucket - not found: %q", id), http.StatusNotFound)
		return
	} else if err := h.Store.DeleteRetentionPolicy(db, rp); err != nil {
		h.httpError(w, fmt.Sprintf("delete bucket %q: %s", id, err.Error()), http.StatusBadRequest)
		return
	} else if err := h.MetaClient.DropRetentionPolicy(db, rp); err != nil {
		h.httpError(w, fmt.Sprintf("delete bucket %q: %s", id, err.Error()), http.StatusBadRequest)
		return
	}
}

func (h *Handler) serveUpdateBucketV2(w http.ResponseWriter, r *http.Request, user meta.User) {
	var bs []byte
	id := r.URL.Query().Get(":dbrp")
	db, rp, ok := h.checkDbRp(w, id)
	if !ok {
		return
	}
	if h.Config.AuthEnabled {
		if user == nil {
			h.httpError(w, fmt.Sprintf("update bucket - user is required to update %q", id), http.StatusForbidden)
			return
		}
		// This is the privilege required in the Enterprise authorization.
		if err := h.QueryAuthorizer.AuthorizeCreateDatabase(user); err != nil {
			h.httpError(w, fmt.Sprintf("update bucket - %q is not authorized to modify %q: %s", user.ID(), id, err.Error()), http.StatusForbidden)
			return
		}
	}
	if r.ContentLength > 0 {
		if h.Config.MaxBodySize > 0 && r.ContentLength > int64(h.Config.MaxBodySize) {
			h.httpError(w, http.StatusText(http.StatusRequestEntityTooLarge), http.StatusRequestEntityTooLarge)
			return
		}

		// This will just be an initial hint for the reader, as the
		// bytes.Buffer will grow as needed when ReadFrom is called
		bs = make([]byte, 0, r.ContentLength)
	}

	buf := bytes.NewBuffer(bs)
	_, err := buf.ReadFrom(r.Body)
	if err != nil {
		h.httpError(w, fmt.Sprintf("update bucket - cannot read request body: %s", err.Error()), http.StatusBadRequest)
		return
	}

	bu := &BucketUpdate{}
	if err = json.Unmarshal(buf.Bytes(), bu); err != nil {
		h.httpError(w, fmt.Sprintf("update bucket - cannot parse request body: %s", err.Error()), http.StatusBadRequest)
		return
	}
	var dur, sgDur time.Duration
	if len(bu.RetentionRules) > 0 {
		dur = time.Second * time.Duration(bu.RetentionRules[0].EverySeconds)
		sgDur = time.Duration(bu.RetentionRules[0].ShardGroupDurationSeconds) * time.Second
	} else {
		dur = meta.DefaultRetentionPolicyDuration
		sgDur = 0
	}
	rf := meta.DefaultRetentionPolicyReplicaN
	m := &meta.RetentionPolicyUpdate{
		Name:               &rp,
		Duration:           &dur,
		ReplicaN:           &rf,
		ShardGroupDuration: &sgDur,
	}

	if err := h.MetaClient.UpdateRetentionPolicy(db, rp, m, false); err != nil {
		h.httpError(w, fmt.Sprintf("update bucket %q: %s", id, err.Error()), http.StatusBadRequest)
		return
	}
	rpi := &meta.RetentionPolicyInfo{
		Name:               *m.Name,
		ReplicaN:           *m.ReplicaN,
		Duration:           *m.Duration,
		ShardGroupDuration: *m.ShardGroupDuration,
	}
	bucket := makeBucket(rpi, db)
	b, err := json.Marshal(bucket)
	if err != nil {
		h.httpError(w, fmt.Sprintf("buckets - cannot marshal bucket %q: %s", bucket.Name, err.Error()), http.StatusBadRequest)
		return
	}
	w.WriteHeader(http.StatusOK)
	w.Write(b)
}

func (h *Handler) serveRetrieveBucketV2(w http.ResponseWriter, r *http.Request, user meta.User) {
	// This is the API for returning a single bucket
	// In V1, BucketID and BucketName are the same: "db/rp"
	id := r.URL.Query().Get(":dbrp")
	db, rp, ok := h.checkDbRp(w, id)
	if !ok {
		return
	}
	if h.Config.AuthEnabled {
		if user == nil {
			h.httpError(w, fmt.Sprintf("retrieve bucket - user is required for database %q", db), http.StatusForbidden)
			return
		}

		if err := h.QueryAuthorizer.AuthorizeDatabase(user, influxql.ReadPrivilege, db); err != nil {
			h.httpError(w, fmt.Sprintf("retrieve bucket - %q is not authorized to read %q: %s", user.ID(), db, err.Error()), http.StatusForbidden)
			return
		}
	}

	if bucket := h.oneBucket(w, fmt.Sprintf("%s/%s", db, rp)); bucket != nil {
		b, err := json.Marshal(bucket)
		if err != nil {
			h.httpError(w, fmt.Sprintf("retrieve bucket marshaling error: %s", err.Error()), http.StatusBadRequest)
			return
		}
		if _, err := w.Write(b); err != nil {
			h.httpError(w, fmt.Sprintf("retrieve bucket error writing response: %s", err.Error()), http.StatusBadRequest)
			return
		}
	}
	return
}

func (h *Handler) serveListBucketsV2(w http.ResponseWriter, r *http.Request, user meta.User) {
	const defaultLimit = 100
	var err error
	var db, rp, after string
	limit := defaultLimit
	offset := 0

	if h.Config.AuthEnabled {
		if user == nil {
			h.httpError(w, "list buckets - user is required", http.StatusForbidden)
			return
		}
	}

	// This is the API for returning an array of buckets
	id := r.URL.Query().Get("id")
	name := r.URL.Query().Get("name")
	if id != "" || name != "" {
		if id != "" && name != "" && id != name {
			h.httpError(w, fmt.Sprintf("list buckets: name: %q and id: %q do not match", name, id), http.StatusBadRequest)
			return
		} else if id != "" {
			name = id
		}
		if b := h.oneBucket(w, name); b != nil {
			h.sendBuckets(w, []Bucket{*b})
			return
		} else {
			h.sendBuckets(w, []Bucket{})
			return
		}
	}

	if after = r.URL.Query().Get("after"); after != "" {
		db, rp, err = bucket2dbrp(after)
		if err != nil {
			h.httpError(w, fmt.Sprintf("list buckets invalid parameter - after=%q: %s", after, err.Error()), http.StatusNotFound)
			return
		}
	}

	if limitStr := r.URL.Query().Get("limit"); limitStr != "" {
		if l, err := strconv.ParseInt(limitStr, 10, 64); err != nil {
			h.httpError(w, fmt.Sprintf("list buckets parameter is not an integer - limit=%q: %s", limitStr, err.Error()), http.StatusBadRequest)
			return
		} else if l <= 0 {
			limit = math.MaxInt
		} else {
			limit = int(l)
		}
	}

	if offsetStr := r.URL.Query().Get("offset"); offsetStr != "" {
		if after != "" {
			h.httpError(w, "list buckets cannot have both \"offset\" and \"after\" arguments", http.StatusBadRequest)
			return
		} else if o, err := strconv.ParseInt(offsetStr, 10, 64); err != nil {
			h.httpError(w, fmt.Sprintf("list buckets parameter is not an integer - offset=%q: %s", offsetStr, err.Error()), http.StatusBadRequest)
			return
		} else {
			offset = int(o)
		}
	}

	dbIndex := 0
	rpIndex := 0
	dbs := h.MetaClient.Databases()
	if after != "" {
		var ok bool
		if dbIndex, rpIndex, ok = findBucketIndex(dbs, db, rp); !ok {
			h.httpError(w, fmt.Sprintf("list buckets \"after\" parameter not found: %q", after), http.StatusNotFound)
			return
		}
	} else if offset > 0 {
		var ok bool
		if dbIndex, rpIndex, ok = findBucketOffsetIndex(dbs, offset); !ok {
			h.sendBuckets(w, []Bucket{})
			return
		}
	}
	sliceCap := limit
	if sliceCap > (2 * defaultLimit) {
		sliceCap = 2 * defaultLimit
	}
	buckets := make([]Bucket, 0, sliceCap)

outer:
	for ; dbIndex < len(dbs); dbIndex++ {
		dbi := dbs[dbIndex]
		if h.Config.AuthEnabled {
			if err := h.QueryAuthorizer.AuthorizeDatabase(user, influxql.ReadPrivilege, dbi.Name); err != nil {
				continue
			}
		}
		for ; rpIndex < len(dbi.RetentionPolicies); rpIndex++ {
			if limit > 0 {
				buckets = append(buckets, *makeBucket(&dbi.RetentionPolicies[rpIndex], dbi.Name))
				limit--
			} else {
				break outer
			}
		}
		rpIndex = 0
	}
	h.sendBuckets(w, buckets)
}

func (h *Handler) oneBucket(w http.ResponseWriter, name string) *Bucket {
	db, rp, err := bucket2dbrp(name)
	if err != nil {
		h.httpError(w, fmt.Sprintf("bucket %q: %s", name, err.Error()), http.StatusNotFound)
		return nil
	} else if dbi := h.MetaClient.Database(db); dbi == nil {
		h.httpError(w, fmt.Sprintf("bucket not found: %q", name), http.StatusNotFound)
		return nil
	} else if rpi := dbi.RetentionPolicy(rp); rpi == nil {
		h.httpError(w, fmt.Sprintf("bucket not found: %q", name), http.StatusNotFound)
		return nil
	} else {
		return makeBucket(rpi, db)
	}
}

func findBucketOffsetIndex(dbs []meta.DatabaseInfo, offset int) (dbIndex int, rpIndex int, ok bool) {
	for i, dbi := range dbs {
		if offset > len(dbi.RetentionPolicies) {
			offset -= len(dbi.RetentionPolicies)
		} else {
			dbIndex = i
			rpIndex = offset
			return dbIndex, rpIndex, true
		}
	}
	return -1, -1, false
}

func findBucketIndex(dbs []meta.DatabaseInfo, db string, rp string) (dbIndex int, rpIndex int, ok bool) {
	for di, dbi := range dbs {
		if dbi.Name != db {
			continue
		} else {
			for ri, rpi := range dbi.RetentionPolicies {
				if rpi.Name == rp {
					if ri < (len(dbi.RetentionPolicies) - 1) {
						return di, ri + 1, true
					} else {
						return di + 1, 0, true
					}
				}
			}
		}
	}
	return -1, -1, false
}

func (h *Handler) sendBuckets(w http.ResponseWriter, buckets []Bucket) {
	bucketsObj := Buckets{buckets}
	b, err := json.Marshal(bucketsObj)
	if err != nil {
		h.httpError(w, fmt.Sprintf("list buckets marshaling error: %s", err.Error()), http.StatusInternalServerError)
		return
	}
	if _, err := w.Write(b); err != nil {
		h.Logger.Info("/api/v2/buckets: list buckets error writing response", zap.Error(err))
	}
}

func makeBucket(rpi *meta.RetentionPolicyInfo, database string) *Bucket {
	name := fmt.Sprintf("%s/%s", database, rpi.Name)
	return &Bucket{
		BucketsBody: BucketsBody{
			BucketUpdate: BucketUpdate{
				Name: name,
				RetentionRules: []RetentionRule{{
					Type:                      "",
					EverySeconds:              int64(rpi.Duration.Seconds()),
					ShardGroupDurationSeconds: int64(rpi.ShardGroupDuration.Seconds()),
				}},
			},
			Rp:         rpi.Name,
			SchemaType: "implicit",
		},
		ID:                  name,
		RetentionPolicyName: rpi.Name,
	}
}

func (h *Handler) serveLabelsNotAllowedV2(w http.ResponseWriter, r *http.Request, user meta.User) {
	h.httpError(w, "bucket labels not supported in this version", http.StatusMethodNotAllowed)
	return
}

func (h *Handler) serveBucketMembersNotAllowedV2(w http.ResponseWriter, r *http.Request, user meta.User) {
	h.httpError(w, "bucket members not supported in this version", http.StatusMethodNotAllowed)
	return
}

func (h *Handler) serveBucketOwnersNotAllowedV2(w http.ResponseWriter, r *http.Request, user meta.User) {
	h.httpError(w, "bucket owners not supported in this version", http.StatusMethodNotAllowed)
	return
}

// serveWriteV2 maps v2 write parameters to a v1 style handler.  the concepts
// of a "bucket" is mapped to v1 "database" and "retention
// policies".
func (h *Handler) serveWriteV2(w http.ResponseWriter, r *http.Request, user meta.User) {
	precision := r.URL.Query().Get("precision")
	switch precision {
	case "ns":
		precision = "n"
	case "us":
		precision = "u"
	case "ms", "s", "":
		// same as v1 so do nothing
	default:
		err := fmt.Sprintf("invalid precision %q (use ns, us, ms or s)", precision)
		h.httpError(w, err, http.StatusBadRequest)
		return
	}

	db, rp, err := bucket2dbrp(r.URL.Query().Get("bucket"))
	if err != nil {
		h.httpError(w, err.Error(), http.StatusNotFound)
		return
	}
	h.serveWrite(db, rp, precision, w, r, user)
}

// serveWriteV1 handles v1 style writes.
func (h *Handler) serveWriteV1(w http.ResponseWriter, r *http.Request, user meta.User) {
	precision := r.URL.Query().Get("precision")
	switch precision {
	case "", "n", "ns", "u", "ms", "s", "m", "h":
		// it's valid
	default:
		err := fmt.Sprintf("invalid precision %q (use n, u, ms, s, m or h)", precision)
		h.httpError(w, err, http.StatusBadRequest)
		return
	}

	db := r.URL.Query().Get("db")
	rp := r.URL.Query().Get("rp")

	h.serveWrite(db, rp, precision, w, r, user)
}

// serveWrite receives incoming series data in line protocol format and writes
// it to the database.
func (h *Handler) serveWrite(database, retentionPolicy, precision string, w http.ResponseWriter, r *http.Request, user meta.User) {
	atomic.AddInt64(&h.stats.WriteRequests, 1)
	atomic.AddInt64(&h.stats.ActiveWriteRequests, 1)
	defer func(start time.Time) {
		atomic.AddInt64(&h.stats.ActiveWriteRequests, -1)
		atomic.AddInt64(&h.stats.WriteRequestDuration, time.Since(start).Nanoseconds())
	}(time.Now())
	h.requestTracker.Add(r, user)

	if database == "" {
		h.httpError(w, "database is required", http.StatusBadRequest)
		return
	}

	if di := h.MetaClient.Database(database); di == nil {
		h.httpError(w, fmt.Sprintf("database not found: %q", database), http.StatusNotFound)
		return
	}

	if h.Config.AuthEnabled {
		if user == nil {
			h.httpError(w, fmt.Sprintf("user is required to write to database %q", database), http.StatusForbidden)
			return
		}

		if err := h.WriteAuthorizer.AuthorizeWrite(user.ID(), database); err != nil {
			h.httpError(w, fmt.Sprintf("%q user is not authorized to write to database %q", user.ID(), database), http.StatusForbidden)
			return
		}
	}

	body := r.Body
	if h.Config.MaxBodySize > 0 {
		body = truncateReader(body, int64(h.Config.MaxBodySize))
	}

	// Handle gzip decoding of the body
	if r.Header.Get("Content-Encoding") == "gzip" {
		b, err := gzip.NewReader(r.Body)
		if err != nil {
			h.httpError(w, err.Error(), http.StatusBadRequest)
			return
		}
		defer b.Close()
		body = b
	}

	var bs []byte
	if r.ContentLength > 0 {
		if h.Config.MaxBodySize > 0 && r.ContentLength > int64(h.Config.MaxBodySize) {
			h.httpError(w, http.StatusText(http.StatusRequestEntityTooLarge), http.StatusRequestEntityTooLarge)
			return
		}

		// This will just be an initial hint for the gzip reader, as the
		// bytes.Buffer will grow as needed when ReadFrom is called
		bs = make([]byte, 0, r.ContentLength)
	}
	buf := bytes.NewBuffer(bs)

	_, err := buf.ReadFrom(body)
	if err != nil {
		if err == errTruncated {
			h.httpError(w, http.StatusText(http.StatusRequestEntityTooLarge), http.StatusRequestEntityTooLarge)
			return
		}

		if h.Config.WriteTracing {
			h.Logger.Info("Write handler unable to read bytes from request body")
		}
		h.httpError(w, err.Error(), http.StatusBadRequest)
		return
	}
	atomic.AddInt64(&h.stats.WriteRequestBytesReceived, int64(buf.Len()))

	if h.Config.WriteTracing {
		h.Logger.Info("Write body received by handler", zap.ByteString("body", buf.Bytes()))
	}

	points, parseError := models.ParsePointsWithPrecision(buf.Bytes(), time.Now().UTC(), precision)
	// Not points parsed correctly so return the error now
	if parseError != nil && len(points) == 0 {
		if parseError.Error() == "EOF" {
			h.writeHeader(w, http.StatusOK)
			return
		}
		h.httpError(w, parseError.Error(), http.StatusBadRequest)
		return
	}

	// Determine required consistency level.
	level := r.URL.Query().Get("consistency")
	consistency := models.ConsistencyLevelOne
	if level != "" {
		var err error
		consistency, err = models.ParseConsistencyLevel(level)
		if err != nil {
			h.httpError(w, err.Error(), http.StatusBadRequest)
			return
		}
	}

	// Write points.
	if err := h.PointsWriter.WritePoints(database, retentionPolicy, consistency, user, points); influxdb.IsClientError(err) {
		atomic.AddInt64(&h.stats.PointsWrittenFail, int64(len(points)))
		h.httpError(w, err.Error(), http.StatusBadRequest)
		return
	} else if influxdb.IsAuthorizationError(err) {
		atomic.AddInt64(&h.stats.PointsWrittenFail, int64(len(points)))
		h.httpError(w, err.Error(), http.StatusForbidden)
		return
	} else if werr, ok := err.(tsdb.PartialWriteError); ok {
		// Note - we don't always collect all the errors before returning from the call,
		// so PointsWrittenOK might overestimate the number of successful points if multiple shards have errors
		atomic.AddInt64(&h.stats.PointsWrittenOK, int64(len(points)-werr.Dropped))
		atomic.AddInt64(&h.stats.PointsWrittenDropped, int64(werr.Dropped))
		h.httpError(w, werr.Error(), http.StatusBadRequest)
		return
	} else if err != nil {
		atomic.AddInt64(&h.stats.PointsWrittenFail, int64(len(points)))
		h.httpError(w, err.Error(), http.StatusInternalServerError)
		return
	} else if parseError != nil {
		// We wrote some of the points
		atomic.AddInt64(&h.stats.PointsWrittenOK, int64(len(points)))
		// The other points failed to parse which means the client sent invalid line protocol.  We return a 400
		// response code as well as the lines that failed to parse.
		h.httpError(w, tsdb.PartialWriteError{Reason: parseError.Error(),
			Database:        database,
			RetentionPolicy: retentionPolicy,
		}.Error(), http.StatusBadRequest)
		return
	}

	atomic.AddInt64(&h.stats.PointsWrittenOK, int64(len(points)))
	h.writeHeader(w, http.StatusNoContent)
}

// serveOptions returns an empty response to comply with OPTIONS pre-flight requests
func (h *Handler) serveOptions(w http.ResponseWriter, r *http.Request) {
	h.writeHeader(w, http.StatusNoContent)
}

// servePing returns a simple response to let the client know the server is running.
func (h *Handler) servePing(w http.ResponseWriter, r *http.Request) {
	verbose := r.URL.Query().Get("verbose")
	atomic.AddInt64(&h.stats.PingRequests, 1)

	if verbose != "" && verbose != "0" && verbose != "false" {
		h.writeHeader(w, http.StatusOK)
		b, _ := json.Marshal(map[string]string{"version": h.Version})
		w.Write(b)
	} else {
		h.writeHeader(w, http.StatusNoContent)
	}
}

// serveHealth maps v2 health endpoint to ping endpoint
func (h *Handler) serveHealth(w http.ResponseWriter, r *http.Request) {
	resp := map[string]interface{}{
		"name":    "influxdb",
		"message": "ready for queries and writes",
		"status":  "pass",
		"checks":  []string{},
		"version": h.Version,
	}
	b, _ := json.Marshal(resp)
	h.writeHeader(w, http.StatusOK)
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	if _, err := w.Write(b); err != nil {
		h.httpError(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

// serveStatus has been deprecated.
func (h *Handler) serveStatus(w http.ResponseWriter, r *http.Request) {
	h.Logger.Info("WARNING: /status has been deprecated.  Use /ping instead.")
	atomic.AddInt64(&h.stats.StatusRequests, 1)
	h.writeHeader(w, http.StatusNoContent)
}

// convertToEpoch converts result timestamps from time.Time to the specified epoch.
func convertToEpoch(r *query.Result, epoch string) {
	divisor := int64(1)

	switch epoch {
	case "u":
		divisor = int64(time.Microsecond)
	case "ms":
		divisor = int64(time.Millisecond)
	case "s":
		divisor = int64(time.Second)
	case "m":
		divisor = int64(time.Minute)
	case "h":
		divisor = int64(time.Hour)
	}

	for _, s := range r.Series {
		for _, v := range s.Values {
			if ts, ok := v[0].(time.Time); ok {
				v[0] = ts.UnixNano() / divisor
			}
		}
	}
}

// servePromWrite receives data in the Prometheus remote write protocol and writes it
// to the database
func (h *Handler) servePromWrite(w http.ResponseWriter, r *http.Request, user meta.User) {
	atomic.AddInt64(&h.stats.WriteRequests, 1)
	atomic.AddInt64(&h.stats.ActiveWriteRequests, 1)
	atomic.AddInt64(&h.stats.PromWriteRequests, 1)
	defer func(start time.Time) {
		atomic.AddInt64(&h.stats.ActiveWriteRequests, -1)
		atomic.AddInt64(&h.stats.WriteRequestDuration, time.Since(start).Nanoseconds())
	}(time.Now())
	h.requestTracker.Add(r, user)

	database := r.URL.Query().Get("db")
	if database == "" {
		h.httpError(w, "database is required", http.StatusBadRequest)
		return
	}

	if di := h.MetaClient.Database(database); di == nil {
		h.httpError(w, fmt.Sprintf("database not found: %q", database), http.StatusNotFound)
		return
	}

	if h.Config.AuthEnabled {
		if user == nil {
			h.httpError(w, fmt.Sprintf("user is required to write to database %q", database), http.StatusForbidden)
			return
		}

		if err := h.WriteAuthorizer.AuthorizeWrite(user.ID(), database); err != nil {
			h.httpError(w, fmt.Sprintf("%q user is not authorized to write to database %q", user.ID(), database), http.StatusForbidden)
			return
		}
	}

	body := r.Body
	if h.Config.MaxBodySize > 0 {
		body = truncateReader(body, int64(h.Config.MaxBodySize))
	}

	var bs []byte
	if r.ContentLength > 0 {
		if h.Config.MaxBodySize > 0 && r.ContentLength > int64(h.Config.MaxBodySize) {
			h.httpError(w, http.StatusText(http.StatusRequestEntityTooLarge), http.StatusRequestEntityTooLarge)
			return
		}

		// This will just be an initial hint for the reader, as the
		// bytes.Buffer will grow as needed when ReadFrom is called
		bs = make([]byte, 0, r.ContentLength)
	}
	buf := bytes.NewBuffer(bs)

	_, err := buf.ReadFrom(body)
	if err != nil {
		if err == errTruncated {
			h.httpError(w, http.StatusText(http.StatusRequestEntityTooLarge), http.StatusRequestEntityTooLarge)
			return
		}

		if h.Config.WriteTracing {
			h.Logger.Info("Prom write handler unable to read bytes from request body")
		}
		h.httpError(w, err.Error(), http.StatusBadRequest)
		return
	}
	atomic.AddInt64(&h.stats.WriteRequestBytesReceived, int64(buf.Len()))

	if h.Config.WriteTracing {
		h.Logger.Info("Prom write body received by handler", zap.ByteString("body", buf.Bytes()))
	}

	reqBuf, err := snappy.Decode(nil, buf.Bytes())
	if err != nil {
		h.httpError(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Convert the Prometheus remote write request to Influx Points
	var req prompb.WriteRequest
	if err := req.Unmarshal(reqBuf); err != nil {
		h.httpError(w, err.Error(), http.StatusBadRequest)
		return
	}

	points, err := prometheus.WriteRequestToPoints(&req)
	if err != nil {
		if h.Config.WriteTracing {
			h.Logger.Info("Prom write handler", zap.Error(err))
		}

		// Check if the error was from something other than dropping invalid values.
		if _, ok := err.(prometheus.DroppedValuesError); !ok {
			h.httpError(w, err.Error(), http.StatusBadRequest)
			return
		}
	}

	// Determine required consistency level.
	level := r.URL.Query().Get("consistency")
	consistency := models.ConsistencyLevelOne
	if level != "" {
		consistency, err = models.ParseConsistencyLevel(level)
		if err != nil {
			h.httpError(w, err.Error(), http.StatusBadRequest)
			return
		}
	}

	// Write points.
	if err := h.PointsWriter.WritePoints(database, r.URL.Query().Get("rp"), consistency, user, points); influxdb.IsClientError(err) {
		atomic.AddInt64(&h.stats.PointsWrittenFail, int64(len(points)))
		h.httpError(w, err.Error(), http.StatusBadRequest)
		return
	} else if influxdb.IsAuthorizationError(err) {
		atomic.AddInt64(&h.stats.PointsWrittenFail, int64(len(points)))
		h.httpError(w, err.Error(), http.StatusForbidden)
		return
	} else if werr, ok := err.(tsdb.PartialWriteError); ok {
		atomic.AddInt64(&h.stats.PointsWrittenOK, int64(len(points)-werr.Dropped))
		atomic.AddInt64(&h.stats.PointsWrittenDropped, int64(werr.Dropped))
		h.httpError(w, werr.Error(), http.StatusBadRequest)
		return
	} else if err != nil {
		atomic.AddInt64(&h.stats.PointsWrittenFail, int64(len(points)))
		h.httpError(w, err.Error(), http.StatusInternalServerError)
		return
	}

	atomic.AddInt64(&h.stats.PointsWrittenOK, int64(len(points)))
	h.writeHeader(w, http.StatusNoContent)
}

// servePromRead will convert a Prometheus remote read request into a storage
// query and returns data in Prometheus remote read protobuf format.
func (h *Handler) servePromRead(w http.ResponseWriter, r *http.Request, user meta.User) {
	atomic.AddInt64(&h.stats.PromReadRequests, 1)
	h.requestTracker.Add(r, user)
	compressed, err := io.ReadAll(r.Body)
	if err != nil {
		h.httpError(w, err.Error(), http.StatusInternalServerError)
		return
	}

	reqBuf, err := snappy.Decode(nil, compressed)
	if err != nil {
		h.httpError(w, err.Error(), http.StatusBadRequest)
		return
	}

	var req prompb.ReadRequest
	if err := req.Unmarshal(reqBuf); err != nil {
		h.httpError(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Query the DB and create a ReadResponse for Prometheus
	db := r.FormValue("db")
	rp := r.FormValue("rp")

	if h.Config.AuthEnabled && h.Config.PromReadAuthEnabled {
		if user == nil {
			h.httpError(w, fmt.Sprintf("user is required to read from database %q", db), http.StatusForbidden)
			return
		}
		if h.QueryAuthorizer.AuthorizeDatabase(user, influxql.ReadPrivilege, db) != nil {
			h.httpError(w, fmt.Sprintf("user %q is not authorized to read from database %q", user.ID(), db), http.StatusForbidden)
			return
		}
	}

	readRequest, err := prometheus.ReadRequestToInfluxStorageRequest(&req, db, rp)
	if err != nil {
		h.httpError(w, err.Error(), http.StatusBadRequest)
		return
	}

	respond := func(resp *prompb.ReadResponse) {
		data, err := resp.Marshal()
		if err != nil {
			h.httpError(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/x-protobuf")
		w.Header().Set("Content-Encoding", "snappy")

		compressed = snappy.Encode(nil, data)
		if _, err := w.Write(compressed); err != nil {
			h.httpError(w, err.Error(), http.StatusInternalServerError)
			return
		}

		atomic.AddInt64(&h.stats.QueryRequestBytesTransmitted, int64(len(compressed)))
	}

	ctx := context.Background()
	rs, err := h.Store.ReadFilter(ctx, readRequest)
	if err != nil {
		h.httpError(w, err.Error(), http.StatusBadRequest)
		return
	}

	resp := &prompb.ReadResponse{
		Results: []*prompb.QueryResult{{}},
	}

	if rs == nil {
		respond(resp)
		return
	}
	defer rs.Close()

	for rs.Next() {
		cur := rs.Cursor()
		if cur == nil {
			// no data for series key + field combination
			continue
		}

		tags := prometheus.RemoveInfluxSystemTags(rs.Tags())
		var unsupportedCursor string
		switch cur := cur.(type) {
		case tsdb.FloatArrayCursor:
			var series *prompb.TimeSeries
			for {
				a := cur.Next()
				if a.Len() == 0 {
					break
				}

				// We have some data for this series.
				if series == nil {
					series = &prompb.TimeSeries{
						Labels: prometheus.ModelTagsToLabelPairs(tags),
					}
				}

				for i, ts := range a.Timestamps {
					series.Samples = append(series.Samples, prompb.Sample{
						Timestamp: ts / int64(time.Millisecond),
						Value:     a.Values[i],
					})
				}
			}

			// There was data for the series.
			if series != nil {
				resp.Results[0].Timeseries = append(resp.Results[0].Timeseries, series)
			}
		case tsdb.IntegerArrayCursor:
			unsupportedCursor = "int64"
		case tsdb.UnsignedArrayCursor:
			unsupportedCursor = "uint"
		case tsdb.BooleanArrayCursor:
			unsupportedCursor = "bool"
		case tsdb.StringArrayCursor:
			unsupportedCursor = "string"
		default:
			panic(fmt.Sprintf("unreachable: %T", cur))
		}
		cur.Close()

		if len(unsupportedCursor) > 0 {
			h.Logger.Info("Prometheus can't read cursor",
				zap.String("cursor_type", unsupportedCursor),
				zap.Stringer("series", tags),
			)
		}
	}

	respond(resp)
}

func (h *Handler) serveFluxQuery(w http.ResponseWriter, r *http.Request, user meta.User) {
	atomic.AddInt64(&h.stats.FluxQueryRequests, 1)
	defer func(start time.Time) {
		atomic.AddInt64(&h.stats.FluxQueryRequestDuration, time.Since(start).Nanoseconds())
	}(time.Now())

	req, err := decodeQueryRequest(r)
	if err != nil {
		h.httpError(w, err.Error(), http.StatusBadRequest)
		return
	}

	ctx := r.Context()
	if val := r.FormValue("node_id"); val != "" {
		if nodeID, err := strconv.ParseUint(val, 10, 64); err == nil {
			ctx = storage.NewContextWithReadOptions(ctx, &storage.ReadOptions{NodeID: nodeID})
		}
	}

	if h.Config.AuthEnabled {
		ctx = meta.NewContextWithUser(ctx, user)
	}

	pr := req.ProxyRequest()

	// Logging
	var (
		stats flux.Statistics
		n     int64
	)
	if h.Config.FluxLogEnabled {
		defer func() {
			h.logFluxQuery(n, stats, pr.Compiler, err)
		}()
	}

	q, err := h.Controller.Query(ctx, pr.Compiler)
	if err != nil {
		h.httpError(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer func() {
		q.Cancel()
		q.Done()
	}()

	// NOTE: We do not write out the headers here.
	// It is possible that if the encoding step fails
	// that we can write an error header so long as
	// the encoder did not write anything.
	// As such we rely on the http.ResponseWriter behavior
	// to write an StatusOK header with the first write.

	switch r.Header.Get("Accept") {
	case "text/csv":
		fallthrough
	default:
		if hd, ok := pr.Dialect.(httpDialect); !ok {
			h.httpError(w, fmt.Sprintf("unsupported dialect over HTTP %T", req.Dialect), http.StatusBadRequest)
			return
		} else {
			hd.SetHeaders(w)
		}
		encoder := pr.Dialect.Encoder()
		results := flux.NewResultIteratorFromQuery(q)
		if h.Config.FluxLogEnabled {
			defer func() {
				stats = results.Statistics()
			}()
		}
		defer results.Release()

		n, err = encoder.Encode(w, results)
		if err != nil {
			if n == 0 {
				// If the encoder did not write anything, we can write an error header.
				h.httpError(w, err.Error(), http.StatusInternalServerError)
			} else {
				atomic.AddInt64(&h.stats.FluxQueryRequestBytesTransmitted, int64(n))
			}
		}
	}
}

func (h *Handler) serveFluxQueryDisabled(w http.ResponseWriter, r *http.Request, user meta.User) {
	h.Logger.Warn("Received flux query but flux-enabled=false in [http] section of InfluxDB config")
	h.httpError(w, "Flux query service disabled. Verify flux-enabled=true in the [http] section of the InfluxDB config.", http.StatusForbidden)
}

func (h *Handler) logFluxQuery(n int64, stats flux.Statistics, compiler flux.Compiler, err error) {
	var q string
	switch c := compiler.(type) {
	//case lang.SpecCompiler:
	//	q = fmt.Sprint(flux.Formatted(c.Spec))
	case lang.FluxCompiler:
		q = c.Query
	}

	h.Logger.Info("Executed Flux query",
		zap.String("compiler_type", string(compiler.CompilerType())),
		zap.Int64("response_size", n),
		zap.String("query", q),
		zap.Error(err),
		zap.Duration("stat_total_duration", stats.TotalDuration),
		zap.Duration("stat_compile_duration", stats.CompileDuration),
		zap.Duration("stat_execute_duration", stats.ExecuteDuration),
		zap.Int64("stat_max_allocated", stats.MaxAllocated),
		zap.Int64("stat_total_allocated", stats.TotalAllocated),
	)
}

// serveExpvar serves internal metrics in /debug/vars format over HTTP.
func (h *Handler) serveExpvar(w http.ResponseWriter, r *http.Request) {
	// Retrieve statistics from the monitor.
	stats, err := h.Monitor.Statistics(nil)
	if err != nil {
		h.httpError(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Retrieve diagnostics from the monitor.
	diags, err := h.Monitor.Diagnostics()
	if err != nil {
		h.httpError(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json; charset=utf-8")

	first := true
	if val := diags["system"]; val != nil {
		jv, err := parseSystemDiagnostics(val)
		if err != nil {
			h.httpError(w, err.Error(), http.StatusInternalServerError)
			return
		}

		data, err := json.Marshal(jv)
		if err != nil {
			h.httpError(w, err.Error(), http.StatusInternalServerError)
			return
		}

		first = false
		fmt.Fprintln(w, "{")
		fmt.Fprintf(w, "\"system\": %s", data)
	} else {
		fmt.Fprintln(w, "{")
	}

	if val := diags["build"]; val != nil {
		jv, err := parseBuildInfo(val)
		if err != nil {
			h.httpError(w, err.Error(), http.StatusInternalServerError)
			return
		}
		data, err := json.Marshal(jv)
		if err != nil {
			h.httpError(w, err.Error(), http.StatusInternalServerError)
			return
		}

		if !first {
			fmt.Fprintln(w, ",")
		}
		first = false
		fmt.Fprintf(w, "\"build\": %s", data)
	}

	if val := expvar.Get("cmdline"); val != nil {
		if !first {
			fmt.Fprintln(w, ",")
		}
		first = false
		fmt.Fprintf(w, "\"cmdline\": %s", val)
	}

	// We're going to print some kind of crypto data, we just
	// need to find the proper source for it.
	{
		var jv map[string]interface{}
		val := diags["crypto"]
		if val != nil {
			jv, err = parseCryptoDiagnostics(val)
			if err != nil {
				if errors.Is(err, ErrDiagnosticsValueMissing) {
					// log missing values, but don't error out
					h.Logger.Warn(err.Error())
				} else {
					h.httpError(w, err.Error(), http.StatusInternalServerError)
					return
				}
			}
		} else {
			jv = ossCryptoDiagnostics()
		}

		data, err := json.Marshal(jv)
		if err != nil {
			h.httpError(w, err.Error(), http.StatusInternalServerError)
			return
		}

		if !first {
			fmt.Fprintln(w, ",")
		}
		first = false
		fmt.Fprintf(w, "\"crypto\": %s", data)
	}

	if val := expvar.Get("memstats"); val != nil {
		if !first {
			fmt.Fprintln(w, ",")
		}
		first = false
		fmt.Fprintf(w, "\"memstats\": %s", val)
	}

	uniqueKeys := make(map[string]int)

	for _, s := range stats {
		val, err := json.Marshal(s)
		if err != nil {
			continue
		}

		// Very hackily create a unique key.
		buf := bytes.NewBufferString(s.Name)
		if path, ok := s.Tags["path"]; ok {
			fmt.Fprintf(buf, ":%s", path)
			if id, ok := s.Tags["id"]; ok {
				fmt.Fprintf(buf, ":%s", id)
			}
		} else if bind, ok := s.Tags["bind"]; ok {
			if proto, ok := s.Tags["proto"]; ok {
				fmt.Fprintf(buf, ":%s", proto)
			}
			fmt.Fprintf(buf, ":%s", bind)
		} else if database, ok := s.Tags["database"]; ok {
			fmt.Fprintf(buf, ":%s", database)
			if rp, ok := s.Tags["retention_policy"]; ok {
				fmt.Fprintf(buf, ":%s", rp)
				if name, ok := s.Tags["name"]; ok {
					fmt.Fprintf(buf, ":%s", name)
				}
				if dest, ok := s.Tags["destination"]; ok {
					fmt.Fprintf(buf, ":%s", dest)
				}
			}
		}
		key := buf.String()
		v := uniqueKeys[key]
		uniqueKeys[key] = v + 1
		if v > 0 {
			fmt.Fprintf(buf, ":%d", v)
			key = buf.String()
		}

		if !first {
			fmt.Fprintln(w, ",")
		}
		first = false
		fmt.Fprintf(w, "%q: ", key)
		w.Write(bytes.TrimSpace(val))
	}
	fmt.Fprintln(w, "\n}")
}

// serveDebugRequests will track requests for a period of time.
func (h *Handler) serveDebugRequests(w http.ResponseWriter, r *http.Request) {
	var d time.Duration
	if s := r.URL.Query().Get("seconds"); s == "" {
		d = DefaultDebugRequestsInterval
	} else if seconds, err := strconv.ParseInt(s, 10, 64); err != nil {
		h.httpError(w, err.Error(), http.StatusBadRequest)
		return
	} else {
		d = time.Duration(seconds) * time.Second
		if d > MaxDebugRequestsInterval {
			h.httpError(w, fmt.Sprintf("exceeded maximum interval time: %s > %s",
				influxql.FormatDuration(d),
				influxql.FormatDuration(MaxDebugRequestsInterval)),
				http.StatusBadRequest)
			return
		}
	}

	var closing <-chan bool
	if notifier, ok := w.(http.CloseNotifier); ok {
		closing = notifier.CloseNotify()
	}

	profile := h.requestTracker.TrackRequests()

	timer := time.NewTimer(d)
	select {
	case <-timer.C:
		profile.Stop()
	case <-closing:
		// Connection was closed early.
		profile.Stop()
		timer.Stop()
		return
	}

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Add("Connection", "close")

	fmt.Fprintln(w, "{")
	first := true
	for req, st := range profile.Requests {
		val, err := json.Marshal(st)
		if err != nil {
			continue
		}

		if !first {
			fmt.Fprintln(w, ",")
		}
		first = false
		fmt.Fprintf(w, "%q: ", req.String())
		w.Write(bytes.TrimSpace(val))
	}
	fmt.Fprintln(w, "\n}")
}

// parseSystemDiagnostics converts the system diagnostics into an appropriate
// format for marshaling to JSON in the /debug/vars format.
func parseSystemDiagnostics(d *diagnostics.Diagnostics) (map[string]interface{}, error) {
	// We don't need PID in this case.
	m := map[string]interface{}{"currentTime": nil, "started": nil, "uptime": nil}
	for key := range m {
		// Find the associated column.
		ci := -1
		for i, col := range d.Columns {
			if col == key {
				ci = i
				break
			}
		}

		if ci == -1 {
			return nil, fmt.Errorf("unable to find column %q", key)
		}

		if len(d.Rows) < 1 || len(d.Rows[0]) <= ci {
			return nil, fmt.Errorf("no data for column %q", key)
		}

		var res interface{}
		switch v := d.Rows[0][ci].(type) {
		case time.Time:
			res = v
		case string:
			// Should be a string representation of a time.Duration
			d, err := time.ParseDuration(v)
			if err != nil {
				return nil, err
			}
			res = int64(d.Seconds())
		default:
			return nil, fmt.Errorf("value for column %q is not parsable (got %T)", key, v)
		}
		m[key] = res
	}
	return m, nil
}

// parseBuildInfo converts the build info diagnostics into an appropriate
// format for marshaling to JSON in the /debug/vars format.
func parseBuildInfo(d *diagnostics.Diagnostics) (map[string]interface{}, error) {
	m := map[string]interface{}{"Version": nil, "Commit": nil, "Branch": nil, "Build Time": nil}
	for key := range m {
		// Find the associated column.
		ci := -1
		for i, col := range d.Columns {
			if col == key {
				ci = i
				break
			}
		}

		if ci == -1 {
			return nil, fmt.Errorf("unable to find column %q", key)
		}

		if len(d.Rows) < 1 || len(d.Rows[0]) <= ci {
			return nil, fmt.Errorf("no data for column %q", key)
		}
		m[key] = d.Rows[0][ci]
	}
	return m, nil
}

// ossCryptoDiagnostics creates a default crypto diagnostics map that
// can be marshaled into JSON for /debug/vars.
func ossCryptoDiagnostics() map[string]interface{} {
	return map[string]interface{}{
		"ensureFIPS":     false,
		"FIPS":           false,
		"implementation": "Go",
		"passwordHash":   "bcrypt",
	}
}

// parseCryptoDiagnostics converts the crypto diagnostics into an appropriate
// format for marshaling to JSON in the /debug/vars format.
func parseCryptoDiagnostics(d *diagnostics.Diagnostics) (map[string]interface{}, error) {
	// We use ossCryptoDiagnostics as a template for columns we need to pull from d.
	// If the column is missing from d, we will nil out the value in m to avoid lying
	// about a value to the user and making troubleshooting harder.
	m := ossCryptoDiagnostics()
	var missing []string

	for key := range m {
		// Find the associated column.
		ci := -1
		for i, col := range d.Columns {
			if col == key {
				ci = i
				break
			}
		}

		// Don't error out if we can't find the column or cell for a given key, just nil
		// out the value in m. There could still be other useful information we gather.
		// Column not found or data cell not found
		if ci == -1 || len(d.Rows) < 1 || len(d.Rows[0]) <= ci {
			m[key] = nil
			missing = append(missing, key)
			continue
		}

		m[key] = d.Rows[0][ci]
	}

	if len(missing) > 0 {
		// If you're getting this error, you probably need to update enterprise.
		return m, fmt.Errorf("parseCryptoDiagnostics: missing %s: %w", strings.Join(missing, ","), ErrDiagnosticsValueMissing)
	}
	return m, nil
}

// httpError writes an error to the client in a standard format.
func (h *Handler) httpError(w http.ResponseWriter, errmsg string, code int) {
	if code == http.StatusUnauthorized {
		// If an unauthorized header will be sent back, add a WWW-Authenticate header
		// as an authorization challenge.
		w.Header().Set("WWW-Authenticate", fmt.Sprintf("Basic realm=\"%s\"", h.Config.Realm))
	} else if code/100 != 2 {
		sz := math.Min(float64(len(errmsg)), 1024.0)
		w.Header().Set("X-InfluxDB-Error", errmsg[:int(sz)])
	}

	response := Response{Err: errors.New(errmsg)}
	if rw, ok := w.(ResponseWriter); ok {
		h.writeHeader(w, code)
		rw.WriteResponse(response)
		return
	}

	// Default implementation if the response writer hasn't been replaced
	// with our special response writer type.
	w.Header().Add("Content-Type", "application/json")
	h.writeHeader(w, code)
	b, _ := json.Marshal(response)
	w.Write(b)
}

// Filters and filter helpers

type credentials struct {
	Method   AuthenticationMethod
	Username string
	Password string
	Token    string
}

func parseToken(token string) (user, pass string, ok bool) {
	if t1, t2, ok := strings.Cut(token, ":"); ok {
		return t1, t2, ok
	}
	return
}

// parseCredentials parses a request and returns the authentication credentials.
// The credentials may be present as URL query params, or as a Basic
// Authentication header.
// As params: http://127.0.0.1/query?u=username&p=password
// As basic auth: http://username:password@127.0.0.1
// As Bearer token in Authorization header: Bearer <JWT_TOKEN_BLOB>
// As Token in Authorization header: Token <username:password>
func parseCredentials(r *http.Request) (*credentials, error) {
	q := r.URL.Query()

	// Check for username and password in URL params.
	if u, p := q.Get("u"), q.Get("p"); u != "" && p != "" {
		return &credentials{
			Method:   UserAuthentication,
			Username: u,
			Password: p,
		}, nil
	}

	// Check for the HTTP Authorization header.
	if s := r.Header.Get("Authorization"); s != "" {
		// Check for Bearer token.
		strs := strings.Split(s, " ")
		if len(strs) == 2 {
			switch strs[0] {
			case "Bearer":
				return &credentials{
					Method: BearerAuthentication,
					Token:  strs[1],
				}, nil
			case "Token":
				if u, p, ok := parseToken(strs[1]); ok {
					return &credentials{
						Method:   UserAuthentication,
						Username: u,
						Password: p,
					}, nil
				}
			}
		}

		// Check for basic auth.
		if u, p, ok := r.BasicAuth(); ok {
			return &credentials{
				Method:   UserAuthentication,
				Username: u,
				Password: p,
			}, nil
		}
	}

	return nil, fmt.Errorf("unable to parse authentication credentials")
}

// authenticate wraps a handler and ensures that if user credentials are passed in
// an attempt is made to authenticate that user. If authentication fails, an error is returned.
//
// There is one exception: if there are no users in the system, authentication is not required. This
// is to facilitate bootstrapping of a system with authentication enabled.
func authenticate(inner func(http.ResponseWriter, *http.Request, meta.User), h *Handler, requireAuthentication bool) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Return early if we are not authenticating
		if !requireAuthentication {
			inner(w, r, nil)
			return
		}
		var user meta.User

		// TODO corylanou: never allow this in the future without users
		if requireAuthentication && h.MetaClient.AdminUserExists() {
			creds, err := parseCredentials(r)
			if err != nil {
				atomic.AddInt64(&h.stats.AuthenticationFailures, 1)
				h.httpError(w, err.Error(), http.StatusUnauthorized)
				return
			}

			switch creds.Method {
			case UserAuthentication:
				if creds.Username == "" {
					atomic.AddInt64(&h.stats.AuthenticationFailures, 1)
					h.httpError(w, "username required", http.StatusUnauthorized)
					return
				}

				user, err = h.MetaClient.Authenticate(creds.Username, creds.Password)
				if err != nil {
					atomic.AddInt64(&h.stats.AuthenticationFailures, 1)
					h.httpError(w, "authorization failed", http.StatusUnauthorized)
					return
				}
			case BearerAuthentication:
				if h.Config.SharedSecret == "" {
					atomic.AddInt64(&h.stats.AuthenticationFailures, 1)
					h.httpError(w, "bearer auth disabled", http.StatusUnauthorized)
					return
				}
				keyLookupFn := func(token *jwt.Token) (interface{}, error) {
					// Check for expected signing method.
					if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
						return nil, fmt.Errorf("unexpected signing method: %v", token.Header["alg"])
					}
					return []byte(h.Config.SharedSecret), nil
				}

				// Parse and validate the token.
				token, err := jwt.Parse(creds.Token, keyLookupFn)
				if err != nil {
					h.httpError(w, err.Error(), http.StatusUnauthorized)
					return
				} else if !token.Valid {
					h.httpError(w, "invalid token", http.StatusUnauthorized)
					return
				}

				claims, ok := token.Claims.(jwt.MapClaims)
				if !ok {
					h.httpError(w, "problem authenticating token", http.StatusInternalServerError)
					h.Logger.Info("Could not assert JWT token claims as jwt.MapClaims")
					return
				}

				// Make sure an expiration was set on the token.
				if exp, ok := claims["exp"].(float64); !ok || exp <= 0.0 {
					h.httpError(w, "token expiration required", http.StatusUnauthorized)
					return
				}

				// Get the username from the token.
				username, ok := claims["username"].(string)
				if !ok {
					h.httpError(w, "username in token must be a string", http.StatusUnauthorized)
					return
				} else if username == "" {
					h.httpError(w, "token must contain a username", http.StatusUnauthorized)
					return
				}

				// Lookup user in the metastore.
				if user, err = h.MetaClient.User(username); err != nil {
					h.httpError(w, err.Error(), http.StatusUnauthorized)
					return
				} else if user == nil {
					h.httpError(w, meta.ErrUserNotFound.Error(), http.StatusUnauthorized)
					return
				}
			default:
				h.httpError(w, "unsupported authentication", http.StatusUnauthorized)
			}

		}
		inner(w, r, user)
	})
}

// cors responds to incoming requests and adds the appropriate cors headers
// TODO: corylanou: add the ability to configure this in our config
func cors(inner http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if origin := r.Header.Get("Origin"); origin != "" {
			w.Header().Set(`Access-Control-Allow-Origin`, origin)
			w.Header().Set(`Access-Control-Allow-Methods`, strings.Join([]string{
				`DELETE`,
				`GET`,
				`OPTIONS`,
				`POST`,
				`PUT`,
				`PATCH`,
			}, ", "))

			w.Header().Set(`Access-Control-Allow-Headers`, strings.Join([]string{
				`Accept`,
				`Accept-Encoding`,
				`Authorization`,
				`Content-Length`,
				`Content-Type`,
				`User-Agent`,
				`X-CSRF-Token`,
				`X-HTTP-Method-Override`,
			}, ", "))

			w.Header().Set(`Access-Control-Expose-Headers`, strings.Join([]string{
				`Date`,
				`X-InfluxDB-Version`,
				`X-InfluxDB-Build`,
			}, ", "))
		}

		if r.Method == "OPTIONS" {
			return
		}

		inner.ServeHTTP(w, r)
	})
}

func requestID(inner http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// X-Request-Id takes priority.
		rid := r.Header.Get("X-Request-Id")

		// If X-Request-Id is empty, then check Request-Id
		if rid == "" {
			rid = r.Header.Get("Request-Id")
		}

		// If Request-Id is empty then generate a v1 UUID.
		if rid == "" {
			rid = uuid.TimeUUID().String()
		}

		// We read Request-Id in other handler code so we'll use that naming
		// convention from this point in the request cycle.
		r.Header.Set("Request-Id", rid)

		// Set the request ID on the response headers.
		// X-Request-Id is the most common name for a request ID header.
		w.Header().Set("X-Request-Id", rid)

		// We will also set Request-Id for backwards compatibility with previous
		// versions of InfluxDB.
		w.Header().Set("Request-Id", rid)

		inner.ServeHTTP(w, r)
	})
}

func (h *Handler) SetHeadersHandler(handler http.Handler) http.Handler {
	return http.HandlerFunc(h.SetHeadersWrapper(handler.ServeHTTP))
}

// wrapper that adds user supplied headers to the response.
func (h *Handler) SetHeadersWrapper(f func(http.ResponseWriter, *http.Request)) func(http.ResponseWriter, *http.Request) {
	if len(h.Config.HTTPHeaders) == 0 {
		return f
	}

	return func(w http.ResponseWriter, r *http.Request) {
		for header, value := range h.Config.HTTPHeaders {
			w.Header().Add(header, value)
		}
		f(w, r)
	}
}

func (h *Handler) logging(inner http.Handler, name string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		l := &responseLogger{w: w}
		inner.ServeHTTP(l, r)

		if h.accessLogFilters.Match(l.Status()) {
			h.CLFLogger.Println(buildLogLine(l, r, start))
		}

		// Log server errors.
		if l.Status()/100 == 5 {
			errStr := l.Header().Get("X-InfluxDB-Error")
			if errStr != "" {
				h.Logger.Error(fmt.Sprintf("[%d] - %q", l.Status(), errStr))
			}
		}
	})
}

func (h *Handler) responseWriter(inner http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w = NewResponseWriter(w, r)
		inner.ServeHTTP(w, r)
	})
}

// if the env var is set, and the value is truthy, then we will *not*
// recover from a panic.
var willCrash bool

func init() {
	var err error
	if willCrash, err = strconv.ParseBool(os.Getenv(query.PanicCrashEnv)); err != nil {
		willCrash = false
	}
}

func (h *Handler) recovery(inner http.Handler, name string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		l := &responseLogger{w: w}

		defer func() {
			if err := recover(); err != nil {
				logLine := buildLogLine(l, r, start)
				logLine = fmt.Sprintf("%s [panic:%s] %s", logLine, err, debug.Stack())
				h.CLFLogger.Println(logLine)
				http.Error(w, http.StatusText(http.StatusInternalServerError), 500)
				atomic.AddInt64(&h.stats.RecoveredPanics, 1) // Capture the panic in _internal stats.

				if willCrash {
					h.CLFLogger.Println("\n\n=====\nAll goroutines now follow:")
					buf := debug.Stack()
					h.CLFLogger.Printf("%s\n", buf)
					os.Exit(1) // If we panic then the Go server will recover.
				}
			}
		}()

		inner.ServeHTTP(l, r)
	})
}

// Store describes the behaviour of the storage packages Store type.
type Store interface {
	ReadFilter(ctx context.Context, req *datatypes.ReadFilterRequest) (reads.ResultSet, error)
	Delete(database string, sources []influxql.Source, condition influxql.Expr) error
	DeleteRetentionPolicy(database, name string) error
}

// Response represents a list of statement results.
type Response struct {
	Results []*query.Result
	Err     error
}

// MarshalJSON encodes a Response struct into JSON.
func (r Response) MarshalJSON() ([]byte, error) {
	// Define a struct that outputs "error" as a string.
	var o struct {
		Results []*query.Result `json:"results,omitempty"`
		Err     string          `json:"error,omitempty"`
	}

	// Copy fields to output struct.
	o.Results = r.Results
	if r.Err != nil {
		o.Err = r.Err.Error()
	}

	return json.Marshal(&o)
}

// UnmarshalJSON decodes the data into the Response struct.
func (r *Response) UnmarshalJSON(b []byte) error {
	var o struct {
		Results []*query.Result `json:"results,omitempty"`
		Err     string          `json:"error,omitempty"`
	}

	err := json.Unmarshal(b, &o)
	if err != nil {
		return err
	}
	r.Results = o.Results
	if o.Err != "" {
		r.Err = errors.New(o.Err)
	}
	return nil
}

// Error returns the first error from any statement.
// Returns nil if no errors occurred on any statements.
func (r *Response) Error() error {
	if r.Err != nil {
		return r.Err
	}
	for _, rr := range r.Results {
		if rr.Err != nil {
			return rr.Err
		}
	}
	return nil
}

// Throttler represents an HTTP throttler that limits the number of concurrent
// requests being processed as well as the number of enqueued requests.
type Throttler struct {
	current  chan struct{}
	enqueued chan struct{}

	// Maximum amount of time requests can wait in queue.
	// Must be set before adding middleware.
	EnqueueTimeout time.Duration

	Logger *zap.Logger
}

// NewThrottler returns a new instance of Throttler that limits to concurrentN.
// requests processed at a time and maxEnqueueN requests waiting to be processed.
func NewThrottler(concurrentN, maxEnqueueN int) *Throttler {
	return &Throttler{
		current:  make(chan struct{}, concurrentN),
		enqueued: make(chan struct{}, concurrentN+maxEnqueueN),
		Logger:   zap.NewNop(),
	}
}

// Handler wraps h in a middleware handler that throttles requests.
func (t *Throttler) Handler(h http.Handler) http.Handler {
	timeout := t.EnqueueTimeout

	// Return original handler if concurrent requests is zero.
	if cap(t.current) == 0 {
		return h
	}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Start a timer to limit enqueued request times.
		var timerCh <-chan time.Time
		if timeout > 0 {
			timer := time.NewTimer(timeout)
			defer timer.Stop()
			timerCh = timer.C
		}

		// Wait for a spot in the queue.
		if cap(t.enqueued) > cap(t.current) {
			select {
			case t.enqueued <- struct{}{}:
				defer func() { <-t.enqueued }()
			default:
				t.Logger.Warn("request throttled, queue full", zap.Duration("d", timeout))
				http.Error(w, "request throttled, queue full", http.StatusServiceUnavailable)
				return
			}
		}

		// First check if we can immediately send in to current because there is
		// available capacity. This helps reduce racyness in tests.
		select {
		case t.current <- struct{}{}:
		default:
			// Wait for a spot in the list of concurrent requests, but allow checking the timeout.
			select {
			case t.current <- struct{}{}:
			case <-timerCh:
				t.Logger.Warn("request throttled, exceeds timeout", zap.Duration("d", timeout))
				http.Error(w, "request throttled, exceeds timeout", http.StatusServiceUnavailable)
				return
			}
		}
		defer func() { <-t.current }()

		// Execute request.
		h.ServeHTTP(w, r)
	})
}

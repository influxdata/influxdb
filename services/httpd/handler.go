package httpd

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"errors"
	"expvar"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/pprof"
	"os"
	"runtime/debug"
	"strconv"
	"strings"
	"time"

	"github.com/bmizerany/pat"
	"github.com/dgrijalva/jwt-go"
	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/influxql"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/services/continuous_querier"
	"github.com/influxdata/influxdb/services/meta"
	"github.com/influxdata/influxdb/uuid"
)

const (
	// DefaultChunkSize specifies the maximum number of points that will
	// be read before sending results back to the engine.
	//
	// This has no relation to the number of bytes that are returned.
	DefaultChunkSize = 10000
)

// AuthenticationMethod defines the type of authentication used.
type AuthenticationMethod int

// Supported authentication methods.
const (
	UserAuthentication AuthenticationMethod = iota
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

// Handler represents an HTTP handler for the InfluxDB server.
type Handler struct {
	mux     *pat.PatternServeMux
	Version string

	MetaClient interface {
		Database(name string) *meta.DatabaseInfo
		Authenticate(username, password string) (ui *meta.UserInfo, err error)
		Users() []meta.UserInfo
		User(username string) (*meta.UserInfo, error)
	}

	QueryAuthorizer interface {
		AuthorizeQuery(u *meta.UserInfo, query *influxql.Query, database string) error
	}

	WriteAuthorizer interface {
		AuthorizeWrite(username, database string) error
	}

	QueryExecutor *influxql.QueryExecutor

	PointsWriter interface {
		WritePoints(database, retentionPolicy string, consistencyLevel models.ConsistencyLevel, points []models.Point) error
	}

	ContinuousQuerier continuous_querier.ContinuousQuerier

	Config    *Config
	Logger    *log.Logger
	CLFLogger *log.Logger
	statMap   *expvar.Map
}

// NewHandler returns a new instance of handler with routes.
func NewHandler(c Config, statMap *expvar.Map) *Handler {
	h := &Handler{
		mux:       pat.New(),
		Config:    &c,
		Logger:    log.New(os.Stderr, "[httpd] ", log.LstdFlags),
		CLFLogger: log.New(os.Stderr, "[httpd] ", 0),
		statMap:   statMap,
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
			"POST", "/write", true, true, h.serveWrite,
		},
		Route{ // Ping
			"ping",
			"GET", "/ping", false, true, h.servePing,
		},
		Route{ // Ping
			"ping-head",
			"HEAD", "/ping", false, true, h.servePing,
		},
		Route{ // Ping w/ status
			"status",
			"GET", "/status", false, true, h.serveStatus,
		},
		Route{ // Ping w/ status
			"status-head",
			"HEAD", "/status", false, true, h.serveStatus,
		},
		// TODO: (corylanou) remove this and associated code
		Route{ // Tell data node to run CQs that should be run
			"process-continuous-queries",
			"POST", "/data/process_continuous_queries", false, false, h.serveProcessContinuousQueries,
		},
	}...)

	return h
}

// AddRoutes sets the provided routes on the handler.
func (h *Handler) AddRoutes(routes ...Route) {
	for _, r := range routes {
		var handler http.Handler

		// If it's a handler func that requires authorization, wrap it in authentication
		if hf, ok := r.HandlerFunc.(func(http.ResponseWriter, *http.Request, *meta.UserInfo)); ok {
			handler = authenticate(hf, h, h.Config.AuthEnabled)
		}

		// This is a normal handler signature and does not require authentication
		if hf, ok := r.HandlerFunc.(func(http.ResponseWriter, *http.Request)); ok {
			handler = http.HandlerFunc(hf)
		}

		if r.Gzipped {
			handler = gzipFilter(handler)
		}
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
	h.statMap.Add(statRequest, 1)
	h.statMap.Add(statRequestsActive, 1)
	start := time.Now()

	// Add version header to all InfluxDB requests.
	w.Header().Add("X-Influxdb-Version", h.Version)

	// FIXME(benbjohnson): Add pprof enabled flag.
	if strings.HasPrefix(r.URL.Path, "/debug/pprof") {
		switch r.URL.Path {
		case "/debug/pprof/cmdline":
			pprof.Cmdline(w, r)
		case "/debug/pprof/profile":
			pprof.Profile(w, r)
		case "/debug/pprof/symbol":
			pprof.Symbol(w, r)
		default:
			pprof.Index(w, r)
		}
	} else if strings.HasPrefix(r.URL.Path, "/debug/vars") {
		serveExpvar(w, r)
	} else {
		h.mux.ServeHTTP(w, r)
	}

	h.statMap.Add(statRequestsActive, -1)
	h.statMap.Add(statRequestDuration, time.Since(start).Nanoseconds())
}

// writeHeader writes the provided status code in the response, and
// updates relevant http error statistics.
func (h *Handler) writeHeader(w http.ResponseWriter, code int) {
	switch code / 100 {
	case 4:
		h.statMap.Add(statClientError, 1)
	case 5:
		h.statMap.Add(statServerError, 1)
	}
	w.WriteHeader(code)
}

func (h *Handler) serveProcessContinuousQueries(w http.ResponseWriter, r *http.Request, user *meta.UserInfo) {
	h.statMap.Add(statCQRequest, 1)

	// If the continuous query service isn't configured, return 404.
	if h.ContinuousQuerier == nil {
		h.writeHeader(w, http.StatusNotImplemented)
		return
	}

	q := r.URL.Query()

	// Get the database name (blank means all databases).
	db := q.Get("db")
	// Get the name of the CQ to run (blank means run all).
	name := q.Get("name")
	// Get the time for which the CQ should be evaluated.
	t := time.Now()
	var err error
	s := q.Get("time")
	if s != "" {
		t, err = time.Parse(time.RFC3339Nano, s)
		if err != nil {
			// Try parsing as an int64 nanosecond timestamp.
			i, err := strconv.ParseInt(s, 10, 64)
			if err != nil {
				h.writeHeader(w, http.StatusBadRequest)
				return
			}
			t = time.Unix(0, i)
		}
	}

	// Pass the request to the CQ service.
	if err := h.ContinuousQuerier.Run(db, name, t); err != nil {
		h.writeHeader(w, http.StatusBadRequest)
		return
	}

	h.writeHeader(w, http.StatusNoContent)
}

// serveQuery parses an incoming query and, if valid, executes the query.
func (h *Handler) serveQuery(w http.ResponseWriter, r *http.Request, user *meta.UserInfo) {
	h.statMap.Add(statQueryRequest, 1)
	defer func(start time.Time) {
		h.statMap.Add(statQueryRequestDuration, time.Since(start).Nanoseconds())
	}(time.Now())

	pretty := r.FormValue("pretty") == "true"
	nodeID, _ := strconv.ParseUint(r.FormValue("node_id"), 10, 64)

	qp := strings.TrimSpace(r.FormValue("q"))
	if qp == "" {
		h.httpError(w, `missing required parameter "q"`, pretty, http.StatusBadRequest)
		return
	}

	epoch := strings.TrimSpace(r.FormValue("epoch"))

	p := influxql.NewParser(strings.NewReader(qp))
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
			h.httpError(w, "error parsing query parameters: "+err.Error(), pretty, http.StatusBadRequest)
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
					h.httpError(w, "error parsing json value: "+err.Error(), pretty, http.StatusBadRequest)
					return
				}
			}
		}
		p.SetParams(params)
	}

	// Parse query from query string.
	query, err := p.ParseQuery()
	if err != nil {
		h.httpError(w, "error parsing query: "+err.Error(), pretty, http.StatusBadRequest)
		return
	}

	// Check authorization.
	if h.Config.AuthEnabled {
		if err := h.QueryAuthorizer.AuthorizeQuery(user, query, db); err != nil {
			if err, ok := err.(meta.ErrAuthorize); ok {
				h.Logger.Printf("Unauthorized request | user: %q | query: %q | database %q\n", err.User, err.Query.String(), err.Database)
			}
			h.httpError(w, "error authorizing query: "+err.Error(), pretty, http.StatusUnauthorized)
			return
		}
	}

	// Parse chunk size. Use default if not provided or unparsable.
	chunked := (r.FormValue("chunked") == "true")
	chunkSize := DefaultChunkSize
	if chunked {
		if n, err := strconv.ParseInt(r.FormValue("chunk_size"), 10, 64); err == nil && int(n) > 0 {
			chunkSize = int(n)
		}
	}

	// Make sure if the client disconnects we signal the query to abort
	closing := make(chan struct{})
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
	} else {
		defer close(closing)
	}

	// Execute query.
	w.Header().Add("Connection", "close")
	w.Header().Add("Content-Type", "application/json")
	results := h.QueryExecutor.ExecuteQuery(query, influxql.ExecutionOptions{
		Database:  db,
		ChunkSize: chunkSize,
		ReadOnly:  r.Method == "GET",
		NodeID:    nodeID,
	}, closing)

	// if we're not chunking, this will be the in memory buffer for all results before sending to client
	resp := Response{Results: make([]*influxql.Result, 0)}

	// Status header is OK once this point is reached.
	h.writeHeader(w, http.StatusOK)

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
			n, _ := w.Write(MarshalJSON(Response{
				Results: []*influxql.Result{r},
			}, pretty))
			if !pretty {
				w.Write([]byte("\n"))
			}
			h.statMap.Add(statQueryRequestBytesTransmitted, int64(n))
			w.(http.Flusher).Flush()
			continue
		}

		// Limit the number of rows that can be returned in a non-chunked response.
		// This is to prevent the server from going OOM when returning a large response.
		// If you want to return more than the default chunk size, then use chunking
		// to process multiple blobs.
		rows += len(r.Series)
		if h.Config.MaxRowLimit > 0 && rows > h.Config.MaxRowLimit {
			break
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
					rowsMerged++
				}
			}

			// Append remaining rows as new rows.
			r.Series = r.Series[rowsMerged:]
			cr.Series = append(cr.Series, r.Series...)
			cr.Messages = append(cr.Messages, r.Messages...)
		} else {
			resp.Results = append(resp.Results, r)
		}
	}

	// If it's not chunked we buffered everything in memory, so write it out
	if !chunked {
		n, _ := w.Write(MarshalJSON(resp, pretty))
		h.statMap.Add(statQueryRequestBytesTransmitted, int64(n))
	}
}

// serveWrite receives incoming series data in line protocol format and writes it to the database.
func (h *Handler) serveWrite(w http.ResponseWriter, r *http.Request, user *meta.UserInfo) {
	h.statMap.Add(statWriteRequest, 1)
	h.statMap.Add(statWriteRequestsActive, 1)
	defer func(start time.Time) {
		h.statMap.Add(statWriteRequestsActive, -1)
		h.statMap.Add(statWriteRequestDuration, time.Since(start).Nanoseconds())
	}(time.Now())

	database := r.URL.Query().Get("db")
	if database == "" {
		h.resultError(w, influxql.Result{Err: fmt.Errorf("database is required")}, http.StatusBadRequest)
		return
	}

	if di := h.MetaClient.Database(database); di == nil {
		h.resultError(w, influxql.Result{Err: fmt.Errorf("database not found: %q", database)}, http.StatusNotFound)
		return
	}

	if h.Config.AuthEnabled && user == nil {
		h.resultError(w, influxql.Result{Err: fmt.Errorf("user is required to write to database %q", database)}, http.StatusUnauthorized)
		return
	}

	if h.Config.AuthEnabled {
		if err := h.WriteAuthorizer.AuthorizeWrite(user.Name, database); err != nil {
			h.resultError(w, influxql.Result{Err: fmt.Errorf("%q user is not authorized to write to database %q", user.Name, database)}, http.StatusUnauthorized)
			return
		}
	}

	// Handle gzip decoding of the body
	body := r.Body
	if r.Header.Get("Content-Encoding") == "gzip" {
		b, err := gzip.NewReader(r.Body)
		if err != nil {
			h.resultError(w, influxql.Result{Err: err}, http.StatusBadRequest)
			return
		}
		defer b.Close()
		body = b
	}

	var bs []byte
	if clStr := r.Header.Get("Content-Length"); clStr != "" {
		if length, err := strconv.Atoi(clStr); err == nil {
			// This will just be an initial hint for the gzip reader, as the
			// bytes.Buffer will grow as needed when ReadFrom is called
			bs = make([]byte, 0, length)
		}
	}
	buf := bytes.NewBuffer(bs)

	_, err := buf.ReadFrom(body)
	if err != nil {
		if h.Config.WriteTracing {
			h.Logger.Print("Write handler unable to read bytes from request body")
		}
		h.resultError(w, influxql.Result{Err: err}, http.StatusBadRequest)
		return
	}
	h.statMap.Add(statWriteRequestBytesReceived, int64(buf.Len()))

	if h.Config.WriteTracing {
		h.Logger.Printf("Write body received by handler: %s", buf.Bytes())
	}

	points, parseError := models.ParsePointsWithPrecision(buf.Bytes(), time.Now().UTC(), r.URL.Query().Get("precision"))
	// Not points parsed correctly so return the error now
	if parseError != nil && len(points) == 0 {
		if parseError.Error() == "EOF" {
			h.writeHeader(w, http.StatusOK)
			return
		}
		h.resultError(w, influxql.Result{Err: parseError}, http.StatusBadRequest)
		return
	}

	// Determine required consistency level.
	level := r.URL.Query().Get("consistency")
	consistency := models.ConsistencyLevelOne
	if level != "" {
		var err error
		consistency, err = models.ParseConsistencyLevel(level)
		if err != nil {
			h.resultError(w, influxql.Result{Err: err}, http.StatusBadRequest)
			return
		}
	}

	// Write points.
	if err := h.PointsWriter.WritePoints(database, r.URL.Query().Get("rp"), consistency, points); influxdb.IsClientError(err) {
		h.statMap.Add(statPointsWrittenFail, int64(len(points)))
		h.resultError(w, influxql.Result{Err: err}, http.StatusBadRequest)
		return
	} else if err != nil {
		h.statMap.Add(statPointsWrittenFail, int64(len(points)))
		h.resultError(w, influxql.Result{Err: err}, http.StatusInternalServerError)
		return
	} else if parseError != nil {
		// We wrote some of the points
		h.statMap.Add(statPointsWrittenOK, int64(len(points)))
		// The other points failed to parse which means the client sent invalid line protocol.  We return a 400
		// response code as well as the lines that failed to parse.
		h.resultError(w, influxql.Result{Err: fmt.Errorf("partial write:\n%v", parseError)}, http.StatusBadRequest)
		return
	}

	h.statMap.Add(statPointsWrittenOK, int64(len(points)))
	h.writeHeader(w, http.StatusNoContent)
}

// serveOptions returns an empty response to comply with OPTIONS pre-flight requests
func (h *Handler) serveOptions(w http.ResponseWriter, r *http.Request) {
	h.writeHeader(w, http.StatusNoContent)
}

// servePing returns a simple response to let the client know the server is running.
func (h *Handler) servePing(w http.ResponseWriter, r *http.Request) {
	h.statMap.Add(statPingRequest, 1)
	h.writeHeader(w, http.StatusNoContent)
}

// serveStatus has been depricated
func (h *Handler) serveStatus(w http.ResponseWriter, r *http.Request) {
	h.Logger.Printf("WARNING: /status has been depricated.  Use /ping instead.")
	h.statMap.Add(statStatusRequest, 1)
	h.writeHeader(w, http.StatusNoContent)
}

// convertToEpoch converts result timestamps from time.Time to the specified epoch.
func convertToEpoch(r *influxql.Result, epoch string) {
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

// MarshalJSON will marshal v to JSON. Pretty prints if pretty is true.
func MarshalJSON(v interface{}, pretty bool) []byte {
	var b []byte
	var err error
	if pretty {
		b, err = json.MarshalIndent(v, "", "    ")
	} else {
		b, err = json.Marshal(v)
	}

	if err != nil {
		return []byte(err.Error())
	}
	return b
}

// serveExpvar serves registered expvar information over HTTP.
func serveExpvar(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	fmt.Fprintf(w, "{\n")
	first := true
	expvar.Do(func(kv expvar.KeyValue) {
		if !first {
			fmt.Fprintf(w, ",\n")
		}
		first = false
		fmt.Fprintf(w, "%q: %s", kv.Key, kv.Value)
	})
	fmt.Fprintf(w, "\n}\n")
}

// h.httpError writes an error to the client in a standard format.
func (h *Handler) httpError(w http.ResponseWriter, error string, pretty bool, code int) {
	w.Header().Add("Content-Type", "application/json")
	h.writeHeader(w, code)
	response := Response{Err: errors.New(error)}
	var b []byte
	if pretty {
		b, _ = json.MarshalIndent(response, "", "    ")
	} else {
		b, _ = json.Marshal(response)
	}
	w.Write(b)
}

func (h *Handler) resultError(w http.ResponseWriter, result influxql.Result, code int) {
	w.Header().Add("Content-Type", "application/json")
	h.writeHeader(w, code)
	_ = json.NewEncoder(w).Encode(&result)
}

// Filters and filter helpers

type credentials struct {
	Method   AuthenticationMethod
	Username string
	Password string
	Token    string
}

// parseCredentials parses a request and returns the authentication credentials.
// The credentials may be present as URL query params, or as a Basic
// Authentication header.
// As params: http://127.0.0.1/query?u=username&p=password
// As basic auth: http://username:password@127.0.0.1
// As Bearer token in Authorization header: Bearer <JWT_TOKEN_BLOB>
func parseCredentials(r *http.Request) (*credentials, error) {
	q := r.URL.Query()

	// Check for the HTTP Authorization header.
	if s := r.Header.Get("Authorization"); s != "" {
		// Check for Bearer token.
		strs := strings.Split(s, " ")
		if len(strs) == 2 && strs[0] == "Bearer" {
			return &credentials{
				Method: BearerAuthentication,
				Token:  strs[1],
			}, nil
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

	// Check for username and password in URL params.
	if u, p := q.Get("u"), q.Get("p"); u != "" && p != "" {
		return &credentials{
			Method:   UserAuthentication,
			Username: u,
			Password: p,
		}, nil
	}

	return nil, fmt.Errorf("unable to parse authentication credentials")
}

// authenticate wraps a handler and ensures that if user credentials are passed in
// an attempt is made to authenticate that user. If authentication fails, an error is returned.
//
// There is one exception: if there are no users in the system, authentication is not required. This
// is to facilitate bootstrapping of a system with authentication enabled.
func authenticate(inner func(http.ResponseWriter, *http.Request, *meta.UserInfo), h *Handler, requireAuthentication bool) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Return early if we are not authenticating
		if !requireAuthentication {
			inner(w, r, nil)
			return
		}
		var user *meta.UserInfo

		// Retrieve user list.
		uis := h.MetaClient.Users()

		// See if admin user exists.
		adminExists := false
		for i := range uis {
			if uis[i].Admin {
				adminExists = true
				break
			}
		}

		// TODO corylanou: never allow this in the future without users
		if requireAuthentication && adminExists {
			creds, err := parseCredentials(r)
			if err != nil {
				h.statMap.Add(statAuthFail, 1)
				h.httpError(w, err.Error(), false, http.StatusUnauthorized)
				return
			}

			switch creds.Method {
			case UserAuthentication:
				if creds.Username == "" {
					h.statMap.Add(statAuthFail, 1)
					h.httpError(w, "username required", false, http.StatusUnauthorized)
					return
				}

				user, err = h.MetaClient.Authenticate(creds.Username, creds.Password)
				if err != nil {
					h.statMap.Add(statAuthFail, 1)
					h.httpError(w, "authorization failed", false, http.StatusUnauthorized)
					return
				}
			case BearerAuthentication:
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
					h.httpError(w, err.Error(), false, http.StatusUnauthorized)
					return
				} else if !token.Valid {
					h.httpError(w, "invalid token", false, http.StatusUnauthorized)
					return
				}

				// Make sure an expiration was set on the token.
				if exp, ok := token.Claims["exp"].(float64); !ok || exp <= 0.0 {
					h.httpError(w, "token expiration required", false, http.StatusUnauthorized)
					return
				}

				// Get the username from the token.
				username, ok := token.Claims["username"].(string)
				if !ok {
					h.httpError(w, "username in token must be a string", false, http.StatusUnauthorized)
					return
				} else if username == "" {
					h.httpError(w, "token must contain a username", false, http.StatusUnauthorized)
					return
				}

				// Lookup user in the metastore.
				if user, err = h.MetaClient.User(username); err != nil {
					h.httpError(w, err.Error(), false, http.StatusUnauthorized)
					return
				} else if user == nil {
					h.httpError(w, meta.ErrUserNotFound.Error(), false, http.StatusUnauthorized)
					return
				}
			default:
				h.httpError(w, "unsupported authentication", false, http.StatusUnauthorized)
			}

		}
		inner(w, r, user)
	})
}

type gzipResponseWriter struct {
	io.Writer
	http.ResponseWriter
}

// WriteHeader sets the provided code as the response status. If the
// specified status is 204 No Content, then the Content-Encoding header
// is removed from the response, to prevent clients expecting gzipped
// encoded bodies from trying to deflate an empty response.
func (w gzipResponseWriter) WriteHeader(code int) {
	if code != http.StatusNoContent {
		w.Header().Set("Content-Encoding", "gzip")
	}
	w.ResponseWriter.WriteHeader(code)
}

func (w gzipResponseWriter) Write(b []byte) (int, error) {
	return w.Writer.Write(b)
}

func (w gzipResponseWriter) Flush() {
	w.Writer.(*gzip.Writer).Flush()
}

func (w gzipResponseWriter) CloseNotify() <-chan bool {
	return w.ResponseWriter.(http.CloseNotifier).CloseNotify()
}

// determines if the client can accept compressed responses, and encodes accordingly
func gzipFilter(inner http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.Contains(r.Header.Get("Accept-Encoding"), "gzip") {
			inner.ServeHTTP(w, r)
			return
		}
		gz := gzip.NewWriter(w)
		defer gz.Close()
		gzw := gzipResponseWriter{Writer: gz, ResponseWriter: w}
		inner.ServeHTTP(gzw, r)
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
			}, ", "))

			w.Header().Set(`Access-Control-Allow-Headers`, strings.Join([]string{
				`Accept`,
				`Accept-Encoding`,
				`Authorization`,
				`Content-Length`,
				`Content-Type`,
				`X-CSRF-Token`,
				`X-HTTP-Method-Override`,
			}, ", "))

			w.Header().Set(`Access-Control-Expose-Headers`, strings.Join([]string{
				`Date`,
				`X-InfluxDB-Version`,
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
		uid := uuid.TimeUUID()
		r.Header.Set("Request-Id", uid.String())
		w.Header().Set("Request-Id", r.Header.Get("Request-Id"))

		inner.ServeHTTP(w, r)
	})
}

func (h *Handler) logging(inner http.Handler, name string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		l := &responseLogger{w: w}
		inner.ServeHTTP(l, r)
		h.CLFLogger.Println(buildLogLine(l, r, start))
	})
}

func (h *Handler) recovery(inner http.Handler, name string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		l := &responseLogger{w: w}

		defer func() {
			if err := recover(); err != nil {
				logLine := buildLogLine(l, r, start)
				logLine = fmt.Sprintf("%s [panic:%s] %s", logLine, err, debug.Stack())
				h.Logger.Println(logLine)
			}
		}()

		inner.ServeHTTP(l, r)
	})
}

// Response represents a list of statement results.
type Response struct {
	Results []*influxql.Result
	Err     error
}

// MarshalJSON encodes a Response struct into JSON.
func (r Response) MarshalJSON() ([]byte, error) {
	// Define a struct that outputs "error" as a string.
	var o struct {
		Results []*influxql.Result `json:"results,omitempty"`
		Err     string             `json:"error,omitempty"`
	}

	// Copy fields to output struct.
	o.Results = r.Results
	if r.Err != nil {
		o.Err = r.Err.Error()
	}

	return json.Marshal(&o)
}

// UnmarshalJSON decodes the data into the Response struct
func (r *Response) UnmarshalJSON(b []byte) error {
	var o struct {
		Results []*influxql.Result `json:"results,omitempty"`
		Err     string             `json:"error,omitempty"`
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

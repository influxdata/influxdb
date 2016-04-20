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
	"strconv"
	"strings"
	"time"

	"github.com/bmizerany/pat"
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

// TODO: Standard response headers (see: HeaderHandler)
// TODO: Compression (see: CompressionHeaderHandler)

// TODO: Check HTTP response codes: 400, 401, 403, 409.

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
	mux                   *pat.PatternServeMux
	requireAuthentication bool
	Version               string

	MetaClient interface {
		Database(name string) (*meta.DatabaseInfo, error)
		Authenticate(username, password string) (ui *meta.UserInfo, err error)
		Users() []meta.UserInfo
	}

	QueryAuthorizer interface {
		AuthorizeQuery(u *meta.UserInfo, query *influxql.Query, database string) error
	}

	QueryExecutor influxql.QueryExecutor

	PointsWriter interface {
		WritePoints(database, retentionPolicy string, consistencyLevel models.ConsistencyLevel, points []models.Point) error
	}

	ContinuousQuerier continuous_querier.ContinuousQuerier

	Logger         *log.Logger
	loggingEnabled bool // Log every HTTP access.
	WriteTrace     bool // Detailed logging of write path
	rowLimit       int
	statMap        *expvar.Map
}

// NewHandler returns a new instance of handler with routes.
func NewHandler(requireAuthentication, loggingEnabled, writeTrace bool, rowLimit int, statMap *expvar.Map) *Handler {
	h := &Handler{
		mux: pat.New(),
		requireAuthentication: requireAuthentication,
		Logger:                log.New(os.Stderr, "[http] ", log.LstdFlags),
		loggingEnabled:        loggingEnabled,
		WriteTrace:            writeTrace,
		rowLimit:              rowLimit,
		statMap:               statMap,
	}

	h.AddRoutes([]Route{
		Route{
			"query-options", // Satisfy CORS checks.
			"OPTIONS", "/query", true, true, h.serveOptions,
		},
		Route{
			"query", // Query serving route.
			"GET", "/query", true, true, h.serveQuery,
		},
		Route{
			"write-options", // Satisfy CORS checks.
			"OPTIONS", "/write", true, true, h.serveOptions,
		},
		Route{
			"write", // Data-ingest route.
			"POST", "/write", true, true, h.serveWrite,
		},
		Route{ // Ping
			"ping",
			"GET", "/ping", true, true, h.servePing,
		},
		Route{ // Ping
			"ping-head",
			"HEAD", "/ping", true, true, h.servePing,
		},
		Route{ // Ping w/ status
			"status",
			"GET", "/status", true, true, h.serveStatus,
		},
		Route{ // Ping w/ status
			"status-head",
			"HEAD", "/status", true, true, h.serveStatus,
		},
		// TODO: (corylanou) remove this and associated code
		Route{ // Tell data node to run CQs that should be run
			"process-continuous-queries",
			"POST", "/data/process_continuous_queries", false, false, h.serveProcessContinuousQueries,
		},
	}...)

	return h
}

// SetRoutes sets the provided routes on the handler.
func (h *Handler) AddRoutes(routes ...Route) {
	for _, r := range routes {
		var handler http.Handler

		// If it's a handler func that requires authorization, wrap it in authorization
		if hf, ok := r.HandlerFunc.(func(http.ResponseWriter, *http.Request, *meta.UserInfo)); ok {
			handler = authenticate(hf, h, h.requireAuthentication)
		}
		// This is a normal handler signature and does not require authorization
		if hf, ok := r.HandlerFunc.(func(http.ResponseWriter, *http.Request)); ok {
			handler = http.HandlerFunc(hf)
		}

		if r.Gzipped {
			handler = gzipFilter(handler)
		}
		handler = versionHeader(handler, h)
		handler = cors(handler)
		handler = requestID(handler)
		if h.loggingEnabled && r.LoggingEnabled {
			handler = logging(handler, r.Name, h.Logger)
		}
		handler = recovery(handler, r.Name, h.Logger) // make sure recovery is always last

		h.mux.Add(r.Method, r.Pattern, handler)

	}
}

// ServeHTTP responds to HTTP request to the handler.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.statMap.Add(statRequest, 1)
	h.statMap.Add(statRequestsActive, 1)
	start := time.Now()

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

func (h *Handler) serveProcessContinuousQueries(w http.ResponseWriter, r *http.Request, user *meta.UserInfo) {
	h.statMap.Add(statCQRequest, 1)

	// If the continuous query service isn't configured, return 404.
	if h.ContinuousQuerier == nil {
		w.WriteHeader(http.StatusNotImplemented)
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
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			t = time.Unix(0, i)
		}
	}

	// Pass the request to the CQ service.
	if err := h.ContinuousQuerier.Run(db, name, t); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// serveQuery parses an incoming query and, if valid, executes the query.
func (h *Handler) serveQuery(w http.ResponseWriter, r *http.Request, user *meta.UserInfo) {
	h.statMap.Add(statQueryRequest, 1)
	defer func(start time.Time) {
		h.statMap.Add(statQueryRequestDuration, time.Since(start).Nanoseconds())
	}(time.Now())

	q := r.URL.Query()
	pretty := q.Get("pretty") == "true"

	qp := strings.TrimSpace(q.Get("q"))
	if qp == "" {
		httpError(w, `missing required parameter "q"`, pretty, http.StatusBadRequest)
		return
	}

	epoch := strings.TrimSpace(q.Get("epoch"))

	p := influxql.NewParser(strings.NewReader(qp))
	db := q.Get("db")

	// Parse query from query string.
	query, err := p.ParseQuery()
	if err != nil {
		httpError(w, "error parsing query: "+err.Error(), pretty, http.StatusBadRequest)
		return
	}

	// Sanitize statements with passwords.
	for _, s := range query.Statements {
		switch stmt := s.(type) {
		case *influxql.CreateUserStatement:
			sanitize(r, stmt.Password)
		case *influxql.SetPasswordUserStatement:
			sanitize(r, stmt.Password)
		}
	}

	// Check authorization.
	if h.requireAuthentication {
		if err := h.QueryAuthorizer.AuthorizeQuery(user, query, db); err != nil {
			if err, ok := err.(meta.ErrAuthorize); ok {
				h.Logger.Printf("unauthorized request | user: %q | query: %q | database %q\n", err.User, err.Query.String(), err.Database)
			}
			httpError(w, "error authorizing query: "+err.Error(), pretty, http.StatusUnauthorized)
			return
		}
	}

	// Parse chunk size. Use default if not provided or unparsable.
	chunked := (q.Get("chunked") == "true")
	chunkSize := DefaultChunkSize
	if chunked {
		if n, err := strconv.ParseInt(q.Get("chunk_size"), 10, 64); err == nil && int(n) > 0 {
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
	w.Header().Add("content-type", "application/json")
	results := h.QueryExecutor.ExecuteQuery(query, db, chunkSize, closing)

	// if we're not chunking, this will be the in memory buffer for all results before sending to client
	resp := Response{Results: make([]*influxql.Result, 0)}

	// Status header is OK once this point is reached.
	w.WriteHeader(http.StatusOK)

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
		if h.rowLimit > 0 && rows > h.rowLimit {
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
	defer func(start time.Time) {
		h.statMap.Add(statWriteRequestDuration, time.Since(start).Nanoseconds())
	}(time.Now())

	database := r.URL.Query().Get("db")
	if database == "" {
		resultError(w, influxql.Result{Err: fmt.Errorf("database is required")}, http.StatusBadRequest)
		return
	}

	if di, err := h.MetaClient.Database(database); err != nil {
		resultError(w, influxql.Result{Err: fmt.Errorf("metastore database error: %s", err)}, http.StatusInternalServerError)
		return
	} else if di == nil {
		resultError(w, influxql.Result{Err: fmt.Errorf("database not found: %q", database)}, http.StatusNotFound)
		return
	}

	if h.requireAuthentication && user == nil {
		resultError(w, influxql.Result{Err: fmt.Errorf("user is required to write to database %q", database)}, http.StatusUnauthorized)
		return
	}

	if h.requireAuthentication && !user.Authorize(influxql.WritePrivilege, database) {
		resultError(w, influxql.Result{Err: fmt.Errorf("%q user is not authorized to write to database %q", user.Name, database)}, http.StatusUnauthorized)
		return
	}

	// Handle gzip decoding of the body
	body := r.Body
	if r.Header.Get("Content-encoding") == "gzip" {
		b, err := gzip.NewReader(r.Body)
		if err != nil {
			resultError(w, influxql.Result{Err: err}, http.StatusBadRequest)
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
		if h.WriteTrace {
			h.Logger.Print("write handler unable to read bytes from request body")
		}
		resultError(w, influxql.Result{Err: err}, http.StatusBadRequest)
		return
	}
	h.statMap.Add(statWriteRequestBytesReceived, int64(buf.Len()))

	if h.WriteTrace {
		h.Logger.Printf("write body received by handler: %s", buf.Bytes())
	}

	points, parseError := models.ParsePointsWithPrecision(buf.Bytes(), time.Now().UTC(), r.URL.Query().Get("precision"))
	// Not points parsed correctly so return the error now
	if parseError != nil && len(points) == 0 {
		if parseError.Error() == "EOF" {
			w.WriteHeader(http.StatusOK)
			return
		}
		resultError(w, influxql.Result{Err: parseError}, http.StatusBadRequest)
		return
	}

	// Determine required consistency level.
	level := r.URL.Query().Get("consistency")
	consistency := models.ConsistencyLevelOne
	if level != "" {
		var err error
		consistency, err = models.ParseConsistencyLevel(level)
		if err != nil {
			resultError(w, influxql.Result{Err: err}, http.StatusBadRequest)
			return
		}
	}

	// Write points.
	if err := h.PointsWriter.WritePoints(database, r.URL.Query().Get("rp"), consistency, points); influxdb.IsClientError(err) {
		h.statMap.Add(statPointsWrittenFail, int64(len(points)))
		resultError(w, influxql.Result{Err: err}, http.StatusBadRequest)
		return
	} else if err != nil {
		h.statMap.Add(statPointsWrittenFail, int64(len(points)))
		resultError(w, influxql.Result{Err: err}, http.StatusInternalServerError)
		return
	} else if parseError != nil {
		// We wrote some of the points
		h.statMap.Add(statPointsWrittenOK, int64(len(points)))
		// The other points failed to parse which means the client sent invalid line protocol.  We return a 400
		// response code as well as the lines that failed to parse.
		resultError(w, influxql.Result{Err: fmt.Errorf("partial write:\n%v", parseError)}, http.StatusBadRequest)
		return
	}

	h.statMap.Add(statPointsWrittenOK, int64(len(points)))
	w.WriteHeader(http.StatusNoContent)
}

// serveOptions returns an empty response to comply with OPTIONS pre-flight requests
func (h *Handler) serveOptions(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusNoContent)
}

// servePing returns a simple response to let the client know the server is running.
func (h *Handler) servePing(w http.ResponseWriter, r *http.Request) {
	h.statMap.Add(statPingRequest, 1)
	w.WriteHeader(http.StatusNoContent)
}

// serveStatus has been depricated
func (h *Handler) serveStatus(w http.ResponseWriter, r *http.Request) {
	h.Logger.Printf("WARNING: /status has been depricated.  Use /ping instead.")
	h.statMap.Add(statStatusRequest, 1)
	w.WriteHeader(http.StatusNoContent)
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

// httpError writes an error to the client in a standard format.
func httpError(w http.ResponseWriter, error string, pretty bool, code int) {
	w.Header().Add("content-type", "application/json")
	w.WriteHeader(code)

	response := Response{Err: errors.New(error)}
	var b []byte
	if pretty {
		b, _ = json.MarshalIndent(response, "", "    ")
	} else {
		b, _ = json.Marshal(response)
	}
	w.Write(b)
}

func resultError(w http.ResponseWriter, result influxql.Result, code int) {
	w.Header().Add("content-type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(&result)
}

// Filters and filter helpers

// parseCredentials returns the username and password encoded in
// a request. The credentials may be present as URL query params, or as
// a Basic Authentication header.
// as params: http://127.0.0.1/query?u=username&p=password
// as basic auth: http://username:password@127.0.0.1
func parseCredentials(r *http.Request) (string, string, error) {
	q := r.URL.Query()

	if u, p := q.Get("u"), q.Get("p"); u != "" && p != "" {
		return u, p, nil
	}
	if u, p, ok := r.BasicAuth(); ok {
		return u, p, nil
	}
	return "", "", fmt.Errorf("unable to parse Basic Auth credentials")
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

		// TODO corylanou: never allow this in the future without users
		if requireAuthentication && len(uis) > 0 {
			username, password, err := parseCredentials(r)
			if err != nil {
				h.statMap.Add(statAuthFail, 1)
				httpError(w, err.Error(), false, http.StatusUnauthorized)
				return
			}
			if username == "" {
				h.statMap.Add(statAuthFail, 1)
				httpError(w, "username required", false, http.StatusUnauthorized)
				return
			}

			user, err = h.MetaClient.Authenticate(username, password)
			if err != nil {
				h.statMap.Add(statAuthFail, 1)
				httpError(w, err.Error(), false, http.StatusUnauthorized)
				return
			}
		}
		inner(w, r, user)
	})
}

type gzipResponseWriter struct {
	io.Writer
	http.ResponseWriter
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
		w.Header().Set("Content-Encoding", "gzip")
		gz := gzip.NewWriter(w)
		defer gz.Close()
		gzw := gzipResponseWriter{Writer: gz, ResponseWriter: w}
		inner.ServeHTTP(gzw, r)
	})
}

// versionHeader takes a HTTP handler and returns a HTTP handler
// and adds the X-INFLUXBD-VERSION header to outgoing responses.
func versionHeader(inner http.Handler, h *Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Add("X-InfluxDB-Version", h.Version)
		inner.ServeHTTP(w, r)
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

func logging(inner http.Handler, name string, weblog *log.Logger) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		l := &responseLogger{w: w}
		inner.ServeHTTP(l, r)
		logLine := buildLogLine(l, r, start)
		weblog.Println(logLine)
	})
}

func recovery(inner http.Handler, name string, weblog *log.Logger) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		l := &responseLogger{w: w}

		defer func() {
			if err := recover(); err != nil {
				logLine := buildLogLine(l, r, start)
				logLine = fmt.Sprintf(`%s [panic:%s]`, logLine, err)
				weblog.Println(logLine)
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

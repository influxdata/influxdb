package httpd

import (
	"compress/gzip"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/bmizerany/pat"
	"github.com/influxdb/influxdb"
	"github.com/influxdb/influxdb/client"
	"github.com/influxdb/influxdb/influxql"
	"github.com/influxdb/influxdb/uuid"
)

// TODO: Standard response headers (see: HeaderHandler)
// TODO: Compression (see: CompressionHeaderHandler)

// TODO: Check HTTP response codes: 400, 401, 403, 409.

type route struct {
	name        string
	method      string
	pattern     string
	gzipped     bool
	log         bool
	handlerFunc interface{}
}

// Handler represents an HTTP handler for the InfluxDB server.
type Handler struct {
	server                *influxdb.Server
	routes                []route
	mux                   *pat.PatternServeMux
	requireAuthentication bool

	Logger     *log.Logger
	WriteTrace bool // Detailed logging of write path
}

// NewHandler returns a new instance of Handler.
func NewHandler(s *influxdb.Server, requireAuthentication bool, version string) *Handler {
	h := &Handler{
		server: s,
		mux:    pat.New(),
		requireAuthentication: requireAuthentication,
		Logger:                log.New(os.Stderr, "[http] ", log.LstdFlags),
	}

	h.routes = append(h.routes,
		route{
			"query", // Query serving route.
			"GET", "/query", true, true, h.serveQuery,
		},
		route{
			"write", // Data-ingest route.
			"OPTIONS", "/write", true, true, h.serveOptions,
		},
		route{
			"write", // Data-ingest route.
			"POST", "/write", true, true, h.serveWrite,
		},
		route{ // List data nodes
			"data_nodes_index",
			"GET", "/data_nodes", true, false, h.serveDataNodes,
		},
		route{ // Create data node
			"data_nodes_create",
			"POST", "/data_nodes", true, false, h.serveCreateDataNode,
		},
		route{ // Delete data node
			"data_nodes_delete",
			"DELETE", "/data_nodes/:id", true, false, h.serveDeleteDataNode,
		},
		route{ // Metastore
			"metastore",
			"GET", "/metastore", false, false, h.serveMetastore,
		},
		route{ // Status
			"status",
			"GET", "/status", true, true, h.serveStatus,
		},
		route{ // Ping
			"ping",
			"GET", "/ping", true, true, h.servePing,
		},
		route{ // Ping
			"ping-head",
			"HEAD", "/ping", true, true, h.servePing,
		},
		route{ // Tell data node to run CQs that should be run
			"process_continuous_queries",
			"POST", "/process_continuous_queries", false, false, h.serveProcessContinuousQueries,
		},
		route{
			"wait", // Wait.
			"GET", "/wait/:index", true, true, h.serveWait,
		},
		route{
			"index", // Index.
			"GET", "/", true, true, h.serveIndex,
		},
		route{
			"dump", // export all points in the given db.
			"GET", "/dump", true, true, h.serveDump,
		},
	)

	for _, r := range h.routes {
		var handler http.Handler

		// If it's a handler func that requires authorization, wrap it in authorization
		if hf, ok := r.handlerFunc.(func(http.ResponseWriter, *http.Request, *influxdb.User)); ok {
			handler = authenticate(hf, h, requireAuthentication)
		}
		// This is a normal handler signature and does not require authorization
		if hf, ok := r.handlerFunc.(func(http.ResponseWriter, *http.Request)); ok {
			handler = http.HandlerFunc(hf)
		}

		if r.gzipped {
			handler = gzipFilter(handler)
		}
		handler = versionHeader(handler, version)
		handler = cors(handler)
		handler = requestID(handler)
		if r.log {
			handler = logging(handler, r.name, h.Logger)
		}
		handler = recovery(handler, r.name, h.Logger) // make sure recovery is always last

		h.mux.Add(r.method, r.pattern, handler)
	}

	return h
}

// ServeHTTP responds to HTTP request to the handler.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.mux.ServeHTTP(w, r)
}

// serveQuery parses an incoming query and, if valid, executes the query.
func (h *Handler) serveQuery(w http.ResponseWriter, r *http.Request, user *influxdb.User) {
	q := r.URL.Query()

	pretty := q.Get("pretty") == "true"

	qp := strings.TrimSpace(q.Get("q"))
	if qp == "" {
		httpError(w, `missing required parameter "q"`, pretty, http.StatusBadRequest)
		return
	}

	p := influxql.NewParser(strings.NewReader(qp))
	db := q.Get("db")

	// Parse query from query string.
	query, err := p.ParseQuery()
	if err != nil {
		httpError(w, "error parsing query: "+err.Error(), pretty, http.StatusBadRequest)
		return
	}

	// Execute query. One result will return for each statement.
	results := h.server.ExecuteQuery(query, db, user)

	// Send results to client.
	httpResults(w, results, pretty)
}

func interfaceToString(v interface{}) string {
	switch t := v.(type) {
	case nil:
		return ""
	case bool:
		return fmt.Sprintf("%v", v)
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, uintptr:
		return fmt.Sprintf("%d", t)
	case float32, float64:
		return fmt.Sprintf("%v", t)
	default:
		return fmt.Sprintf("%v", t)
	}
}

type Point struct {
	Name      string                 `json:"name"`
	Timestamp time.Time              `json:"timestamp"`
	Tags      map[string]string      `json:"tags"`
	Fields    map[string]interface{} `json:"fields"`
}

type Batch struct {
	Database        string  `json:"database"`
	RetentionPolicy string  `json:"retentionPolicy"`
	Points          []Point `json:"points"`
}

// Return all the measurements from the given DB
func (h *Handler) showMeasurements(db string, user *influxdb.User) ([]string, error) {
	var measurements []string
	results := h.server.ExecuteQuery(&influxql.Query{Statements: []influxql.Statement{&influxql.ShowMeasurementsStatement{}}}, db, user)
	if results.Err != nil {
		return measurements, results.Err
	}

	for _, result := range results.Results {
		for _, row := range result.Series {
			for _, tuple := range (*row).Values {
				for _, cell := range tuple {
					measurements = append(measurements, interfaceToString(cell))
				}
			}
		}
	}
	return measurements, nil
}

// serveDump returns all points in the given database as a plaintext list of JSON structs.
// To get all points:
// Find all measurements (show measurements).
// For each measurement do select * from <measurement> group by *
func (h *Handler) serveDump(w http.ResponseWriter, r *http.Request, user *influxdb.User) {
	q := r.URL.Query()
	db := q.Get("db")
	pretty := q.Get("pretty") == "true"
	delim := []byte("\n")
	measurements, err := h.showMeasurements(db, user)
	if err != nil {
		httpError(w, "error with dump: "+err.Error(), pretty, http.StatusInternalServerError)
		return
	}

	// Fetch all the points for each measurement.
	// From the 'select' query below, we get:
	//
	// columns:[col1, col2, col3, ...]
	// - and -
	// values:[[val1, val2, val3, ...], [val1, val2, val3, ...], [val1, val2, val3, ...]...]
	//
	// We need to turn that into multiple rows like so...
	// fields:{col1 : values[0][0], col2 : values[0][1], col3 : values[0][2]}
	// fields:{col1 : values[1][0], col2 : values[1][1], col3 : values[1][2]}
	// fields:{col1 : values[2][0], col2 : values[2][1], col3 : values[2][2]}
	//
	for _, measurement := range measurements {
		queryString := fmt.Sprintf("select * from %s group by *", measurement)
		p := influxql.NewParser(strings.NewReader(queryString))
		query, err := p.ParseQuery()
		if err != nil {
			httpError(w, "error with dump: "+err.Error(), pretty, http.StatusInternalServerError)
			return
		}
		//
		results := h.server.ExecuteQuery(query, db, user)
		for _, result := range results.Results {
			for _, row := range result.Series {
				points := make([]Point, 1)
				var point Point
				point.Name = row.Name
				point.Tags = row.Tags
				point.Fields = make(map[string]interface{})
				for _, tuple := range row.Values {
					for subscript, cell := range tuple {
						if row.Columns[subscript] == "time" {
							point.Timestamp, _ = cell.(time.Time)
							continue
						}
						point.Fields[row.Columns[subscript]] = cell
					}
					points[0] = point
					batch := &Batch{
						Points:          points,
						Database:        db,
						RetentionPolicy: "default",
					}
					buf, err := json.Marshal(&batch)

					// TODO: Make this more legit in the future
					// Since we're streaming data as chunked responses, this error could
					// be in the middle of an already-started data stream. Until Go 1.5,
					// we can't really support proper trailer headers, so we'll just
					// wait until then: https://code.google.com/p/go/issues/detail?id=7759
					if err != nil {
						w.Write([]byte("*** SERVER-SIDE ERROR. MISSING DATA ***"))
						w.Write(delim)
						return
					}
					w.Write(buf)
					w.Write(delim)
				}
			}
		}
	}
}

// serveWrite receives incoming series data and writes it to the database.
func (h *Handler) serveWrite(w http.ResponseWriter, r *http.Request, user *influxdb.User) {
	var bp client.BatchPoints
	var dec *json.Decoder

	if h.WriteTrace {
		b, err := ioutil.ReadAll(r.Body)
		if err != nil {
			h.Logger.Print("write handler failed to read bytes from request body")
		} else {
			h.Logger.Printf("write body received by handler: %s", string(b))
		}
		dec = json.NewDecoder(strings.NewReader(string(b)))
	} else {
		dec = json.NewDecoder(r.Body)
		defer r.Body.Close()
	}

	var writeError = func(result influxdb.Result, statusCode int) {
		w.Header().Add("content-type", "application/json")
		w.WriteHeader(statusCode)
		_ = json.NewEncoder(w).Encode(&result)
		return
	}

	if err := dec.Decode(&bp); err != nil {
		if err.Error() == "EOF" {
			w.WriteHeader(http.StatusOK)
			return
		}
		writeError(influxdb.Result{Err: err}, http.StatusInternalServerError)
		return
	}

	if bp.Database == "" {
		writeError(influxdb.Result{Err: fmt.Errorf("database is required")}, http.StatusInternalServerError)
		return
	}

	if !h.server.DatabaseExists(bp.Database) {
		writeError(influxdb.Result{Err: fmt.Errorf("database not found: %q", bp.Database)}, http.StatusNotFound)
		return
	}

	if h.requireAuthentication && user == nil {
		writeError(influxdb.Result{Err: fmt.Errorf("user is required to write to database %q", bp.Database)}, http.StatusUnauthorized)
		return
	}

	if h.requireAuthentication && !user.Authorize(influxql.WritePrivilege, bp.Database) {
		writeError(influxdb.Result{Err: fmt.Errorf("%q user is not authorized to write to database %q", user.Name, bp.Database)}, http.StatusUnauthorized)
		return
	}

	points, err := influxdb.NormalizeBatchPoints(bp)
	if err != nil {
		writeError(influxdb.Result{Err: err}, http.StatusInternalServerError)
		return
	}

	if index, err := h.server.WriteSeries(bp.Database, bp.RetentionPolicy, points); err != nil {
		writeError(influxdb.Result{Err: err}, http.StatusInternalServerError)
		return
	} else {
		w.Header().Add("X-InfluxDB-Index", fmt.Sprintf("%d", index))
	}
}

// serveMetastore returns a copy of the metastore.
func (h *Handler) serveMetastore(w http.ResponseWriter, r *http.Request) {
	// Set headers.
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Disposition", `attachment; filename="meta"`)

	if err := h.server.CopyMetastore(w); err != nil {
		httpError(w, err.Error(), false, http.StatusInternalServerError)
	}
}

// serveStatus returns a set of states that the server is currently in.
func (h *Handler) serveStatus(w http.ResponseWriter, r *http.Request) {
	w.Header().Add("content-type", "application/json")

	pretty := r.URL.Query().Get("pretty") == "true"

	data := struct {
		Id    uint64 `json:"id"`
		Index uint64 `json:"index"`
	}{
		Id:    h.server.ID(),
		Index: h.server.Index(),
	}
	var b []byte
	if pretty {
		b, _ = json.MarshalIndent(data, "", "    ")
	} else {
		b, _ = json.Marshal(data)
	}
	w.Write(b)
}

// serveOptions returns an empty response to comply with OPTIONS pre-flight requests
func (h *Handler) serveOptions(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusNoContent)
}

// servePing returns a simple response to let the client know the server is running.
func (h *Handler) servePing(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusNoContent)
}

// serveIndex returns the current index of the node as the body of the response
func (h *Handler) serveIndex(w http.ResponseWriter, r *http.Request) {
	w.Write([]byte(fmt.Sprintf("%d", h.server.Index())))
}

// serveWait returns the current index of the node as the body of the response
// Takes optional parameters:
//     index - If specified, will poll for index before returning
//     timeout (optional) - time in milliseconds to wait until index is met before erring out
//               default timeout if not specified really big (max int64)
func (h *Handler) serveWait(w http.ResponseWriter, r *http.Request) {
	index, _ := strconv.ParseUint(r.URL.Query().Get(":index"), 10, 64)
	timeout, _ := strconv.Atoi(r.URL.Query().Get("timeout"))

	if index == 0 {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	var (
		timedOut int32
		aborted  int32
		timer    = time.NewTimer(time.Duration(math.MaxInt64))
	)
	defer timer.Stop()
	if timeout > 0 {
		timer.Reset(time.Duration(timeout) * time.Millisecond)
		go func() {
			<-timer.C
			atomic.StoreInt32(&timedOut, 1)
		}()
	}

	if notify, ok := w.(http.CloseNotifier); ok {
		go func() {
			<-notify.CloseNotify()
			atomic.StoreInt32(&aborted, 1)
		}()
	}

	for {
		if idx := h.server.Index(); idx >= index {
			w.Write([]byte(fmt.Sprintf("%d", idx)))
			break
		} else if atomic.LoadInt32(&aborted) == 1 {
			break
		} else if atomic.LoadInt32(&timedOut) == 1 {
			w.WriteHeader(http.StatusRequestTimeout)
			break
		} else {
			time.Sleep(10 * time.Millisecond)
		}
	}
}

// serveDataNodes returns a list of all data nodes in the cluster.
func (h *Handler) serveDataNodes(w http.ResponseWriter, r *http.Request) {
	// Generate a list of objects for encoding to the API.
	a := make([]*dataNodeJSON, 0)
	for _, n := range h.server.DataNodes() {
		a = append(a, &dataNodeJSON{
			ID:  n.ID,
			URL: n.URL.String(),
		})
	}

	w.Header().Add("content-type", "application/json")
	_ = json.NewEncoder(w).Encode(a)
}

// serveCreateDataNode creates a new data node in the cluster.
func (h *Handler) serveCreateDataNode(w http.ResponseWriter, r *http.Request) {
	// Read in data node from request body.
	var n dataNodeJSON
	if err := json.NewDecoder(r.Body).Decode(&n); err != nil {
		httpError(w, err.Error(), false, http.StatusBadRequest)
		return
	}

	// Parse the URL.
	u, err := url.Parse(n.URL)
	if err != nil {
		httpError(w, "invalid data node url", false, http.StatusBadRequest)
		return
	}

	// Create the data node.
	if err := h.server.CreateDataNode(u); err == influxdb.ErrDataNodeExists {
		httpError(w, err.Error(), false, http.StatusConflict)
		return
	} else if err != nil {
		httpError(w, err.Error(), false, http.StatusInternalServerError)
		return
	}

	// Retrieve data node reference.
	node := h.server.DataNodeByURL(u)

	// Write new node back to client.
	w.Header().Add("content-type", "application/json")
	w.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(w).Encode(&dataNodeJSON{ID: node.ID, URL: node.URL.String()})
}

// serveDeleteDataNode removes an existing node.
func (h *Handler) serveDeleteDataNode(w http.ResponseWriter, r *http.Request) {
	// Parse node id.
	nodeID, err := strconv.ParseUint(r.URL.Query().Get(":id"), 10, 64)
	if err != nil {
		httpError(w, "invalid node id", false, http.StatusBadRequest)
		return
	}

	// Delete the node.
	if err := h.server.DeleteDataNode(nodeID); err == influxdb.ErrDataNodeNotFound {
		httpError(w, err.Error(), false, http.StatusNotFound)
		return
	} else if err != nil {
		httpError(w, err.Error(), false, http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// serveProcessContinuousQueries will execute any continuous queries that should be run
func (h *Handler) serveProcessContinuousQueries(w http.ResponseWriter, r *http.Request) {
	if err := h.server.RunContinuousQueries(); err != nil {
		httpError(w, err.Error(), false, http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusAccepted)
}

type dataNodeJSON struct {
	ID  uint64 `json:"id"`
	URL string `json:"url"`
}

func isAuthorizationError(err error) bool {
	_, ok := err.(influxdb.ErrAuthorize)
	return ok
}

func isMeasurementNotFoundError(err error) bool {
	s := err.Error()
	return strings.HasPrefix(s, "measurement") && strings.HasSuffix(s, "not found") || strings.Contains(s, "measurement not found")
}

func isFieldNotFoundError(err error) bool {
	return (strings.HasPrefix(err.Error(), "field not found"))
}

// httpResult writes a Results array to the client.
func httpResults(w http.ResponseWriter, results influxdb.Results, pretty bool) {
	w.Header().Add("content-type", "application/json")

	if results.Error() != nil {
		if isAuthorizationError(results.Error()) {
			w.WriteHeader(http.StatusUnauthorized)
		} else if isMeasurementNotFoundError(results.Error()) {
			w.WriteHeader(http.StatusOK)
		} else if isFieldNotFoundError(results.Error()) {
			w.WriteHeader(http.StatusOK)
		} else {
			w.WriteHeader(http.StatusInternalServerError)
		}
	}

	var b []byte
	if pretty {
		b, _ = json.MarshalIndent(results, "", "    ")
	} else {
		b, _ = json.Marshal(results)
	}
	w.Write(b)
}

// httpError writes an error to the client in a standard format.
func httpError(w http.ResponseWriter, error string, pretty bool, code int) {
	w.Header().Add("content-type", "application/json")
	w.WriteHeader(code)

	results := influxdb.Results{Err: errors.New(error)}
	var b []byte
	if pretty {
		b, _ = json.MarshalIndent(results, "", "    ")
	} else {
		b, _ = json.Marshal(results)
	}
	w.Write(b)
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
	} else {
		return "", "", fmt.Errorf("unable to parse Basic Auth credentials")
	}
}

// authenticate wraps a handler and ensures that if user credentials are passed in
// an attempt is made to authenticate that user. If authentication fails, an error is returned.
//
// There is one exception: if there are no users in the system, authentication is not required. This
// is to facilitate bootstrapping of a system with authentication enabled.
func authenticate(inner func(http.ResponseWriter, *http.Request, *influxdb.User), h *Handler, requireAuthentication bool) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Return early if we are not authenticating
		if !requireAuthentication {
			inner(w, r, nil)
			return
		}
		var user *influxdb.User

		// TODO corylanou: never allow this in the future without users
		if requireAuthentication && h.server.UserCount() > 0 {
			username, password, err := parseCredentials(r)
			if err != nil {
				httpError(w, err.Error(), false, http.StatusUnauthorized)
				return
			}
			if username == "" {
				httpError(w, "username required", false, http.StatusUnauthorized)
				return
			}

			user, err = h.server.Authenticate(username, password)
			if err != nil {
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

// versionHeader taks a HTTP handler and returns a HTTP handler
// and adds the X-INFLUXBD-VERSION header to outgoing responses.
func versionHeader(inner http.Handler, version string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Add("X-InfluxDB-Version", version)
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
		inner.ServeHTTP(l, r)
		if err := recover(); err != nil {
			logLine := buildLogLine(l, r, start)
			logLine = fmt.Sprintf(`%s [err:%s]`, logLine, err)
			weblog.Println(logLine)
		}
	})
}

// SnapshotHandler streams out a snapshot from the server.
type SnapshotHandler struct {
	CreateSnapshotWriter func() (*influxdb.SnapshotWriter, error)
}

func (h *SnapshotHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Read in previous snapshot from request body.
	var prev influxdb.Snapshot
	if err := json.NewDecoder(r.Body).Decode(&prev); err != nil && err != io.EOF {
		httpError(w, "error reading previous snapshot: "+err.Error(), false, http.StatusBadRequest)
		return
	}

	// Retrieve a snapshot from the server.
	sw, err := h.CreateSnapshotWriter()
	if err != nil {
		httpError(w, "error creating snapshot writer: "+err.Error(), false, http.StatusInternalServerError)
		return
	}
	defer sw.Close()

	// Subtract existing snapshot from writer.
	sw.Snapshot = sw.Snapshot.Diff(&prev)

	// Write to response.
	if _, err := sw.WriteTo(w); err != nil {
		httpError(w, "error writing snapshot: "+err.Error(), false, http.StatusInternalServerError)
		return
	}
}

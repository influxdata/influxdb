package httpd

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"code.google.com/p/go-uuid/uuid"

	"github.com/bmizerany/pat"
	"github.com/influxdb/influxdb"
	"github.com/influxdb/influxdb/client"
	"github.com/influxdb/influxdb/influxql"
)

// TODO: Standard response headers (see: HeaderHandler)
// TODO: Compression (see: CompressionHeaderHandler)

// TODO: Check HTTP response codes: 400, 401, 403, 409.

type route struct {
	name        string
	method      string
	pattern     string
	handlerFunc interface{}
}

// Handler represents an HTTP handler for the InfluxDB server.
type Handler struct {
	server                *influxdb.Server
	routes                []route
	mux                   *pat.PatternServeMux
	requireAuthentication bool
}

// NewHandler returns a new instance of Handler.
func NewHandler(s *influxdb.Server, requireAuthentication bool, version string) *Handler {
	h := &Handler{
		server: s,
		mux:    pat.New(),
		requireAuthentication: requireAuthentication,
	}

	weblog := log.New(os.Stderr, `[http] `, 0)

	h.routes = append(h.routes,
		route{
			"query", // Query serving route.
			"GET", "/query", h.serveQuery,
		},
		route{
			"write", // Data-ingest route.
			"POST", "/write", h.serveWrite,
		},
		route{ // List data nodes
			"data_nodes_index",
			"GET", "/data_nodes", h.serveDataNodes,
		},
		route{ // Create data node
			"data_nodes_create",
			"POST", "/data_nodes", h.serveCreateDataNode,
		},
		route{ // Delete data node
			"data_nodes_delete",
			"DELETE", "/data_nodes/:id", h.serveDeleteDataNode,
		},
		route{ // Metastore
			"metastore",
			"GET", "/metastore", h.serveMetastore,
		},
		route{ // Ping
			"ping",
			"GET", "/ping", h.servePing,
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

		handler = versionHeader(handler, version)
		handler = cors(handler)
		handler = requestID(handler)
		handler = logging(handler, r.name, weblog)
		handler = recovery(handler, r.name, weblog) // make sure recovery is always last

		h.mux.Add(r.method, r.pattern, handler)
	}

	return h
}

//ServeHTTP responds to HTTP request to the handler.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.mux.ServeHTTP(w, r)
}

// serveQuery parses an incoming query and, if valid, executes the query.
func (h *Handler) serveQuery(w http.ResponseWriter, r *http.Request, user *influxdb.User) {
	q := r.URL.Query()
	p := influxql.NewParser(strings.NewReader(q.Get("q")))
	db := q.Get("db")
	pretty := q.Get("pretty") == "true"

	// Parse query from query string.
	query, err := p.ParseQuery()
	if err != nil {
		httpError(w, "error parsing query: "+err.Error(), http.StatusBadRequest)
		return
	}

	// If authentication is enabled and there are no users yet, make sure
	// the first statement is creating a new cluster admin.
	if h.requireAuthentication && h.server.UserCount() == 0 {
		stmt, ok := query.Statements[0].(*influxql.CreateUserStatement)
		if !ok || stmt.Privilege == nil || *stmt.Privilege != influxql.AllPrivileges {
			httpError(w, "must create cluster admin", http.StatusUnauthorized)
			return
		}
	}

	// Execute query. One result will return for each statement.
	results := h.server.ExecuteQuery(query, db, user)

	// Send results to client.
	httpResults(w, results, pretty)
}

// BatchWrite is used to send batch write data to the http /write endpoint
type BatchWrite struct {
	Points          []client.Point    `json:"points"`
	Database        string            `json:"database"`
	RetentionPolicy string            `json:"retentionPolicy"`
	Tags            map[string]string `json:"tags"`
	Timestamp       time.Time         `json:"timestamp"`
	Precision       string            `json:"precision"`
}

// UnmarshalJSON decodes the data into the batchWrite struct
func (br *BatchWrite) UnmarshalJSON(b []byte) error {
	var normal struct {
		Points          []client.Point    `json:"points"`
		Database        string            `json:"database"`
		RetentionPolicy string            `json:"retentionPolicy"`
		Tags            map[string]string `json:"tags"`
		Timestamp       time.Time         `json:"timestamp"`
		Precision       string            `json:"precision"`
	}
	var epoch struct {
		Points          []client.Point    `json:"points"`
		Database        string            `json:"database"`
		RetentionPolicy string            `json:"retentionPolicy"`
		Tags            map[string]string `json:"tags"`
		Timestamp       int64             `json:"timestamp"`
		Precision       string            `json:"precision"`
	}

	if err := func() error {
		var err error
		if err = json.Unmarshal(b, &epoch); err != nil {
			return err
		}
		// Convert from epoch to time.Time
		ts, err := client.EpochToTime(epoch.Timestamp, epoch.Precision)
		if err != nil {
			return err
		}
		br.Points = epoch.Points
		br.Database = epoch.Database
		br.RetentionPolicy = epoch.RetentionPolicy
		br.Tags = epoch.Tags
		br.Timestamp = ts
		br.Precision = epoch.Precision
		return nil
	}(); err == nil {
		return nil
	}

	if err := json.Unmarshal(b, &normal); err != nil {
		return err
	}
	normal.Timestamp = client.SetPrecision(normal.Timestamp, normal.Precision)
	br.Points = normal.Points
	br.Database = normal.Database
	br.RetentionPolicy = normal.RetentionPolicy
	br.Tags = normal.Tags
	br.Timestamp = normal.Timestamp
	br.Precision = normal.Precision

	return nil
}

// serveWrite receives incoming series data and writes it to the database.
func (h *Handler) serveWrite(w http.ResponseWriter, r *http.Request, user *influxdb.User) {
	var br BatchWrite

	dec := json.NewDecoder(r.Body)

	var writeError = func(result influxdb.Result, statusCode int) {
		w.WriteHeader(statusCode)
		w.Header().Add("content-type", "application/json")
		_ = json.NewEncoder(w).Encode(&result)
		return
	}

	for {
		if err := dec.Decode(&br); err != nil {
			if err.Error() == "EOF" {
				w.WriteHeader(http.StatusOK)
				return
			}
			writeError(influxdb.Result{Err: err}, http.StatusInternalServerError)
			return
		}

		if br.Database == "" {
			writeError(influxdb.Result{Err: fmt.Errorf("database is required")}, http.StatusInternalServerError)
			return
		}

		if !h.server.DatabaseExists(br.Database) {
			writeError(influxdb.Result{Err: fmt.Errorf("database not found: %q", br.Database)}, http.StatusNotFound)
			return
		}

		if h.requireAuthentication && !user.Authorize(influxql.WritePrivilege, br.Database) {
			writeError(influxdb.Result{Err: fmt.Errorf("%q user is not authorized to write to database %q", user.Name, br.Database)}, http.StatusUnauthorized)
			return
		}

		for _, p := range br.Points {
			if p.Timestamp.Time().IsZero() {
				p.Timestamp = client.Timestamp(br.Timestamp)
			}
			if p.Precision == "" && br.Precision != "" {
				p.Precision = br.Precision
			}
			p.Timestamp = client.Timestamp(client.SetPrecision(p.Timestamp.Time(), p.Precision))
			if len(br.Tags) > 0 {
				for k := range br.Tags {
					if p.Tags[k] == "" {
						p.Tags[k] = br.Tags[k]
					}
				}
			}
			// Need to convert from a client.Point to a influxdb.Point
			iPoint := influxdb.Point{
				Name:      p.Name,
				Tags:      p.Tags,
				Timestamp: p.Timestamp.Time(),
				Values:    p.Values,
			}
			if _, err := h.server.WriteSeries(br.Database, br.RetentionPolicy, []influxdb.Point{iPoint}); err != nil {
				writeError(influxdb.Result{Err: err}, http.StatusInternalServerError)
				return
			}
		}
	}
}

// serveMetastore returns a copy of the metastore.
func (h *Handler) serveMetastore(w http.ResponseWriter, r *http.Request) {
	// Set headers.
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Disposition", `attachment; filename="meta"`)

	if err := h.server.CopyMetastore(w); err != nil {
		httpError(w, err.Error(), http.StatusInternalServerError)
	}
}

// servePing returns a simple response to let the client know the server is running.
func (h *Handler) servePing(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusNoContent)
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
		httpError(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Parse the URL.
	u, err := url.Parse(n.URL)
	if err != nil {
		httpError(w, "invalid data node url", http.StatusBadRequest)
		return
	}

	// Create the data node.
	if err := h.server.CreateDataNode(u); err == influxdb.ErrDataNodeExists {
		httpError(w, err.Error(), http.StatusConflict)
		return
	} else if err != nil {
		httpError(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Retrieve data node reference.
	node := h.server.DataNodeByURL(u)

	// Create a new replica on the broker.
	if err := h.server.Client().CreateReplica(node.ID); err != nil {
		httpError(w, err.Error(), http.StatusBadGateway)
		return
	}

	// Write new node back to client.
	w.WriteHeader(http.StatusCreated)
	w.Header().Add("content-type", "application/json")
	_ = json.NewEncoder(w).Encode(&dataNodeJSON{ID: node.ID, URL: node.URL.String()})
}

// serveDeleteDataNode removes an existing node.
func (h *Handler) serveDeleteDataNode(w http.ResponseWriter, r *http.Request) {
	// Parse node id.
	nodeID, err := strconv.ParseUint(r.URL.Query().Get(":id"), 10, 64)
	if err != nil {
		httpError(w, "invalid node id", http.StatusBadRequest)
		return
	}

	// Delete the node.
	if err := h.server.DeleteDataNode(nodeID); err == influxdb.ErrDataNodeNotFound {
		httpError(w, err.Error(), http.StatusNotFound)
		return
	} else if err != nil {
		httpError(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

type dataNodeJSON struct {
	ID  uint64 `json:"id"`
	URL string `json:"url"`
}

func isAuthorizationError(err error) bool {
	type authorize interface {
		authorize()
	}
	_, ok := err.(authorize)
	return ok
}

// httpResult writes a Results array to the client.
func httpResults(w http.ResponseWriter, results influxdb.Results, pretty bool) {
	if results.Error() != nil {
		if isAuthorizationError(results.Error()) {
			w.WriteHeader(http.StatusUnauthorized)
		} else {
			w.WriteHeader(http.StatusInternalServerError)
		}
	}
	w.Header().Add("content-type", "application/json")
	var b []byte
	if pretty {
		b, _ = json.MarshalIndent(results, "", "    ")
	} else {
		b, _ = json.Marshal(results)
	}
	w.Write(b)
}

// httpError writes an error to the client in a standard format.
func httpError(w http.ResponseWriter, error string, code int) {
	w.Header().Add("content-type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(influxdb.Results{Err: errors.New(error)})
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
				httpError(w, err.Error(), http.StatusUnauthorized)
				return
			}
			if username == "" {
				httpError(w, "username required", http.StatusUnauthorized)
				return
			}

			user, err = h.server.Authenticate(username, password)
			if err != nil {
				httpError(w, err.Error(), http.StatusUnauthorized)
				return
			}
		}
		inner(w, r, user)
	})
}

// versionHeader taks a HTTP handler and returns a HTTP handler
// and adds the X-INFLUXBD-VERSION header to outgoing responses.
func versionHeader(inner http.Handler, version string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Add("X-Influxdb-Version", version)
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
		uid := uuid.NewUUID()
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

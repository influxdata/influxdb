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

	"github.com/bmizerany/pat"
	"github.com/influxdb/influxdb"
	"github.com/influxdb/influxdb/influxql"
)

// TODO: Standard response headers (see: HeaderHandler)
// TODO: Compression (see: CompressionHeaderHandler)

// TODO: Check HTTP response codes: 400, 401, 403, 409.

type route struct {
	name        string
	method      string
	pattern     string
	handlerFunc http.HandlerFunc
}

// Handler represents an HTTP handler for the InfluxDB server.
type Handler struct {
	server                *influxdb.Server
	routes                []route
	mux                   *pat.PatternServeMux
	user                  *influxdb.User
	requireAuthentication bool
}

// NewHandler returns a new instance of Handler.
func NewHandler(s *influxdb.Server, requireAuthentication bool) *Handler {
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

		handler = r.handlerFunc
		handler = authorize(handler, h, requireAuthentication)
		handler = cors(handler)
		handler = logging(handler, r.name, weblog)
		handler = recovery(handler, r.name, weblog) // make sure recovery is always last

		h.mux.Add(r.method, r.pattern, handler)
	}

	return h
}

//ServeHTTP responds to HTTP request to the handler.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Add("X-Influxdb-Version", h.server.Version())
	h.mux.ServeHTTP(w, r)
}

// serveQuery parses an incoming query and, if valid, executes the query.
func (h *Handler) serveQuery(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	p := influxql.NewParser(strings.NewReader(q.Get("q")))
	db := q.Get("db")

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
	results := h.server.ExecuteQuery(query, db, h.user)

	// Send results to client.
	httpResults(w, results)
}

type batchWrite struct {
	Points          []influxdb.Point  `json:"points"`
	Database        string            `json:"database"`
	RetentionPolicy string            `json:"retentionPolicy"`
	Tags            map[string]string `json:"tags"`
	Timestamp       time.Time         `json:"timestamp"`
}

// serveWrite receives incoming series data and writes it to the database.
func (h *Handler) serveWrite(w http.ResponseWriter, r *http.Request) {
	var br batchWrite

	dec := json.NewDecoder(r.Body)
	dec.UseNumber()

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

		// TODO corylanou: Check if user can write to specified database
		//if !user_can_write(br.Database) {
		//writeError(&Result{Err: fmt.Errorf("%q user is not authorized to write to database %q", u.Name)}, http.StatusUnauthorized)
		//return
		//}

		for _, p := range br.Points {
			if p.Timestamp.IsZero() {
				p.Timestamp = br.Timestamp
			}
			if len(br.Tags) > 0 {
				for k, _ := range br.Tags {
					if p.Tags[k] == "" {
						p.Tags[k] = br.Tags[k]
					}
				}
			}
			if _, err := h.server.WriteSeries(br.Database, br.RetentionPolicy, []influxdb.Point{p}); err != nil {
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
	w.WriteHeader(http.StatusOK)
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
func httpResults(w http.ResponseWriter, results influxdb.Results) {
	if results.Error() != nil {
		if isAuthorizationError(results.Error()) {
			w.WriteHeader(http.StatusUnauthorized)
		} else {
			w.WriteHeader(http.StatusInternalServerError)
		}
	}
	w.Header().Add("content-type", "application/json")
	_ = json.NewEncoder(w).Encode(results)
}

// httpError writes an error to the client in a standard format.
func httpError(w http.ResponseWriter, error string, code int) {
	var results influxdb.Results
	results = append(results, &influxdb.Result{Err: errors.New(error)})

	w.Header().Add("content-type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(results)
}

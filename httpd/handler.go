package httpd

import (
	"encoding/base64"
	"encoding/json"
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

// getUsernameAndPassword returns the username and password encoded in
// a request. The credentials may be present as URL query params, or as
// a Basic Authentication header.
func getUsernameAndPassword(r *http.Request) (string, string, error) {
	q := r.URL.Query()
	username, password := q.Get("u"), q.Get("p")
	if username != "" && password != "" {
		return username, password, nil
	}
	auth := r.Header.Get("Authorization")
	if auth == "" {
		return "", "", nil
	}
	fields := strings.Split(auth, " ")
	if len(fields) != 2 {
		return "", "", fmt.Errorf("invalid Basic Authentication header")
	}
	bs, err := base64.StdEncoding.DecodeString(fields[1])
	if err != nil {
		return "", "", fmt.Errorf("invalid Base64 encoding")
	}
	fields = strings.Split(string(bs), ":")
	if len(fields) != 2 {
		return "", "", fmt.Errorf("invalid Basic Authentication value")
	}
	return fields[0], fields[1], nil
}

type route struct {
	name        string
	method      string
	pattern     string
	handlerFunc http.HandlerFunc
}

// Handler represents an HTTP handler for the InfluxDB server.
type Handler struct {
	server *influxdb.Server
	addr   string
	routes []route
	mux    *pat.PatternServeMux
	user   *influxdb.User

	// The InfluxDB verion returned by the HTTP response header.
	Version string // TODO corylanou: this never gets set, so it reports improperly when we right out headers
}

// NewHandler returns a new instance of Handler.

func NewHandler(s *influxdb.Server, requireAuthentication bool) *Handler {
	h := &Handler{
		server: s,
		mux:    pat.New(),
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
	w.Header().Add("X-Influxdb-Version", h.Version)
	h.mux.ServeHTTP(w, r)
}

// makeAuthenticationHandler takes a custom handler and returns a standard handler, ensuring that
// if user credentials are passed in, an attempt is made to authenticate that user. If authentication
// fails, an error is returned to the user.
//
// There is one exception: if there are no users in the system, authentication is not required. This
// is to facilitate bootstrapping of a system with authentication enabled.
//func (h *Handler) makeAuthenticationHandler(fn func(http.ResponseWriter, *http.Request, *influxdb.User)) http.HandlerFunc {
//return func(w http.ResponseWriter, r *http.Request) {
//var user *influxdb.User
//if h.AuthenticationEnabled && len(h.server.Users()) > 0 {
//username, password, err := getUsernameAndPassword(r)
//if err != nil {
//h.error(w, err.Error(), http.StatusUnauthorized)
//return
//}
//if username == "" {
//h.error(w, "username required", http.StatusUnauthorized)
//return
//}

//user, err = h.server.Authenticate(username, password)
//if err != nil {
//h.error(w, err.Error(), http.StatusUnauthorized)
//return
//}
//}
//fn(w, r, user)
//}
//}

// serveQuery parses an incoming query and, if valid, executes the query.
func (h *Handler) serveQuery(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	p := influxql.NewParser(strings.NewReader(q.Get("q")))
	db := q.Get("db")

	// Parse query from query string.
	query, err := p.ParseQuery()
	if err != nil {
		h.error(w, "error parsing query: "+err.Error(), http.StatusBadRequest)
		return
	}

	// Execute query. One result will return for each statement.
	results := h.server.ExecuteQuery(query, db, h.user)

	// If any statement errored then set the response status code.
	if results.Error() != nil {
		w.WriteHeader(http.StatusInternalServerError)
	}

	// Write resultset.
	_ = json.NewEncoder(w).Encode(results)
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
		h.error(w, err.Error(), http.StatusInternalServerError)
	}
}

// servePing returns a simple response to let the client know the server is running.
func (h *Handler) servePing(w http.ResponseWriter, r *http.Request) {}

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
		h.error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Parse the URL.
	u, err := url.Parse(n.URL)
	if err != nil {
		h.error(w, "invalid data node url", http.StatusBadRequest)
		return
	}

	// Create the data node.
	if err := h.server.CreateDataNode(u); err == influxdb.ErrDataNodeExists {
		h.error(w, err.Error(), http.StatusConflict)
		return
	} else if err != nil {
		h.error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Retrieve data node reference.
	node := h.server.DataNodeByURL(u)

	// Create a new replica on the broker.
	if err := h.server.Client().CreateReplica(node.ID); err != nil {
		h.error(w, err.Error(), http.StatusBadGateway)
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
		h.error(w, "invalid node id", http.StatusBadRequest)
		return
	}

	// Delete the node.
	if err := h.server.DeleteDataNode(nodeID); err == influxdb.ErrDataNodeNotFound {
		h.error(w, err.Error(), http.StatusNotFound)
		return
	} else if err != nil {
		h.error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

type dataNodeJSON struct {
	ID  uint64 `json:"id"`
	URL string `json:"url"`
}

// error returns an error to the client in a standard format.
func (h *Handler) error(w http.ResponseWriter, error string, code int) {
	// TODO: Return error as JSON.
	http.Error(w, error, code)
}

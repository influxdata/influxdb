package influxdb

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/bmizerany/pat"
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

// Handler represents an HTTP handler for the InfluxDB server.
type Handler struct {
	server *Server
	mux    *pat.PatternServeMux

	// Whether endpoints require authentication.
	AuthenticationEnabled bool

	// The InfluxDB verion returned by the HTTP response header.
	Version string
}

// NewHandler returns a new instance of Handler.
func NewHandler(s *Server) *Handler {
	h := &Handler{
		server: s,
		mux:    pat.New(),
	}

	// Query serving route.
	h.mux.Get("/query", h.makeAuthenticationHandler(h.serveQuery))

	// Data-ingest route.
	h.mux.Post("/write", h.makeAuthenticationHandler(h.serveWrite))

	// Data node routes.
	h.mux.Get("/data_nodes", h.makeAuthenticationHandler(h.serveDataNodes))
	h.mux.Post("/data_nodes", h.makeAuthenticationHandler(h.serveCreateDataNode))
	h.mux.Del("/data_nodes/:id", h.makeAuthenticationHandler(h.serveDeleteDataNode))

	// Utilities
	h.mux.Get("/metastore", h.makeAuthenticationHandler(h.serveMetastore))
	h.mux.Get("/ping", h.makeAuthenticationHandler(h.servePing))
	h.mux.Get("/process_continuous_queries", h.makeAuthenticationHandler(h.serveProcessContinuousQueries))

	return h
}

// ServeHTTP responds to HTTP request to the handler.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Add("Access-Control-Allow-Origin", "*")
	w.Header().Add("Access-Control-Max-Age", "2592000")
	w.Header().Add("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE")
	w.Header().Add("Access-Control-Allow-Headers", "Origin, X-Requested-With, Content-Type, Accept")
	w.Header().Add("X-Influxdb-Version", h.Version)

	// If this is a CORS OPTIONS request then send back okie-dokie.
	if r.Method == "OPTIONS" {
		w.WriteHeader(http.StatusOK)
		return
	}

	// Otherwise handle it via pat.
	h.mux.ServeHTTP(w, r)
}

// makeAuthenticationHandler takes a custom handler and returns a standard handler, ensuring that
// if user credentials are passed in, an attempt is made to authenticate that user. If authentication
// fails, an error is returned to the user.
//
// There is one exception: if there are no users in the system, authentication is not required. This
// is to facilitate bootstrapping of a system with authentication enabled.
func (h *Handler) makeAuthenticationHandler(fn func(http.ResponseWriter, *http.Request, *User)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var user *User
		if h.AuthenticationEnabled && len(h.server.Users()) > 0 {
			username, password, err := getUsernameAndPassword(r)
			if err != nil {
				h.error(w, err.Error(), http.StatusUnauthorized)
				return
			}
			if username == "" {
				h.error(w, "username required", http.StatusUnauthorized)
				return
			}

			user, err = h.server.Authenticate(username, password)
			if err != nil {
				h.error(w, err.Error(), http.StatusUnauthorized)
				return
			}
		}
		fn(w, r, user)
	}
}

// serveQuery parses an incoming query and, if valid, executes the query.
func (h *Handler) serveQuery(w http.ResponseWriter, r *http.Request, u *User) {
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
	results := h.server.ExecuteQuery(query, db, u)

	// If any statement errored then set the response status code.
	if results.Error() != nil {
		w.WriteHeader(http.StatusInternalServerError)
	}

	// Write resultset.
	_ = json.NewEncoder(w).Encode(results)
}

type batchWrite struct {
	Points          []Point           `json:"points"`
	Database        string            `json:"database"`
	RetentionPolicy string            `json:"retentionPolicy"`
	Tags            map[string]string `json:"tags"`
	Timestamp       time.Time         `json:"timestamp"`
}

// serveWrite receives incoming series data and writes it to the database.
func (h *Handler) serveWrite(w http.ResponseWriter, r *http.Request, u *User) {
	var br batchWrite

	dec := json.NewDecoder(r.Body)
	dec.UseNumber()

	var writeError = func(result Result, statusCode int) {
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
			writeError(Result{Err: err}, http.StatusInternalServerError)
			return
		}

		if br.Database == "" {
			writeError(Result{Err: fmt.Errorf("database is required")}, http.StatusInternalServerError)
			return
		}

		if h.server.databases[br.Database] == nil {
			writeError(Result{Err: fmt.Errorf("database not found: %q", br.Database)}, http.StatusNotFound)
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
			if _, err := h.server.WriteSeries(br.Database, br.RetentionPolicy, []Point{p}); err != nil {
				writeError(Result{Err: err}, http.StatusInternalServerError)
				return
			}
		}
	}
}

// serveMetastore returns a copy of the metastore.
func (h *Handler) serveMetastore(w http.ResponseWriter, r *http.Request, u *User) {
	// Set headers.
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Disposition", `attachment; filename="meta"`)

	if err := h.server.CopyMetastore(w); err != nil {
		h.error(w, err.Error(), http.StatusInternalServerError)
	}
}

// servePing returns a simple response to let the client know the server is running.
func (h *Handler) servePing(w http.ResponseWriter, r *http.Request, u *User) {}

// serveDataNodes returns a list of all data nodes in the cluster.
func (h *Handler) serveDataNodes(w http.ResponseWriter, r *http.Request, u *User) {
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
func (h *Handler) serveCreateDataNode(w http.ResponseWriter, r *http.Request, _ *User) {
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
	if err := h.server.CreateDataNode(u); err == ErrDataNodeExists {
		h.error(w, err.Error(), http.StatusConflict)
		return
	} else if err != nil {
		h.error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Retrieve data node reference.
	node := h.server.DataNodeByURL(u)

	// Create a new replica on the broker.
	if err := h.server.client.CreateReplica(node.ID); err != nil {
		h.error(w, err.Error(), http.StatusBadGateway)
		return
	}

	// Write new node back to client.
	w.WriteHeader(http.StatusCreated)
	w.Header().Add("content-type", "application/json")
	_ = json.NewEncoder(w).Encode(&dataNodeJSON{ID: node.ID, URL: node.URL.String()})
}

// serveDeleteDataNode removes an existing node.
func (h *Handler) serveDeleteDataNode(w http.ResponseWriter, r *http.Request, u *User) {
	// Parse node id.
	nodeID, err := strconv.ParseUint(r.URL.Query().Get(":id"), 10, 64)
	if err != nil {
		h.error(w, "invalid node id", http.StatusBadRequest)
		return
	}

	// Delete the node.
	if err := h.server.DeleteDataNode(nodeID); err == ErrDataNodeNotFound {
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

func (h *Handler) serveProcessContinuousQueries(w http.ResponseWriter, r *http.Request, u *User) {
	if err := h.server.RunContinuousQueries(); err != nil {
		h.error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusAccepted)
}

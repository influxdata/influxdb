package influxdb

import (
	"encoding/json"
	"net/http"
	"strings"

	"github.com/bmizerany/pat"
	"github.com/influxdb/influxdb/influxql"
)

// TODO: Standard response headers (see: HeaderHandler)
// TODO: Compression (see: CompressionHeaderHandler)

// TODO: Check HTTP response codes: 400, 401, 403, 409.

// Handler represents an HTTP handler for the InfluxDB server.
type Handler struct {
	server *Server
	mux    *pat.PatternServeMux

	// The InfluxDB verion returned by the HTTP response header.
	Version string
}

// NewHandler returns a new instance of Handler.
func NewHandler(s *Server) *Handler {
	h := &Handler{
		server: s,
		mux:    pat.New(),
	}

	// Authentication route
	h.mux.Get("/authenticate", http.HandlerFunc(h.serveAuthenticate))

	// User routes.
	h.mux.Get("/users", http.HandlerFunc(h.serveUsers))
	h.mux.Post("/users", http.HandlerFunc(h.serveCreateUser))
	h.mux.Put("/users/:user", http.HandlerFunc(h.serveUpdateUser))
	h.mux.Del("/users/:user", http.HandlerFunc(h.serveDeleteUser))

	// Database routes
	h.mux.Get("/db", http.HandlerFunc(h.serveDatabases))
	h.mux.Post("/db", http.HandlerFunc(h.serveCreateDatabase))
	h.mux.Del("/db/:name", http.HandlerFunc(h.serveDeleteDatabase))

	// Series routes.
	h.mux.Get("/db/:db/series", http.HandlerFunc(h.serveQuery))
	h.mux.Post("/db/:db/series", http.HandlerFunc(h.serveWriteSeries))

	// Shard routes.
	h.mux.Get("/db/:db/shards", http.HandlerFunc(h.serveShards))
	h.mux.Del("/db/:db/shards/:id", http.HandlerFunc(h.serveDeleteShard))

	// Retention policy routes.
	h.mux.Get("/db/:db/retention_policies", http.HandlerFunc(h.serveRetentionPolicies))
	h.mux.Post("/db/:db/retention_policies", http.HandlerFunc(h.serveCreateRetentionPolicy))
	h.mux.Put("/db/:db/retention_policies/:name", http.HandlerFunc(h.serveUpdateRetentionPolicy))
	h.mux.Del("/db/:db/retention_policies/:name", http.HandlerFunc(h.serveDeleteRetentionPolicy))

	// Utilities
	h.mux.Get("/ping", http.HandlerFunc(h.servePing))

	// Cluster config endpoints
	h.mux.Get("/cluster/servers", http.HandlerFunc(h.serveServers))
	h.mux.Del("/cluster/servers/:id", http.HandlerFunc(h.serveDeleteServer))

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

// serveQuery parses an incoming query and returns the results.
func (h *Handler) serveQuery(w http.ResponseWriter, r *http.Request) {
	// TODO: Authentication.

	// Parse query from query string.
	urlQry := r.URL.Query()
	_, err := influxql.NewParser(strings.NewReader(urlQry.Get("q"))).ParseQuery()
	if err != nil {
		h.error(w, "parse error: "+err.Error(), http.StatusBadRequest)
		return
	}

	// Retrieve database from server.
	/*
		db := h.server.Database(urlQry.Get(":db"))
		if db == nil {
			h.error(w, ErrDatabaseNotFound.Error(), http.StatusNotFound)
			return
		}
	*/

	// Parse the time precision from the query params.
	/*
		precision, err := parseTimePrecision(urlQry.Get("time_precision"))
		if err != nil {
			h.error(w, err.Error(), http.StatusBadRequest)
			return
		}
	*/

	// Execute query against the database.
	/*
		if err := db.ExecuteQuery(q); err != nil {
			h.error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	*/
}

// serveWriteSeries receives incoming series data and writes it to the database.
func (h *Handler) serveWriteSeries(w http.ResponseWriter, r *http.Request) {
	// TODO: Authentication.

	/* TEMPORARILY REMOVED FOR PROTOBUFS.
	// Retrieve database from server.
	db := h.server.Database(r.URL.Query().Get(":db"))
	if db == nil {
		h.error(w, ErrDatabaseNotFound.Error(), http.StatusNotFound)
		return
	}

	// Parse time precision from query parameters.
	precision, err := parseTimePrecision(r.URL.Query().Get("time_precision"))
	if err != nil {
		h.error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Setup HTTP request reader. Wrap in a gzip reader if encoding set in header.
	reader := r.Body
	if r.Header.Get("Content-Encoding") == "gzip" {
		if reader, err = gzip.NewReader(r.Body); err != nil {
			h.error(w, err.Error(), http.StatusBadRequest)
			return
		}
	}

	// Decode series from reader.
	ss := []*serializedSeries{}
	dec := json.NewDecoder(reader)
	dec.UseNumber()
	if err := dec.Decode(&ss); err != nil {
		h.error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Convert the wire format to the internal representation of the time series.
	series, err := serializedSeriesSlice(ss).series(precision)
	if err != nil {
		h.error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Write series data to the database.
	// TODO: Allow multiple series written to DB at once.
	for _, s := range series {
		if err := db.WriteSeries(s); err != nil {
			h.error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}
	*/
}

// serveDatabases returns a list of all databases on the server.
func (h *Handler) serveDatabases(w http.ResponseWriter, r *http.Request) {

	// Retrieve databases from the server.
	databases := h.server.Databases()

	// JSON encode databases to the response.
	w.Header().Add("content-type", "application/json")
	_ = json.NewEncoder(w).Encode(databases)
}

// serveCreateDatabase creates a new database on the server.
func (h *Handler) serveCreateDatabase(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Name string `json:"name"`
	}

	// Decode the request from the body.
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		h.error(w, err.Error(), http.StatusBadRequest)
		return
	} else if req.Name == "" {
		h.error(w, ErrDatabaseNameRequired.Error(), http.StatusBadRequest)
		return
	}

	// Create the database.
	if err := h.server.CreateDatabase(req.Name); err == ErrDatabaseExists {
		h.error(w, err.Error(), http.StatusConflict)
		return
	} else if err != nil {
		h.error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusCreated)
}

// serveDeleteDatabase deletes an existing database on the server.
func (h *Handler) serveDeleteDatabase(w http.ResponseWriter, r *http.Request) {
	name := r.URL.Query().Get(":name")
	if err := h.server.DeleteDatabase(name); err == ErrDatabaseNotFound {
		h.error(w, err.Error(), http.StatusNotFound)
		return
	} else if err != nil {
		h.error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// serveAuthenticate authenticates a user.
func (h *Handler) serveAuthenticate(w http.ResponseWriter, r *http.Request) {}

// serveUsers returns data about a single user.
func (h *Handler) serveUsers(w http.ResponseWriter, r *http.Request) {

	// Generate a list of objects for encoding to the API.
	a := make([]*userJSON, 0)
	for _, u := range h.server.Users() {
		a = append(a, &userJSON{
			Name:  u.Name,
			Admin: u.Admin,
		})
	}

	w.Header().Add("content-type", "application/json")
	_ = json.NewEncoder(w).Encode(a)
}

type userJSON struct {
	Name     string `json:"name"`
	Password string `json:"password,omitempty"`
	Admin    bool   `json:"admin,omitempty"`
}

// serveCreateUser creates a new user.
func (h *Handler) serveCreateUser(w http.ResponseWriter, r *http.Request) {
	// Read in user from request body.
	var u userJSON
	if err := json.NewDecoder(r.Body).Decode(&u); err != nil {
		h.error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Create the user.
	if err := h.server.CreateUser(u.Name, u.Password, u.Admin); err == ErrUserExists {
		h.error(w, err.Error(), http.StatusConflict)
		return
	} else if err != nil {
		h.error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

// serveUpdateUser updates an existing user.
func (h *Handler) serveUpdateUser(w http.ResponseWriter, r *http.Request) {
	// Read in user from request body.
	var u userJSON
	if err := json.NewDecoder(r.Body).Decode(&u); err != nil {
		h.error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Create the user.
	if err := h.server.UpdateUser(r.URL.Query().Get(":user"), u.Password); err == ErrUserNotFound {
		h.error(w, err.Error(), http.StatusNotFound)
		return
	} else if err != nil {
		h.error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// serveDeleteUser removes an existing user.
func (h *Handler) serveDeleteUser(w http.ResponseWriter, r *http.Request) {
	// Delete the user.
	if err := h.server.DeleteUser(r.URL.Query().Get(":user")); err == ErrUserNotFound {
		h.error(w, err.Error(), http.StatusNotFound)
		return
	} else if err != nil {
		h.error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// servePing returns a simple response to let the client know the server is running.
func (h *Handler) servePing(w http.ResponseWriter, r *http.Request) {}

// serveShards returns a list of shards.
func (h *Handler) serveShards(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()

	// Retrieves shards for the database.
	shards, err := h.server.Shards(q.Get(":db"))
	if err == ErrDatabaseNotFound {
		h.error(w, err.Error(), http.StatusNotFound)
		return
	} else if err != nil {
		h.error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Write data to the response.
	w.Header().Add("content-type", "application/json")
	_ = json.NewEncoder(w).Encode(shards)
}

// serveDeleteShard removes an existing shard.
func (h *Handler) serveDeleteShard(w http.ResponseWriter, r *http.Request) {}

// serveRetentionPolicies returns a list of retention policys.
func (h *Handler) serveRetentionPolicies(w http.ResponseWriter, r *http.Request) {
	// Retrieve policies by database.
	policies, err := h.server.RetentionPolicies(r.URL.Query().Get(":db"))
	if err == ErrDatabaseNotFound {
		h.error(w, err.Error(), http.StatusNotFound)
		return
	} else if err != nil {
		h.error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Write data to response body.
	w.Header().Add("content-type", "application/json")
	_ = json.NewEncoder(w).Encode(policies)
}

// serveCreateRetentionPolicy creates a new retention policy.
func (h *Handler) serveCreateRetentionPolicy(w http.ResponseWriter, r *http.Request) {
	// Decode the policy from the body.
	var policy RetentionPolicy
	if err := json.NewDecoder(r.Body).Decode(&policy); err != nil {
		h.error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Create the retention policy.
	if err := h.server.CreateRetentionPolicy(r.URL.Query().Get(":db"), &policy); err == ErrDatabaseNotFound {
		h.error(w, err.Error(), http.StatusNotFound)
		return
	} else if err == ErrRetentionPolicyExists {
		h.error(w, err.Error(), http.StatusConflict)
		return
	} else if err != nil {
		h.error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusCreated)
}

// serveUpdateRetentionPolicy updates an existing retention policy.
func (h *Handler) serveUpdateRetentionPolicy(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	db, name := q.Get(":db"), q.Get(":name")

	// Decode the new policy values from the body.
	var policy RetentionPolicy
	if err := json.NewDecoder(r.Body).Decode(&policy); err != nil {
		h.error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Update the retention policy.
	if err := h.server.UpdateRetentionPolicy(db, name, &policy); err == ErrDatabaseNotFound || err == ErrRetentionPolicyNotFound {
		h.error(w, err.Error(), http.StatusNotFound)
		return
	} else if err != nil {
		h.error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// serveDeleteRetentionPolicy removes an existing retention policy.
func (h *Handler) serveDeleteRetentionPolicy(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	db, name := q.Get(":db"), q.Get(":name")

	// Delete the retention policy.
	if err := h.server.DeleteRetentionPolicy(db, name); err == ErrDatabaseNotFound || err == ErrRetentionPolicyNotFound {
		h.error(w, err.Error(), http.StatusNotFound)
		return
	} else if err != nil {
		h.error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// serveServers returns a list of servers in the cluster.
func (h *Handler) serveServers(w http.ResponseWriter, r *http.Request) {}

// serveDeleteServer removes a server from the cluster.
func (h *Handler) serveDeleteServer(w http.ResponseWriter, r *http.Request) {}

// error returns an error to the client in a standard format.
func (h *Handler) error(w http.ResponseWriter, error string, code int) {
	// TODO: Return error as JSON.
	http.Error(w, error, code)
}

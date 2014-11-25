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

	// Series routes.
	h.mux.Get("/db/:db/series", http.HandlerFunc(h.serveQuery))
	h.mux.Post("/db/:db/series", http.HandlerFunc(h.serveWriteSeries))
	h.mux.Del("/db/:db/series/:series", http.HandlerFunc(h.serveDeleteSeries))
	h.mux.Get("/db", http.HandlerFunc(h.serveDatabases))
	h.mux.Post("/db", http.HandlerFunc(h.serveCreateDatabase))
	h.mux.Del("/db/:name", http.HandlerFunc(h.serveDeleteDatabase))

	// Cluster admins routes.
	h.mux.Get("/cluster_admins/authenticate", http.HandlerFunc(h.serveAuthenticateClusterAdmin))
	h.mux.Get("/cluster_admins", http.HandlerFunc(h.serveClusterAdmins))
	h.mux.Post("/cluster_admins", http.HandlerFunc(h.serveCreateClusterAdmin))
	h.mux.Post("/cluster_admins/:user", http.HandlerFunc(h.serveUpdateClusterAdmin))
	h.mux.Del("/cluster_admins/:user", http.HandlerFunc(h.serveDeleteClusterAdmin))

	// Database users routes.
	h.mux.Get("/db/:db/authenticate", http.HandlerFunc(h.serveAuthenticateDBUser))
	h.mux.Get("/db/:db/users", http.HandlerFunc(h.serveDBUsers))
	h.mux.Post("/db/:db/users", http.HandlerFunc(h.serveCreateDBUser))
	h.mux.Get("/db/:db/users/:user", http.HandlerFunc(h.serveDBUser))
	h.mux.Post("/db/:db/users/:user", http.HandlerFunc(h.serveUpdateDBUser))
	h.mux.Del("/db/:db/users/:user", http.HandlerFunc(h.serveDeleteDBUser))

	// Utilities
	h.mux.Get("/ping", http.HandlerFunc(h.servePing))
	h.mux.Get("/interfaces", http.HandlerFunc(h.serveInterfaces))

	// Shard routes.
	h.mux.Get("/db/:db/shards", http.HandlerFunc(h.serveShards))
	h.mux.Del("/db/:db/shards/:id", http.HandlerFunc(h.serveDeleteShard))

	// retention policy routes.
	h.mux.Get("/db/:db/retention_policies", http.HandlerFunc(h.serveRetentionPolicys))
	h.mux.Get("/db/:db/retention_policies/:name/shards", http.HandlerFunc(h.serveShardsByRetentionPolicy))
	h.mux.Post("/db/:db/retention_policies", http.HandlerFunc(h.serveCreateRetentionPolicy))
	h.mux.Post("/db/:db/retention_policies/:name", http.HandlerFunc(h.serveUpdateRetentionPolicy))
	h.mux.Del("/db/:db/retention_policies/:name", http.HandlerFunc(h.serveDeleteRetentionPolicy))

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
	values := r.URL.Query()
	_, err := influxql.NewParser(strings.NewReader(values.Get("q"))).ParseQuery()
	if err != nil {
		h.error(w, "parse error: "+err.Error(), http.StatusBadRequest)
		return
	}

	// Retrieve database from server.
	db := h.server.Database(values.Get(":db"))
	if db == nil {
		h.error(w, ErrDatabaseNotFound.Error(), http.StatusNotFound)
		return
	}

	// Parse the time precision from the query params.
	/*
		precision, err := parseTimePrecision(values.Get("time_precision"))
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

// serveDeleteSeries deletes a given series.
func (h *Handler) serveDeleteSeries(w http.ResponseWriter, r *http.Request) {}

// serveDatabases returns a list of all databases on the server.
func (h *Handler) serveDatabases(w http.ResponseWriter, r *http.Request) {
	// TODO: Authentication

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

	// TODO: Authentication

	// Decode the request from the body.
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.error(w, err.Error(), http.StatusBadRequest)
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
	if err := h.server.DeleteDatabase(name); err != ErrDatabaseNotFound {
		h.error(w, err.Error(), http.StatusNotFound)
		return
	} else if err != nil {
		h.error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// serveAuthenticateClusterAdmin authenticates a user as a ClusterAdmin.
func (h *Handler) serveAuthenticateClusterAdmin(w http.ResponseWriter, r *http.Request) {}

// serveClusterAdmins returns data about a single cluster admin.
func (h *Handler) serveClusterAdmins(w http.ResponseWriter, r *http.Request) {}

// serveCreateClusterAdmin creates a new cluster admin.
func (h *Handler) serveCreateClusterAdmin(w http.ResponseWriter, r *http.Request) {}

// serveUpdateClusterAdmin updates an existing cluster admin.
func (h *Handler) serveUpdateClusterAdmin(w http.ResponseWriter, r *http.Request) {}

// serveDeleteClusterAdmin removes an existing cluster admin.
func (h *Handler) serveDeleteClusterAdmin(w http.ResponseWriter, r *http.Request) {}

// serveAuthenticateDBUser authenticates a user as a database user.
func (h *Handler) serveAuthenticateDBUser(w http.ResponseWriter, r *http.Request) {}

// serveDBUsers returns data about a single database user.
func (h *Handler) serveDBUsers(w http.ResponseWriter, r *http.Request) {}

// serveCreateDBUser creates a new database user.
func (h *Handler) serveCreateDBUser(w http.ResponseWriter, r *http.Request) {}

// serveDBUser returns data about a single database user.
func (h *Handler) serveDBUser(w http.ResponseWriter, r *http.Request) {}

// serveUpdateDBUser updates an existing database user.
func (h *Handler) serveUpdateDBUser(w http.ResponseWriter, r *http.Request) {}

// serveDeleteDBUser removes an existing database user.
func (h *Handler) serveDeleteDBUser(w http.ResponseWriter, r *http.Request) {}

// servePing returns a simple response to let the client know the server is running.
func (h *Handler) servePing(w http.ResponseWriter, r *http.Request) {}

// serveInterfaces returns a list of available interfaces.
func (h *Handler) serveInterfaces(w http.ResponseWriter, r *http.Request) {}

// serveShards returns a list of shards.
func (h *Handler) serveShards(w http.ResponseWriter, r *http.Request) {}

// serveShardsByRetentionPolicy returns a list of shards for a given retention policy.
func (h *Handler) serveShardsByRetentionPolicy(w http.ResponseWriter, r *http.Request) {}

// serveDeleteShard removes an existing shard.
func (h *Handler) serveDeleteShard(w http.ResponseWriter, r *http.Request) {}

// serveRetentionPolicys returns a list of retention policys.
func (h *Handler) serveRetentionPolicys(w http.ResponseWriter, r *http.Request) {}

// serveCreateRetentionPolicy creates a new retention policy.
func (h *Handler) serveCreateRetentionPolicy(w http.ResponseWriter, r *http.Request) {}

// serveUpdateRetentionPolicy updates an existing retention policy.
func (h *Handler) serveUpdateRetentionPolicy(w http.ResponseWriter, r *http.Request) {}

// serveDeleteRetentionPolicy removes an existing retention policy.
func (h *Handler) serveDeleteRetentionPolicy(w http.ResponseWriter, r *http.Request) {}

// serveServers returns a list of servers in the cluster.
func (h *Handler) serveServers(w http.ResponseWriter, r *http.Request) {}

// serveDeleteServer removes a server from the cluster.
func (h *Handler) serveDeleteServer(w http.ResponseWriter, r *http.Request) {}

// error returns an error to the client in a standard format.
func (h *Handler) error(w http.ResponseWriter, error string, code int) {
	// TODO: Return error as JSON.
	http.Error(w, error, code)
}

/*

func (self *HTTPServer) dropSeries(w libhttp.ResponseWriter, r *libhttp.Request) {
	db := r.URL.Query().Get(":db")
	series := r.URL.Query().Get(":series")

	self.tryAsDbUserAndClusterAdmin(w, r, func(user User) (int, interface{}) {
		f := func(s *protocol.Series) error {
			return nil
		}
		seriesWriter := NewSeriesWriter(f)
		err := self.coordinator.RunQuery(user, db, fmt.Sprintf("drop series %s", series), seriesWriter)
		if err != nil {
			return errorToStatusCode(err), err.Error()
		}
		return libhttp.StatusNoContent, nil
	})
}

type Point struct {
	Timestamp      int64         `json:"timestamp"`
	SequenceNumber uint32        `json:"sequenceNumber"`
	Values         []interface{} `json:"values"`
}

// // cluster admins management interface

func toBytes(body interface{}, pretty bool) ([]byte, string, error) {
	if body == nil {
		return nil, "text/plain", nil
	}
	switch x := body.(type) {
	case string:
		return []byte(x), "text/plain", nil
	case []byte:
		return x, "text/plain", nil
	default:
		// only JSON output is prettied up.
		var b []byte
		var e error
		if pretty {
			b, e = json.MarshalIndent(body, "", "    ")
		} else {
			b, e = json.Marshal(body)
		}
		return b, "application/json", e
	}
}

func yieldUser(user User, yield func(User) (int, interface{}), pretty bool) (int, string, []byte) {
	statusCode, body := yield(user)
	bodyContent, contentType, err := toBytes(body, pretty)
	if err != nil {
		return libhttp.StatusInternalServerError, "text/plain", []byte(err.Error())
	}

	return statusCode, contentType, bodyContent
}

func getUsernameAndPassword(r *libhttp.Request) (string, string, error) {
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
		return "", "", fmt.Errorf("Bad auth header")
	}

	bs, err := base64.StdEncoding.DecodeString(fields[1])
	if err != nil {
		return "", "", fmt.Errorf("Bad encoding")
	}

	fields = strings.Split(string(bs), ":")
	if len(fields) != 2 {
		return "", "", fmt.Errorf("Bad auth value")
	}

	return fields[0], fields[1], nil
}

func (self *HTTPServer) tryAsClusterAdmin(w libhttp.ResponseWriter, r *libhttp.Request, yield func(User) (int, interface{})) {
	username, password, err := getUsernameAndPassword(r)
	if err != nil {
		w.WriteHeader(libhttp.StatusBadRequest)
		w.Write([]byte(err.Error()))
		return
	}

	if username == "" {
		w.Header().Add("WWW-Authenticate", "Basic realm=\"influxdb\"")
		w.Header().Add("Content-Type", "text/plain")
		w.WriteHeader(libhttp.StatusUnauthorized)
		w.Write([]byte("Invalid database/username/password"))
		return
	}

	user, err := self.userManager.AuthenticateClusterAdmin(username, password)
	if err != nil {
		w.Header().Add("WWW-Authenticate", "Basic realm=\"influxdb\"")
		w.Header().Add("Content-Type", "text/plain")
		w.WriteHeader(libhttp.StatusUnauthorized)
		w.Write([]byte(err.Error()))
		return
	}
	statusCode, contentType, body := yieldUser(user, yield, (r.URL.Query().Get("pretty") == "true"))
	if statusCode < 0 {
		return
	}

	if statusCode == libhttp.StatusUnauthorized {
		w.Header().Add("WWW-Authenticate", "Basic realm=\"influxdb\"")
	}
	w.Header().Add("content-type", contentType)
	w.WriteHeader(statusCode)
	if len(body) > 0 {
		w.Write(body)
	}
}

type NewUser struct {
	Name     string `json:"name"`
	Password string `json:"password"`
	IsAdmin  bool   `json:"isAdmin"`
	ReadFrom string `json:"readFrom"`
	WriteTo  string `json:"writeTo"`
}

type UpdateClusterAdminUser struct {
	Password string `json:"password"`
}

type ApiUser struct {
	Name string `json:"name"`
}

type UserDetail struct {
	Name     string `json:"name"`
	IsAdmin  bool   `json:"isAdmin"`
	WriteTo  string `json:"writeTo"`
	ReadFrom string `json:"readFrom"`
}

type ContinuousQuery struct {
	Id    int64  `json:"id"`
	Query string `json:"query"`
}

type NewContinuousQuery struct {
	Query string `json:"query"`
}

func (self *HTTPServer) listClusterAdmins(w libhttp.ResponseWriter, r *libhttp.Request) {
	self.tryAsClusterAdmin(w, r, func(u User) (int, interface{}) {
		names, err := self.userManager.ListClusterAdmins(u)
		if err != nil {
			return errorToStatusCode(err), err.Error()
		}
		users := make([]*ApiUser, 0, len(names))
		for _, name := range names {
			users = append(users, &ApiUser{name})
		}
		return libhttp.StatusOK, users
	})
}

func (self *HTTPServer) authenticateClusterAdmin(w libhttp.ResponseWriter, r *libhttp.Request) {
	self.tryAsClusterAdmin(w, r, func(u User) (int, interface{}) {
		return libhttp.StatusOK, nil
	})
}

func (self *HTTPServer) createClusterAdmin(w libhttp.ResponseWriter, r *libhttp.Request) {
	newUser := &NewUser{}
	decoder := json.NewDecoder(r.Body)
	err := decoder.Decode(newUser)
	if err != nil {
		w.WriteHeader(libhttp.StatusBadRequest)
		w.Write([]byte(err.Error()))
		return
	}

	self.tryAsClusterAdmin(w, r, func(u User) (int, interface{}) {
		username := newUser.Name
		if err := self.userManager.CreateClusterAdminUser(u, username, newUser.Password); err != nil {
			errorStr := err.Error()
			return errorToStatusCode(err), errorStr
		}
		return libhttp.StatusOK, nil
	})
}

func (self *HTTPServer) deleteClusterAdmin(w libhttp.ResponseWriter, r *libhttp.Request) {
	newUser := r.URL.Query().Get(":user")

	self.tryAsClusterAdmin(w, r, func(u User) (int, interface{}) {
		if err := self.userManager.DeleteClusterAdminUser(u, newUser); err != nil {
			return errorToStatusCode(err), err.Error()
		}
		return libhttp.StatusOK, nil
	})
}

func (self *HTTPServer) updateClusterAdmin(w libhttp.ResponseWriter, r *libhttp.Request) {
	updateClusterAdminUser := &UpdateClusterAdminUser{}
	decoder := json.NewDecoder(r.Body)
	err := decoder.Decode(updateClusterAdminUser)
	if err != nil {
		w.WriteHeader(libhttp.StatusInternalServerError)
		w.Write([]byte(err.Error()))
		return
	}

	newUser := r.URL.Query().Get(":user")

	self.tryAsClusterAdmin(w, r, func(u User) (int, interface{}) {
		if err := self.userManager.ChangeClusterAdminPassword(u, newUser, updateClusterAdminUser.Password); err != nil {
			return errorToStatusCode(err), err.Error()
		}
		return libhttp.StatusOK, nil
	})
}

// // db users management interface

func (self *HTTPServer) authenticateDbUser(w libhttp.ResponseWriter, r *libhttp.Request) {
	code, body := self.tryAsDbUser(w, r, func(u User) (int, interface{}) {
		return libhttp.StatusOK, nil
	})
	w.WriteHeader(code)
	if len(body) > 0 {
		w.Write(body)
	}
}

func (self *HTTPServer) tryAsDbUser(w libhttp.ResponseWriter, r *libhttp.Request, yield func(User) (int, interface{})) (int, []byte) {
	username, password, err := getUsernameAndPassword(r)
	if err != nil {
		return libhttp.StatusBadRequest, []byte(err.Error())
	}

	db := r.URL.Query().Get(":db")

	if username == "" {
		w.Header().Add("WWW-Authenticate", "Basic realm=\"influxdb\"")
		return libhttp.StatusUnauthorized, []byte("Invalid database/username/password")
	}

	user, err := self.userManager.AuthenticateDbUser(db, username, password)
	if err != nil {
		w.Header().Add("WWW-Authenticate", "Basic realm=\"influxdb\"")
		return libhttp.StatusUnauthorized, []byte(err.Error())
	}

	statusCode, contentType, v := yieldUser(user, yield, (r.URL.Query().Get("pretty") == "true"))
	if statusCode == libhttp.StatusUnauthorized {
		w.Header().Add("WWW-Authenticate", "Basic realm=\"influxdb\"")
	}
	w.Header().Add("content-type", contentType)
	return statusCode, v
}

func (self *HTTPServer) tryAsDbUserAndClusterAdmin(w libhttp.ResponseWriter, r *libhttp.Request, yield func(User) (int, interface{})) {
	log.Debug("Trying to auth as a db user")
	statusCode, body := self.tryAsDbUser(w, r, yield)
	if statusCode == libhttp.StatusUnauthorized {
		log.Debug("Authenticating as a db user failed with %s (%d)", string(body), statusCode)
		// tryAsDbUser will set this header, since we're retrying
		// we should delete the header and let tryAsClusterAdmin
		// set it properly
		w.Header().Del("WWW-Authenticate")
		self.tryAsClusterAdmin(w, r, yield)
		return
	}

	if statusCode < 0 {
		return
	}

	w.WriteHeader(statusCode)

	if len(body) > 0 {
		w.Write(body)
	}
	return
}

func (self *HTTPServer) listDbUsers(w libhttp.ResponseWriter, r *libhttp.Request) {
	db := r.URL.Query().Get(":db")

	self.tryAsDbUserAndClusterAdmin(w, r, func(u User) (int, interface{}) {
		dbUsers, err := self.userManager.ListDbUsers(u, db)
		if err != nil {
			return errorToStatusCode(err), err.Error()
		}

		users := make([]*UserDetail, 0, len(dbUsers))
		for _, dbUser := range dbUsers {
			users = append(users, &UserDetail{dbUser.GetName(), dbUser.IsDbAdmin(db), dbUser.GetWritePermission(), dbUser.GetReadPermission()})
		}
		return libhttp.StatusOK, users
	})
}

func (self *HTTPServer) showDbUser(w libhttp.ResponseWriter, r *libhttp.Request) {
	db := r.URL.Query().Get(":db")
	username := r.URL.Query().Get(":user")

	self.tryAsDbUserAndClusterAdmin(w, r, func(u User) (int, interface{}) {
		user, err := self.userManager.GetDbUser(u, db, username)
		if err != nil {
			return errorToStatusCode(err), err.Error()
		}

		userDetail := &UserDetail{user.GetName(), user.IsDbAdmin(db), user.GetWritePermission(), user.GetReadPermission()}

		return libhttp.StatusOK, userDetail
	})
}

func (self *HTTPServer) createDbUser(w libhttp.ResponseWriter, r *libhttp.Request) {
	newUser := &NewUser{}
	decoder := json.NewDecoder(r.Body)
	err := decoder.Decode(newUser)
	if err != nil {
		w.WriteHeader(libhttp.StatusBadRequest)
		w.Write([]byte(err.Error()))
		return
	}

	db := r.URL.Query().Get(":db")

	self.tryAsDbUserAndClusterAdmin(w, r, func(u User) (int, interface{}) {
		permissions := []string{}
		if newUser.ReadFrom != "" || newUser.WriteTo != "" {
			if newUser.ReadFrom == "" || newUser.WriteTo == "" {
				return libhttp.StatusBadRequest, "You have to provide read and write permissions"
			}
			permissions = append(permissions, newUser.ReadFrom, newUser.WriteTo)
		}

		username := newUser.Name
		if err := self.userManager.CreateDbUser(u, db, username, newUser.Password, permissions...); err != nil {
			log.Error("Cannot create user: %s", err)
			return errorToStatusCode(err), err.Error()
		}
		log.Debug("Created user %s", username)
		if newUser.IsAdmin {
			err = self.userManager.SetDbAdmin(u, db, newUser.Name, true)
			if err != nil {
				return libhttp.StatusInternalServerError, err.Error()
			}
		}
		log.Debug("Successfully changed %s password", username)
		return libhttp.StatusOK, nil
	})
}

func (self *HTTPServer) deleteDbUser(w libhttp.ResponseWriter, r *libhttp.Request) {
	newUser := r.URL.Query().Get(":user")
	db := r.URL.Query().Get(":db")

	self.tryAsDbUserAndClusterAdmin(w, r, func(u User) (int, interface{}) {
		if err := self.userManager.DeleteDbUser(u, db, newUser); err != nil {
			return errorToStatusCode(err), err.Error()
		}
		return libhttp.StatusOK, nil
	})
}

func (self *HTTPServer) updateDbUser(w libhttp.ResponseWriter, r *libhttp.Request) {
	updateUser := make(map[string]interface{})
	decoder := json.NewDecoder(r.Body)
	err := decoder.Decode(&updateUser)
	if err != nil {
		w.WriteHeader(libhttp.StatusBadRequest)
		w.Write([]byte(err.Error()))
		return
	}

	newUser := r.URL.Query().Get(":user")
	db := r.URL.Query().Get(":db")

	self.tryAsDbUserAndClusterAdmin(w, r, func(u User) (int, interface{}) {
		if pwd, ok := updateUser["password"]; ok {
			newPassword, ok := pwd.(string)
			if !ok {
				return libhttp.StatusBadRequest, "password must be string"
			}

			if err := self.userManager.ChangeDbUserPassword(u, db, newUser, newPassword); err != nil {
				return errorToStatusCode(err), err.Error()
			}
		}

		if readPermissions, ok := updateUser["readFrom"]; ok {
			writePermissions, ok := updateUser["writeTo"]
			if !ok {
				return libhttp.StatusBadRequest, "Changing permissions requires passing readFrom and writeTo"
			}

			if err := self.userManager.ChangeDbUserPermissions(u, db, newUser, readPermissions.(string), writePermissions.(string)); err != nil {
				return errorToStatusCode(err), err.Error()
			}
		}

		if admin, ok := updateUser["admin"]; ok {
			isAdmin, ok := admin.(bool)
			if !ok {
				return libhttp.StatusBadRequest, "admin must be boolean"
			}

			if err := self.userManager.SetDbAdmin(u, db, newUser, isAdmin); err != nil {
				return errorToStatusCode(err), err.Error()
			}
		}
		return libhttp.StatusOK, nil
	})
}

func (self *HTTPServer) ping(w libhttp.ResponseWriter, r *libhttp.Request) {
	w.WriteHeader(libhttp.StatusOK)
	w.Write([]byte("{\"status\":\"ok\"}"))
}

func (self *HTTPServer) listInterfaces(w libhttp.ResponseWriter, r *libhttp.Request) {
	statusCode, contentType, body := yieldUser(nil, func(u User) (int, interface{}) {
		entries, err := ioutil.ReadDir(filepath.Join(self.adminAssetsDir, "interfaces"))

		if err != nil {
			return errorToStatusCode(err), err.Error()
		}

		directories := make([]string, 0, len(entries))
		for _, entry := range entries {
			if entry.IsDir() {
				directories = append(directories, entry.Name())
			}
		}
		return libhttp.StatusOK, directories
	}, (r.URL.Query().Get("pretty") == "true"))

	w.Header().Add("content-type", contentType)
	w.WriteHeader(statusCode)
	if len(body) > 0 {
		w.Write(body)
	}
}

func (self *HTTPServer) listServers(w libhttp.ResponseWriter, r *libhttp.Request) {
	self.tryAsClusterAdmin(w, r, func(u User) (int, interface{}) {
		servers := self.clusterConfig.Servers()
		serverMaps := make([]map[string]interface{}, len(servers), len(servers))

		leaderRaftConnectString, _ := self.raftServer.GetLeaderRaftConnectString()
		leaderRaftName := self.raftServer.GetLeaderRaftName()
		for i, s := range servers {
			serverMaps[i] = map[string]interface{}{
				"id": s.Id,
				"protobufConnectString":   s.ProtobufConnectionString,
				"isUp":                    s.IsUp(), //FIXME: IsUp is not consistent
				"raftName":                s.RaftName,
				"raftConnectionString":    s.RaftConnectionString,
				"leaderRaftName":          leaderRaftName,
				"leaderRaftConnectString": leaderRaftConnectString,
				"isLeader":                self.raftServer.IsLeaderByRaftName(s.RaftName)}
		}
		return libhttp.StatusOK, serverMaps
	})
}

func (self *HTTPServer) removeServers(w libhttp.ResponseWriter, r *libhttp.Request) {
	self.tryAsClusterAdmin(w, r, func(u User) (int, interface{}) {
		id, err := strconv.ParseInt(r.URL.Query().Get(":id"), 10, 32)
		if err != nil {
			return errorToStatusCode(err), err.Error()
		}

		err = self.raftServer.RemoveServer(uint32(id))
		if err != nil {
			return errorToStatusCode(err), err.Error()
		}
		err = self.dropServerShards(uint32(id))
		if err != nil {
			return libhttp.StatusInternalServerError, err.Error()
		}
		return libhttp.StatusOK, nil
	})
}

func (self *HTTPServer) dropServerShards(serverId uint32) error {
	shards := self.clusterConfig.GetShards()
	for _, s := range shards {
		for _, si := range s.ServerIds() {
			if si == serverId {
				err := self.raftServer.DropShard(uint32(s.Id()), []uint32{serverId})
				if err != nil {
					return err
				}
			}
		}
	}
	return nil
}

type newShardInfo struct {
	StartTime int64               `json:"startTime"`
	EndTime   int64               `json:"endTime"`
	Shards    []newShardServerIds `json:"shards"`
	SpaceName string              `json:"spaceName"`
	Database  string              `json:"database"`
}

type newShardServerIds struct {
	ServerIds []uint32 `json:"serverIds"`
}

func (self *HTTPServer) createShard(w libhttp.ResponseWriter, r *libhttp.Request) {
	self.tryAsClusterAdmin(w, r, func(u User) (int, interface{}) {
		newShards := &newShardInfo{}
		decoder := json.NewDecoder(r.Body)
		err := decoder.Decode(&newShards)
		if err != nil {
			return libhttp.StatusInternalServerError, err.Error()
		}
		shards := make([]*cluster.NewShardData, 0)

		for _, s := range newShards.Shards {
			newShardData := &cluster.NewShardData{
				StartTime: time.Unix(newShards.StartTime, 0),
				EndTime:   time.Unix(newShards.EndTime, 0),
				ServerIds: s.ServerIds,
				SpaceName: newShards.SpaceName,
				Database:  newShards.Database,
			}
			shards = append(shards, newShardData)
		}
		_, err = self.raftServer.CreateShards(shards)
		if err != nil {
			return libhttp.StatusInternalServerError, err.Error()
		}
		return libhttp.StatusAccepted, nil
	})
}

func (self *HTTPServer) getShards(w libhttp.ResponseWriter, r *libhttp.Request) {
	self.tryAsClusterAdmin(w, r, func(u User) (int, interface{}) {
		shards := self.clusterConfig.GetShards()
		shardMaps := make([]map[string]interface{}, 0, len(shards))
		for _, s := range shards {
			shardMaps = append(shardMaps, map[string]interface{}{
				"id":        s.Id(),
				"endTime":   s.EndTime().Unix(),
				"startTime": s.StartTime().Unix(),
				"serverIds": s.ServerIds(),
				"spaceName": s.SpaceName,
				"database":  s.Database})
		}
		return libhttp.StatusOK, shardMaps
	})
}

func (self *HTTPServer) dropShard(w libhttp.ResponseWriter, r *libhttp.Request) {
	self.tryAsClusterAdmin(w, r, func(u User) (int, interface{}) {
		id, err := strconv.ParseInt(r.URL.Query().Get(":id"), 10, 64)
		if err != nil {
			return libhttp.StatusInternalServerError, err.Error()
		}
		serverIdInfo := &newShardServerIds{}
		decoder := json.NewDecoder(r.Body)
		err = decoder.Decode(&serverIdInfo)
		if err != nil {
			return libhttp.StatusInternalServerError, err.Error()
		}
		if len(serverIdInfo.ServerIds) < 1 {
			return libhttp.StatusBadRequest, errors.New("Request must include an object with an array of 'serverIds'").Error()
		}

		err = self.raftServer.DropShard(uint32(id), serverIdInfo.ServerIds)
		if err != nil {
			return libhttp.StatusInternalServerError, err.Error()
		}
		return libhttp.StatusAccepted, nil
	})
}

func (self *HTTPServer) convertShardsToMap(shards []*cluster.ShardData) []interface{} {
	result := make([]interface{}, 0)
	for _, shard := range shards {
		s := make(map[string]interface{})
		s["id"] = shard.Id()
		s["startTime"] = shard.StartTime().Unix()
		s["endTime"] = shard.EndTime().Unix()
		s["serverIds"] = shard.ServerIds()
		result = append(result, s)
	}
	return result
}

func (self *HTTPServer) getRetentionPolicys(w libhttp.ResponseWriter, r *libhttp.Request) {
	self.tryAsClusterAdmin(w, r, func(u User) (int, interface{}) {
		return libhttp.StatusOK, self.clusterConfig.GetRetentionPolicys()
	})
}

func (self *HTTPServer) createRetentionPolicy(w libhttp.ResponseWriter, r *libhttp.Request) {
	self.tryAsClusterAdmin(w, r, func(u User) (int, interface{}) {
		space := &cluster.RetentionPolicy{}
		decoder := json.NewDecoder(r.Body)
		err := decoder.Decode(space)
		if err != nil {
			return libhttp.StatusInternalServerError, err.Error()
		}
		space.Database = r.URL.Query().Get(":db")
		err = space.Validate(self.clusterConfig, true)
		if err != nil {
			return libhttp.StatusBadRequest, err.Error()
		}
		err = self.raftServer.CreateRetentionPolicy(space)
		if err != nil {
			return libhttp.StatusInternalServerError, err.Error()
		}
		return libhttp.StatusOK, nil
	})
}

func (self *HTTPServer) dropRetentionPolicy(w libhttp.ResponseWriter, r *libhttp.Request) {
	self.tryAsClusterAdmin(w, r, func(u User) (int, interface{}) {
		name := r.URL.Query().Get(":name")
		db := r.URL.Query().Get(":db")
		if err := self.raftServer.DropRetentionPolicy(db, name); err != nil {
			return libhttp.StatusInternalServerError, err.Error()
		}
		return libhttp.StatusOK, nil
	})
}

type DatabaseConfig struct {
	Spaces            []*cluster.RetentionPolicy `json:"spaces"`
	ContinuousQueries []string              `json:"continuousQueries"`
}

func (self *HTTPServer) configureDatabase(w libhttp.ResponseWriter, r *libhttp.Request) {
	self.tryAsClusterAdmin(w, r, func(u User) (int, interface{}) {
		databaseConfig := &DatabaseConfig{}
		decoder := json.NewDecoder(r.Body)
		err := decoder.Decode(databaseConfig)
		if err != nil {
			return libhttp.StatusInternalServerError, err.Error()
		}
		database := r.URL.Query().Get(":db")

		// validate before creating anything
		for _, queryString := range databaseConfig.ContinuousQueries {
			q, err := parser.ParseQuery(queryString)
			if err != nil {
				return libhttp.StatusBadRequest, err.Error()
			}
			for _, query := range q {
				if !query.IsContinuousQuery() {
					return libhttp.StatusBadRequest, fmt.Errorf("This query isn't a continuous query. Use 'into'. %s", query.GetQueryString())
				}
			}
		}

		// validate retention policys
		for _, space := range databaseConfig.Spaces {
			err := space.Validate(self.clusterConfig, false)
			if err != nil {
				return libhttp.StatusBadRequest, err.Error()
			}
		}

		err = self.coordinator.CreateDatabase(u, database)
		if err != nil {
			return libhttp.StatusBadRequest, err.Error()
		}
		for _, space := range databaseConfig.Spaces {
			space.Database = database
			err = self.raftServer.CreateRetentionPolicy(space)
			if err != nil {
				return libhttp.StatusInternalServerError, err.Error()
			}
		}
		for _, queryString := range databaseConfig.ContinuousQueries {
			err := self.coordinator.RunQuery(u, database, queryString, cluster.NilProcessor{})
			if err != nil {
				return libhttp.StatusInternalServerError, err.Error()
			}
		}
		return libhttp.StatusCreated, nil
	})
}

func (self *HTTPServer) updateRetentionPolicy(w libhttp.ResponseWriter, r *libhttp.Request) {
	self.tryAsClusterAdmin(w, r, func(u User) (int, interface{}) {
		space := &cluster.RetentionPolicy{}
		decoder := json.NewDecoder(r.Body)
		err := decoder.Decode(space)
		if err != nil {
			return libhttp.StatusInternalServerError, err.Error()
		}
		space.Database = r.URL.Query().Get(":db")
		space.Name = r.URL.Query().Get(":name")
		if !self.clusterConfig.DatabaseExists(space.Database) {
			return libhttp.StatusNotAcceptable, "Can't update a retention policy for a database that doesn't exist"
		}
		if !self.clusterConfig.RetentionPolicyExists(space) {
			return libhttp.StatusNotAcceptable, "Can't update a retention policy that doesn't exist"
		}

		if err := self.raftServer.UpdateRetentionPolicy(space); err != nil {
			return libhttp.StatusInternalServerError, err.Error()
		}
		return libhttp.StatusOK, nil
	})
}

func (self *HTTPServer) getClusterConfiguration(w libhttp.ResponseWriter, r *libhttp.Request) {
	self.tryAsClusterAdmin(w, r, func(u User) (int, interface{}) {
		return libhttp.StatusOK, self.clusterConfig.SerializableConfiguration()
	})
}

func HeaderHandler(handler libhttp.HandlerFunc, version string) libhttp.HandlerFunc {
	return func(rw libhttp.ResponseWriter, req *libhttp.Request) {
		rw.Header().Add("Access-Control-Allow-Origin", "*")
		rw.Header().Add("Access-Control-Max-Age", "2592000")
		rw.Header().Add("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE")
		rw.Header().Add("Access-Control-Allow-Headers", "Origin, X-Requested-With, Content-Type, Accept")
		rw.Header().Add("X-Influxdb-Version", version)
		handler(rw, req)
	}
}

func CompressionHeaderHandler(handler libhttp.HandlerFunc, version string) libhttp.HandlerFunc {
	return HeaderHandler(CompressionHandler(true, handler), version)
}

type Flusher interface {
	Flush() error
}

type CompressedResponseWriter struct {
	responseWriter     libhttp.ResponseWriter
	writer             io.Writer
	compressionFlusher Flusher
	responseFlusher    libhttp.Flusher
}

func NewCompressionResponseWriter(useCompression bool, rw libhttp.ResponseWriter, req *libhttp.Request) *CompressedResponseWriter {
	responseFlusher, _ := rw.(libhttp.Flusher)

	if req.Header.Get("Accept-Encoding") != "" {
		encodings := strings.Split(req.Header.Get("Accept-Encoding"), ",")

		for _, val := range encodings {
			if val == "gzip" {
				rw.Header().Set("Content-Encoding", "gzip")
				w, _ := gzip.NewWriterLevel(rw, gzip.BestSpeed)
				return &CompressedResponseWriter{rw, w, w, responseFlusher}
			} else if val == "deflate" {
				rw.Header().Set("Content-Encoding", "deflate")
				w, _ := zlib.NewWriterLevel(rw, zlib.BestSpeed)
				return &CompressedResponseWriter{rw, w, w, responseFlusher}
			}
		}
	}

	return &CompressedResponseWriter{rw, rw, nil, responseFlusher}
}

func (self *CompressedResponseWriter) Header() libhttp.Header {
	return self.responseWriter.Header()
}

func (self *CompressedResponseWriter) Write(bs []byte) (int, error) {
	return self.writer.Write(bs)
}

func (self *CompressedResponseWriter) Flush() {
	if self.compressionFlusher != nil {
		self.compressionFlusher.Flush()
	}

	if self.responseFlusher != nil {
		self.responseFlusher.Flush()
	}
}

func (self *CompressedResponseWriter) WriteHeader(responseCode int) {
	self.responseWriter.WriteHeader(responseCode)
}

func CompressionHandler(enableCompression bool, handler libhttp.HandlerFunc) libhttp.HandlerFunc {
	if !enableCompression {
		return handler
	}

	return func(rw libhttp.ResponseWriter, req *libhttp.Request) {
		crw := NewCompressionResponseWriter(true, rw, req)
		handler(crw, req)
		switch x := crw.writer.(type) {
		case *gzip.Writer:
			x.Close()
		case *zlib.Writer:
			x.Close()
		}
	}
}

*/

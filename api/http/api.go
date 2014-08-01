package http

import (
	"bytes"
	"compress/gzip"
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net"
	libhttp "net/http"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	log "code.google.com/p/log4go"
	"github.com/bmizerany/pat"
	"github.com/influxdb/influxdb/cluster"
	. "github.com/influxdb/influxdb/common"
	"github.com/influxdb/influxdb/coordinator"
	"github.com/influxdb/influxdb/parser"
	"github.com/influxdb/influxdb/protocol"
)

type HttpServer struct {
	conn           net.Listener
	sslConn        net.Listener
	httpPort       string
	httpSslPort    string
	httpSslCert    string
	adminAssetsDir string
	coordinator    coordinator.Coordinator
	userManager    UserManager
	shutdown       chan bool
	clusterConfig  *cluster.ClusterConfiguration
	raftServer     *coordinator.RaftServer
	readTimeout    time.Duration
}

func NewHttpServer(httpPort string, readTimeout time.Duration, adminAssetsDir string, theCoordinator coordinator.Coordinator, userManager UserManager, clusterConfig *cluster.ClusterConfiguration, raftServer *coordinator.RaftServer) *HttpServer {
	self := &HttpServer{}
	self.httpPort = httpPort
	self.adminAssetsDir = adminAssetsDir
	self.coordinator = theCoordinator
	self.userManager = userManager
	self.shutdown = make(chan bool, 2)
	self.clusterConfig = clusterConfig
	self.raftServer = raftServer
	self.readTimeout = readTimeout
	return self
}

const (
	INVALID_CREDENTIALS_MSG  = "Invalid database/username/password"
	JSON_PRETTY_PRINT_INDENT = "    "
)

func isPretty(r *libhttp.Request) bool {
	return r.URL.Query().Get("pretty") == "true"
}

func (self *HttpServer) EnableSsl(addr, certPath string) {
	if addr == "" || certPath == "" {
		// don't enable ssl unless both the address and the certificate
		// path aren't empty
		log.Info("Ssl will be disabled since the ssl port or certificate path weren't set")
		return
	}

	self.httpSslPort = addr
	self.httpSslCert = certPath
	return
}

func (self *HttpServer) ListenAndServe() {
	var err error
	if self.httpPort != "" {
		self.conn, err = net.Listen("tcp", self.httpPort)
		if err != nil {
			log.Error("Listen: ", err)
		}
	}
	self.Serve(self.conn)
}

func (self *HttpServer) registerEndpoint(p *pat.PatternServeMux, method string, pattern string, f libhttp.HandlerFunc) {
	version := self.clusterConfig.GetLocalConfiguration().Version
	switch method {
	case "get":
		p.Get(pattern, CompressionHeaderHandler(f, version))
	case "post":
		p.Post(pattern, HeaderHandler(f, version))
	case "del":
		p.Del(pattern, HeaderHandler(f, version))
	}
	p.Options(pattern, HeaderHandler(self.sendCrossOriginHeader, version))
}

func (self *HttpServer) Serve(listener net.Listener) {
	defer func() { self.shutdown <- true }()

	self.conn = listener
	p := pat.New()

	// Run the given query and return an array of series or a chunked response
	// with each batch of points we get back
	self.registerEndpoint(p, "get", "/db/:db/series", self.query)

	// Write points to the given database
	self.registerEndpoint(p, "post", "/db/:db/series", self.writePoints)
	self.registerEndpoint(p, "del", "/db/:db/series/:series", self.dropSeries)
	self.registerEndpoint(p, "get", "/db", self.listDatabases)
	self.registerEndpoint(p, "post", "/db", self.createDatabase)
	self.registerEndpoint(p, "del", "/db/:name", self.dropDatabase)

	// cluster admins management interface
	self.registerEndpoint(p, "get", "/cluster_admins", self.listClusterAdmins)
	self.registerEndpoint(p, "get", "/cluster_admins/authenticate", self.authenticateClusterAdmin)
	self.registerEndpoint(p, "post", "/cluster_admins", self.createClusterAdmin)
	self.registerEndpoint(p, "post", "/cluster_admins/:user", self.updateClusterAdmin)
	self.registerEndpoint(p, "del", "/cluster_admins/:user", self.deleteClusterAdmin)

	// db users management interface
	self.registerEndpoint(p, "get", "/db/:db/authenticate", self.authenticateDbUser)
	self.registerEndpoint(p, "get", "/db/:db/users", self.listDbUsers)
	self.registerEndpoint(p, "post", "/db/:db/users", self.createDbUser)
	self.registerEndpoint(p, "get", "/db/:db/users/:user", self.showDbUser)
	self.registerEndpoint(p, "del", "/db/:db/users/:user", self.deleteDbUser)
	self.registerEndpoint(p, "post", "/db/:db/users/:user", self.updateDbUser)

	// continuous queries management interface
	self.registerEndpoint(p, "get", "/db/:db/continuous_queries", self.listDbContinuousQueries)
	self.registerEndpoint(p, "post", "/db/:db/continuous_queries", self.createDbContinuousQueries)
	self.registerEndpoint(p, "del", "/db/:db/continuous_queries/:id", self.deleteDbContinuousQueries)

	// healthcheck
	self.registerEndpoint(p, "get", "/ping", self.ping)

	// force a raft log compaction
	self.registerEndpoint(p, "post", "/raft/force_compaction", self.forceRaftCompaction)

	// fetch current list of available interfaces
	self.registerEndpoint(p, "get", "/interfaces", self.listInterfaces)

	// cluster config endpoints
	self.registerEndpoint(p, "get", "/cluster/servers", self.listServers)
	self.registerEndpoint(p, "del", "/cluster/servers/:id", self.removeServers)
	self.registerEndpoint(p, "post", "/cluster/shards", self.createShard)
	self.registerEndpoint(p, "get", "/cluster/shards", self.getShards)
	self.registerEndpoint(p, "del", "/cluster/shards/:id", self.dropShard)
	self.registerEndpoint(p, "get", "/cluster/shard_spaces", self.getShardSpaces)
	self.registerEndpoint(p, "post", "/cluster/shard_spaces", self.createShardSpace)
	self.registerEndpoint(p, "del", "/cluster/shard_spaces/:db/:name", self.dropShardSpace)
	self.registerEndpoint(p, "post", "/cluster/database_configs/:db", self.configureDatabase)

	// return whether the cluster is in sync or not
	self.registerEndpoint(p, "get", "/sync", self.isInSync)

	if listener == nil {
		self.startSsl(p)
		return
	}

	go self.startSsl(p)
	self.serveListener(listener, p)
}

func (self *HttpServer) startSsl(p *pat.PatternServeMux) {
	defer func() { self.shutdown <- true }()

	// return if the ssl port or cert weren't set
	if self.httpSslPort == "" || self.httpSslCert == "" {
		return
	}

	log.Info("Starting SSL api on port %s using certificate in %s", self.httpSslPort, self.httpSslCert)

	cert, err := tls.LoadX509KeyPair(self.httpSslCert, self.httpSslCert)
	if err != nil {
		panic(err)
	}

	self.sslConn, err = tls.Listen("tcp", self.httpSslPort, &tls.Config{
		Certificates: []tls.Certificate{cert},
	})
	if err != nil {
		panic(err)
	}

	self.serveListener(self.sslConn, p)
}

func (self *HttpServer) serveListener(listener net.Listener, p *pat.PatternServeMux) {
	srv := &libhttp.Server{Handler: p, ReadTimeout: self.readTimeout}
	if err := srv.Serve(listener); err != nil && !strings.Contains(err.Error(), "closed network") {
		panic(err)
	}
}

func (self *HttpServer) Close() {
	if self.conn != nil {
		log.Info("Closing http server")
		self.conn.Close()
		log.Info("Waiting for all requests to finish before killing the process")
		select {
		case <-time.After(time.Second * 5):
			log.Error("There seems to be a hanging request. Closing anyway")
		case <-self.shutdown:
		}
	}
}

type Writer interface {
	yield(*protocol.Series) error
	done()
}

type AllPointsWriter struct {
	memSeries map[string]*protocol.Series
	w         libhttp.ResponseWriter
	precision TimePrecision
	pretty    bool
}

func (self *AllPointsWriter) yield(series *protocol.Series) error {
	oldSeries := self.memSeries[*series.Name]
	if oldSeries == nil {
		self.memSeries[*series.Name] = series
		return nil
	}

	self.memSeries[series.GetName()] = MergeSeries(self.memSeries[series.GetName()], series)
	return nil
}

func (self *AllPointsWriter) done() {
	data, err := serializeMultipleSeries(self.memSeries, self.precision, self.pretty)
	if err != nil {
		self.w.WriteHeader(libhttp.StatusInternalServerError)
		self.w.Write([]byte(err.Error()))
		return
	}
	self.w.Header().Add("content-type", "application/json")
	self.w.WriteHeader(libhttp.StatusOK)
	self.w.Write(data)
}

type ChunkWriter struct {
	w                libhttp.ResponseWriter
	precision        TimePrecision
	wroteContentType bool
	pretty           bool
}

func (self *ChunkWriter) yield(series *protocol.Series) error {
	data, err := serializeSingleSeries(series, self.precision, self.pretty)
	if err != nil {
		return err
	}
	if !self.wroteContentType {
		self.wroteContentType = true
		self.w.Header().Add("content-type", "application/json")
	}
	self.w.WriteHeader(libhttp.StatusOK)
	self.w.Write(data)
	self.w.(libhttp.Flusher).Flush()
	return nil
}

func (self *ChunkWriter) done() {
}

func TimePrecisionFromString(s string) (TimePrecision, error) {
	switch s {
	case "u":
		return MicrosecondPrecision, nil
	case "m":
		log.Warn("time_precision=m will be disabled in future release, use time_precision=ms instead")
		fallthrough
	case "ms":
		return MillisecondPrecision, nil
	case "s":
		return SecondPrecision, nil
	case "":
		return MillisecondPrecision, nil
	}

	return 0, fmt.Errorf("Unknown time precision %s", s)
}

func (self *HttpServer) forceRaftCompaction(w libhttp.ResponseWriter, r *libhttp.Request) {
	self.tryAsClusterAdmin(w, r, func(user User) (int, interface{}) {
		self.coordinator.ForceCompaction(user)
		return libhttp.StatusOK, "OK"
	})
}

func (self *HttpServer) sendCrossOriginHeader(w libhttp.ResponseWriter, r *libhttp.Request) {
	w.WriteHeader(libhttp.StatusOK)
}

func (self *HttpServer) query(w libhttp.ResponseWriter, r *libhttp.Request) {
	query := r.URL.Query().Get("q")
	db := r.URL.Query().Get(":db")
	pretty := isPretty(r)

	self.tryAsDbUserAndClusterAdmin(w, r, func(user User) (int, interface{}) {

		precision, err := TimePrecisionFromString(r.URL.Query().Get("time_precision"))
		if err != nil {
			return libhttp.StatusBadRequest, err.Error()
		}

		var writer Writer
		if r.URL.Query().Get("chunked") == "true" {
			writer = &ChunkWriter{w, precision, false, pretty}
		} else {
			writer = &AllPointsWriter{map[string]*protocol.Series{}, w, precision, pretty}
		}
		seriesWriter := NewSeriesWriter(writer.yield)
		err = self.coordinator.RunQuery(user, db, query, seriesWriter)
		if err != nil {
			if e, ok := err.(*parser.QueryError); ok {
				return errorToStatusCode(err), e.PrettyPrint()
			}
			return errorToStatusCode(err), err.Error()
		}

		writer.done()
		return -1, nil
	})
}

func errorToStatusCode(err error) int {
	switch err.(type) {
	case AuthenticationError:
		return libhttp.StatusUnauthorized // HTTP 401
	case AuthorizationError:
		return libhttp.StatusForbidden // HTTP 403
	case DatabaseExistsError:
		return libhttp.StatusConflict // HTTP 409
	default:
		return libhttp.StatusBadRequest // HTTP 400
	}
}

func (self *HttpServer) writePoints(w libhttp.ResponseWriter, r *libhttp.Request) {
	db := r.URL.Query().Get(":db")
	precision, err := TimePrecisionFromString(r.URL.Query().Get("time_precision"))
	if err != nil {
		w.WriteHeader(libhttp.StatusBadRequest)
		w.Write([]byte(err.Error()))
		return
	}

	self.tryAsDbUserAndClusterAdmin(w, r, func(user User) (int, interface{}) {
		reader := r.Body
		encoding := r.Header.Get("Content-Encoding")
		switch encoding {
		case "gzip":
			reader, err = gzip.NewReader(r.Body)
			if err != nil {
				return libhttp.StatusInternalServerError, err.Error()
			}
		default:
			// assume it's plain text
		}

		series, err := ioutil.ReadAll(reader)
		if err != nil {
			return libhttp.StatusInternalServerError, err.Error()
		}
		decoder := json.NewDecoder(bytes.NewBuffer(series))
		decoder.UseNumber()
		serializedSeries := []*SerializedSeries{}
		err = decoder.Decode(&serializedSeries)
		if err != nil {
			return libhttp.StatusBadRequest, err.Error()
		}

		// convert the wire format to the internal representation of the time series
		dataStoreSeries := make([]*protocol.Series, 0, len(serializedSeries))
		for _, s := range serializedSeries {
			if len(s.Points) == 0 {
				continue
			}

			series, err := ConvertToDataStoreSeries(s, precision)
			if err != nil {
				return libhttp.StatusBadRequest, err.Error()
			}

			dataStoreSeries = append(dataStoreSeries, series)
		}

		err = self.coordinator.WriteSeriesData(user, db, dataStoreSeries)

		if err != nil {
			return errorToStatusCode(err), err.Error()
		}

		return libhttp.StatusOK, nil
	})
}

type createDatabaseRequest struct {
	Name string `json:"name"`
}

func (self *HttpServer) listDatabases(w libhttp.ResponseWriter, r *libhttp.Request) {
	self.tryAsClusterAdmin(w, r, func(u User) (int, interface{}) {
		databases, err := self.coordinator.ListDatabases(u)
		if err != nil {
			return errorToStatusCode(err), err.Error()
		}
		return libhttp.StatusOK, databases
	})
}

func (self *HttpServer) createDatabase(w libhttp.ResponseWriter, r *libhttp.Request) {
	self.tryAsClusterAdmin(w, r, func(user User) (int, interface{}) {
		createRequest := &createDatabaseRequest{}
		decoder := json.NewDecoder(r.Body)
		err := decoder.Decode(createRequest)
		if err != nil {
			return libhttp.StatusBadRequest, err.Error()
		}
		err = self.coordinator.CreateDatabase(user, createRequest.Name)
		if err != nil {
			log.Error("Cannot create database %s. Error: %s", createRequest.Name, err)
			return errorToStatusCode(err), err.Error()
		}
		log.Debug("Created database %s", createRequest.Name)
		return libhttp.StatusCreated, nil
	})
}

func (self *HttpServer) dropDatabase(w libhttp.ResponseWriter, r *libhttp.Request) {
	self.tryAsClusterAdmin(w, r, func(user User) (int, interface{}) {
		name := r.URL.Query().Get(":name")
		err := self.coordinator.DropDatabase(user, name)
		if err != nil {
			return errorToStatusCode(err), err.Error()
		}
		return libhttp.StatusNoContent, nil
	})
}

func (self *HttpServer) dropSeries(w libhttp.ResponseWriter, r *libhttp.Request) {
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

func serializeSingleSeries(series *protocol.Series, precision TimePrecision, pretty bool) ([]byte, error) {
	arg := map[string]*protocol.Series{"": series}
	if pretty {
		return json.MarshalIndent(SerializeSeries(arg, precision)[0], "", JSON_PRETTY_PRINT_INDENT)
	} else {
		return json.Marshal(SerializeSeries(arg, precision)[0])
	}
}

func serializeMultipleSeries(series map[string]*protocol.Series, precision TimePrecision, pretty bool) ([]byte, error) {
	if pretty {
		return json.MarshalIndent(SerializeSeries(series, precision), "", JSON_PRETTY_PRINT_INDENT)
	} else {
		return json.Marshal(SerializeSeries(series, precision))
	}
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
			b, e = json.MarshalIndent(body, "", JSON_PRETTY_PRINT_INDENT)
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

func (self *HttpServer) tryAsClusterAdmin(w libhttp.ResponseWriter, r *libhttp.Request, yield func(User) (int, interface{})) {
	username, password, err := getUsernameAndPassword(r)
	if err != nil {
		w.WriteHeader(libhttp.StatusBadRequest)
		w.Write([]byte(err.Error()))
		return
	}

	if username == "" {
		w.Header().Add("WWW-Authenticate", "Basic realm=\"influxdb\"")
		w.WriteHeader(libhttp.StatusUnauthorized)
		w.Write([]byte(INVALID_CREDENTIALS_MSG))
		return
	}

	user, err := self.userManager.AuthenticateClusterAdmin(username, password)
	if err != nil {
		w.Header().Add("WWW-Authenticate", "Basic realm=\"influxdb\"")
		w.WriteHeader(libhttp.StatusUnauthorized)
		w.Write([]byte(err.Error()))
		return
	}
	statusCode, contentType, body := yieldUser(user, yield, isPretty(r))
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

func (self *HttpServer) listClusterAdmins(w libhttp.ResponseWriter, r *libhttp.Request) {
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

func (self *HttpServer) authenticateClusterAdmin(w libhttp.ResponseWriter, r *libhttp.Request) {
	self.tryAsClusterAdmin(w, r, func(u User) (int, interface{}) {
		return libhttp.StatusOK, nil
	})
}

func (self *HttpServer) createClusterAdmin(w libhttp.ResponseWriter, r *libhttp.Request) {
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

func (self *HttpServer) deleteClusterAdmin(w libhttp.ResponseWriter, r *libhttp.Request) {
	newUser := r.URL.Query().Get(":user")

	self.tryAsClusterAdmin(w, r, func(u User) (int, interface{}) {
		if err := self.userManager.DeleteClusterAdminUser(u, newUser); err != nil {
			return errorToStatusCode(err), err.Error()
		}
		return libhttp.StatusOK, nil
	})
}

func (self *HttpServer) updateClusterAdmin(w libhttp.ResponseWriter, r *libhttp.Request) {
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

func (self *HttpServer) authenticateDbUser(w libhttp.ResponseWriter, r *libhttp.Request) {
	code, body := self.tryAsDbUser(w, r, func(u User) (int, interface{}) {
		return libhttp.StatusOK, nil
	})
	w.WriteHeader(code)
	if len(body) > 0 {
		w.Write(body)
	}
}

func (self *HttpServer) tryAsDbUser(w libhttp.ResponseWriter, r *libhttp.Request, yield func(User) (int, interface{})) (int, []byte) {
	username, password, err := getUsernameAndPassword(r)
	if err != nil {
		return libhttp.StatusBadRequest, []byte(err.Error())
	}

	db := r.URL.Query().Get(":db")

	if username == "" {
		w.Header().Add("WWW-Authenticate", "Basic realm=\"influxdb\"")
		return libhttp.StatusUnauthorized, []byte(INVALID_CREDENTIALS_MSG)
	}

	user, err := self.userManager.AuthenticateDbUser(db, username, password)
	if err != nil {
		w.Header().Add("WWW-Authenticate", "Basic realm=\"influxdb\"")
		return libhttp.StatusUnauthorized, []byte(err.Error())
	}

	statusCode, contentType, v := yieldUser(user, yield, isPretty(r))
	if statusCode == libhttp.StatusUnauthorized {
		w.Header().Add("WWW-Authenticate", "Basic realm=\"influxdb\"")
	}
	w.Header().Add("content-type", contentType)
	return statusCode, v
}

func (self *HttpServer) tryAsDbUserAndClusterAdmin(w libhttp.ResponseWriter, r *libhttp.Request, yield func(User) (int, interface{})) {
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

func (self *HttpServer) listDbUsers(w libhttp.ResponseWriter, r *libhttp.Request) {
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

func (self *HttpServer) showDbUser(w libhttp.ResponseWriter, r *libhttp.Request) {
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

func (self *HttpServer) createDbUser(w libhttp.ResponseWriter, r *libhttp.Request) {
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

func (self *HttpServer) deleteDbUser(w libhttp.ResponseWriter, r *libhttp.Request) {
	newUser := r.URL.Query().Get(":user")
	db := r.URL.Query().Get(":db")

	self.tryAsDbUserAndClusterAdmin(w, r, func(u User) (int, interface{}) {
		if err := self.userManager.DeleteDbUser(u, db, newUser); err != nil {
			return errorToStatusCode(err), err.Error()
		}
		return libhttp.StatusOK, nil
	})
}

func (self *HttpServer) updateDbUser(w libhttp.ResponseWriter, r *libhttp.Request) {
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

func (self *HttpServer) ping(w libhttp.ResponseWriter, r *libhttp.Request) {
	w.WriteHeader(libhttp.StatusOK)
	w.Write([]byte("{\"status\":\"ok\"}"))
}

func (self *HttpServer) listInterfaces(w libhttp.ResponseWriter, r *libhttp.Request) {
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
	}, isPretty(r))

	w.Header().Add("content-type", contentType)
	w.WriteHeader(statusCode)
	if len(body) > 0 {
		w.Write(body)
	}
}

func (self *HttpServer) listDbContinuousQueries(w libhttp.ResponseWriter, r *libhttp.Request) {
	db := r.URL.Query().Get(":db")

	self.tryAsDbUserAndClusterAdmin(w, r, func(u User) (int, interface{}) {
		series, err := self.coordinator.ListContinuousQueries(u, db)
		if err != nil {
			return errorToStatusCode(err), err.Error()
		}

		queries := make([]ContinuousQuery, 0, len(series[0].Points))

		for _, point := range series[0].Points {
			queries = append(queries, ContinuousQuery{Id: *point.Values[0].Int64Value, Query: *point.Values[1].StringValue})
		}

		return libhttp.StatusOK, queries
	})
}

func (self *HttpServer) createDbContinuousQueries(w libhttp.ResponseWriter, r *libhttp.Request) {
	db := r.URL.Query().Get(":db")

	self.tryAsDbUserAndClusterAdmin(w, r, func(u User) (int, interface{}) {
		// note: id isn't used when creating a new continuous query
		values := &ContinuousQuery{}
		decoder := json.NewDecoder(r.Body)
		err := decoder.Decode(values)
		if err != nil {
			return libhttp.StatusInternalServerError, err.Error()
		}

		if err := self.coordinator.CreateContinuousQuery(u, db, values.Query); err != nil {
			return errorToStatusCode(err), err.Error()
		}
		return libhttp.StatusOK, nil
	})
}

func (self *HttpServer) deleteDbContinuousQueries(w libhttp.ResponseWriter, r *libhttp.Request) {
	db := r.URL.Query().Get(":db")
	id, _ := strconv.ParseInt(r.URL.Query().Get(":id"), 10, 64)

	self.tryAsDbUserAndClusterAdmin(w, r, func(u User) (int, interface{}) {
		if err := self.coordinator.DeleteContinuousQuery(u, db, uint32(id)); err != nil {
			return errorToStatusCode(err), err.Error()
		}
		return libhttp.StatusOK, nil
	})
}

func (self *HttpServer) listServers(w libhttp.ResponseWriter, r *libhttp.Request) {
	self.tryAsClusterAdmin(w, r, func(u User) (int, interface{}) {
		servers := self.clusterConfig.Servers()
		serverMaps := make([]map[string]interface{}, len(servers), len(servers))
		for i, s := range servers {
			serverMaps[i] = map[string]interface{}{"id": s.Id, "protobufConnectString": s.ProtobufConnectionString}
		}
		return libhttp.StatusOK, serverMaps
	})
}

func (self *HttpServer) removeServers(w libhttp.ResponseWriter, r *libhttp.Request) {
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

func (self *HttpServer) dropServerShards(serverId uint32) error {
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

func (self *HttpServer) createShard(w libhttp.ResponseWriter, r *libhttp.Request) {
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

func (self *HttpServer) getShards(w libhttp.ResponseWriter, r *libhttp.Request) {
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

// Note: this is meant for testing purposes only and doesn't guarantee
// data integrity and shouldn't be used in client code.
func (self *HttpServer) isInSync(w libhttp.ResponseWriter, r *libhttp.Request) {
	self.tryAsClusterAdmin(w, r, func(u User) (int, interface{}) {
		if self.clusterConfig.HasUncommitedWrites() {
			return 500, "false"
		}

		if !self.raftServer.CommittedAllChanges() {
			return 500, "false"
		}

		return 200, "true"
	})
}

func (self *HttpServer) dropShard(w libhttp.ResponseWriter, r *libhttp.Request) {
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

func (self *HttpServer) convertShardsToMap(shards []*cluster.ShardData) []interface{} {
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

func (self *HttpServer) getShardSpaces(w libhttp.ResponseWriter, r *libhttp.Request) {
	self.tryAsClusterAdmin(w, r, func(u User) (int, interface{}) {
		return libhttp.StatusOK, self.clusterConfig.GetShardSpaces()
	})
}

func (self *HttpServer) createShardSpace(w libhttp.ResponseWriter, r *libhttp.Request) {
	self.tryAsClusterAdmin(w, r, func(u User) (int, interface{}) {
		space := &cluster.ShardSpace{}
		decoder := json.NewDecoder(r.Body)
		err := decoder.Decode(space)
		if err != nil {
			return libhttp.StatusInternalServerError, err.Error()
		}
		err = space.Validate(self.clusterConfig)
		if err != nil {
			return libhttp.StatusBadRequest, err.Error()
		}
		err = self.raftServer.CreateShardSpace(space)
		if err != nil {
			return libhttp.StatusInternalServerError, err.Error()
		}
		return libhttp.StatusOK, nil
	})
}

func (self *HttpServer) dropShardSpace(w libhttp.ResponseWriter, r *libhttp.Request) {
	self.tryAsClusterAdmin(w, r, func(u User) (int, interface{}) {
		name := r.URL.Query().Get(":name")
		db := r.URL.Query().Get(":db")
		if err := self.raftServer.DropShardSpace(db, name); err != nil {
			return libhttp.StatusInternalServerError, err.Error()
		}
		return libhttp.StatusOK, nil
	})
}

type DatabaseConfig struct {
	Spaces            []*cluster.ShardSpace `json:"spaces"`
	ContinuousQueries []string              `json:"continuousQueries"`
}

func (self *HttpServer) configureDatabase(w libhttp.ResponseWriter, r *libhttp.Request) {
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
				if !query.SelectQuery.IsContinuousQuery() {
					return libhttp.StatusBadRequest, fmt.Errorf("This query isn't a continuous query. Use 'into'. %s", query.QueryString)
				}
			}
		}

		err = self.coordinator.CreateDatabase(u, database)
		if err != nil {
			return libhttp.StatusBadRequest, err.Error()
		}
		for _, space := range databaseConfig.Spaces {
			space.Database = database
			err = self.raftServer.CreateShardSpace(space)
			if err != nil {
				return libhttp.StatusInternalServerError, err.Error()
			}
		}
		for _, queryString := range databaseConfig.ContinuousQueries {
			q, _ := parser.ParseQuery(queryString)
			for _, query := range q {
				err := self.coordinator.CreateContinuousQuery(u, database, query.QueryString)
				if err != nil {
					return libhttp.StatusInternalServerError, err.Error()
				}
			}
		}
		return libhttp.StatusCreated, nil
	})
}

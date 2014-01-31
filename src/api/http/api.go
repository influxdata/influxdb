package http

import (
	log "code.google.com/p/log4go"
	"common"
	"coordinator"
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"engine"
	"fmt"
	"github.com/bmizerany/pat"
	"io/ioutil"
	"net"
	libhttp "net/http"
	"path/filepath"
	"protocol"
	"regexp"
	"strconv"
	"strings"
	"time"
)

var VALID_TABLE_NAMES *regexp.Regexp

func init() {
	var err error
	VALID_TABLE_NAMES, err = regexp.Compile("^[a-zA-Z][a-zA-Z0-9._-]*$")
	if err != nil {
		panic(err)
	}
}

type HttpServer struct {
	conn           net.Listener
	sslConn        net.Listener
	httpPort       string
	httpSslPort    string
	httpSslCert    string
	adminAssetsDir string
	engine         engine.EngineI
	coordinator    coordinator.Coordinator
	userManager    coordinator.UserManager
	shutdown       chan bool
}

func NewHttpServer(httpPort string, adminAssetsDir string, theEngine engine.EngineI, theCoordinator coordinator.Coordinator, userManager coordinator.UserManager) *HttpServer {
	self := &HttpServer{}
	self.httpPort = httpPort
	self.adminAssetsDir = adminAssetsDir
	self.engine = theEngine
	self.coordinator = theCoordinator
	self.userManager = userManager
	self.shutdown = make(chan bool, 2)
	return self
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
	switch method {
	case "get":
		p.Get(pattern, CorsHeaderHandler(f))
	case "post":
		p.Post(pattern, CorsHeaderHandler(f))
	case "del":
		p.Del(pattern, CorsHeaderHandler(f))
	}
	p.Options(pattern, CorsHeaderHandler(self.sendCrossOriginHeader))
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

	go self.startSsl(p)

	if listener == nil {
		return
	}

	if err := libhttp.Serve(listener, p); err != nil && !strings.Contains(err.Error(), "closed network") {
		panic(err)
	}
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

	if err := libhttp.Serve(self.sslConn, p); err != nil && !strings.Contains(err.Error(), "closed network") {
		panic(err)
	}
}

func (self *HttpServer) Close() {
	if self.conn != nil {
		log.Info("Closing http server")
		self.conn.Close()
		log.Info("Waiting for all requests to finish before killing the process")
		for i := 0; i < 2; i++ {
			select {
			case <-time.After(time.Second * 5):
				log.Error("There seems to be a hanging request. Closing anyway")
			case <-self.shutdown:
			}
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
}

func (self *AllPointsWriter) yield(series *protocol.Series) error {
	oldSeries := self.memSeries[*series.Name]
	if oldSeries == nil {
		self.memSeries[*series.Name] = series
		return nil
	}

	oldSeries.Points = append(oldSeries.Points, series.Points...)
	return nil
}

func (self *AllPointsWriter) done() {
	data, err := serializeMultipleSeries(self.memSeries, self.precision)
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
}

func (self *ChunkWriter) yield(series *protocol.Series) error {
	data, err := serializeSingleSeries(series, self.precision)
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

type TimePrecision int

const (
	MicrosecondPrecision TimePrecision = iota
	MillisecondPrecision
	SecondPrecision
)

var TRUE = true

func TimePrecisionFromString(s string) (TimePrecision, error) {
	switch s {
	case "u":
		return MicrosecondPrecision, nil
	case "m":
		return MillisecondPrecision, nil
	case "s":
		return SecondPrecision, nil
	case "":
		return MillisecondPrecision, nil
	}

	return 0, fmt.Errorf("Unknown time precision %s", s)
}

func (self *HttpServer) forceRaftCompaction(w libhttp.ResponseWriter, r *libhttp.Request) {
	self.tryAsClusterAdmin(w, r, func(user common.User) (int, interface{}) {
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

	self.tryAsDbUserAndClusterAdmin(w, r, func(user common.User) (int, interface{}) {

		precision, err := TimePrecisionFromString(r.URL.Query().Get("time_precision"))
		if err != nil {
			return libhttp.StatusBadRequest, err.Error()
		}

		var writer Writer
		if r.URL.Query().Get("chunked") == "true" {
			writer = &ChunkWriter{w, precision, false}
		} else {
			writer = &AllPointsWriter{map[string]*protocol.Series{}, w, precision}
		}
		forceLocal := r.URL.Query().Get("force_local") == "true"
		err = self.engine.RunQuery(user, db, query, forceLocal, writer.yield)
		if err != nil {
			return errorToStatusCode(err), err.Error()
		}

		writer.done()
		return -1, nil
	})
}

func removeField(fields []string, name string) []string {
	index := -1
	for idx, field := range fields {
		if field == name {
			index = idx
			break
		}
	}

	if index == -1 {
		return fields
	}

	return append(fields[:index], fields[index+1:]...)
}

func removeTimestampFieldDefinition(fields []string) []string {
	fields = removeField(fields, "time")
	return removeField(fields, "sequence_number")
}

func convertToDataStoreSeries(s *SerializedSeries, precision TimePrecision) (*protocol.Series, error) {
	if !VALID_TABLE_NAMES.MatchString(s.Name) {
		return nil, fmt.Errorf("%s is not a valid series name", s.Name)
	}

	points := []*protocol.Point{}
	for _, point := range s.Points {
		values := []*protocol.FieldValue{}
		var timestamp *int64
		var sequence *uint64

		for idx, field := range s.Columns {
			value := point[idx]
			if field == "time" {
				switch value.(type) {
				case float64:
					_timestamp := int64(value.(float64))
					switch precision {
					case SecondPrecision:
						_timestamp *= 1000
						fallthrough
					case MillisecondPrecision:
						_timestamp *= 1000
					}

					timestamp = &_timestamp
					continue
				default:
					return nil, fmt.Errorf("time field must be float but is %T (%v)", value, value)
				}
			}

			if field == "sequence_number" {
				switch value.(type) {
				case float64:
					_sequenceNumber := uint64(value.(float64))
					sequence = &_sequenceNumber
					continue
				default:
					return nil, fmt.Errorf("sequence_number field must be float but is %T (%v)", value, value)
				}
			}

			switch v := value.(type) {
			case string:
				values = append(values, &protocol.FieldValue{StringValue: &v})
			case float64:
				if i := int64(v); float64(i) == v {
					values = append(values, &protocol.FieldValue{Int64Value: &i})
				} else {
					values = append(values, &protocol.FieldValue{DoubleValue: &v})
				}
			case bool:
				values = append(values, &protocol.FieldValue{BoolValue: &v})
			case nil:
				values = append(values, &protocol.FieldValue{IsNull: &TRUE})
			default:
				// if we reached this line then the dynamic type didn't match
				return nil, fmt.Errorf("Unknown type %T", value)
			}
		}
		points = append(points, &protocol.Point{
			Values:         values,
			Timestamp:      timestamp,
			SequenceNumber: sequence,
		})
	}

	fields := removeTimestampFieldDefinition(s.Columns)

	series := &protocol.Series{
		Name:   &s.Name,
		Fields: fields,
		Points: points,
	}
	return series, nil
}

func errorToStatusCode(err error) int {
	switch err.(type) {
	case common.AuthorizationError:
		return libhttp.StatusUnauthorized
	default:
		return libhttp.StatusBadRequest
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

	self.tryAsDbUserAndClusterAdmin(w, r, func(user common.User) (int, interface{}) {
		series, err := ioutil.ReadAll(r.Body)
		if err != nil {
			return libhttp.StatusInternalServerError, err.Error()
		}
		serializedSeries := []*SerializedSeries{}
		err = json.Unmarshal(series, &serializedSeries)
		if err != nil {
			return libhttp.StatusBadRequest, err.Error()
		}

		// convert the wire format to the internal representation of the time series
		for _, s := range serializedSeries {
			if len(s.Points) == 0 {
				continue
			}

			series, err := convertToDataStoreSeries(s, precision)
			if err != nil {
				return libhttp.StatusBadRequest, err.Error()
			}

			err = self.coordinator.WriteSeriesData(user, db, series)

			if err != nil {
				return errorToStatusCode(err), err.Error()
			}
		}
		return libhttp.StatusOK, nil
	})
}

type createDatabaseRequest struct {
	Name              string `json:"name"`
	ReplicationFactor uint8  `json:"replicationFactor"`
}

func (self *HttpServer) listDatabases(w libhttp.ResponseWriter, r *libhttp.Request) {
	self.tryAsClusterAdmin(w, r, func(u common.User) (int, interface{}) {
		databases, err := self.coordinator.ListDatabases(u)
		if err != nil {
			return errorToStatusCode(err), err.Error()
		}
		return libhttp.StatusOK, databases
	})
}

func (self *HttpServer) createDatabase(w libhttp.ResponseWriter, r *libhttp.Request) {
	self.tryAsClusterAdmin(w, r, func(user common.User) (int, interface{}) {
		body, err := ioutil.ReadAll(r.Body)
		if err != nil {
			return libhttp.StatusInternalServerError, err.Error()
		}
		createRequest := &createDatabaseRequest{}
		err = json.Unmarshal(body, createRequest)
		if err != nil {
			return libhttp.StatusBadRequest, err.Error()
		}
		err = self.coordinator.CreateDatabase(user, createRequest.Name, createRequest.ReplicationFactor)
		if err != nil {
			log.Error("Cannot create database %s. Error: %s", createRequest.Name, err)
			return errorToStatusCode(err), err.Error()
		}
		log.Debug("Created database %s with replication factor %d", createRequest.Name, createRequest.ReplicationFactor)
		return libhttp.StatusCreated, nil
	})
}

func (self *HttpServer) dropDatabase(w libhttp.ResponseWriter, r *libhttp.Request) {
	self.tryAsClusterAdmin(w, r, func(user common.User) (int, interface{}) {
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

	self.tryAsDbUserAndClusterAdmin(w, r, func(user common.User) (int, interface{}) {
		err := self.coordinator.DropSeries(user, db, series)
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

type SerializedSeries struct {
	Name    string          `json:"name"`
	Columns []string        `json:"columns"`
	Points  [][]interface{} `json:"points"`
}

func serializeSingleSeries(series *protocol.Series, precision TimePrecision) ([]byte, error) {
	arg := map[string]*protocol.Series{"": series}
	return json.Marshal(serializeSeries(arg, precision)[0])
}

func serializeMultipleSeries(series map[string]*protocol.Series, precision TimePrecision) ([]byte, error) {
	return json.Marshal(serializeSeries(series, precision))
}

func serializeSeries(memSeries map[string]*protocol.Series, precision TimePrecision) []*SerializedSeries {
	serializedSeries := []*SerializedSeries{}

	for _, series := range memSeries {
		includeSequenceNumber := true
		if len(series.Points) > 0 && series.Points[0].SequenceNumber == nil {
			includeSequenceNumber = false
		}

		columns := []string{"time"}
		if includeSequenceNumber {
			columns = append(columns, "sequence_number")
		}
		for _, field := range series.Fields {
			columns = append(columns, field)
		}

		points := [][]interface{}{}
		for _, row := range series.Points {
			timestamp := *row.GetTimestampInMicroseconds()
			switch precision {
			case SecondPrecision:
				timestamp /= 1000
				fallthrough
			case MillisecondPrecision:
				timestamp /= 1000
			}

			rowValues := []interface{}{timestamp}
			if includeSequenceNumber {
				rowValues = append(rowValues, *row.SequenceNumber)
			}
			for _, value := range row.Values {
				rowValues = append(rowValues, value.GetValue())
			}
			points = append(points, rowValues)
		}

		serializedSeries = append(serializedSeries, &SerializedSeries{
			Name:    *series.Name,
			Columns: columns,
			Points:  points,
		})
	}
	return serializedSeries
}

// // cluster admins management interface

func toBytes(body interface{}) ([]byte, string, error) {
	if body == nil {
		return nil, "text/plain", nil
	}
	switch x := body.(type) {
	case string:
		return []byte(x), "text/plain", nil
	case []byte:
		return x, "text/plain", nil
	default:
		body, err := json.Marshal(body)
		return body, "application/json", err
	}
}

func yieldUser(user common.User, yield func(common.User) (int, interface{})) (int, string, []byte) {
	statusCode, body := yield(user)
	bodyContent, contentType, err := toBytes(body)
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

func (self *HttpServer) tryAsClusterAdmin(w libhttp.ResponseWriter, r *libhttp.Request, yield func(common.User) (int, interface{})) {
	username, password, err := getUsernameAndPassword(r)
	if err != nil {
		w.WriteHeader(libhttp.StatusBadRequest)
		w.Write([]byte(err.Error()))
		return
	}

	if username == "" {
		w.Header().Add("WWW-Authenticate", "Basic realm=\"influxdb\"")
		w.WriteHeader(libhttp.StatusUnauthorized)
		w.Write([]byte("Invalid username/password"))
		return
	}

	user, err := self.userManager.AuthenticateClusterAdmin(username, password)
	if err != nil {
		w.Header().Add("WWW-Authenticate", "Basic realm=\"influxdb\"")
		w.WriteHeader(libhttp.StatusUnauthorized)
		w.Write([]byte(err.Error()))
		return
	}
	statusCode, contentType, body := yieldUser(user, yield)
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
}

type UpdateClusterAdminUser struct {
	Password string `json:"password"`
}

type User struct {
	Name string `json:"username"`
}

type NewContinuousQuery struct {
	Query string `json:"query"`
}

func (self *HttpServer) listClusterAdmins(w libhttp.ResponseWriter, r *libhttp.Request) {
	self.tryAsClusterAdmin(w, r, func(u common.User) (int, interface{}) {
		names, err := self.userManager.ListClusterAdmins(u)
		if err != nil {
			return errorToStatusCode(err), err.Error()
		}
		users := make([]*User, 0, len(names))
		for _, name := range names {
			users = append(users, &User{name})
		}
		return libhttp.StatusOK, users
	})
}

func (self *HttpServer) authenticateClusterAdmin(w libhttp.ResponseWriter, r *libhttp.Request) {
	self.tryAsClusterAdmin(w, r, func(u common.User) (int, interface{}) {
		return libhttp.StatusOK, nil
	})
}

func (self *HttpServer) createClusterAdmin(w libhttp.ResponseWriter, r *libhttp.Request) {
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		w.WriteHeader(libhttp.StatusInternalServerError)
		w.Write([]byte(err.Error()))
		return
	}
	newUser := &NewUser{}
	err = json.Unmarshal(body, newUser)
	if err != nil {
		w.WriteHeader(libhttp.StatusBadRequest)
		w.Write([]byte(err.Error()))
		return
	}

	self.tryAsClusterAdmin(w, r, func(u common.User) (int, interface{}) {
		username := newUser.Name
		if err := self.userManager.CreateClusterAdminUser(u, username); err != nil {
			errorStr := err.Error()
			return errorToStatusCode(err), errorStr
		}
		if err := self.userManager.ChangeClusterAdminPassword(u, username, newUser.Password); err != nil {
			return errorToStatusCode(err), err.Error()
		}
		return libhttp.StatusOK, nil
	})
}

func (self *HttpServer) deleteClusterAdmin(w libhttp.ResponseWriter, r *libhttp.Request) {
	newUser := r.URL.Query().Get(":user")

	self.tryAsClusterAdmin(w, r, func(u common.User) (int, interface{}) {
		if err := self.userManager.DeleteClusterAdminUser(u, newUser); err != nil {
			return errorToStatusCode(err), err.Error()
		}
		return libhttp.StatusOK, nil
	})
}

func (self *HttpServer) updateClusterAdmin(w libhttp.ResponseWriter, r *libhttp.Request) {
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		w.WriteHeader(libhttp.StatusInternalServerError)
		w.Write([]byte(err.Error()))
		return
	}

	updateClusterAdminUser := &UpdateClusterAdminUser{}
	json.Unmarshal(body, updateClusterAdminUser)

	newUser := r.URL.Query().Get(":user")

	self.tryAsClusterAdmin(w, r, func(u common.User) (int, interface{}) {
		if err := self.userManager.ChangeClusterAdminPassword(u, newUser, updateClusterAdminUser.Password); err != nil {
			return errorToStatusCode(err), err.Error()
		}
		return libhttp.StatusOK, nil
	})
}

// // db users management interface

func (self *HttpServer) authenticateDbUser(w libhttp.ResponseWriter, r *libhttp.Request) {
	code, body := self.tryAsDbUser(w, r, func(u common.User) (int, interface{}) {
		return libhttp.StatusOK, nil
	})
	w.WriteHeader(code)
	if len(body) > 0 {
		w.Write(body)
	}
}

func (self *HttpServer) tryAsDbUser(w libhttp.ResponseWriter, r *libhttp.Request, yield func(common.User) (int, interface{})) (int, []byte) {
	username, password, err := getUsernameAndPassword(r)
	if err != nil {
		return libhttp.StatusBadRequest, []byte(err.Error())
	}

	db := r.URL.Query().Get(":db")

	if username == "" {
		w.Header().Add("WWW-Authenticate", "Basic realm=\"influxdb\"")
		return libhttp.StatusUnauthorized, []byte("Invalid username/password")
	}

	user, err := self.userManager.AuthenticateDbUser(db, username, password)
	if err != nil {
		w.Header().Add("WWW-Authenticate", "Basic realm=\"influxdb\"")
		return libhttp.StatusUnauthorized, []byte(err.Error())
	}

	statusCode, contentType, v := yieldUser(user, yield)
	if statusCode == libhttp.StatusUnauthorized {
		w.Header().Add("WWW-Authenticate", "Basic realm=\"influxdb\"")
	}
	w.Header().Add("content-type", contentType)
	return statusCode, v
}

func (self *HttpServer) tryAsDbUserAndClusterAdmin(w libhttp.ResponseWriter, r *libhttp.Request, yield func(common.User) (int, interface{})) {
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

	if statusCode > 0 {
		w.WriteHeader(statusCode)
	}
	if len(body) > 0 {
		w.Write(body)
	}
	return
}

func (self *HttpServer) listDbUsers(w libhttp.ResponseWriter, r *libhttp.Request) {
	db := r.URL.Query().Get(":db")

	self.tryAsDbUserAndClusterAdmin(w, r, func(u common.User) (int, interface{}) {
		names, err := self.userManager.ListDbUsers(u, db)
		if err != nil {
			return errorToStatusCode(err), err.Error()
		}

		users := make([]*User, 0, len(names))
		for _, name := range names {
			users = append(users, &User{name})
		}
		return libhttp.StatusOK, users
	})
}

func (self *HttpServer) createDbUser(w libhttp.ResponseWriter, r *libhttp.Request) {
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		w.WriteHeader(libhttp.StatusInternalServerError)
		w.Write([]byte(err.Error()))
		return
	}

	newUser := &NewUser{}
	err = json.Unmarshal(body, newUser)
	if err != nil {
		w.WriteHeader(libhttp.StatusBadRequest)
		w.Write([]byte(err.Error()))
		return
	}

	db := r.URL.Query().Get(":db")

	self.tryAsDbUserAndClusterAdmin(w, r, func(u common.User) (int, interface{}) {
		username := newUser.Name
		if err := self.userManager.CreateDbUser(u, db, username); err != nil {
			log.Error("Cannot create user: %s", err)
			return errorToStatusCode(err), err.Error()
		}
		log.Debug("Created user %s", username)
		if err := self.userManager.ChangeDbUserPassword(u, db, username, newUser.Password); err != nil {
			log.Error("Cannot change user password: %s", err)
			// there is probably something wrong if we could create
			// the user but not change the password. so return
			// 500
			return libhttp.StatusInternalServerError, err.Error()
		}
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

	self.tryAsDbUserAndClusterAdmin(w, r, func(u common.User) (int, interface{}) {
		if err := self.userManager.DeleteDbUser(u, db, newUser); err != nil {
			return errorToStatusCode(err), err.Error()
		}
		return libhttp.StatusOK, nil
	})
}

func (self *HttpServer) updateDbUser(w libhttp.ResponseWriter, r *libhttp.Request) {
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		w.WriteHeader(libhttp.StatusInternalServerError)
		w.Write([]byte(err.Error()))
		return
	}

	updateUser := make(map[string]interface{})
	err = json.Unmarshal(body, &updateUser)
	if err != nil {
		w.WriteHeader(libhttp.StatusBadRequest)
		w.Write([]byte(err.Error()))
		return
	}

	newUser := r.URL.Query().Get(":user")
	db := r.URL.Query().Get(":db")

	self.tryAsDbUserAndClusterAdmin(w, r, func(u common.User) (int, interface{}) {
		if pwd, ok := updateUser["password"]; ok {
			newPassword, ok := pwd.(string)
			if !ok {
				return libhttp.StatusBadRequest, "password must be string"
			}

			if err := self.userManager.ChangeDbUserPassword(u, db, newUser, newPassword); err != nil {
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
	statusCode, contentType, body := yieldUser(nil, func(u common.User) (int, interface{}) {
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
	})

	w.Header().Add("content-type", contentType)
	w.WriteHeader(statusCode)
	if len(body) > 0 {
		w.Write(body)
	}
}

func (self *HttpServer) listDbContinuousQueries(w libhttp.ResponseWriter, r *libhttp.Request) {
	db := r.URL.Query().Get(":db")

	self.tryAsDbUserAndClusterAdmin(w, r, func(u common.User) (int, interface{}) {
		queries, err := self.coordinator.ListContinuousQueries(u, db)
		if err != nil {
			return errorToStatusCode(err), err.Error()
		}

		return libhttp.StatusOK, queries
	})
}

func (self *HttpServer) createDbContinuousQueries(w libhttp.ResponseWriter, r *libhttp.Request) {
	db := r.URL.Query().Get(":db")

	self.tryAsDbUserAndClusterAdmin(w, r, func(u common.User) (int, interface{}) {
		var values interface{}
		body, err := ioutil.ReadAll(r.Body)
		if err != nil {
			return libhttp.StatusInternalServerError, err.Error()
		}
		json.Unmarshal(body, &values)
		query := values.(map[string]interface{})["query"].(string)
		fmt.Println(query)

		if err := self.coordinator.CreateContinuousQuery(u, db, query); err != nil {
			return errorToStatusCode(err), err.Error()
		}
		return libhttp.StatusOK, nil
	})
}

func (self *HttpServer) deleteDbContinuousQueries(w libhttp.ResponseWriter, r *libhttp.Request) {
	db := r.URL.Query().Get(":db")
	id, _ := strconv.ParseInt(r.URL.Query().Get(":id"), 10, 64)

	self.tryAsDbUserAndClusterAdmin(w, r, func(u common.User) (int, interface{}) {
		if err := self.coordinator.DeleteContinuousQuery(u, db, uint32(id)); err != nil {
			return errorToStatusCode(err), err.Error()
		}
		return libhttp.StatusOK, nil
	})
}

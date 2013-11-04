package http

import (
	log "code.google.com/p/log4go"
	"common"
	"coordinator"
	"encoding/json"
	"engine"
	"fmt"
	"github.com/bmizerany/pat"
	"io/ioutil"
	"net"
	libhttp "net/http"
	"protocol"
	"strings"
)

type HttpServer struct {
	conn        net.Listener
	httpAddr    string
	engine      engine.EngineI
	coordinator coordinator.Coordinator
	userManager coordinator.UserManager
	shutdown    chan bool
}

func NewHttpServer(httpAddr string, theEngine engine.EngineI, theCoordinator coordinator.Coordinator, userManager coordinator.UserManager) *HttpServer {
	self := &HttpServer{}
	self.httpAddr = httpAddr
	self.engine = theEngine
	self.coordinator = theCoordinator
	self.userManager = userManager
	self.shutdown = make(chan bool)
	return self
}

func (self *HttpServer) ListenAndServe() {
	conn, err := net.Listen("tcp", self.httpAddr)
	if err != nil {
		log.Error("Listen: ", err)
	}
	self.Serve(conn)
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
	self.conn = listener
	p := pat.New()

	// Run the given query and return an array of series or a chunked response
	// with each batch of points we get back
	self.registerEndpoint(p, "get", "/db/:db/series", self.query)

	// Write points to the given database
	self.registerEndpoint(p, "post", "/db/:db/series", self.writePoints)
	self.registerEndpoint(p, "get", "/dbs", self.listDatabases)
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
	self.registerEndpoint(p, "post", "/db/:db/admins/:user", self.setDbAdmin)
	self.registerEndpoint(p, "del", "/db/:db/admins/:user", self.unsetDbAdmin)

	if err := libhttp.Serve(listener, p); err != nil && !strings.Contains(err.Error(), "closed network") {
		panic(err)
	}
	self.shutdown <- true
}

func (self *HttpServer) Close() {
	log.Info("Closing http server")
	self.conn.Close()
	log.Info("Waiting for all requests to finish before killing the process")
	<-self.shutdown
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
		self.w.Write([]byte(err.Error()))
		self.w.WriteHeader(libhttp.StatusInternalServerError)
		return
	}
	self.w.Write(data)
	self.w.WriteHeader(libhttp.StatusOK)
}

type ChunkWriter struct {
	w         libhttp.ResponseWriter
	precision TimePrecision
}

func (self *ChunkWriter) yield(series *protocol.Series) error {
	data, err := serializeSingleSeries(series, self.precision)
	if err != nil {
		return err
	}
	self.w.Write(data)
	self.w.WriteHeader(libhttp.StatusOK)
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

func (self *HttpServer) sendCrossOriginHeader(w libhttp.ResponseWriter, r *libhttp.Request) {
	w.WriteHeader(libhttp.StatusOK)
}

func (self *HttpServer) query(w libhttp.ResponseWriter, r *libhttp.Request) {
	query := r.URL.Query().Get("q")
	db := r.URL.Query().Get(":db")

	statusCode, body := self.tryAsDbUser(w, r, func(user common.User) (int, interface{}) {

		precision, err := TimePrecisionFromString(r.URL.Query().Get("time_precision"))
		if err != nil {
			w.WriteHeader(libhttp.StatusBadRequest)
			w.Write([]byte(err.Error()))
		}

		var writer Writer
		if r.URL.Query().Get("chunked") == "true" {
			writer = &ChunkWriter{w, precision}
		} else {
			writer = &AllPointsWriter{map[string]*protocol.Series{}, w, precision}
		}
		err = self.engine.RunQuery(user, db, query, writer.yield)
		if err != nil {
			return libhttp.StatusInternalServerError, err.Error()
		}

		writer.done()
		return libhttp.StatusOK, nil
	})
	w.WriteHeader(statusCode)
	if len(body) > 0 {
		w.Write(body)
	}
}

func removeTimestampFieldDefinition(fields []string) []string {
	timestampIdx := -1
	for idx, field := range fields {
		if field == "time" {
			timestampIdx = idx
			break
		}
	}

	if timestampIdx == -1 {
		return fields
	}

	fields[len(fields)-1], fields[timestampIdx] = fields[timestampIdx], fields[len(fields)-1]
	return fields[:len(fields)-1]
}

func convertToDataStoreSeries(s *SerializedSeries, precision TimePrecision) (*protocol.Series, error) {
	points := []*protocol.Point{}
	for _, point := range s.Points {
		values := []*protocol.FieldValue{}
		var timestamp *int64

		for idx, field := range s.Columns {
			value := point[idx]
			if field == "time" {
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
				continue
			case bool:
				values = append(values, &protocol.FieldValue{BoolValue: &v})
				continue
			default:
				// if we reached this line then the dynamic type didn't match
				return nil, fmt.Errorf("Unknown type %T", value)
			}
		}
		points = append(points, &protocol.Point{
			Values:    values,
			Timestamp: timestamp,
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

func (self *HttpServer) writePoints(w libhttp.ResponseWriter, r *libhttp.Request) {
	db := r.URL.Query().Get(":db")
	precision, err := TimePrecisionFromString(r.URL.Query().Get("time_precision"))
	if err != nil {
		w.WriteHeader(libhttp.StatusBadRequest)
		w.Write([]byte(err.Error()))
	}

	statusCode, body := self.tryAsDbUser(w, r, func(user common.User) (int, interface{}) {
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
				return libhttp.StatusUnauthorized, err.Error()
			}
		}
		return libhttp.StatusOK, nil
	})

	w.WriteHeader(statusCode)
	if len(body) > 0 {
		w.Write(body)
	}
}

type createDatabaseRequest struct {
	Name   string `json:"name"`
	ApiKey string `json:apiKey"`
}

type Database struct {
	Name string `json:"name"`
}

func (self *HttpServer) listDatabases(w libhttp.ResponseWriter, r *libhttp.Request) {
	self.tryAsClusterAdmin(w, r, func(u common.User) (int, interface{}) {
		dbNames, err := self.coordinator.ListDatabases(u)
		if err != nil {
			return libhttp.StatusUnauthorized, err.Error()
		}
		databases := make([]*Database, 0, len(dbNames))
		for _, db := range dbNames {
			databases = append(databases, &Database{db})
		}
		body, err := json.Marshal(databases)
		if err != nil {
			return libhttp.StatusInternalServerError, err.Error()
		}
		return libhttp.StatusOK, body
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
		err = self.coordinator.CreateDatabase(user, createRequest.Name)
		if err != nil {
			return libhttp.StatusUnauthorized, err.Error()
		}
		return libhttp.StatusCreated, nil
	})
}

func (self *HttpServer) dropDatabase(w libhttp.ResponseWriter, r *libhttp.Request) {
	self.tryAsClusterAdmin(w, r, func(user common.User) (int, interface{}) {
		name := r.URL.Query().Get(":name")
		err := self.coordinator.DropDatabase(user, name)
		if err != nil {
			return libhttp.StatusBadRequest, err.Error()
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
		columns := []string{"time", "sequence_number"}
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

			rowValues := []interface{}{timestamp, *row.SequenceNumber}
			for _, value := range row.Values {
				if value != nil {
					rowValues = append(rowValues, value.GetValue())
				} else {
					rowValues = append(rowValues, nil)
				}
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

func toBytes(body interface{}) ([]byte, error) {
	if body == nil {
		return nil, nil
	}
	switch x := body.(type) {
	case string:
		return []byte(x), nil
	case []byte:
		return x, nil
	default:
		return json.Marshal(body)
	}
}

func yieldUser(user common.User, yield func(common.User) (int, interface{})) (int, []byte) {
	statusCode, body := yield(user)
	bodyContent, err := toBytes(body)
	if err != nil {
		return libhttp.StatusInternalServerError, []byte(err.Error())
	}

	return statusCode, bodyContent
}

func (self *HttpServer) tryAsClusterAdmin(w libhttp.ResponseWriter, r *libhttp.Request, yield func(common.User) (int, interface{})) {
	username := r.URL.Query().Get("u")
	password := r.URL.Query().Get("p")

	if username == "" {
		w.WriteHeader(libhttp.StatusUnauthorized)
		w.Write([]byte("Invalid username/password"))
		return
	}

	user, err := self.userManager.AuthenticateClusterAdmin(username, password)
	if err != nil {
		w.WriteHeader(libhttp.StatusUnauthorized)
		w.Write([]byte(err.Error()))
		return
	}
	statusCode, body := yieldUser(user, yield)
	w.WriteHeader(statusCode)
	if len(body) > 0 {
		w.Write(body)
	}
}

type NewUser struct {
	Name     string `json:"username"`
	Password string `json:"password"`
}

type UpdateUser struct {
	Password string `json:"password"`
}

type User struct {
	Name string `json:"username"`
}

func (self *HttpServer) listClusterAdmins(w libhttp.ResponseWriter, r *libhttp.Request) {
	self.tryAsClusterAdmin(w, r, func(u common.User) (int, interface{}) {
		names, err := self.userManager.ListClusterAdmins(u)
		if err != nil {
			return libhttp.StatusUnauthorized, err.Error()
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
		if err := self.userManager.CreateClusterAdminUser(u, newUser.Name); err != nil {
			return libhttp.StatusUnauthorized, err.Error()
		}
		if err := self.userManager.ChangeClusterAdminPassword(u, newUser.Name, newUser.Password); err != nil {
			return libhttp.StatusInternalServerError, err.Error()
		}
		return libhttp.StatusOK, nil
	})
}

func (self *HttpServer) deleteClusterAdmin(w libhttp.ResponseWriter, r *libhttp.Request) {
	newUser := r.URL.Query().Get(":user")

	self.tryAsClusterAdmin(w, r, func(u common.User) (int, interface{}) {
		if err := self.userManager.DeleteClusterAdminUser(u, newUser); err != nil {
			return libhttp.StatusUnauthorized, err.Error()
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

	updateUser := &UpdateUser{}
	json.Unmarshal(body, updateUser)

	newUser := r.URL.Query().Get(":user")

	self.tryAsClusterAdmin(w, r, func(u common.User) (int, interface{}) {
		if err := self.userManager.ChangeClusterAdminPassword(u, newUser, updateUser.Password); err != nil {
			return libhttp.StatusUnauthorized, err.Error()
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
	username := r.URL.Query().Get("u")
	password := r.URL.Query().Get("p")
	db := r.URL.Query().Get(":db")

	if username == "" {
		return libhttp.StatusUnauthorized, []byte("Invalid username/password")
	}

	user, err := self.userManager.AuthenticateDbUser(db, username, password)
	if err != nil {
		return libhttp.StatusUnauthorized, []byte(err.Error())
	}

	return yieldUser(user, yield)
}

func (self *HttpServer) tryAsDbUserAndClusterAdmin(w libhttp.ResponseWriter, r *libhttp.Request, yield func(common.User) (int, interface{})) {
	statusCode, body := self.tryAsDbUser(w, r, yield)
	if statusCode == libhttp.StatusUnauthorized {
		self.tryAsClusterAdmin(w, r, yield)
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

	self.tryAsDbUserAndClusterAdmin(w, r, func(u common.User) (int, interface{}) {
		names, err := self.userManager.ListDbUsers(u, db)
		if err != nil {
			return libhttp.StatusUnauthorized, err.Error()
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
		if err := self.userManager.CreateDbUser(u, db, newUser.Name); err != nil {
			return libhttp.StatusUnauthorized, err.Error()
		}
		if err := self.userManager.ChangeDbUserPassword(u, db, newUser.Name, newUser.Password); err != nil {
			return libhttp.StatusUnauthorized, err.Error()
		}
		return libhttp.StatusOK, nil
	})
}

func (self *HttpServer) deleteDbUser(w libhttp.ResponseWriter, r *libhttp.Request) {
	newUser := r.URL.Query().Get(":user")
	db := r.URL.Query().Get(":db")

	self.tryAsDbUserAndClusterAdmin(w, r, func(u common.User) (int, interface{}) {
		if err := self.userManager.DeleteDbUser(u, db, newUser); err != nil {
			return libhttp.StatusUnauthorized, err.Error()
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

	updateUser := &UpdateUser{}
	err = json.Unmarshal(body, updateUser)
	if err != nil {
		w.WriteHeader(libhttp.StatusBadRequest)
		w.Write([]byte(err.Error()))
		return
	}

	newUser := r.URL.Query().Get(":user")
	db := r.URL.Query().Get(":db")

	self.tryAsDbUserAndClusterAdmin(w, r, func(u common.User) (int, interface{}) {
		if err := self.userManager.ChangeDbUserPassword(u, db, newUser, updateUser.Password); err != nil {
			return libhttp.StatusUnauthorized, err.Error()
		}
		return libhttp.StatusOK, nil
	})
}

func (self *HttpServer) setDbAdmin(w libhttp.ResponseWriter, r *libhttp.Request) {
	self.commonSetDbAdmin(w, r, true)
}

func (self *HttpServer) unsetDbAdmin(w libhttp.ResponseWriter, r *libhttp.Request) {
	self.commonSetDbAdmin(w, r, false)
}

func (self *HttpServer) commonSetDbAdmin(w libhttp.ResponseWriter, r *libhttp.Request, isAdmin bool) {
	newUser := r.URL.Query().Get(":user")
	db := r.URL.Query().Get(":db")

	self.tryAsDbUserAndClusterAdmin(w, r, func(u common.User) (int, interface{}) {
		if err := self.userManager.SetDbAdmin(u, db, newUser, isAdmin); err != nil {
			return libhttp.StatusUnauthorized, err.Error()
		}
		return libhttp.StatusOK, nil
	})
}

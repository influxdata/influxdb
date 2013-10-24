package http

import (
	log "code.google.com/p/log4go"
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
	self.registerEndpoint(p, "post", "/db", self.createDatabase)

	// cluster admins management interface

	self.registerEndpoint(p, "post", "/cluster_admins", self.createClusterAdmin)
	self.registerEndpoint(p, "post", "/cluster_admins/:user", self.updateClusterAdmin)
	self.registerEndpoint(p, "del", "/cluster_admins/:user", self.deleteClusterAdmin)

	// db users management interface

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
	err = self.engine.RunQuery(db, query, writer.yield)
	if err != nil {
		w.Write([]byte(err.Error()))
		w.WriteHeader(libhttp.StatusInternalServerError)
		return
	}

	writer.done()
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

func (self *HttpServer) writePoints(w libhttp.ResponseWriter, r *libhttp.Request) {
	db := r.URL.Query().Get(":db")
	precision, err := TimePrecisionFromString(r.URL.Query().Get("time_precision"))
	if err != nil {
		w.WriteHeader(libhttp.StatusBadRequest)
		w.Write([]byte(err.Error()))
	}

	series, err := ioutil.ReadAll(r.Body)
	if err != nil {
		w.WriteHeader(libhttp.StatusInternalServerError)
		return
	}
	serializedSeries := []*SerializedSeries{}
	err = json.Unmarshal(series, &serializedSeries)
	if err != nil {
		w.WriteHeader(libhttp.StatusBadRequest)
		w.Write([]byte(err.Error()))
		return
	}

	// convert the wire format to the internal representation of the time series
	for _, s := range serializedSeries {
		if len(s.Points) == 0 {
			continue
		}

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
					w.WriteHeader(libhttp.StatusBadRequest)
					return
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

		self.coordinator.WriteSeriesData(db, series)
	}
}

type createDatabaseRequest struct {
	Name   string `json:"name"`
	ApiKey string `json:apiKey"`
}

func (self *HttpServer) createDatabase(w libhttp.ResponseWriter, r *libhttp.Request) {
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		w.WriteHeader(libhttp.StatusInternalServerError)
		return
	}
	createRequest := &createDatabaseRequest{}
	err = json.Unmarshal(body, createRequest)
	if err != nil {
		w.WriteHeader(libhttp.StatusBadRequest)
		w.Write([]byte(err.Error()))
		return
	}
	apiKey := r.URL.Query().Get("api_key")
	err = self.coordinator.CreateDatabase(createRequest.Name, createRequest.ApiKey, apiKey)
	if err != nil {
		w.WriteHeader(libhttp.StatusBadRequest)
		w.Write([]byte(err.Error()))
		return
	}
	w.WriteHeader(libhttp.StatusCreated)
}

type Point struct {
	Timestamp      int64         `json:"timestamp"`
	SequenceNumber uint32        `json:"sequenceNumber"`
	Values         []interface{} `json:"values"`
}

type SerializedSeries struct {
	Name           string          `json:"name"`
	Columns        []string        `json:"columns"`
	IntegerColumns []int           `json:"integer_columns,omitempty"`
	Points         [][]interface{} `json:"points"`
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

func (self *HttpServer) tryAsClusterAdmin(w libhttp.ResponseWriter, r *libhttp.Request, yield func(coordinator.User) (int, string)) {
	username := r.URL.Query().Get("u")
	password := r.URL.Query().Get("p")

	if username == "" {
		w.WriteHeader(libhttp.StatusUnauthorized)
		w.Write([]byte("Invalid username/password"))
	}

	user, err := self.userManager.AuthenticateClusterAdmin(username, password)
	if err != nil {
		w.WriteHeader(libhttp.StatusUnauthorized)
		w.Write([]byte(err.Error()))
	}

	statusCode, errStr := yield(user)
	w.WriteHeader(statusCode)
	if statusCode != libhttp.StatusOK {
		w.Write([]byte(errStr))
	}
}

type NewUser struct {
	Name     string `json:"username"`
	Password string `json:"password"`
}

type UpdateUser struct {
	Password string `json:"password"`
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

	self.tryAsClusterAdmin(w, r, func(u coordinator.User) (int, string) {
		if err := self.userManager.CreateClusterAdminUser(u, newUser.Name); err != nil {
			return libhttp.StatusUnauthorized, err.Error()
		}
		if err := self.userManager.ChangeClusterAdminPassword(u, newUser.Name, newUser.Password); err != nil {
			return libhttp.StatusInternalServerError, err.Error()
		}
		return libhttp.StatusOK, ""
	})
}

func (self *HttpServer) deleteClusterAdmin(w libhttp.ResponseWriter, r *libhttp.Request) {
	newUser := r.URL.Query().Get(":user")

	self.tryAsClusterAdmin(w, r, func(u coordinator.User) (int, string) {
		if err := self.userManager.DeleteClusterAdminUser(u, newUser); err != nil {
			return libhttp.StatusUnauthorized, err.Error()
		}
		return libhttp.StatusOK, ""
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

	self.tryAsClusterAdmin(w, r, func(u coordinator.User) (int, string) {
		if err := self.userManager.ChangeClusterAdminPassword(u, newUser, updateUser.Password); err != nil {
			return libhttp.StatusUnauthorized, err.Error()
		}
		return libhttp.StatusOK, ""
	})
}

// // db users management interface

func (self *HttpServer) tryAsDbUserAndClusterAdmin(w libhttp.ResponseWriter, r *libhttp.Request, yield func(coordinator.User) (int, string)) {
	username := r.URL.Query().Get("u")
	password := r.URL.Query().Get("p")
	db := r.URL.Query().Get(":db")

	if username == "" {
		w.WriteHeader(libhttp.StatusUnauthorized)
		w.Write([]byte("Invalid username/password"))
	}

	user, err := self.userManager.AuthenticateDbUser(db, username, password)
	if err != nil {
		self.tryAsClusterAdmin(w, r, yield)
		return
	}

	statusCode, _ := yield(user)
	if statusCode != libhttp.StatusOK {
		self.tryAsClusterAdmin(w, r, yield)
		return
	}

	w.WriteHeader(statusCode)
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

	self.tryAsDbUserAndClusterAdmin(w, r, func(u coordinator.User) (int, string) {
		if err := self.userManager.CreateDbUser(u, db, newUser.Name); err != nil {
			return libhttp.StatusUnauthorized, err.Error()
		}
		if err := self.userManager.ChangeDbUserPassword(u, db, newUser.Name, newUser.Password); err != nil {
			return libhttp.StatusUnauthorized, err.Error()
		}
		return libhttp.StatusOK, ""
	})
}

func (self *HttpServer) deleteDbUser(w libhttp.ResponseWriter, r *libhttp.Request) {
	newUser := r.URL.Query().Get(":user")
	db := r.URL.Query().Get(":db")

	self.tryAsDbUserAndClusterAdmin(w, r, func(u coordinator.User) (int, string) {
		if err := self.userManager.DeleteDbUser(u, db, newUser); err != nil {
			return libhttp.StatusUnauthorized, err.Error()
		}
		return libhttp.StatusOK, ""
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

	self.tryAsDbUserAndClusterAdmin(w, r, func(u coordinator.User) (int, string) {
		if err := self.userManager.ChangeDbUserPassword(u, db, newUser, updateUser.Password); err != nil {
			return libhttp.StatusUnauthorized, err.Error()
		}
		return libhttp.StatusOK, ""
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

	self.tryAsDbUserAndClusterAdmin(w, r, func(u coordinator.User) (int, string) {
		if err := self.userManager.SetDbAdmin(u, db, newUser, isAdmin); err != nil {
			return libhttp.StatusUnauthorized, err.Error()
		}
		return libhttp.StatusOK, ""
	})
}

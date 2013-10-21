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
	shutdown    chan bool
}

func NewHttpServer(httpAddr string, theEngine engine.EngineI, theCoordinator coordinator.Coordinator) *HttpServer {
	self := &HttpServer{}
	self.httpAddr = httpAddr
	self.engine = theEngine
	self.coordinator = theCoordinator
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

func (self *HttpServer) Serve(listener net.Listener) {
	self.conn = listener
	p := pat.New()

	// Run the given query and return an array of series or a chunked response
	// with each batch of points we get back
	p.Get("/db/:db/series", libhttp.HandlerFunc(self.query))

	// Write points to the given database
	p.Post("/db/:db/series", CorsHeaderHandler(self.writePoints))
	p.Post("/db", CorsHeaderHandler(self.createDatabase))

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

// [
//   {"name": "seriesname", "columns": ["count", "type"], "points": [[3, "asdf"], [1, "foo"]]},
//   {}
// ]

// [
//   {"name": "seriesname", "columns": ["time", "email"], "points": [[], []]}
// ]
func removeTimestampFieldDefinition(fields []*protocol.FieldDefinition) []*protocol.FieldDefinition {
	timestampIdx := -1
	for idx, field := range fields {
		if *field.Name == "time" {
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

		fields := []*protocol.FieldDefinition{}
		for idx, column := range s.Columns {
			var fieldType protocol.FieldDefinition_Type
		outer:
			switch s.Points[0][idx].(type) {
			case int:
				fieldType = protocol.FieldDefinition_INT64
			case float64:
				for _, intIdx := range s.IntegerColumns {
					if intIdx == idx {
						fieldType = protocol.FieldDefinition_INT64
						break outer
					}
				}
				fieldType = protocol.FieldDefinition_DOUBLE
			case string:
				fieldType = protocol.FieldDefinition_STRING
			case bool:
				fieldType = protocol.FieldDefinition_BOOL
			}

			_column := column
			fields = append(fields, &protocol.FieldDefinition{
				Name: &_column,
				Type: &fieldType,
			})
		}

		points := []*protocol.Point{}
		for _, point := range s.Points {
			values := []*protocol.FieldValue{}
			var timestamp *int64

			for idx, field := range fields {
				if *field.Name == "time" {
					_timestamp := int64(point[idx].(float64))
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

				switch *field.Type {
				case protocol.FieldDefinition_STRING:
					if str, ok := point[idx].(string); ok {
						values = append(values, &protocol.FieldValue{StringValue: &str})
						continue
					}
				case protocol.FieldDefinition_DOUBLE:
					if double, ok := point[idx].(float64); ok {
						values = append(values, &protocol.FieldValue{DoubleValue: &double})
						continue
					}
				case protocol.FieldDefinition_INT64:
					if double, ok := point[idx].(float64); ok {
						integer := int64(double)
						values = append(values, &protocol.FieldValue{Int64Value: &integer})
						continue
					}
				case protocol.FieldDefinition_BOOL:
					if boolean, ok := point[idx].(bool); ok {
						values = append(values, &protocol.FieldValue{BoolValue: &boolean})
						continue
					}
				}

				// if we reached this line then the dynamic type didn't match
				w.WriteHeader(libhttp.StatusBadRequest)
				return
			}
			points = append(points, &protocol.Point{
				Values:    values,
				Timestamp: timestamp,
			})
		}

		fields = removeTimestampFieldDefinition(fields)

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
			columns = append(columns, *field.Name)
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
			for idx, value := range row.Values {
				switch *series.Fields[idx].Type {
				case protocol.FieldDefinition_STRING:
					rowValues = append(rowValues, *value.StringValue)
				case protocol.FieldDefinition_INT64:
					rowValues = append(rowValues, *value.Int64Value)
				case protocol.FieldDefinition_DOUBLE:
					rowValues = append(rowValues, *value.DoubleValue)
				case protocol.FieldDefinition_BOOL:
					rowValues = append(rowValues, *value.BoolValue)
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

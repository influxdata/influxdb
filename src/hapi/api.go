package hapi

import (
	log "code.google.com/p/log4go"
	"encoding/json"
	"engine"
	"github.com/bmizerany/pat"
	"github.com/fitstar/falcore"
	"github.com/fitstar/falcore/filter"
	"net"
	"net/http"
	"protocol"
	"strings"
)

type HttpServer struct {
	conn     net.Listener
	Server   *falcore.Server
	config   *Configuration
	engine   engine.EngineI
	shutdown chan bool
}

func NewHttpServer(config *Configuration, theEngine engine.EngineI) *HttpServer {
	self := &HttpServer{}
	self.config = config
	self.engine = theEngine
	self.shutdown = make(chan bool)
	return self
}

func (self *HttpServer) ListenAndServe() {
	conn, err := net.Listen("tcp", self.config.HttpAddr)
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
	p.Get("/api/db/:db/series", CorsAndCompressionHeaderHandler(self.query))

	// Write points to the given database
	p.Post("/api/db/:db/series", CorsHeaderHandler(self.writePoints))

	pipeline := falcore.NewPipeline()
	pipeline.Upstream.PushBack(filter.NewHandlerFilter(p))
	self.Server = falcore.NewServer(-1, pipeline)
	file, err := listener.(*net.TCPListener).File()
	if err != nil {
		panic(err)
	}
	if err := self.Server.FdListen(int(file.Fd())); err != nil {
		panic(err)
	}

	if err := self.Server.ListenAndServe(); err != nil && !strings.Contains(err.Error(), "closed network") {
		panic(err)
	}
	self.shutdown <- true
}

func (self *HttpServer) Close() {
	log.Info("Closing http server")
	self.Server.StopAccepting()
	log.Info("Waiting for all requests to finish before killing the process")
	<-self.shutdown
}

func allPointsYield(w http.ResponseWriter) (map[string]*protocol.Series, func(*protocol.Series) error) {
	memSeries := map[string]*protocol.Series{}

	return memSeries, func(series *protocol.Series) error {
		oldSeries := memSeries[*series.Name]
		if oldSeries == nil {
			memSeries[*series.Name] = series
			return nil
		}

		oldSeries.Points = append(oldSeries.Points, series.Points...)
		return nil
	}
}

func (self *HttpServer) query(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query().Get("q")
	db := r.URL.Query().Get(":db")
	memSeeries, yield := allPointsYield(w)
	err := self.engine.RunQuery(db, query, yield)
	if err != nil {
		w.Write([]byte(err.Error()))
		w.WriteHeader(http.StatusInternalServerError)
	}

	data, err := serializeSeries(memSeeries)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error()))
	}
	w.Write(data)
	w.WriteHeader(http.StatusOK)
}

func (self *HttpServer) writePoints(w http.ResponseWriter, r *http.Request) {
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

func serializeSeries(memSeries map[string]*protocol.Series) ([]byte, error) {
	serializedSeries := []*SerializedSeries{}

	for _, series := range memSeries {
		columns := []string{"time", "sequence_number"}
		for _, field := range series.Fields {
			columns = append(columns, *field.Name)
		}

		points := [][]interface{}{}
		for _, row := range series.Points {
			rowValues := []interface{}{*row.Timestamp, *row.SequenceNumber}
			for idx, value := range row.Values {
				switch *series.Fields[idx].Type {
				case protocol.FieldDefinition_STRING:
					rowValues = append(rowValues, *value.StringValue)
				case protocol.FieldDefinition_INT32:
					rowValues = append(rowValues, *value.IntValue)
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

	return json.Marshal(serializedSeries)
}

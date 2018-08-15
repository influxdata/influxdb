package http

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"

	"github.com/influxdata/platform/query"
	"github.com/influxdata/platform/query/csv"
	"github.com/julienschmidt/httprouter"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

const (
	queryPath = "/v1/query"

	statsTrailer = "Influx-Query-Statistics"
)

type QueryHandler struct {
	*httprouter.Router

	Logger *zap.Logger

	csvDialect csv.Dialect

	QueryService     query.QueryService
	CompilerMappings query.CompilerMappings
}

// NewQueryHandler returns a new instance of QueryHandler.
func NewQueryHandler() *QueryHandler {
	h := &QueryHandler{
		Router: httprouter.New(),
		csvDialect: csv.Dialect{
			ResultEncoderConfig: csv.DefaultEncoderConfig(),
		},
	}

	h.HandlerFunc("GET", "/ping", h.handlePing)
	h.HandlerFunc("POST", queryPath, h.handlePostQuery)
	return h
}

// handlePing returns a simple response to let the client know the server is running.
func (h *QueryHandler) handlePing(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusNoContent)
}

// handlePostQuery is the HTTP handler for the POST /v1/query route.
func (h *QueryHandler) handlePostQuery(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	var req query.Request
	req.WithCompilerMappings(h.CompilerMappings)
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		EncodeError(ctx, err, w)
		return
	}

	results, err := h.QueryService.Query(ctx, &req)
	if err != nil {
		EncodeError(ctx, err, w)
		return
	}
	// Always cancel the results to free resources.
	// If all results were consumed cancelling does nothing.
	defer results.Cancel()

	// Setup headers
	stats, hasStats := results.(query.Statisticser)
	if hasStats {
		w.Header().Set("Trailer", statsTrailer)
	}

	// NOTE: We do not write out the headers here.
	// It is possible that if the encoding step fails
	// that we can write an error header so long as
	// the encoder did not write anything.
	// As such we rely on the http.ResponseWriter behavior
	// to write an StatusOK header with the first write.

	switch r.Header.Get("Accept") {
	case "text/csv":
		fallthrough
	default:
		h.csvDialect.SetHeaders(w)
		encoder := h.csvDialect.Encoder()
		n, err := encoder.Encode(w, results)
		if err != nil {
			if n == 0 {
				// If the encoder did not write anything, we can write an error header.
				EncodeError(ctx, err, w)
			} else {
				h.Logger.Info("Failed to encode client response",
					zap.Error(err),
				)
			}
		}
	}

	if hasStats {
		data, err := json.Marshal(stats.Statistics())
		if err != nil {
			h.Logger.Info("Failed to encode statistics", zap.Error(err))
			return
		}
		// Write statisitcs trailer
		w.Header().Set(statsTrailer, string(data))
	}
}

// PrometheusCollectors satisifies the prom.PrometheusCollector interface.
func (h *QueryHandler) PrometheusCollectors() []prometheus.Collector {
	// TODO: gather and return relevant metrics.
	return nil
}

type QueryService struct {
	Addr               string
	Token              string
	InsecureSkipVerify bool
}

func (s *QueryService) Query(ctx context.Context, req *query.Request) (query.ResultIterator, error) {
	u, err := newURL(s.Addr, queryPath)
	if err != nil {
		return nil, err
	}
	var body bytes.Buffer
	if err := json.NewEncoder(&body).Encode(req); err != nil {
		return nil, err
	}

	hreq, err := http.NewRequest("POST", u.String(), &body)
	if err != nil {
		return nil, err
	}
	hreq.Header.Set("Authorization", s.Token)
	hreq = hreq.WithContext(ctx)

	hc := newClient(u.Scheme, s.InsecureSkipVerify)
	resp, err := hc.Do(hreq)
	if err != nil {
		return nil, err
	}
	if err := CheckError(resp); err != nil {
		return nil, err
	}

	var decoder query.MultiResultDecoder
	switch resp.Header.Get("Content-Type") {
	case "text/csv":
		fallthrough
	default:
		decoder = csv.NewMultiResultDecoder(csv.ResultDecoderConfig{})
	}
	results, err := decoder.Decode(resp.Body)
	if err != nil {
		return nil, err
	}

	statResults := &statsResultIterator{
		results: results,
		resp:    resp,
	}
	return statResults, nil
}

// statsResultIterator implements query.ResultIterator and query.Statisticser by reading the HTTP trailers.
type statsResultIterator struct {
	results    query.ResultIterator
	resp       *http.Response
	statisitcs query.Statistics
	err        error
}

func (s *statsResultIterator) More() bool {
	return s.results.More()
}

func (s *statsResultIterator) Next() query.Result {
	return s.results.Next()
}

func (s *statsResultIterator) Cancel() {
	s.results.Cancel()
	s.readStats()
}

func (s *statsResultIterator) Err() error {
	err := s.results.Err()
	if err != nil {
		return err
	}
	return s.err
}

func (s *statsResultIterator) Statistics() query.Statistics {
	return s.statisitcs
}

// readStats reads the query statisitcs off the response trailers.
func (s *statsResultIterator) readStats() {
	data := s.resp.Trailer.Get(statsTrailer)
	if data != "" {
		s.err = json.Unmarshal([]byte(data), &s.statisitcs)
	}
}

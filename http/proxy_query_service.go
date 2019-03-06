package http

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/influxdata/flux"
	"github.com/influxdata/flux/iocounter"
	icontext "github.com/influxdata/influxdb/context"
	"github.com/influxdata/influxdb/query"
	"github.com/julienschmidt/httprouter"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

const (
	proxyQueryPath    = "/api/v2/queryproxysvc"
	QueryStatsTrailer = "Influx-Query-Statistics"
)

type ProxyQueryHandler struct {
	*httprouter.Router

	Logger *zap.Logger

	Name string

	ProxyQueryService query.ProxyQueryService

	CompilerMappings flux.CompilerMappings
	DialectMappings  flux.DialectMappings
}

// NewProxyQueryHandler returns a new instance of ProxyQueryHandler.
func NewProxyQueryHandler(name string) *ProxyQueryHandler {
	h := &ProxyQueryHandler{
		Router: NewRouter(),
		Name:   name,
	}

	h.HandlerFunc("GET", "/ping", h.handlePing)
	h.HandlerFunc("POST", proxyQueryPath, h.handlePostQuery)
	return h
}

// handlePing returns a simple response to let the client know the server is running.
func (h *ProxyQueryHandler) handlePing(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusNoContent)
}

// HTTPDialect is an encoding dialect that can write metadata to HTTP headers
type HTTPDialect interface {
	SetHeaders(w http.ResponseWriter)
}

// handlePostQuery handles query requests.
func (h *ProxyQueryHandler) handlePostQuery(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	var req query.ProxyRequest
	req.WithCompilerMappings(h.CompilerMappings)
	req.WithDialectMappings(h.DialectMappings)
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		EncodeError(ctx, err, w)
		return
	}

	w.Header().Set("Trailer", QueryStatsTrailer)

	hd, ok := req.Dialect.(HTTPDialect)
	if !ok {
		EncodeError(ctx, fmt.Errorf("unsupported dialect over HTTP %T", req.Dialect), w)
		return
	}
	hd.SetHeaders(w)

	cw := iocounter.Writer{Writer: w}
	stats, err := h.ProxyQueryService.Query(ctx, &cw, &req)
	if err != nil {
		if cw.Count() == 0 {
			// Only record the error headers IFF nothing has been written to w.
			EncodeError(ctx, err, w)
			return
		}
		h.Logger.Info("Error writing response to client",
			zap.String("handler", h.Name),
			zap.Error(err),
		)
		return
	}

	// Write statistics trailer
	data, err := json.Marshal(stats)
	if err != nil {
		h.Logger.Info("Failed to encode statistics", zap.Error(err))
		return
	}

	w.Header().Set(QueryStatsTrailer, string(data))
}

// PrometheusCollectors satisifies the prom.PrometheusCollector interface.
func (h *ProxyQueryHandler) PrometheusCollectors() []prometheus.Collector {
	// TODO: gather and return relevant metrics.
	return nil
}

type ProxyQueryService struct {
	Addr               string
	Token              string
	InsecureSkipVerify bool
}

// Ping checks to see if the server is responding to a ping request.
func (s *ProxyQueryService) Ping(ctx context.Context) error {
	u, err := newURL(s.Addr, "/ping")
	if err != nil {
		return err
	}

	hreq, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		return err
	}
	SetToken(s.Token, hreq)
	hreq = hreq.WithContext(ctx)

	hc := newClient(u.Scheme, s.InsecureSkipVerify)
	resp, err := hc.Do(hreq)
	if err != nil {
		return err
	}

	return CheckError(resp)
}

func (s *ProxyQueryService) Query(ctx context.Context, w io.Writer, req *query.ProxyRequest) (flux.Statistics, error) {
	u, err := newURL(s.Addr, proxyQueryPath)
	if err != nil {
		return flux.Statistics{}, err
	}
	var body bytes.Buffer
	if err := json.NewEncoder(&body).Encode(req); err != nil {
		return flux.Statistics{}, err
	}

	hreq, err := http.NewRequest("POST", u.String(), &body)
	if err != nil {
		return flux.Statistics{}, err
	}

	token := s.Token
	if token == "" {
		token, err = icontext.GetToken(ctx)
		if err != nil {
			return flux.Statistics{}, err
		}
	}

	SetToken(token, hreq)
	hreq = hreq.WithContext(ctx)

	hc := newClient(u.Scheme, s.InsecureSkipVerify)
	resp, err := hc.Do(hreq)
	if err != nil {
		return flux.Statistics{}, err
	}
	defer resp.Body.Close()
	if err := CheckError(resp); err != nil {
		return flux.Statistics{}, err
	}

	if _, err = io.Copy(w, resp.Body); err != nil {
		return flux.Statistics{}, err
	}

	data := []byte(resp.Trailer.Get(QueryStatsTrailer))
	var stats flux.Statistics
	if err := json.Unmarshal(data, &stats); err != nil {
		return stats, err
	}

	return stats, nil
}

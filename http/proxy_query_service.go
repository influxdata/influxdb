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
	"github.com/influxdata/influxdb"
	influxdbcontext "github.com/influxdata/influxdb/context"
	"github.com/influxdata/influxdb/kit/check"
	"github.com/influxdata/influxdb/kit/tracing"
	"github.com/influxdata/influxdb/query"
	"github.com/julienschmidt/httprouter"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

const (
	proxyQueryPath    = "/queryproxysvc"
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
	const op = "http/handlePostQueryproxysvc"
	ctx := r.Context()

	var req query.ProxyRequest
	req.WithCompilerMappings(h.CompilerMappings)
	req.WithDialectMappings(h.DialectMappings)
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		err := &influxdb.Error{
			Code: influxdb.EInvalid,
			Msg:  "failed to decode request body",
			Op:   op,
			Err:  err,
		}
		EncodeError(ctx, err, w)
		return
	}
	if req.Request.Authorization == nil {
		err := &influxdb.Error{
			Code: influxdb.EUnauthorized,
			Msg:  "authorization is missing in the query request",
			Op:   op,
		}
		EncodeError(ctx, err, w)
		return
	}
	ctx = influxdbcontext.SetAuthorizer(ctx, req.Request.Authorization)

	w.Header().Set("Trailer", QueryStatsTrailer)

	hd, ok := req.Dialect.(HTTPDialect)
	if !ok {
		err := &influxdb.Error{
			Code: influxdb.EInvalid,
			Msg:  fmt.Sprintf("unsupported dialect over HTTP: %T", req.Dialect),
			Op:   op,
		}
		EncodeError(ctx, err, w)
		return
	}
	hd.SetHeaders(w)

	cw := iocounter.Writer{Writer: w}
	stats, err := h.ProxyQueryService.Query(ctx, &cw, &req)
	if err != nil {
		if cw.Count() == 0 {
			// Only record the error headers IFF nothing has been written to w.
			err := &influxdb.Error{
				Code: influxdb.EInternal,
				Msg:  "failed to execute query against proxy service",
				Op:   op,
				Err:  err,
			}
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

func (s *ProxyQueryService) Check(ctx context.Context) check.Response {
	resp := check.Response{Name: "Query Service"}
	if err := s.Ping(ctx); err != nil {
		resp.Status = check.StatusFail
		resp.Message = err.Error()
	} else {
		resp.Status = check.StatusPass
	}

	return resp
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
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()
	u, err := newURL(s.Addr, proxyQueryPath)
	if err != nil {
		return flux.Statistics{}, tracing.LogError(span, err)
	}
	var body bytes.Buffer
	if err := json.NewEncoder(&body).Encode(req); err != nil {
		return flux.Statistics{}, tracing.LogError(span, err)
	}

	hreq, err := http.NewRequest("POST", u.String(), &body)
	if err != nil {
		return flux.Statistics{}, tracing.LogError(span, err)
	}

	hreq = hreq.WithContext(ctx)
	tracing.InjectToHTTPRequest(span, hreq)

	hc := newClient(u.Scheme, s.InsecureSkipVerify)
	resp, err := hc.Do(hreq)
	if err != nil {
		return flux.Statistics{}, tracing.LogError(span, err)
	}
	defer resp.Body.Close()
	if err := CheckError(resp); err != nil {
		return flux.Statistics{}, tracing.LogError(span, err)
	}

	if _, err = io.Copy(w, resp.Body); err != nil {
		return flux.Statistics{}, tracing.LogError(span, err)
	}

	data := []byte(resp.Trailer.Get(QueryStatsTrailer))
	var stats flux.Statistics
	if len(data) > 0 {
		// FIXME(jsternberg): The queryd service always sends these,
		// but envoy does not currently return them properly.
		// https://github.com/influxdata/idpe/issues/2841
		if err := json.Unmarshal(data, &stats); err != nil {
			return stats, tracing.LogError(span, err)
		}
	}

	return stats, nil
}

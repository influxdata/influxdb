package http

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/influxdata/platform/query"
	"github.com/julienschmidt/httprouter"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

const (
	proxyQueryPath = "/v1/query"
)

type ProxyQueryHandler struct {
	*httprouter.Router

	Logger *zap.Logger

	ProxyQueryService query.ProxyQueryService

	CompilerMappings query.CompilerMappings
	DialectMappings  query.DialectMappings
}

// NewProxyQueryHandler returns a new instance of ProxyQueryHandler.
func NewProxyQueryHandler() *ProxyQueryHandler {
	h := &ProxyQueryHandler{
		Router: httprouter.New(),
	}

	h.HandlerFunc("POST", proxyQueryPath, h.handlePostQuery)
	return h
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

	hd, ok := req.Dialect.(HTTPDialect)
	if !ok {
		EncodeError(ctx, fmt.Errorf("unsupported dialect over HTTP %T", req.Dialect), w)
		return
	}
	hd.SetHeaders(w)

	n, err := h.ProxyQueryService.Query(ctx, w, &req)
	if err != nil {
		if n == 0 {
			// Only record the error headers IFF nothing has been written to w.
			EncodeError(ctx, err, w)
			return
		}
		h.Logger.Info("Error writing response to client",
			zap.String("handler", "transpilerde"),
			zap.Error(err),
		)
	}
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

func (s *ProxyQueryService) Query(ctx context.Context, w io.Writer, req *query.ProxyRequest) (int64, error) {
	u, err := newURL(s.Addr, proxyQueryPath)
	if err != nil {
		return 0, err
	}
	var body bytes.Buffer
	if err := json.NewEncoder(&body).Encode(req); err != nil {
		return 0, err
	}

	hreq, err := http.NewRequest("POST", u.String(), &body)
	if err != nil {
		return 0, err
	}
	hreq.Header.Set("Authorization", s.Token)

	hc := newClient(u.Scheme, s.InsecureSkipVerify)
	resp, err := hc.Do(hreq)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	if err := CheckError(resp); err != nil {
		return 0, err
	}
	return io.Copy(w, resp.Body)
}

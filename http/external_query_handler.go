package http

import (
	"fmt"
	"net/http"

	"github.com/influxdata/platform"
	"github.com/influxdata/platform/query"
	"github.com/julienschmidt/httprouter"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

// ExternalQueryHandler implements the /query API endpoint defined in the swagger doc.
// This only implements the POST method and only supports Spec or Flux queries.
type ExternalQueryHandler struct {
	*httprouter.Router

	Logger *zap.Logger

	ProxyQueryService   query.ProxyQueryService
	OrganizationService platform.OrganizationService
}

// NewExternalQueryHandler returns a new instance of QueryHandler.
func NewExternalQueryHandler() *ExternalQueryHandler {
	h := &ExternalQueryHandler{
		Router: httprouter.New(),
		Logger: zap.NewNop(),
	}

	h.HandlerFunc("POST", "/query", h.handlePostQuery)
	return h
}

func (h *ExternalQueryHandler) handlePostQuery(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	// this handler doesn't support authorization checks.
	var nilauth *platform.Authorization
	req, err := decodeProxyQueryRequest(ctx, r, nilauth, h.OrganizationService)
	if err != nil {
		EncodeError(ctx, err, w)
		return
	}

	hd, ok := req.Dialect.(HTTPDialect)
	if !ok {
		EncodeError(ctx, fmt.Errorf("unsupported dialect over HTTP %T", req.Dialect), w)
		return
	}
	hd.SetHeaders(w)

	n, err := h.ProxyQueryService.Query(ctx, w, req)
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
func (h *ExternalQueryHandler) PrometheusCollectors() []prometheus.Collector {
	// TODO: gather and return relevant metrics.
	return nil
}

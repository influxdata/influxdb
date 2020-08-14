package http

import (
	"net/http"

	platform "github.com/influxdata/influxdb/v2"
	influxqld "github.com/influxdata/influxdb/v2/influxql"
	"github.com/influxdata/influxdb/v2/influxql/control"
	"github.com/influxdata/influxdb/v2/query"
	"go.uber.org/zap"
)

// InfluxqlHandler mimics the /query handler from influxdb, but, enriches
// with org and forwards requests to the transpiler service.
type InfluxqlHandler struct {
	*InfluxQLBackend
	LegacyHandlerConfig
	Metrics *control.ControllerMetrics
}

type InfluxQLBackend struct {
	platform.HTTPErrorHandler
	Logger                *zap.Logger
	AuthorizationService  platform.AuthorizationService
	OrganizationService   platform.OrganizationService
	ProxyQueryService     query.ProxyQueryService
	InfluxqldQueryService influxqld.ProxyQueryService
}

// NewInfluxQLBackend constructs an InfluxQLBackend from a LegacyBackend.
func NewInfluxQLBackend(b *LegacyBackend) *InfluxQLBackend {
	return &InfluxQLBackend{
		HTTPErrorHandler:      b.HTTPErrorHandler,
		Logger:                b.Logger.With(zap.String("handler", "influxql")),
		AuthorizationService:  b.AuthorizationService,
		OrganizationService:   b.OrganizationService,
		InfluxqldQueryService: b.InfluxqldQueryService,
	}
}

// NewInfluxQLHandler returns a new instance of InfluxqlHandler to handle influxql v1 queries
func NewInfluxQLHandler(b *InfluxQLBackend, config LegacyHandlerConfig) *InfluxqlHandler {
	return &InfluxqlHandler{
		InfluxQLBackend:     b,
		LegacyHandlerConfig: config,
		Metrics:             control.NewControllerMetrics([]string{}),
	}
}

func (h *InfluxqlHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	h.handleInfluxqldQuery(w, req)
}

// DefaultChunkSize is the default number of points to write in
// one chunk.
const DefaultChunkSize = 10000

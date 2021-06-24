package legacy

import (
	http2 "net/http"

	"github.com/influxdata/influxdb/v2/kit/platform/errors"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/http/metric"
	"github.com/influxdata/influxdb/v2/influxql"
	"github.com/influxdata/influxdb/v2/kit/cli"
	"github.com/influxdata/influxdb/v2/query"
	"github.com/influxdata/influxdb/v2/storage"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

// Handler is a collection of all the service handlers.
type Handler struct {
	errors.HTTPErrorHandler
	PointsWriterHandler *WriteHandler
	PingHandler         *PingHandler
	InfluxQLHandler     *InfluxqlHandler
}

type Backend struct {
	errors.HTTPErrorHandler
	Logger            *zap.Logger
	MaxBatchSizeBytes int64

	WriteEventRecorder    metric.EventRecorder
	AuthorizationService  influxdb.AuthorizationService
	OrganizationService   influxdb.OrganizationService
	BucketService         influxdb.BucketService
	PointsWriter          storage.PointsWriter
	DBRPMappingServiceV2  influxdb.DBRPMappingServiceV2
	ProxyQueryService     query.ProxyQueryService
	InfluxqldQueryService influxql.ProxyQueryService
}

// HandlerConfig provides configuration for the legacy handler.
type HandlerConfig struct {
	Version           string
	DefaultRoutingKey string
}

func NewHandlerConfig() *HandlerConfig {
	return &HandlerConfig{Version: influxdb.GetBuildInfo().Version}
}

// Opts returns the CLI options for use with kit/cli.
// Currently set values on c are provided as the defaults.
func (c *HandlerConfig) Opts() []cli.Opt {
	return []cli.Opt{
		{
			DestP:   &c.DefaultRoutingKey,
			Flag:    "influxql-default-routing-key",
			Default: "defaultQueue",
			Desc:    "Default routing key for publishing new query requests",
		},
	}
}

func (h *Handler) ServeHTTP(w http2.ResponseWriter, r *http2.Request) {
	if r.URL.Path == "/write" {
		h.PointsWriterHandler.ServeHTTP(w, r)
		return
	}

	if r.URL.Path == "/ping" {
		h.PingHandler.ServeHTTP(w, r)
		return
	}

	if r.URL.Path == "/query" {
		h.InfluxQLHandler.ServeHTTP(w, r)
		return
	}

	w.WriteHeader(http2.StatusNotFound)
}

func (h *Handler) PrometheusCollectors() []prometheus.Collector {
	return h.InfluxQLHandler.PrometheusCollectors()
}

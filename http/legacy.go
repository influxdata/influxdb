package http

import (
	"net/http"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/http/metric"
	"github.com/influxdata/influxdb/v2/influxql"
	"github.com/influxdata/influxdb/v2/kit/cli"
	"github.com/influxdata/influxdb/v2/query"
	"github.com/influxdata/influxdb/v2/storage"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

// LegacyHandler is a collection of all the service handlers.
type LegacyHandler struct {
	influxdb.HTTPErrorHandler
	PointsWriterHandler *WriteHandler
	PingHandler         *PingHandler
	InfluxQLHandler     *InfluxqlHandler
}

type LegacyBackend struct {
	influxdb.HTTPErrorHandler
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

// NewLegacyBackend constructs a legacy backend from an api backend.
func NewLegacyBackend(b *APIBackend) *LegacyBackend {
	return &LegacyBackend{
		HTTPErrorHandler: b.HTTPErrorHandler,
		Logger:           b.Logger,
		// TODO(sgc): /write support
		//MaxBatchSizeBytes:     b.APIBackend.MaxBatchSizeBytes,
		AuthorizationService:  b.AuthorizationService,
		OrganizationService:   b.OrganizationService,
		BucketService:         b.BucketService,
		PointsWriter:          b.PointsWriter,
		DBRPMappingServiceV2:  b.DBRPService,
		ProxyQueryService:     b.InfluxQLService,
		InfluxqldQueryService: b.InfluxqldService,
		WriteEventRecorder:    b.WriteEventRecorder,
	}
}

// LegacyHandlerConfig provides configuration for the legacy handler.
type LegacyHandlerConfig struct {
	Version           string
	DefaultRoutingKey string
}

func NewLegacyHandlerConfig() *LegacyHandlerConfig {
	return &LegacyHandlerConfig{}
}

// Opts returns the CLI options for use with kit/cli.
// Currently set values on c are provided as the defaults.
func (c *LegacyHandlerConfig) Opts() []cli.Opt {
	return []cli.Opt{
		{
			DestP:   &c.DefaultRoutingKey,
			Flag:    "influxql-default-routing-key",
			Default: "defaultQueue",
			Desc:    "Default routing key for publishing new query requests",
		},
	}
}

// NewLegacyHandler constructs a legacy handler from a backend.
func NewLegacyHandler(b *LegacyBackend, config LegacyHandlerConfig) *LegacyHandler {
	h := &LegacyHandler{
		HTTPErrorHandler: b.HTTPErrorHandler,
	}

	//pointsWriterBackend := NewPointsWriterBackend(b)
	//h.PointsWriterHandler = NewWriterHandler(pointsWriterBackend,
	//	WithMaxBatchSizeBytes(b.MaxBatchSizeBytes))

	influxqlBackend := NewInfluxQLBackend(b)
	// TODO(desa): what to do for auth here?
	h.InfluxQLHandler = NewInfluxQLHandler(influxqlBackend, config)

	h.PingHandler = NewPingHandler(config.Version)
	return h
}

func (h *LegacyHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
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

	w.WriteHeader(http.StatusNotFound)
}

func (h *LegacyHandler) PrometheusCollectors() []prometheus.Collector {
	return h.InfluxQLHandler.PrometheusCollectors()
}

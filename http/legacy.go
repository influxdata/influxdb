package http

import (
	"github.com/influxdata/influxdb/v2/http/legacy"
)

// newLegacyBackend constructs a legacy backend from an api backend.
func newLegacyBackend(b *APIBackend) *legacy.Backend {
	return &legacy.Backend{
		HTTPErrorHandler: b.HTTPErrorHandler,
		Logger:           b.Logger,
		// TODO(sgc): /write support
		//MaxBatchSizeBytes:     b.APIBackend.MaxBatchSizeBytes,
		AuthorizationService:  b.AuthorizationService,
		OrganizationService:   b.OrganizationService,
		BucketService:         b.BucketService,
		PointsWriter:          b.PointsWriter,
		DBRPMappingService:    b.DBRPService,
		ProxyQueryService:     b.InfluxQLService,
		InfluxqldQueryService: b.InfluxqldService,
		WriteEventRecorder:    b.WriteEventRecorder,
	}
}

// newLegacyHandler constructs a legacy handler from a backend.
func newLegacyHandler(b *legacy.Backend, config legacy.HandlerConfig) *legacy.Handler {
	h := &legacy.Handler{
		HTTPErrorHandler: b.HTTPErrorHandler,
	}

	pointsWriterBackend := legacy.NewPointsWriterBackend(b)
	h.PointsWriterHandler = legacy.NewWriterHandler(pointsWriterBackend, legacy.WithMaxBatchSizeBytes(b.MaxBatchSizeBytes))

	influxqlBackend := legacy.NewInfluxQLBackend(b)
	h.InfluxQLHandler = legacy.NewInfluxQLHandler(influxqlBackend, config)

	h.PingHandler = legacy.NewPingHandler(config.Version)
	return h
}

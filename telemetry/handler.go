package telemetry

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"time"

	"go.uber.org/zap"

	"github.com/influxdata/influxdb/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
)

const (
	// DefaultTimeout is the length of time servicing the metrics before canceling.
	DefaultTimeout = 10 * time.Second
	// DefaultMaxBytes is the largest request body read.
	DefaultMaxBytes = 1024000
)

var (
	// ErrMetricsTimestampPresent is returned when the prometheus metrics has timestamps set.
	// Not sure why, but, pushgateway does not allow timestamps.
	ErrMetricsTimestampPresent = fmt.Errorf("pushed metrics must not have timestamp")
)

// PushGateway handles receiving prometheus push metrics and forwards them to the Store.
type PushGateway struct {
	Timeout  time.Duration // handler returns after this duration with an error; defaults to 5 seconds
	MaxBytes int64         // maximum number of bytes to read from the body; defaults to 1024000
	Logger   *zap.Logger

	Store        Store
	Transformers []prometheus.Transformer
}

// NewPushGateway constructs the PushGateway.
func NewPushGateway(logger *zap.Logger, store Store, xforms ...prometheus.Transformer) *PushGateway {
	if len(xforms) == 0 {
		xforms = append(xforms, &AddTimestamps{})
	}
	return &PushGateway{
		Store:        store,
		Transformers: xforms,
		Logger:       logger,
		Timeout:      DefaultTimeout,
		MaxBytes:     DefaultMaxBytes,
	}
}

// Handler accepts prometheus metrics send via the Push client and sends those
// metrics into the store.
func (p *PushGateway) Handler(w http.ResponseWriter, r *http.Request) {
	if p.Timeout == 0 {
		p.Timeout = DefaultTimeout
	}

	if p.MaxBytes == 0 {
		p.MaxBytes = DefaultMaxBytes
	}

	ctx, cancel := context.WithTimeout(
		r.Context(),
		p.Timeout,
	)
	defer cancel()

	r = r.WithContext(ctx)

	mfs, err := decodePostMetricsRequest(r, p.MaxBytes)
	if err != nil {
		p.Logger.Error("unable to decode metrics", zap.Error(err))
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if err := valid(mfs); err != nil {
		p.Logger.Error("invalid metrics", zap.Error(err))
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	for _, transformer := range p.Transformers {
		mfs = transformer.Transform(mfs)
	}

	data, err := prometheus.EncodeJSON(mfs)
	if err != nil {
		p.Logger.Error("unable to encode metric families", zap.Error(err))
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if err := p.Store.WriteMessage(ctx, data); err != nil {
		p.Logger.Error("unable to write to store", zap.Error(err))
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusAccepted)
}

func decodePostMetricsRequest(req *http.Request, maxBytes int64) ([]*dto.MetricFamily, error) {
	format := expfmt.ResponseFormat(req.Header)
	if format == expfmt.FmtUnknown {
		return nil, fmt.Errorf("unknown format metrics format")
	}

	// protect against reading too many bytes
	r := io.LimitReader(req.Body, maxBytes)
	defer req.Body.Close()

	mfs, err := prometheus.DecodeExpfmt(r, format)
	if err != nil {
		return nil, err
	}
	return mfs, nil
}

// prom's pushgateway does not allow timestamps for some reason.
func valid(mfs []*dto.MetricFamily) error {
	// Checks if any timestamps have been specified.
	for i := range mfs {
		for j := range mfs[i].Metric {
			if mfs[i].Metric[j].TimestampMs != nil {
				return ErrMetricsTimestampPresent
			}
		}
	}
	return nil
}

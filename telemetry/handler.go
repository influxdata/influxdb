package telemetry

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/influxdata/influxdb/v2/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"go.uber.org/zap"
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
// If Format is not set, the format of the inbound metrics are used.
type PushGateway struct {
	Timeout  time.Duration // handler returns after this duration with an error; defaults to 5 seconds
	MaxBytes int64         // maximum number of bytes to read from the body; defaults to 1024000
	log      *zap.Logger

	Store        Store
	Transformers []prometheus.Transformer

	Encoder prometheus.Encoder
}

// NewPushGateway constructs the PushGateway.
func NewPushGateway(log *zap.Logger, store Store, xforms ...prometheus.Transformer) *PushGateway {
	if len(xforms) == 0 {
		xforms = append(xforms, &AddTimestamps{})
	}
	return &PushGateway{
		Store:        store,
		Transformers: xforms,
		log:          log,
		Timeout:      DefaultTimeout,
		MaxBytes:     DefaultMaxBytes,
	}
}

// Handler accepts prometheus metrics send via the Push client and sends those
// metrics into the store.
func (p *PushGateway) Handler(w http.ResponseWriter, r *http.Request) {
	// redirect to agreement to give our users information about
	// this collected data.
	switch r.Method {
	case http.MethodGet, http.MethodHead:
		http.Redirect(w, r, "https://www.influxdata.com/telemetry", http.StatusSeeOther)
		return
	case http.MethodPost, http.MethodPut:
	default:
		w.Header().Set("Allow", "GET, HEAD, PUT, POST")
		http.Error(w,
			http.StatusText(http.StatusMethodNotAllowed),
			http.StatusMethodNotAllowed,
		)
		return
	}

	if p.Timeout == 0 {
		p.Timeout = DefaultTimeout
	}

	if p.MaxBytes == 0 {
		p.MaxBytes = DefaultMaxBytes
	}

	if p.Encoder == nil {
		p.Encoder = &prometheus.Expfmt{
			Format: expfmt.FmtText,
		}
	}

	ctx, cancel := context.WithTimeout(
		r.Context(),
		p.Timeout,
	)
	defer cancel()

	r = r.WithContext(ctx)
	defer r.Body.Close()

	format, err := metricsFormat(r.Header)
	if err != nil {
		p.log.Error("Metrics format not support", zap.Error(err))
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	mfs, err := decodePostMetricsRequest(r.Body, format, p.MaxBytes)
	if err != nil {
		p.log.Error("Unable to decode metrics", zap.Error(err))
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if err := valid(mfs); err != nil {
		p.log.Error("Invalid metrics", zap.Error(err))
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	for _, transformer := range p.Transformers {
		mfs = transformer.Transform(mfs)
	}

	data, err := p.Encoder.Encode(mfs)
	if err != nil {
		p.log.Error("Unable to encode metric families", zap.Error(err))
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if err := p.Store.WriteMessage(ctx, data); err != nil {
		p.log.Error("Unable to write to store", zap.Error(err))
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusAccepted)
}

func metricsFormat(headers http.Header) (expfmt.Format, error) {
	format := expfmt.ResponseFormat(headers)
	if format == expfmt.FmtUnknown {
		return "", fmt.Errorf("unknown format metrics format")
	}
	return format, nil
}

func decodePostMetricsRequest(body io.Reader, format expfmt.Format, maxBytes int64) ([]*dto.MetricFamily, error) {
	// protect against reading too many bytes
	r := io.LimitReader(body, maxBytes)

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

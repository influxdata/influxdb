package metric

import (
	"fmt"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

type (
	// using for a struct here as it will be extended
	fnOpts struct {
		method string
		start  time.Time
		err    error
	}

	counterFn func(vec *prometheus.CounterVec, o fnOpts)

	histogramFn func(vec *prometheus.HistogramVec, o fnOpts)

	newVecOpts struct {
		help       string
		labelNames []string

		counterFn   counterFn
		histogramFn histogramFn
	}
)

type metricOpts struct {
	namespace        string
	service          string
	serviceSuffix    string
	counterMetrics   map[string]newVecOpts
	histogramMetrics map[string]newVecOpts
}

func (o metricOpts) serviceName() string {
	if o.serviceSuffix != "" {
		return fmt.Sprintf("%s_%s", o.service, o.serviceSuffix)
	}
	return o.service
}

// ClientOptFn is an option used by a metric middleware.
type ClientOptFn func(*metricOpts)

// WithSuffix returns a metric option that applies a suffix to the service name of the metric.
func WithSuffix(suffix string) ClientOptFn {
	return func(opts *metricOpts) {
		opts.serviceSuffix = suffix
	}
}

func ApplyMetricOpts(opts ...ClientOptFn) *metricOpts {
	o := metricOpts{}
	for _, opt := range opts {
		opt(&o)
	}
	return &o
}

func (o *metricOpts) ApplySuffix(prefix string) string {
	if o.serviceSuffix != "" {
		return fmt.Sprintf("%s_%s", prefix, o.serviceSuffix)
	}
	return prefix
}

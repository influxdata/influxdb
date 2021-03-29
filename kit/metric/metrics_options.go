package metric

import (
	"fmt"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

type (
	// CollectFnOpts provides arguments to the collect operation of a metric.
	CollectFnOpts struct {
		Method          string
		Start           time.Time
		Err             error
		AdditionalProps map[string]interface{}
	}

	CounterFn func(vec *prometheus.CounterVec, o CollectFnOpts)

	HistogramFn func(vec *prometheus.HistogramVec, o CollectFnOpts)

	// VecOpts expands on the
	VecOpts struct {
		Name       string
		Help       string
		LabelNames []string

		CounterFn   CounterFn
		HistogramFn HistogramFn
	}
)

type metricOpts struct {
	namespace        string
	service          string
	serviceSuffix    string
	counterMetrics   map[string]VecOpts
	histogramMetrics map[string]VecOpts
}

func (o metricOpts) serviceName() string {
	if o.serviceSuffix != "" {
		return fmt.Sprintf("%s_%s", o.service, o.serviceSuffix)
	}
	return o.service
}

// ClientOptFn is an option used by a metric middleware.
type ClientOptFn func(*metricOpts)

// WithVec sets a new counter vector to be collected.
func WithVec(opts VecOpts) ClientOptFn {
	return func(o *metricOpts) {
		if opts.CounterFn != nil {
			if o.counterMetrics == nil {
				o.counterMetrics = make(map[string]VecOpts)
			}
			o.counterMetrics[opts.Name] = opts
		}
	}
}

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

package tenant

import "fmt"

type metricOpts struct {
	serviceSuffix string
}

func defaultOpts() *metricOpts {
	return &metricOpts{}
}

func (o *metricOpts) applySuffix(prefix string) string {
	if o.serviceSuffix != "" {
		return fmt.Sprintf("%s_%s", prefix, o.serviceSuffix)
	}
	return prefix
}

// MetricsOption is an option used by a metric middleware.
type MetricsOption func(*metricOpts)

// WithSuffix returns a metric option that applies a suffix to the service name of the metric.
func WithSuffix(suffix string) MetricsOption {
	return func(opts *metricOpts) {
		opts.serviceSuffix = suffix
	}
}

func applyOpts(opts ...MetricsOption) *metricOpts {
	o := defaultOpts()
	for _, opt := range opts {
		opt(o)
	}
	return o
}

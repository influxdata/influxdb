package metric

import (
	"fmt"
	"time"

	"github.com/influxdata/influxdb/v2"
	"github.com/prometheus/client_golang/prometheus"
)

// REDClient is a metrics client for collection RED metrics.
type REDClient struct {
	// RED metrics
	reqs *prometheus.CounterVec
	errs *prometheus.CounterVec
	durs *prometheus.HistogramVec
}

// New creates a new REDClient.
func New(reg prometheus.Registerer, service string) *REDClient {
	// MiddlewareMetrics is a metrics service middleware for the notification endpoint service.
	const namespace = "service"

	reqs := prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: service,
		Name:      "call_total",
		Help:      fmt.Sprintf("Number of calls to the %s service", service),
	}, []string{"method"})

	errs := prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: service,
		Name:      "error_total",
		Help:      fmt.Sprintf("Number of errors encountered when calling the %s service", service),
	}, []string{"method", "code"})

	durs := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: namespace,
		Subsystem: service,
		Name:      "duration",
		Help:      fmt.Sprintf("Duration of %s service calls", service),
	}, []string{"method"})

	reg.MustRegister(reqs, errs, durs)

	return &REDClient{
		reqs: reqs,
		errs: errs,
		durs: durs,
	}
}

// Record returns a record fn that is called on any given return err. If an error is encountered
// it will register the err metric. The err is never altered.
func (c *REDClient) Record(method string) func(error) error {
	start := time.Now()
	return func(err error) error {
		c.reqs.With(prometheus.Labels{"method": method})

		if err != nil {
			c.errs.With(prometheus.Labels{
				"method": method,
				"code":   influxdb.ErrorCode(err),
			}).Inc()
		}

		c.durs.With(prometheus.Labels{"method": method}).Observe(time.Since(start).Seconds())

		return err
	}
}

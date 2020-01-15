package metrics

import (
	"fmt"
	"time"

	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/kit/prom"
	"github.com/prometheus/client_golang/prometheus"
)

// Client is a service metrics client.
type Client struct {
	// RED metrics
	reqs *prometheus.CounterVec
	errs *prometheus.CounterVec
	durs *prometheus.HistogramVec
}

// MustNew creates a new service metric client or dies trying.
func MustNew(reg *prom.Registry, namespace, subsystem string) *Client {
	c, err := New(reg, namespace, subsystem)
	if err != nil {
		panic(err)
	}
	return c
}

// New creates a new service metric client.
func New(reg *prom.Registry, namespace, subsystem string) (*Client, error) {
	reqs := prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: subsystem,
		Name:      "call_total",
		Help:      fmt.Sprintf("Number of calls to the %s %s", subsystem, namespace),
	}, []string{"method"})

	errs := prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: subsystem,
		Name:      "error_total",
		Help:      fmt.Sprintf("Number of errors encountered when calling the %s %s", subsystem, namespace),
	}, []string{"method ", "code"})

	durs := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: namespace,
		Subsystem: subsystem,
		Name:      "duration",
		Help:      fmt.Sprintf("Duration of notification %s %s calls", subsystem, namespace),
	}, []string{"method"})

	for _, vec := range []prometheus.Collector{reqs, errs, durs} {
		if err := reg.Register(vec); err != nil {
			return nil, err
		}
	}

	return &Client{
		reqs: reqs,
		errs: errs,
		durs: durs,
	}, nil
}

// RED captures all request, error, and duration(RED) metrics.
func (c *Client) RED(method string) func(error) error {
	start := time.Now()
	return func(err error) error {
		c.reqs.With(prometheus.Labels{"method": method})

		if err != nil {
			code := "unknown"
			if errCode := influxdb.ErrorCode(err); errCode != "" {
				code = errCode
			}
			c.errs.With(prometheus.Labels{
				"method": method,
				"code":   code,
			}).Inc()
		}

		c.durs.With(prometheus.Labels{"method": method}).Observe(time.Since(start).Seconds())

		return err
	}
}

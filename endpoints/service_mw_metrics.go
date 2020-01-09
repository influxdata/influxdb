package endpoints

import (
	"context"
	"time"

	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/kit/prom"
	"github.com/prometheus/client_golang/prometheus"
)

type mwMetrics struct {
	// RED metrics
	reqs *prometheus.CounterVec
	errs *prometheus.CounterVec
	durs *prometheus.HistogramVec

	next influxdb.NotificationEndpointService
}

var _ influxdb.NotificationEndpointService = (*mwTracing)(nil)

// MiddlewareMetrics is a metrics service middleware for the notification endpoint service.
func MiddlewareMetrics(reg *prom.Registry) ServiceMW {
	const namespace = "service"
	const subsystem = "endpoints"

	reqs := prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: subsystem,
		Name:      "call_total",
		Help:      "Number of calls to the notification endpoint service",
	}, []string{"method"})

	errs := prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: subsystem,
		Name:      "error_total",
		Help:      "Number of errors encountered when calling the notification endpoint service",
	}, []string{"method ", "code"})

	durs := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: namespace,
		Subsystem: subsystem,
		Name:      "duration",
		Help:      "Duration of notification endpoint service calls",
	}, []string{"method"})

	reg.MustRegister(reqs, errs, durs)

	return func(svc influxdb.NotificationEndpointService) influxdb.NotificationEndpointService {
		return &mwMetrics{
			reqs: reqs,
			errs: errs,
			durs: durs,
			next: svc,
		}
	}
}

func (mw *mwMetrics) Delete(ctx context.Context, id influxdb.ID) error {
	return mw.updateMetrics("delete")(mw.next.Delete(ctx, id))
}

func (mw *mwMetrics) FindByID(ctx context.Context, id influxdb.ID) (influxdb.NotificationEndpoint, error) {
	m := mw.updateMetrics("find_by_id")
	edp, err := mw.next.FindByID(ctx, id)
	return edp, m(err)
}

func (mw *mwMetrics) Find(ctx context.Context, f influxdb.NotificationEndpointFilter, opt ...influxdb.FindOptions) ([]influxdb.NotificationEndpoint, error) {
	m := mw.updateMetrics("find")
	edps, err := mw.next.Find(ctx, f, opt...)
	return edps, m(err)
}

func (mw *mwMetrics) Create(ctx context.Context, userID influxdb.ID, endpoint influxdb.NotificationEndpoint) error {
	return mw.updateMetrics("create")(mw.next.Create(ctx, userID, endpoint))
}

func (mw *mwMetrics) Update(ctx context.Context, update influxdb.EndpointUpdate) (influxdb.NotificationEndpoint, error) {
	opName := "update_" + update.UpdateType
	m := mw.updateMetrics(opName)
	edp, err := mw.next.Update(ctx, update)
	return edp, m(err)
}

func (mw *mwMetrics) updateMetrics(method string) func(error) error {
	start := time.Now()
	return func(err error) error {
		mw.reqs.With(prometheus.Labels{"method": method})

		if err != nil {
			code := "unknown"
			if errCode := influxdb.ErrorCode(err); errCode != "" {
				code = errCode
			}
			mw.errs.With(prometheus.Labels{
				"method": method,
				"code":   code,
			}).Inc()
		}

		mw.durs.With(prometheus.Labels{"method": method}).Observe(time.Since(start).Seconds())

		return err
	}
}

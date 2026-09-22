package prometheus_test

import (
	"context"
	"testing"

	"github.com/influxdata/influxdb/v2/http/metric"
	"github.com/influxdata/influxdb/v2/kit/prom"
	"github.com/influxdata/influxdb/v2/kit/prom/promtest"
	"github.com/influxdata/influxdb/v2/prometheus"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
)

func TestEventRecorder_UserResponseBytes(t *testing.T) {
	ctx := context.Background()
	events := []metric.Event{
		{OrgID: 1, UserID: 10, Endpoint: "/query", ResponseBytes: 100, Status: 200},
		{OrgID: 1, UserID: 10, Endpoint: "/query", ResponseBytes: 50, Status: 200},
		{OrgID: 1, UserID: 10, Endpoint: "/api/v2/query", ResponseBytes: 7, Status: 200},
		{OrgID: 1, UserID: 11, Endpoint: "/query", ResponseBytes: 3, Status: 500},
		// Authorizer without a user ID: nothing to attribute to.
		{OrgID: 1, Endpoint: "/query", ResponseBytes: 69, Status: 200},
	}

	t.Run("disabled by default", func(t *testing.T) {
		reg := prom.NewRegistry(zaptest.NewLogger(t))
		r := prometheus.NewEventRecorder("query")
		reg.MustRegister(r.PrometheusCollectors()...)
		for _, e := range events {
			r.Record(ctx, e)
		}

		for _, mf := range promtest.MustGather(t, reg) {
			require.NotEqual(t, "http_query_user_response_bytes", mf.GetName())
		}
		// Org-level metrics are unaffected.
		m := promtest.MustFindMetric(t, promtest.MustGather(t, reg), "http_query_response_bytes",
			map[string]string{"org_id": "0000000000000001", "endpoint": "/query", "status": "200"})
		require.Equal(t, float64(219), m.GetCounter().GetValue())
	})

	t.Run("enabled", func(t *testing.T) {
		reg := prom.NewRegistry(zaptest.NewLogger(t))
		r := prometheus.NewEventRecorder("query", prometheus.WithUserResponseBytes())
		reg.MustRegister(r.PrometheusCollectors()...)
		for _, e := range events {
			r.Record(ctx, e)
		}

		mfs := promtest.MustGather(t, reg)
		m := promtest.MustFindMetric(t, mfs, "http_query_user_response_bytes",
			map[string]string{"user_id": "000000000000000a", "endpoint": "/query"})
		require.Equal(t, float64(150), m.GetCounter().GetValue())

		m = promtest.MustFindMetric(t, mfs, "http_query_user_response_bytes",
			map[string]string{"user_id": "000000000000000a", "endpoint": "/api/v2/query"})
		require.Equal(t, float64(7), m.GetCounter().GetValue())

		m = promtest.MustFindMetric(t, mfs, "http_query_user_response_bytes",
			map[string]string{"user_id": "000000000000000b", "endpoint": "/query"})
		require.Equal(t, float64(3), m.GetCounter().GetValue())

		// The unattributed request lands in the aggregate counter only.
		for _, mf := range mfs {
			if mf.GetName() != "http_query_user_response_bytes" {
				continue
			}
			for _, m := range mf.GetMetric() {
				for _, l := range m.GetLabel() {
					if l.GetName() == "user_id" {
						require.NotEmpty(t, l.GetValue(), "per-user series recorded with empty user_id")
					}
				}
			}
		}
		m = promtest.MustFindMetric(t, mfs, "http_query_response_bytes",
			map[string]string{"org_id": "0000000000000001", "endpoint": "/query", "status": "200"})
		require.Equal(t, float64(219), m.GetCounter().GetValue())
	})
}

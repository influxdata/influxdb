package bolt_test

import (
	"testing"

	"github.com/influxdata/influxdb/v2/kit/prom"
	"github.com/influxdata/influxdb/v2/kit/prom/promtest"
	"go.uber.org/zap/zaptest"
)

func TestInitialMetrics(t *testing.T) {
	client, teardown, err := NewTestClient(t)
	if err != nil {
		t.Fatalf("unable to setup bolt client: %v", err)
	}
	defer teardown()

	reg := prom.NewRegistry(zaptest.NewLogger(t))
	reg.MustRegister(client)

	mfs, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}

	metrics := map[string]int{
		"influxdb_organizations_total": 0,
		"influxdb_buckets_total":       0,
		"influxdb_users_total":         0,
		"influxdb_tokens_total":        0,
		"influxdb_dashboards_total":    0,
		"boltdb_reads_total":           0,
	}
	for name, count := range metrics {
		c := promtest.MustFindMetric(t, mfs, name, nil)
		if got := c.GetCounter().GetValue(); int(got) != count {
			t.Errorf("expected %s counter to be %d, got %v", name, count, got)
		}
	}
}

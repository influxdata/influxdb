package bolt_test

import (
	"context"
	"testing"

	platform "github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/kit/prom"
	"github.com/influxdata/influxdb/kit/prom/promtest"
)

func TestInitialMetrics(t *testing.T) {
	client, teardown, err := NewTestClient()
	if err != nil {
		t.Fatalf("unable to setup bolt client: %v", err)
	}
	defer teardown()

	reg := prom.NewRegistry()
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

func TestMetrics_Onboarding(t *testing.T) {
	client, teardown, err := NewTestClient()
	if err != nil {
		t.Fatalf("unable to setup bolt client: %v", err)
	}
	defer teardown()

	reg := prom.NewRegistry()
	reg.MustRegister(client)

	ctx := context.Background()
	if _, _ = client.Generate(ctx,
		&platform.OnboardingRequest{
			User:     "u1",
			Password: "p1",
			Org:      "o1",
			Bucket:   "b1",
		}); err != nil {
		t.Fatalf("unable to setup onboarding: %v", err)
	}

	_ = client.CreateDashboard(ctx, &platform.Dashboard{
		OrganizationID: platform.ID(1),
	})

	mfs, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}

	metrics := map[string]int{
		"influxdb_organizations_total": 1,
		"influxdb_buckets_total":       1,
		"influxdb_users_total":         1,
		"influxdb_tokens_total":        1,
		"influxdb_dashboards_total":    1,
		"boltdb_reads_total":           1,
	}

	for name, count := range metrics {
		c := promtest.MustFindMetric(t, mfs, name, nil)
		if got := c.GetCounter().GetValue(); int(got) != count {
			t.Errorf("expected %s counter to be %d, got %v", name, count, got)
		}
	}
}

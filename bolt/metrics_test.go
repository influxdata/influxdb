package bolt_test

import (
	"context"
	"testing"

	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/kv"
	"github.com/influxdata/influxdb/kit/prom"
	"github.com/influxdata/influxdb/kit/prom/promtest"
)

func TestInitialMetrics(t *testing.T) {
	client, teardown, err := NewTestKVStore()
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
		"influxdb_authorizations_total":    0,
		"influxdb_buckets_total":           0,
		"influxdb_dashboards_total":        0,
		"influxdb_notificationRules_total": 0,
		"influxdb_orgs_total":              0,
		"influxdb_telegrafs_total":         0,
		"influxdb_scrapers_total":          0,
		"influxdb_users_total":             0,
		"influxdb_labels_total":            0,
		"influxdb_variables_total":         0,
	}
	for name, count := range metrics {
		c := promtest.MustFindMetric(t, mfs, name, nil)
		if got := c.GetCounter().GetValue(); int(got) != count {
			t.Errorf("expected %s counter to be %d, got %v", name, count, got)
		}
	}
}

func TestKVStoreMetrics_Onboarding(t *testing.T) {
	client, teardown, err := NewTestKVStore()
	if err != nil {
		t.Fatalf("unable to setup bolt client: %v", err)
	}
	defer teardown()

	reg := prom.NewRegistry()
	reg.MustRegister(client)

	ctx := context.Background()
	svc := kv.NewService(client)
	svc.Initialize(ctx)
	req := &influxdb.OnboardingRequest{
		User:     "u1",
		Password: "password1",
		Org:      "o1",
		Bucket:   "b1",
	}

	_, err = svc.Generate(ctx, req)
	if err != nil {
		t.Fatalf("unable to setup onboarding: %v", err)
	}

	err = svc.CreateDashboard(ctx, &influxdb.Dashboard{
		OrganizationID: influxdb.ID(1),
	})
	if err != nil {
		t.Fatalf("unable to create dashboard: %v", err)
	}

	mfs, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}

	metrics := map[string]int{
		"influxdb_authorizations_total": 1,
		"influxdb_buckets_total":        1,
		"influxdb_dashboards_total":     1,
		"influxdb_orgs_total":           1,
		"influxdb_users_total":          1,
		"boltdb_reads_total":            1,
	}

	for name, count := range metrics {
		c := promtest.MustFindMetric(t, mfs, name, nil)
		if got := c.GetCounter().GetValue(); int(got) != count {
			t.Errorf("expected %s counter to be %d, got %v", name, count, got)
		}
	}
}

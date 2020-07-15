package kv_test

import (
	"context"
	"testing"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kv"
	influxdbtesting "github.com/influxdata/influxdb/v2/testing"
	"go.uber.org/zap/zaptest"
)

func TestBoltOrganizationService(t *testing.T) {
	influxdbtesting.OrganizationService(initBoltOrganizationService, t)
}

func initBoltOrganizationService(f influxdbtesting.OrganizationFields, t *testing.T) (influxdb.OrganizationService, string, func()) {
	s, closeBolt, err := NewTestBoltStore(t)
	if err != nil {
		t.Fatalf("failed to create new kv store: %v", err)
	}

	svc, op, closeSvc := initOrganizationService(s, f, t)
	return svc, op, func() {
		closeSvc()
		closeBolt()
	}
}

func initOrganizationService(s kv.SchemaStore, f influxdbtesting.OrganizationFields, t *testing.T) (influxdb.OrganizationService, string, func()) {
	ctx := context.Background()
	svc := kv.NewService(zaptest.NewLogger(t), s)
	svc.OrgBucketIDs = f.OrgBucketIDs
	svc.IDGenerator = f.IDGenerator
	svc.TimeGenerator = f.TimeGenerator
	if f.TimeGenerator == nil {
		svc.TimeGenerator = influxdb.RealTimeGenerator{}
	}

	for _, u := range f.Organizations {
		if err := svc.PutOrganization(ctx, u); err != nil {
			t.Fatalf("failed to populate organizations")
		}
	}

	return svc, kv.OpPrefix, func() {
		for _, u := range f.Organizations {
			if err := svc.DeleteOrganization(ctx, u.ID); err != nil {
				t.Logf("failed to remove organizations: %v", err)
			}
		}
	}
}

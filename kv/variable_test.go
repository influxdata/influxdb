package kv_test

import (
	"context"
	"testing"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kv"
	"github.com/influxdata/influxdb/v2/mock"
	influxdbtesting "github.com/influxdata/influxdb/v2/testing"
	"go.uber.org/zap/zaptest"
)

func TestBoltVariableService(t *testing.T) {
	influxdbtesting.VariableService(initBoltVariableService, t)
}

func initBoltVariableService(f influxdbtesting.VariableFields, t *testing.T) (influxdb.VariableService, string, func()) {
	s, closeBolt, err := NewTestBoltStore(t)
	if err != nil {
		t.Fatalf("failed to create new kv store: %v", err)
	}

	svc, op, closeSvc := initVariableService(s, f, t)
	return svc, op, func() {
		closeSvc()
		closeBolt()
	}
}

func initVariableService(s kv.SchemaStore, f influxdbtesting.VariableFields, t *testing.T) (influxdb.VariableService, string, func()) {
	ctx := context.Background()
	svc := kv.NewService(zaptest.NewLogger(t), s, &mock.OrganizationService{})
	svc.IDGenerator = f.IDGenerator
	svc.TimeGenerator = f.TimeGenerator
	if svc.TimeGenerator == nil {
		svc.TimeGenerator = influxdb.RealTimeGenerator{}
	}

	for _, variable := range f.Variables {
		if err := svc.ReplaceVariable(ctx, variable); err != nil {
			t.Fatalf("failed to populate test variables: %v", err)
		}
	}

	done := func() {
		for _, variable := range f.Variables {
			if err := svc.DeleteVariable(ctx, variable.ID); err != nil {
				t.Logf("failed to clean up variables bolt test: %v", err)
			}
		}
	}

	return svc, kv.OpPrefix, done
}

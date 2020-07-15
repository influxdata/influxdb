package kv_test

import (
	"context"
	"testing"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kv"
	influxdbtesting "github.com/influxdata/influxdb/v2/testing"
	"go.uber.org/zap/zaptest"
)

func TestBoltLabelService(t *testing.T) {
	influxdbtesting.LabelService(initBoltLabelService, t)
}

func initBoltLabelService(f influxdbtesting.LabelFields, t *testing.T) (influxdb.LabelService, string, func()) {
	s, closeBolt, err := NewTestBoltStore(t)
	if err != nil {
		t.Fatalf("failed to create new kv store: %v", err)
	}

	svc, op, closeSvc := initLabelService(s, f, t)
	return svc, op, func() {
		closeSvc()
		closeBolt()
	}
}

func initLabelService(s kv.SchemaStore, f influxdbtesting.LabelFields, t *testing.T) (influxdb.LabelService, string, func()) {
	ctx := context.Background()
	svc := kv.NewService(zaptest.NewLogger(t), s)
	svc.IDGenerator = f.IDGenerator

	for _, l := range f.Labels {
		if err := svc.PutLabel(ctx, l); err != nil {
			t.Fatalf("failed to populate labels: %v", err)
		}
	}

	for _, m := range f.Mappings {
		if err := svc.PutLabelMapping(ctx, m); err != nil {
			t.Fatalf("failed to populate label mappings: %v", err)
		}
	}

	return svc, kv.OpPrefix, func() {
		for _, l := range f.Labels {
			if err := svc.DeleteLabel(ctx, l.ID); err != nil {
				t.Logf("failed to remove label: %v", err)
			}
		}
	}
}

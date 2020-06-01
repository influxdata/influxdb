package label_test

import (
	"context"
	"testing"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kit/feature"
	"github.com/influxdata/influxdb/v2/kv"
	"github.com/influxdata/influxdb/v2/label"
	influxdbtesting "github.com/influxdata/influxdb/v2/testing"
	"go.uber.org/zap/zaptest"
)

func TestLabelServiceController(t *testing.T) {
	influxdbtesting.LabelService(initBoltLabelServiceController, t)
}

func initBoltLabelServiceController(f influxdbtesting.LabelFields, t *testing.T) (influxdb.LabelService, string, func()) {
	newSvc, s, closer := initBoltLabelService(f, t)
	st, _, _ := NewTestBoltStore(t)
	oldSvc, _, _ := initOldLabelService(st, f, t)

	flagger := feature.DefaultFlagger()
	return label.NewLabelController(flagger, oldSvc, newSvc), s, closer
}

func initOldLabelService(s kv.Store, f influxdbtesting.LabelFields, t *testing.T) (influxdb.LabelService, string, func()) {
	svc := kv.NewService(zaptest.NewLogger(t), s)
	svc.IDGenerator = f.IDGenerator

	ctx := context.Background()
	if err := svc.Initialize(ctx); err != nil {
		t.Fatalf("error initializing label service: %v", err)
	}
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

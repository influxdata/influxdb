package label_test

import (
	"context"
	"testing"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kv"
	"github.com/influxdata/influxdb/v2/label"
	"github.com/influxdata/influxdb/v2/mock"
	influxdbtesting "github.com/influxdata/influxdb/v2/testing"
)

func TestBoltLabelService(t *testing.T) {
	influxdbtesting.LabelService(initBoltLabelService, t)
}

func initBoltLabelService(f influxdbtesting.LabelFields, t *testing.T) (influxdb.LabelService, string, func()) {
	s, closeBolt, err := influxdbtesting.NewTestBoltStore(t)
	if err != nil {
		t.Fatalf("failed to create new kv store: %v", err)
	}

	svc, op, closeSvc := initLabelService(s, f, t)
	return svc, op, func() {
		closeSvc()
		closeBolt()
	}
}

func initLabelService(s kv.Store, f influxdbtesting.LabelFields, t *testing.T) (influxdb.LabelService, string, func()) {
	st, err := label.NewStore(s)
	if err != nil {
		t.Fatalf("failed to create label store: %v", err)
	}

	if f.IDGenerator != nil {
		st.IDGenerator = f.IDGenerator
	}

	svc := label.NewService(st)
	ctx := context.Background()

	for _, l := range f.Labels {
		mock.SetIDForFunc(&st.IDGenerator, l.ID, func() {
			if err := svc.CreateLabel(ctx, l); err != nil {
				t.Fatalf("failed to populate labels: %v", err)
			}
		})
	}

	for _, m := range f.Mappings {
		if err := svc.CreateLabelMapping(ctx, m); err != nil {
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

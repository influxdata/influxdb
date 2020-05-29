package kv_test

import (
	"context"
	"testing"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kv"
	influxdbtesting "github.com/influxdata/influxdb/v2/testing"
	"go.uber.org/zap/zaptest"
)

func TestBoltTelegrafService(t *testing.T) {
	influxdbtesting.TelegrafConfigStore(initBoltTelegrafService, t)
}

func initBoltTelegrafService(f influxdbtesting.TelegrafConfigFields, t *testing.T) (influxdb.TelegrafConfigStore, func()) {
	s, closeBolt, err := NewTestBoltStore(t)
	if err != nil {
		t.Fatalf("failed to create new kv store: %v", err)
	}

	svc, closeSvc := initTelegrafService(s, f, t)
	return svc, func() {
		closeSvc()
		closeBolt()
	}
}

func initTelegrafService(s kv.Store, f influxdbtesting.TelegrafConfigFields, t *testing.T) (influxdb.TelegrafConfigStore, func()) {
	svc := kv.NewService(zaptest.NewLogger(t), s)
	svc.IDGenerator = f.IDGenerator

	ctx := context.Background()
	if err := svc.Initialize(ctx); err != nil {
		t.Fatalf("error initializing user service: %v", err)
	}

	for _, tc := range f.TelegrafConfigs {
		if err := svc.PutTelegrafConfig(ctx, tc); err != nil {
			t.Fatalf("failed to populate telegraf config: %v", err)
		}
	}

	for _, m := range f.UserResourceMappings {
		if err := svc.CreateUserResourceMapping(ctx, m); err != nil {
			t.Fatalf("failed to populate user resource mapping: %v", err)
		}
	}

	return svc, func() {
		for _, tc := range f.TelegrafConfigs {
			if err := svc.DeleteTelegrafConfig(ctx, tc.ID); err != nil {
				t.Logf("failed to remove telegraf config: %v", err)
			}
		}
	}
}

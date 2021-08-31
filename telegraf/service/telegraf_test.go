package service_test

import (
	"context"
	"testing"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kv"
	telegrafservice "github.com/influxdata/influxdb/v2/telegraf/service"
	telegraftesting "github.com/influxdata/influxdb/v2/telegraf/service/testing"
	itesting "github.com/influxdata/influxdb/v2/testing"
)

func TestBoltTelegrafService(t *testing.T) {
	telegraftesting.TelegrafConfigStore(initBoltTelegrafService, t)
}

func initBoltTelegrafService(f telegraftesting.TelegrafConfigFields, t *testing.T) (influxdb.TelegrafConfigStore, func()) {
	s, closeBolt, err := itesting.NewTestBoltStore(t)
	if err != nil {
		t.Fatalf("failed to create new kv store: %v", err)
	}

	svc, closeSvc := initTelegrafService(s, f, t)
	return svc, func() {
		closeSvc()
		closeBolt()
	}
}

func initTelegrafService(s kv.SchemaStore, f telegraftesting.TelegrafConfigFields, t *testing.T) (influxdb.TelegrafConfigStore, func()) {
	ctx := context.Background()

	svc := telegrafservice.New(s)
	svc.IDGenerator = f.IDGenerator

	for _, tc := range f.TelegrafConfigs {
		if err := svc.PutTelegrafConfig(ctx, tc); err != nil {
			t.Fatalf("failed to populate telegraf config: %v", err)
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

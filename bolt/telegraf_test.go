package bolt_test

import (
	"context"
	"testing"

	platform "github.com/influxdata/influxdb"
	platformtesting "github.com/influxdata/influxdb/testing"
)

func initTelegrafConfigStore(f platformtesting.TelegrafConfigFields, t *testing.T) (platform.TelegrafConfigStore, func()) {
	c, closeFn, err := NewTestClient()
	if err != nil {
		t.Fatalf("failed to create new bolt client: %v", err)
	}
	c.IDGenerator = f.IDGenerator
	ctx := context.TODO()
	for _, tc := range f.TelegrafConfigs {
		if err := c.PutTelegrafConfig(ctx, tc); err != nil {
			t.Fatalf("failed to populate telegraf config: %s", err.Error())
		}
	}
	for _, m := range f.UserResourceMappings {
		if err := c.CreateUserResourceMapping(ctx, m); err != nil {
			t.Fatalf("failed to populate user resource mapping")
		}
	}
	return c, func() {
		defer closeFn()
		for _, tc := range f.TelegrafConfigs {
			if err := c.DeleteTelegrafConfig(ctx, tc.ID); err != nil {
				t.Logf("failed to remove telegraf config: %v", err)
			}
		}
	}
}

func TestTelegrafConfigStore(t *testing.T) {
	platformtesting.TelegrafConfigStore(initTelegrafConfigStore, t)
}

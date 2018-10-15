package inmem

import (
	"context"
	"testing"

	"github.com/influxdata/platform"
	platformtesting "github.com/influxdata/platform/testing"
)

func initTelegrafStore(f platformtesting.TelegrafConfigFields, t *testing.T) (platform.TelegrafConfigStore, func()) {
	s := NewService()
	s.IDGenerator = f.IDGenerator
	ctx := context.Background()
	for _, m := range f.UserResourceMappings {
		if err := s.PutUserResourceMapping(ctx, m); err != nil {
			t.Fatalf("failed to populate user resource mapping")
		}
	}
	for _, tc := range f.TelegrafConfigs {
		if err := s.putTelegrafConfig(ctx, tc); err != nil {
			t.Fatalf("failed to populate telegraf configs")
		}
	}
	return s, func() {}
}

func TestTelegrafStore(t *testing.T) {
	platformtesting.TelegrafConfigStore(initTelegrafStore, t)
}

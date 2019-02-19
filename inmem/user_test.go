package inmem

import (
	"context"
	"testing"

	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/kv"
	influxdbtesting "github.com/influxdata/influxdb/testing"
)

func initUserService(f influxdbtesting.UserFields, t *testing.T) (influxdb.UserService, string, func()) {
	s := NewKVStore()
	svc := kv.NewService(s)
	svc.IDGenerator = f.IDGenerator
	ctx := context.Background()
	if err := svc.Initialize(ctx); err != nil {
		t.Fatalf("error initializing user service: %v", err)
	}

	for _, u := range f.Users {
		if err := svc.PutUser(ctx, u); err != nil {
			t.Fatalf("failed to populate users")
		}
	}
	return svc, "kv/", func() {
		for _, u := range f.Users {
			if err := svc.DeleteUser(ctx, u.ID); err != nil {
				t.Logf("failed to remove users: %v", err)
			}
		}
	}
}

func TestUserService(t *testing.T) {
	t.Parallel()
	influxdbtesting.UserService(initUserService, t)
}

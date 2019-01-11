package bolt_test

import (
	"context"
	"testing"

	platform "github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/bolt"
	platformtesting "github.com/influxdata/influxdb/testing"
)

func initUserService(f platformtesting.UserFields, t *testing.T) (platform.UserService, string, func()) {
	c, closeFn, err := NewTestClient()
	if err != nil {
		t.Fatalf("failed to create new kv store: %v", err)
	}
	c.IDGenerator = f.IDGenerator

	ctx := context.Background()
	for _, u := range f.Users {
		if err := c.PutUser(ctx, u); err != nil {
			t.Fatalf("failed to populate users")
		}
	}
	return c, bolt.OpPrefix, func() {
		defer closeFn()
		for _, u := range f.Users {
			if err := c.DeleteUser(ctx, u.ID); err != nil {
				t.Logf("failed to remove users: %v", err)
			}
		}
	}
}

func TestUserService(t *testing.T) {
	platformtesting.UserService(initUserService, t)
}

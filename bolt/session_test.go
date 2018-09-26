package bolt_test

import (
	"context"
	"testing"

	"github.com/influxdata/platform"
	platformtesting "github.com/influxdata/platform/testing"
)

func initSessionService(f platformtesting.SessionFields, t *testing.T) (platform.SessionService, func()) {
	c, closeFn, err := NewTestClient()
	if err != nil {
		t.Fatalf("failed to create new bolt client: %v", err)
	}
	c.IDGenerator = f.IDGenerator
	c.TokenGenerator = f.TokenGenerator
	ctx := context.Background()
	for _, u := range f.Users {
		if err := c.PutUser(ctx, u); err != nil {
			t.Fatalf("failed to populate users")
		}
	}
	for _, s := range f.Sessions {
		if err := c.PutSession(ctx, s); err != nil {
			t.Fatalf("failed to populate sessions")
		}
	}
	return c, func() {
		defer closeFn()
		for _, u := range f.Users {
			if err := c.DeleteUser(ctx, u.ID); err != nil {
				t.Logf("failed to remove users: %v", err)
			}
		}
	}
}

func TestSessionService(t *testing.T) {
	platformtesting.SessionService(initSessionService, t)
}

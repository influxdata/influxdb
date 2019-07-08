package bolt_test

import (
	"context"
	"testing"

	platform "github.com/influxdata/influxdb"
	platformtesting "github.com/influxdata/influxdb/testing"
)

func initPasswordsService(f platformtesting.PasswordFields, t *testing.T) (platform.PasswordsService, func()) {
	c, closeFn, err := NewTestClient()
	if err != nil {
		t.Fatalf("failed to create new bolt client: %v", err)
	}
	c.IDGenerator = f.IDGenerator
	ctx := context.Background()
	for _, u := range f.Users {
		if err := c.PutUser(ctx, u); err != nil {
			t.Fatalf("failed to populate users")
		}
	}

	for i := range f.Passwords {
		if err := c.SetPassword(ctx, f.Users[i].Name, f.Passwords[i]); err != nil {
			t.Fatalf("error setting passsword user, %s %s: %v", f.Users[i].Name, f.Passwords[i], err)
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

func TestPasswords(t *testing.T) {
	t.Parallel()
	platformtesting.SetPassword(initPasswordsService, t)
}

func TestPasswords_CompareAndSet(t *testing.T) {
	t.Parallel()
	platformtesting.CompareAndSetPassword(initPasswordsService, t)
}

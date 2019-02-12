package inmem

import (
	"context"
	"testing"

	platform "github.com/influxdata/influxdb"
	platformtesting "github.com/influxdata/influxdb/testing"
)

func initPasswordsService(f platformtesting.PasswordFields, t *testing.T) (platform.PasswordsService, func()) {
	s := NewService()
	s.IDGenerator = f.IDGenerator
	ctx := context.Background()
	for _, u := range f.Users {
		if err := s.PutUser(ctx, u); err != nil {
			t.Fatalf("failed to populate users")
		}
	}

	for i := range f.Passwords {
		if err := s.SetPassword(ctx, f.Users[i].Name, f.Passwords[i]); err != nil {
			t.Fatalf("error setting passsword user, %s %s: %v", f.Users[i].Name, f.Passwords[i], err)
		}
	}

	return s, func() {}
}

func TestPasswords(t *testing.T) {
	t.Parallel()
	platformtesting.PasswordsService(initPasswordsService, t)
}

func TestPasswords_CompareAndSet(t *testing.T) {
	t.Parallel()
	platformtesting.CompareAndSetPassword(initPasswordsService, t)
}

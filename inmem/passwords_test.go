package inmem

import (
	"context"
	"testing"

	platform "github.com/influxdata/influxdb"
	platformtesting "github.com/influxdata/influxdb/testing"
)

func initPasswordsService(f platformtesting.UserFields, t *testing.T) (platform.PasswordsService, func()) {
	s := NewService()
	s.IDGenerator = f.IDGenerator
	ctx := context.Background()
	for _, u := range f.Users {
		if err := s.PutUser(ctx, u); err != nil {
			t.Fatalf("failed to populate users")
		}
	}
	return s, func() {}
}

func TestPasswords(t *testing.T) {
	t.Parallel()
	platformtesting.Passwords(initPasswordsService, t)
}

func TestPasswords_CompareAndSet(t *testing.T) {
	t.Parallel()
	platformtesting.CompareAndSetPassword(initPasswordsService, t)
}

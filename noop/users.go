package noop

import (
	"context"
	"fmt"

	"github.com/influxdata/chronograf"
)

// ensure UsersStore implements chronograf.UsersStore
var _ chronograf.UsersStore = &UsersStore{}

type UsersStore struct{}

func (s *UsersStore) All(context.Context) ([]chronograf.User, error) {
	return nil, fmt.Errorf("no users found")
}

func (s *UsersStore) Add(context.Context, *chronograf.User) (*chronograf.User, error) {
	return nil, fmt.Errorf("failed to add user")
}

func (s *UsersStore) Delete(context.Context, *chronograf.User) error {
	return fmt.Errorf("failed to delete user")
}

func (s *UsersStore) Get(ctx context.Context, q chronograf.UserQuery) (*chronograf.User, error) {
	return nil, chronograf.ErrUserNotFound
}

func (s *UsersStore) Update(context.Context, *chronograf.User) error {
	return fmt.Errorf("failed to update user")
}

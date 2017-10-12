package bolt

import (
	"context"
	"fmt"
	"sync"

	"github.com/influxdata/chronograf"
)

// Ensure RolesStore implements chronograf.RolesStore.
var _ chronograf.RolesStore = &RolesStore{}

// RolesStore uses bolt to store and retrieve roles
type RolesStore struct {
	mu         sync.RWMutex
	roles      map[string]chronograf.Role
	usersStore *UsersStore
}

// NewRolesStore returns a *RolesStore populated with the
// default Chonograf User Roles
func newRolesStore(u *UsersStore) *RolesStore {
	s := &RolesStore{
		usersStore: u,
		roles:      map[string]chronograf.Role{},
	}

	for _, role := range chronograf.DefaultUserRoles {
		s.roles[role.Name] = role
	}

	return s
}

// All returns all roles and all users associated with each role.
func (s *RolesStore) All(context.Context) ([]chronograf.Role, error) {
	panic("not implemented")
}

// Add returns an error and a nil users. It is not intended to be used currently.
func (s *RolesStore) Add(context.Context, *chronograf.Role) (*chronograf.Role, error) {
	return nil, fmt.Errorf("Add not supported for boltdb roles store")
}

// Delete returns an error. It is not intended to be used currently.
func (s *RolesStore) Delete(context.Context, *chronograf.Role) error {
	return fmt.Errorf("Delete not supported for boltdb roles store")
}

// Get retrieves a role and all users associated with it.
func (s *RolesStore) Get(ctx context.Context, name string) (*chronograf.Role, error) {
	panic("not implemented")
}

// Update returns an error. It is not intended to be used currently.
func (s *RolesStore) Update(context.Context, *chronograf.Role) error {
	return fmt.Errorf("Update not supported for boltdb roles store")
}

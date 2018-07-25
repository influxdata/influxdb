package mocks

import (
	"context"

	"github.com/influxdata/platform/chronograf"
)

var _ chronograf.RolesStore = &RolesStore{}

// RolesStore mock allows all functions to be set for testing
type RolesStore struct {
	AllF    func(context.Context) ([]chronograf.Role, error)
	AddF    func(context.Context, *chronograf.Role) (*chronograf.Role, error)
	DeleteF func(context.Context, *chronograf.Role) error
	GetF    func(ctx context.Context, name string) (*chronograf.Role, error)
	UpdateF func(context.Context, *chronograf.Role) error
}

// All lists all Roles from the RolesStore
func (s *RolesStore) All(ctx context.Context) ([]chronograf.Role, error) {
	return s.AllF(ctx)
}

// Add a new Role in the RolesStore
func (s *RolesStore) Add(ctx context.Context, u *chronograf.Role) (*chronograf.Role, error) {
	return s.AddF(ctx, u)
}

// Delete the Role from the RolesStore
func (s *RolesStore) Delete(ctx context.Context, u *chronograf.Role) error {
	return s.DeleteF(ctx, u)
}

// Get retrieves a Role if name exists.
func (s *RolesStore) Get(ctx context.Context, name string) (*chronograf.Role, error) {
	return s.GetF(ctx, name)
}

// Update the Role's permissions or users
func (s *RolesStore) Update(ctx context.Context, u *chronograf.Role) error {
	return s.UpdateF(ctx, u)
}

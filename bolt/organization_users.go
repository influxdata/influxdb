package bolt

import (
	"context"
	"fmt"

	"github.com/influxdata/chronograf"
)

// Ensure OrganizationUsersStore implements chronograf.OrganizationUsersStore.
var _ chronograf.UsersStore = &OrganizationUsersStore{}

// OrganizationUsersStore uses bolt to store and retrieve users
type OrganizationUsersStore struct {
	client *Client
}

const organizationKey = "organizationID"

// userHasValidRoles ensures that each User Role has both an associated OrganizationID and a Name
func userHasValidRoles(orgID string, u *chronograf.User) error {
	if u == nil || u.Roles == nil {
		return nil
	}
	for _, r := range u.Roles {
		if r.OrganizationID == "" {
			return fmt.Errorf("user role must have an OrganizationID")
		}
		if r.OrganizationID != orgID {
			return fmt.Errorf("organizationID %s does not match %s", r.OrganizationID, orgID)
		}
		if r.Name == "" {
			return fmt.Errorf("user role must have a Name")
		}
	}
	return nil
}

// Get searches the OrganizationUsersStore for user with name
func (s *OrganizationUsersStore) Get(ctx context.Context, q chronograf.UserQuery) (*chronograf.User, error) {
	if ctx == nil {
		return nil, fmt.Errorf("expect non nil context")
	}
	orgID, ok := ctx.Value(organizationKey).(string)
	if !ok {
		return nil, fmt.Errorf("expected organization key to be a string")
	}
	if orgID == "" {
		return nil, fmt.Errorf("expected organization key to be set")
	}
	usr, err := s.client.UsersStore.Get(ctx, q)
	if err != nil {
		return nil, err
	}

	// filter Roles that are not scoped to this Organization
	roles := usr.Roles[:0]
	for _, r := range usr.Roles {
		if r.OrganizationID == orgID {
			roles = append(roles, r)
		}
	}
	if len(roles) == 0 {
		// This means that the user has no roles in an organization
		// TODO: should we return the user without any roles or ErrUserNotFound?
		return nil, chronograf.ErrUserNotFound
	}
	usr.Roles = roles
	return usr, nil
}

// Add a new Users in the OrganizationUsersStore.
func (s *OrganizationUsersStore) Add(ctx context.Context, u *chronograf.User) (*chronograf.User, error) {
	if ctx == nil {
		return nil, fmt.Errorf("expect non nil context")
	}
	orgID, ok := ctx.Value(organizationKey).(string)
	if !ok {
		return nil, fmt.Errorf("expected organization key to be set")
	}
	if err := userHasValidRoles(orgID, u); err != nil {
		return nil, err
	}
	return s.client.UsersStore.Add(ctx, u)
}

// Delete the users from the OrganizationUsersStore
func (s *OrganizationUsersStore) Delete(ctx context.Context, usr *chronograf.User) error {
	if ctx == nil {
		return fmt.Errorf("expect non nil context")
	}
	orgID, ok := ctx.Value(organizationKey).(string)
	if !ok {
		return fmt.Errorf("expected organization key to be set")
	}
	u, err := s.client.UsersStore.Get(ctx, chronograf.UserQuery{ID: &usr.ID})
	if err != nil {
		return err
	}
	// delete Roles that are not scoped to this Organization
	roles := u.Roles[:0]
	for _, r := range u.Roles {
		if r.OrganizationID != orgID {
			roles = append(roles, r)
		}
	}
	u.Roles = roles
	return s.client.UsersStore.Update(ctx, u)
}

// Update a user
func (s *OrganizationUsersStore) Update(ctx context.Context, usr *chronograf.User) error {
	if ctx == nil {
		return fmt.Errorf("expect non nil context")
	}
	orgID, ok := ctx.Value(organizationKey).(string)
	if !ok {
		return fmt.Errorf("expected organization key to be set")
	}
	if err := userHasValidRoles(orgID, usr); err != nil {
		return err
	}
	u, err := s.client.UsersStore.Get(ctx, chronograf.UserQuery{ID: &usr.ID})
	if err != nil {
		return err
	}
	// filter Roles that are not scoped to this Organization
	roles := u.Roles[:0]
	for _, r := range u.Roles {
		if r.OrganizationID != orgID {
			roles = append(roles, r)
		}
	}

	// recombine roles from usr
	u.Roles = append(roles, usr.Roles...)

	return s.client.UsersStore.Update(ctx, u)
}

// All returns all users
func (s *OrganizationUsersStore) All(ctx context.Context) ([]chronograf.User, error) {
	if ctx == nil {
		return nil, fmt.Errorf("expect non nil context")
	}
	orgID, ok := ctx.Value(organizationKey).(string)
	if !ok {
		return nil, fmt.Errorf("expected organization key to be set")
	}
	usrs, err := s.client.UsersStore.All(ctx)
	if err != nil {
		return nil, err
	}

	// Filtering users that have no associtation to an organization
	us := usrs[:0]
	for _, usr := range usrs {
		roles := usr.Roles[:0]
		for _, r := range usr.Roles {
			if r.OrganizationID == orgID {
				roles = append(roles, r)
			}
		}
		if len(roles) != 0 {
			// Only add users if they have a role in the associated organization
			usr.Roles = roles
			us = append(us, usr)
		}
	}

	return us, nil
}

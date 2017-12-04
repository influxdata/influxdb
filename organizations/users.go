package organizations

import (
	"context"
	"fmt"

	"github.com/influxdata/chronograf"
)

// Ensure UsersStore implements chronograf.UsersStore.
var _ chronograf.UsersStore = &UsersStore{}

// UsersStore facade on a UserStore that filters a users roles
// by organization.
//
// The high level idea here is to use the same underlying store for all users.
// In particular, this is done by having all the users Roles field be a set of
// all of the users roles in all organizations. Each CRUD method here takes care
// to ensure that the only roles that are modified are the roles for the organization
// that was provided on the UsersStore.
type UsersStore struct {
	organization string
	store        chronograf.UsersStore
}

// NewUsersStore creates a new UsersStore from an existing
// chronograf.UserStore and an organization string
func NewUsersStore(s chronograf.UsersStore, org string) *UsersStore {
	return &UsersStore{
		store:        s,
		organization: org,
	}
}

// validOrganizationRoles ensures that each User Role has both an associated Organization and a Name
func validOrganizationRoles(orgID string, u *chronograf.User) error {
	if u == nil || u.Roles == nil {
		return nil
	}
	for _, r := range u.Roles {
		if r.Organization == "" {
			return fmt.Errorf("user role must have an Organization")
		}
		if r.Organization != orgID {
			return fmt.Errorf("organizationID %s does not match %s", r.Organization, orgID)
		}
		if r.Name == "" {
			return fmt.Errorf("user role must have a Name")
		}
	}
	return nil
}

// Get searches the UsersStore for using the query.
// The roles returned on the user are filtered to only contain roles that are for the organization
// specified on the organization store.
func (s *UsersStore) Get(ctx context.Context, q chronograf.UserQuery) (*chronograf.User, error) {
	err := validOrganization(ctx)
	if err != nil {
		return nil, err
	}

	usr, err := s.store.Get(ctx, q)
	if err != nil {
		return nil, err
	}

	// This filters a users roles so that the resulting struct only contains roles
	// from the organization on the UsersStore.
	roles := usr.Roles[:0]
	for _, r := range usr.Roles {
		if r.Organization == s.organization {
			roles = append(roles, r)
		}
	}

	if len(roles) == 0 {
		// This means that the user does not belong to the organization
		// and therefore, is not found.
		return nil, chronograf.ErrUserNotFound
	}

	usr.Roles = roles
	return usr, nil
}

// Add creates a new User in the UsersStore. It validates that the user provided only
// has roles for the organization set on the UsersStore.
// If a user is not found in the underlying, it calls the underlying UsersStore Add method.
// If a user is found, it removes any existing roles a user has for an organization and appends
// the roles specified on the provided user and calls the uderlying UsersStore Update method.
func (s *UsersStore) Add(ctx context.Context, u *chronograf.User) (*chronograf.User, error) {
	err := validOrganization(ctx)
	if err != nil {
		return nil, err
	}

	// Validates that the users roles are only for the current organization.
	if err := validOrganizationRoles(s.organization, u); err != nil {
		return nil, err
	}

	// retrieve the user from the underlying store
	usr, err := s.store.Get(ctx, chronograf.UserQuery{
		Name:     &u.Name,
		Provider: &u.Provider,
		Scheme:   &u.Scheme,
	})

	switch err {
	case nil:
		// If there is no error continue to the rest of the code
		break
	case chronograf.ErrUserNotFound:
		// If user is not found in the backed store, attempt to add the user
		return s.store.Add(ctx, u)
	default:
		// return the error
		return nil, err
	}

	// Filter the retrieved users roles so that the resulting struct only contains roles
	// that are not from the organization on the UsersStore.
	roles := usr.Roles[:0]
	for _, r := range usr.Roles {
		if r.Organization != s.organization {
			roles = append(roles, r)
		}
	}

	// If the user already has a role in the organization then the user
	// cannot be "created".
	// This can be thought of as:
	// (total # of roles a user has) - (# of roles not in the organization) = (# of roles in organization)
	// if this value is greater than 1 the user cannot be "added".
	numRolesInOrganization := len(usr.Roles) - len(roles)
	if numRolesInOrganization > 0 {
		return nil, chronograf.ErrUserAlreadyExists
	}

	// Set the users roles to be the union of the roles set on the provided user
	// and the user that was found in the underlying store
	usr.Roles = append(roles, u.Roles...)

	// Update the user in the underlying store
	if err := s.store.Update(ctx, usr); err != nil {
		return nil, err
	}

	// Return the provided user with ID set
	u.ID = usr.ID
	return u, nil
}

// Delete a user from the UsersStore. This is done by stripping a user of
// any roles it has in the organization speicified on the UsersStore.
func (s *UsersStore) Delete(ctx context.Context, usr *chronograf.User) error {
	err := validOrganization(ctx)
	if err != nil {
		return err
	}

	// retrieve the user from the underlying store
	u, err := s.store.Get(ctx, chronograf.UserQuery{ID: &usr.ID})
	if err != nil {
		return err
	}

	// Filter the retrieved users roles so that the resulting slice contains
	// roles that are not scoped to the organization provided
	roles := u.Roles[:0]
	for _, r := range u.Roles {
		if r.Organization != s.organization {
			roles = append(roles, r)
		}
	}
	u.Roles = roles
	return s.store.Update(ctx, u)
}

// Update a user in the UsersStore.
func (s *UsersStore) Update(ctx context.Context, usr *chronograf.User) error {
	err := validOrganization(ctx)
	if err != nil {
		return err
	}

	// Validates that the users roles are only for the current organization.
	if err := validOrganizationRoles(s.organization, usr); err != nil {
		return err
	}

	// retrieve the user from the underlying store
	u, err := s.store.Get(ctx, chronograf.UserQuery{ID: &usr.ID})
	if err != nil {
		return err
	}

	// Filter the retrieved users roles so that the resulting slice contains
	// roles that are not scoped to the organization provided
	roles := u.Roles[:0]
	for _, r := range u.Roles {
		if r.Organization != s.organization {
			roles = append(roles, r)
		}
	}

	// Make a copy of the usr so that we dont modify the underlying add roles on to
	// the user that was passed in
	user := *usr

	// Set the users roles to be the union of the roles set on the provided user
	// and the user that was found in the underlying store
	user.Roles = append(roles, usr.Roles...)

	return s.store.Update(ctx, &user)
}

// All returns all users where roles have been filters to be exclusively for
// the organization provided on the UsersStore.
func (s *UsersStore) All(ctx context.Context) ([]chronograf.User, error) {
	err := validOrganization(ctx)
	if err != nil {
		return nil, err
	}

	// retrieve all users from the underlying UsersStore
	usrs, err := s.store.All(ctx)
	if err != nil {
		return nil, err
	}

	// Filter users to only contain users that have at least one role
	// in the provided organization.
	us := usrs[:0]
	for _, usr := range usrs {
		roles := usr.Roles[:0]
		// This filters a users roles so that the resulting struct only contains roles
		// from the organization on the UsersStore.
		for _, r := range usr.Roles {
			if r.Organization == s.organization {
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

// Num returns the number of users in the UsersStore
// This is unperformant, but should rarely be used.
func (s *UsersStore) Num(ctx context.Context) (int, error) {
	err := validOrganization(ctx)
	if err != nil {
		return 0, err
	}

	// retrieve all users from the underlying UsersStore
	usrs, err := s.All(ctx)
	if err != nil {
		return 0, err
	}

	return len(usrs), nil
}

package tenant

import (
	"context"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/authorizer"
	"github.com/influxdata/influxdb/v2/kit/platform"
)

var _ influxdb.UserService = (*AuthedUserService)(nil)

// TODO (al): remove authorizer/user when the user service moves to tenant

// AuthedUserService wraps a influxdb.UserService and authorizes actions
// against it appropriately.
type AuthedUserService struct {
	s influxdb.UserService
}

// NewAuthedUserService constructs an instance of an authorizing user service.
func NewAuthedUserService(s influxdb.UserService) *AuthedUserService {
	return &AuthedUserService{
		s: s,
	}
}

// FindUserByID checks to see if the authorizer on context has read access to the id provided.
func (s *AuthedUserService) FindUserByID(ctx context.Context, id platform.ID) (*influxdb.User, error) {
	if _, _, err := authorizer.AuthorizeReadResource(ctx, influxdb.UsersResourceType, id); err != nil {
		return nil, err
	}
	return s.s.FindUserByID(ctx, id)
}

// FindUser retrieves the user and checks to see if the authorizer on context has read access to the user.
func (s *AuthedUserService) FindUser(ctx context.Context, filter influxdb.UserFilter) (*influxdb.User, error) {
	u, err := s.s.FindUser(ctx, filter)
	if err != nil {
		return nil, err
	}
	if _, _, err := authorizer.AuthorizeReadResource(ctx, influxdb.UsersResourceType, u.ID); err != nil {
		return nil, err
	}
	return u, nil
}

// FindUsers retrieves all users that match the provided filter and then filters the list down to only the resources that are authorized.
func (s *AuthedUserService) FindUsers(ctx context.Context, filter influxdb.UserFilter, opt ...influxdb.FindOptions) ([]*influxdb.User, int, error) {
	// TODO (desa): we'll likely want to push this operation into the database eventually since fetching the whole list of data
	// will likely be expensive.
	us, _, err := s.s.FindUsers(ctx, filter, opt...)
	if err != nil {
		return nil, 0, err
	}
	return authorizer.AuthorizeFindUsers(ctx, us)
}

// CreateUser checks to see if the authorizer on context has write access to the global users resource.
func (s *AuthedUserService) CreateUser(ctx context.Context, o *influxdb.User) error {
	if _, _, err := authorizer.AuthorizeWriteGlobal(ctx, influxdb.UsersResourceType); err != nil {
		return err
	}
	return s.s.CreateUser(ctx, o)
}

// UpdateUser checks to see if the authorizer on context has write access to the user provided.
func (s *AuthedUserService) UpdateUser(ctx context.Context, id platform.ID, upd influxdb.UserUpdate) (*influxdb.User, error) {
	if _, _, err := authorizer.AuthorizeWriteResource(ctx, influxdb.UsersResourceType, id); err != nil {
		return nil, err
	}
	return s.s.UpdateUser(ctx, id, upd)
}

// DeleteUser checks to see if the authorizer on context has write access to the user provided.
func (s *AuthedUserService) DeleteUser(ctx context.Context, id platform.ID) error {
	if _, _, err := authorizer.AuthorizeWriteResource(ctx, influxdb.UsersResourceType, id); err != nil {
		return err
	}
	return s.s.DeleteUser(ctx, id)
}

func (s *AuthedUserService) FindPermissionForUser(ctx context.Context, id platform.ID) (influxdb.PermissionSet, error) {
	if _, _, err := authorizer.AuthorizeReadResource(ctx, influxdb.UsersResourceType, id); err != nil {
		return nil, err
	}
	return s.s.FindPermissionForUser(ctx, id)
}

// AuthedPasswordService is a new authorization middleware for a password service.
type AuthedPasswordService struct {
	s influxdb.PasswordsService
}

// NewAuthedPasswordService wraps an existing password service with auth middleware.
func NewAuthedPasswordService(svc influxdb.PasswordsService) *AuthedPasswordService {
	return &AuthedPasswordService{s: svc}
}

// SetPassword overrides the password of a known user.
func (s *AuthedPasswordService) SetPassword(ctx context.Context, userID platform.ID, password string) error {
	if _, _, err := authorizer.AuthorizeWriteResource(ctx, influxdb.UsersResourceType, userID); err != nil {
		return err
	}
	return s.s.SetPassword(ctx, userID, password)
}

// ComparePassword checks if the password matches the password recorded.
// Passwords that do not match return errors.
func (s *AuthedPasswordService) ComparePassword(ctx context.Context, userID platform.ID, password string) error {
	panic("not implemented")
}

// CompareAndSetPassword checks the password and if they match
// updates to the new password.
func (s *AuthedPasswordService) CompareAndSetPassword(ctx context.Context, userID platform.ID, old string, new string) error {
	panic("not implemented")
}

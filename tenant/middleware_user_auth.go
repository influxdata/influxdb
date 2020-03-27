package tenant

import (
	"context"

	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/authorizer"
)

var _ influxdb.UserService = (*AuthedUserService)(nil)

// TODO (al): remove authorizer/user when the user service moves to tenant

// AuthedUserService wraps a influxdb.UserService and authorizes actions
// against it appropriately.
type AuthedUserService struct {
	s influxdb.UserService
}

// NewUserService constructs an instance of an authorizing user serivce.
func NewUserService(s influxdb.UserService) *AuthedUserService {
	return &AuthedUserService{
		s: s,
	}
}

// FindUserByID checks to see if the authorizer on context has read access to the id provided.
func (s *AuthedUserService) FindUserByID(ctx context.Context, id influxdb.ID) (*influxdb.User, error) {
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
func (s *AuthedUserService) UpdateUser(ctx context.Context, id influxdb.ID, upd influxdb.UserUpdate) (*influxdb.User, error) {
	if _, _, err := authorizer.AuthorizeWriteResource(ctx, influxdb.UsersResourceType, id); err != nil {
		return nil, err
	}
	return s.s.UpdateUser(ctx, id, upd)
}

// DeleteUser checks to see if the authorizer on context has write access to the user provided.
func (s *AuthedUserService) DeleteUser(ctx context.Context, id influxdb.ID) error {
	if _, _, err := authorizer.AuthorizeWriteResource(ctx, influxdb.UsersResourceType, id); err != nil {
		return err
	}
	return s.s.DeleteUser(ctx, id)
}

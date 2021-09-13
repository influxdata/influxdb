package authorizer

import (
	"context"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/kit/platform/errors"
)

var _ influxdb.UserService = (*UserService)(nil)

// UserService wraps a influxdb.UserService and authorizes actions
// against it appropriately.
type UserService struct {
	s influxdb.UserService
}

// NewUserService constructs an instance of an authorizing user service.
func NewUserService(s influxdb.UserService) *UserService {
	return &UserService{
		s: s,
	}
}

// FindUserByID checks to see if the authorizer on context has read access to the id provided.
func (s *UserService) FindUserByID(ctx context.Context, id platform.ID) (*influxdb.User, error) {
	if _, _, err := AuthorizeReadResource(ctx, influxdb.UsersResourceType, id); err != nil {
		return nil, err
	}
	return s.s.FindUserByID(ctx, id)
}

// FindUser retrieves the user and checks to see if the authorizer on context has read access to the user.
func (s *UserService) FindUser(ctx context.Context, filter influxdb.UserFilter) (*influxdb.User, error) {
	u, err := s.s.FindUser(ctx, filter)
	if err != nil {
		return nil, err
	}
	if _, _, err := AuthorizeReadResource(ctx, influxdb.UsersResourceType, u.ID); err != nil {
		return nil, err
	}
	return u, nil
}

// FindUsers retrieves all users that match the provided filter and then filters the list down to only the resources that are authorized.
func (s *UserService) FindUsers(ctx context.Context, filter influxdb.UserFilter, opt ...influxdb.FindOptions) ([]*influxdb.User, int, error) {
	// TODO: we'll likely want to push this operation into the database eventually since fetching the whole list of data
	// will likely be expensive.
	us, _, err := s.s.FindUsers(ctx, filter, opt...)
	if err != nil {
		return nil, 0, err
	}
	return AuthorizeFindUsers(ctx, us)
}

// CreateUser checks to see if the authorizer on context has write access to the global users resource.
func (s *UserService) CreateUser(ctx context.Context, o *influxdb.User) error {
	if _, _, err := AuthorizeWriteGlobal(ctx, influxdb.UsersResourceType); err != nil {
		return err
	}
	return s.s.CreateUser(ctx, o)
}

// UpdateUser checks to see if the authorizer on context has write access to the user provided.
func (s *UserService) UpdateUser(ctx context.Context, id platform.ID, upd influxdb.UserUpdate) (*influxdb.User, error) {
	if _, _, err := AuthorizeWriteResource(ctx, influxdb.UsersResourceType, id); err != nil {
		return nil, err
	}
	return s.s.UpdateUser(ctx, id, upd)
}

// DeleteUser checks to see if the authorizer on context has write access to the user provided.
func (s *UserService) DeleteUser(ctx context.Context, id platform.ID) error {
	if _, _, err := AuthorizeWriteResource(ctx, influxdb.UsersResourceType, id); err != nil {
		return err
	}
	return s.s.DeleteUser(ctx, id)
}

func (s *UserService) FindPermissionForUser(ctx context.Context, uid platform.ID) (influxdb.PermissionSet, error) {
	return nil, &errors.Error{
		Code: errors.EInternal,
		Msg:  "not implemented",
	}
}

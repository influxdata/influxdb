package authorizer

import (
	"context"

	"github.com/influxdata/influxdb"
)

var _ influxdb.UserService = (*UserService)(nil)

// UserService wraps a influxdb.UserService and authorizes actions
// against it appropriately.
type UserService struct {
	s influxdb.UserService
}

// NewUserService constructs an instance of an authorizing user serivce.
func NewUserService(s influxdb.UserService) *UserService {
	return &UserService{
		s: s,
	}
}

func newUserPermission(a influxdb.Action, id influxdb.ID) (*influxdb.Permission, error) {
	p := &influxdb.Permission{
		Action: a,
		Resource: influxdb.Resource{
			Type: influxdb.UsersResourceType,
			ID:   &id,
		},
	}

	return p, p.Valid()
}

func authorizeReadUser(ctx context.Context, id influxdb.ID) error {
	p, err := newUserPermission(influxdb.ReadAction, id)
	if err != nil {
		return err
	}

	if err := IsAllowed(ctx, *p); err != nil {
		return err
	}

	return nil
}

func authorizeWriteUser(ctx context.Context, id influxdb.ID) error {
	p, err := newUserPermission(influxdb.WriteAction, id)
	if err != nil {
		return err
	}

	if err := IsAllowed(ctx, *p); err != nil {
		return err
	}

	return nil
}

// FindUserByID checks to see if the authorizer on context has read access to the id provided.
func (s *UserService) FindUserByID(ctx context.Context, id influxdb.ID) (*influxdb.User, error) {
	if err := authorizeReadUser(ctx, id); err != nil {
		return nil, err
	}

	return s.s.FindUserByID(ctx, id)
}

// FindUser retrieves the user and checks to see if the authorizer on context has read access to the user.
func (s *UserService) FindUser(ctx context.Context, filter influxdb.UserFilter) (*influxdb.User, error) {
	o, err := s.s.FindUser(ctx, filter)
	if err != nil {
		return nil, err
	}

	if err := authorizeReadUser(ctx, o.ID); err != nil {
		return nil, err
	}

	return o, nil
}

// FindUsers retrieves all users that match the provided filter and then filters the list down to only the resources that are authorized.
func (s *UserService) FindUsers(ctx context.Context, filter influxdb.UserFilter, opt ...influxdb.FindOptions) ([]*influxdb.User, int, error) {
	// TODO: we'll likely want to push this operation into the database eventually since fetching the whole list of data
	// will likely be expensive.
	os, _, err := s.s.FindUsers(ctx, filter, opt...)
	if err != nil {
		return nil, 0, err
	}

	// This filters without allocating
	// https://github.com/golang/go/wiki/SliceTricks#filtering-without-allocating
	users := os[:0]
	for _, o := range os {
		err := authorizeReadUser(ctx, o.ID)
		if err != nil && influxdb.ErrorCode(err) != influxdb.EUnauthorized {
			return nil, 0, err
		}

		if influxdb.ErrorCode(err) == influxdb.EUnauthorized {
			continue
		}

		users = append(users, o)
	}

	return users, len(users), nil
}

// CreateUser checks to see if the authorizer on context has write access to the global users resource.
func (s *UserService) CreateUser(ctx context.Context, o *influxdb.User) error {
	p, err := influxdb.NewGlobalPermission(influxdb.WriteAction, influxdb.UsersResourceType)
	if err != nil {
		return err
	}

	if err := IsAllowed(ctx, *p); err != nil {
		return err
	}

	return s.s.CreateUser(ctx, o)
}

// UpdateUser checks to see if the authorizer on context has write access to the user provided.
func (s *UserService) UpdateUser(ctx context.Context, id influxdb.ID, upd influxdb.UserUpdate) (*influxdb.User, error) {
	if err := authorizeWriteUser(ctx, id); err != nil {
		return nil, err
	}

	return s.s.UpdateUser(ctx, id, upd)
}

// DeleteUser checks to see if the authorizer on context has write access to the user provided.
func (s *UserService) DeleteUser(ctx context.Context, id influxdb.ID) error {
	if err := authorizeWriteUser(ctx, id); err != nil {
		return err
	}

	return s.s.DeleteUser(ctx, id)
}

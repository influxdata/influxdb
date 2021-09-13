package mock

import (
	"context"

	platform "github.com/influxdata/influxdb/v2"
	platform2 "github.com/influxdata/influxdb/v2/kit/platform"
)

var _ platform.UserService = (*UserService)(nil)

// UserService is a mock implementation of a retention.UserService, which
// also makes it a suitable mock to use wherever an platform.UserService is required.
type UserService struct {
	// Methods for a platform.UserService
	FindUserByIDFn          func(context.Context, platform2.ID) (*platform.User, error)
	FindUsersFn             func(context.Context, platform.UserFilter, ...platform.FindOptions) ([]*platform.User, int, error)
	CreateUserFn            func(context.Context, *platform.User) error
	DeleteUserFn            func(context.Context, platform2.ID) error
	FindUserFn              func(context.Context, platform.UserFilter) (*platform.User, error)
	UpdateUserFn            func(context.Context, platform2.ID, platform.UserUpdate) (*platform.User, error)
	FindPermissionForUserFn func(context.Context, platform2.ID) (platform.PermissionSet, error)
}

// NewUserService returns a mock of UserService where its methods will return zero values.
func NewUserService() *UserService {
	return &UserService{
		FindUserByIDFn: func(context.Context, platform2.ID) (*platform.User, error) { return nil, nil },
		FindUserFn:     func(context.Context, platform.UserFilter) (*platform.User, error) { return nil, nil },
		CreateUserFn:   func(context.Context, *platform.User) error { return nil },
		UpdateUserFn:   func(context.Context, platform2.ID, platform.UserUpdate) (*platform.User, error) { return nil, nil },
		DeleteUserFn:   func(context.Context, platform2.ID) error { return nil },
		FindUsersFn: func(context.Context, platform.UserFilter, ...platform.FindOptions) ([]*platform.User, int, error) {
			return nil, 0, nil
		},
		FindPermissionForUserFn: func(context.Context, platform2.ID) (platform.PermissionSet, error) { return nil, nil },
	}
}

// FindUserByID returns a single User by ID.
func (s *UserService) FindUserByID(ctx context.Context, id platform2.ID) (*platform.User, error) {
	return s.FindUserByIDFn(ctx, id)
}

// FindUsers returns a list of Users that match filter and the total count of matching Users.
func (s *UserService) FindUsers(ctx context.Context, filter platform.UserFilter, opts ...platform.FindOptions) ([]*platform.User, int, error) {
	return s.FindUsersFn(ctx, filter, opts...)
}

// CreateUser creates a new User and sets b.ID with the new identifier.
func (s *UserService) CreateUser(ctx context.Context, User *platform.User) error {
	return s.CreateUserFn(ctx, User)
}

// DeleteUser removes a User by ID.
func (s *UserService) DeleteUser(ctx context.Context, id platform2.ID) error {
	return s.DeleteUserFn(ctx, id)
}

// FindUser finds the first user that matches a filter
func (s *UserService) FindUser(ctx context.Context, filter platform.UserFilter) (*platform.User, error) {
	return s.FindUserFn(ctx, filter)
}

// UpdateUser updates a user
func (s *UserService) UpdateUser(ctx context.Context, id platform2.ID, upd platform.UserUpdate) (*platform.User, error) {
	return s.UpdateUserFn(ctx, id, upd)
}

func (s *UserService) FindPermissionForUser(ctx context.Context, uid platform2.ID) (platform.PermissionSet, error) {
	return s.FindPermissionForUserFn(ctx, uid)
}

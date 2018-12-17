package mock

import (
	"context"

	"github.com/influxdata/platform"
)

var _ platform.UserService = &UserService{}

// NewUserService returns a mock of NewUserService
// where its methods will return zero values.
func NewUserService() *UserService {
	return &UserService{
		FindUserByIDFn: func(context.Context, platform.ID) (*platform.User, error) { return nil, nil },
		FindUserFn:     func(context.Context, platform.UserFilter) (*platform.User, error) { return nil, nil },
		CreateUserFn:   func(context.Context, *platform.User) error { return nil },
		UpdateUserFn:   func(context.Context, platform.ID, platform.UserUpdate) (*platform.User, error) { return nil, nil },
		DeleteUserFn:   func(context.Context, platform.ID) error { return nil },
		FindUsersFn: func(context.Context, platform.UserFilter, ...platform.FindOptions) ([]*platform.User, int, error) {
			return nil, 0, nil
		},
	}
}

type UserService struct {
	FindUserByIDFn func(context.Context, platform.ID) (*platform.User, error)
	FindUserFn     func(context.Context, platform.UserFilter) (*platform.User, error)
	FindUsersFn    func(context.Context, platform.UserFilter, ...platform.FindOptions) ([]*platform.User, int, error)
	CreateUserFn   func(context.Context, *platform.User) error
	UpdateUserFn   func(context.Context, platform.ID, platform.UserUpdate) (*platform.User, error)
	DeleteUserFn   func(context.Context, platform.ID) error
}

func (s *UserService) FindUserByID(ctx context.Context, id platform.ID) (*platform.User, error) {
	return s.FindUserByIDFn(ctx, id)
}

func (s *UserService) FindUser(ctx context.Context, filter platform.UserFilter) (*platform.User, error) {
	return s.FindUserFn(ctx, filter)
}

func (s *UserService) FindUsers(ctx context.Context, filter platform.UserFilter, opt ...platform.FindOptions) ([]*platform.User, int, error) {
	return s.FindUsersFn(ctx, filter, opt...)
}
func (s *UserService) CreateUser(ctx context.Context, u *platform.User) error {
	return s.CreateUserFn(ctx, u)
}

func (s *UserService) UpdateUser(ctx context.Context, id platform.ID, update platform.UserUpdate) (*platform.User, error) {
	return s.UpdateUserFn(ctx, id, update)
}

func (s *UserService) DeleteUser(ctx context.Context, id platform.ID) error {
	return s.DeleteUserFn(ctx, id)
}

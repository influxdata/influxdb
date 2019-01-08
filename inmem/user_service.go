package inmem

import (
	"context"
	"fmt"

	platform "github.com/influxdata/influxdb"
)

var _ platform.UserService = (*Service)(nil)

func (s *Service) loadUser(id platform.ID) (*platform.User, *platform.Error) {
	i, ok := s.userKV.Load(id.String())
	if !ok {
		return nil, &platform.Error{
			Code: platform.ENotFound,
			Msg:  "user not found",
		}
	}

	b, ok := i.(*platform.User)
	if !ok {
		return nil, &platform.Error{
			Code: platform.EInternal,
			Msg:  fmt.Sprintf("type %T is not a user", i),
		}
	}
	return b, nil
}

func (s *Service) forEachUser(ctx context.Context, fn func(b *platform.User) bool) error {
	var err error
	s.userKV.Range(func(k, v interface{}) bool {
		o, ok := v.(*platform.User)
		if !ok {
			err = fmt.Errorf("type %T is not a user", v)
			return false
		}

		return fn(o)
	})

	return err
}

// FindUserByID returns a single user by ID.
func (s *Service) FindUserByID(ctx context.Context, id platform.ID) (u *platform.User, err error) {
	var pe *platform.Error
	u, pe = s.loadUser(id)
	if pe != nil {
		err = &platform.Error{
			Op:  OpPrefix + platform.OpFindUserByID,
			Err: pe,
		}
	}
	return u, err
}

func (s *Service) findUserByName(ctx context.Context, n string) (*platform.User, error) {
	return s.FindUser(ctx, platform.UserFilter{Name: &n})
}

// FindUser returns the first user that matches a filter.
func (s *Service) FindUser(ctx context.Context, filter platform.UserFilter) (*platform.User, error) {
	op := OpPrefix + platform.OpFindUser
	if filter.ID != nil {
		u, err := s.FindUserByID(ctx, *filter.ID)
		if err != nil {
			return nil, &platform.Error{
				Op:  op,
				Err: err,
			}
		}
		return u, nil
	}

	if filter.Name != nil {
		var u *platform.User

		err := s.forEachUser(ctx, func(user *platform.User) bool {
			if user.Name == *filter.Name {
				u = user
				return false
			}
			return true
		})

		if err != nil {
			return nil, err
		}

		if u == nil {
			return nil, &platform.Error{
				Code: platform.ENotFound,
				Op:   op,
				Msg:  "user not found",
			}
		}

		return u, nil
	}

	return nil, &platform.Error{
		Code: platform.EInvalid,
		Op:   op,
		Msg:  "expected filter to contain name",
	}
}

// FindUsers will retrieve a list of users from storage.
func (s *Service) FindUsers(ctx context.Context, filter platform.UserFilter, opt ...platform.FindOptions) ([]*platform.User, int, error) {
	op := OpPrefix + platform.OpFindUsers
	if filter.ID != nil {
		u, err := s.FindUserByID(ctx, *filter.ID)
		if err != nil {
			return nil, 0, &platform.Error{
				Err: err,
				Op:  op,
			}
		}

		return []*platform.User{u}, 1, nil
	}
	if filter.Name != nil {
		u, err := s.FindUser(ctx, filter)
		if err != nil {
			return nil, 0, &platform.Error{
				Err: err,
				Op:  op,
			}
		}

		return []*platform.User{u}, 1, nil
	}

	users := []*platform.User{}

	err := s.forEachUser(ctx, func(user *platform.User) bool {
		users = append(users, user)
		return true
	})

	if err != nil {
		return nil, 0, err
	}

	return users, len(users), nil
}

// CreateUser will create an user into storage.
func (s *Service) CreateUser(ctx context.Context, u *platform.User) error {
	if _, err := s.FindUser(ctx, platform.UserFilter{Name: &u.Name}); err == nil {
		return &platform.Error{
			Code: platform.EConflict,
			Op:   OpPrefix + platform.OpCreateUser,
			Msg:  fmt.Sprintf("user with name %s already exists", u.Name),
		}
	}
	u.ID = s.IDGenerator.ID()
	s.PutUser(ctx, u)
	return nil
}

// PutUser put a user into storage.
func (s *Service) PutUser(ctx context.Context, o *platform.User) error {
	s.userKV.Store(o.ID.String(), o)
	return nil
}

// UpdateUser update a user in storage.
func (s *Service) UpdateUser(ctx context.Context, id platform.ID, upd platform.UserUpdate) (*platform.User, error) {
	o, err := s.FindUserByID(ctx, id)
	if err != nil {
		return nil, &platform.Error{
			Err: err,
			Op:  OpPrefix + platform.OpUpdateUser,
		}
	}

	if upd.Name != nil {
		o.Name = *upd.Name
	}

	s.userKV.Store(o.ID.String(), o)

	return o, nil
}

// DeleteUser remove a user from storage.
func (s *Service) DeleteUser(ctx context.Context, id platform.ID) error {
	if _, err := s.FindUserByID(ctx, id); err != nil {
		return &platform.Error{
			Err: err,
			Op:  OpPrefix + platform.OpDeleteUser,
		}
	}
	s.userKV.Delete(id.String())
	return nil
}

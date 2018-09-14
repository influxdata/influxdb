package inmem

import (
	"context"
	"fmt"

	"github.com/influxdata/platform"
)

func (s *Service) loadUser(id platform.ID) (*platform.User, error) {
	i, ok := s.userKV.Load(id.String())
	if !ok {
		return nil, fmt.Errorf("user not found")
	}

	b, ok := i.(*platform.User)
	if !ok {
		return nil, fmt.Errorf("type %T is not a user", i)
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
func (s *Service) FindUserByID(ctx context.Context, id platform.ID) (*platform.User, error) {
	return s.loadUser(id)
}

func (c *Service) findUserByName(ctx context.Context, n string) (*platform.User, error) {
	return c.FindUser(ctx, platform.UserFilter{Name: &n})
}

// FindUser returns the first user that matches a filter.
func (s *Service) FindUser(ctx context.Context, filter platform.UserFilter) (*platform.User, error) {
	if filter.Name != nil {
		var o *platform.User

		err := s.forEachUser(ctx, func(org *platform.User) bool {
			if org.Name == *filter.Name {
				o = org
				return false
			}
			return true
		})

		if err != nil {
			return nil, err
		}

		if o == nil {
			return nil, fmt.Errorf("user not found")
		}

		return o, nil
	}

	return nil, fmt.Errorf("expected filter to contain name")
}

func (s *Service) FindUsers(ctx context.Context, filter platform.UserFilter, opt ...platform.FindOptions) ([]*platform.User, int, error) {
	if filter.ID != nil {
		o, err := s.FindUserByID(ctx, *filter.ID)
		if err != nil {
			return nil, 0, err
		}

		return []*platform.User{o}, 1, nil
	}
	if filter.Name != nil {
		o, err := s.FindUser(ctx, filter)
		if err != nil {
			return nil, 0, err
		}

		return []*platform.User{o}, 1, nil
	}

	orgs := []*platform.User{}

	err := s.forEachUser(ctx, func(org *platform.User) bool {
		orgs = append(orgs, org)
		return true
	})

	if err != nil {
		return nil, 0, err
	}

	return orgs, len(orgs), nil
}

func (s *Service) CreateUser(ctx context.Context, u *platform.User) error {
	if _, err := s.FindUser(ctx, platform.UserFilter{Name: &u.Name}); err == nil {
		return fmt.Errorf("user with name %s already exists", u.Name)
	}
	u.ID = s.IDGenerator.ID()
	s.PutUser(ctx, u)
	return nil
}

func (s *Service) PutUser(ctx context.Context, o *platform.User) error {
	s.userKV.Store(o.ID.String(), o)
	return nil
}

func (s *Service) UpdateUser(ctx context.Context, id platform.ID, upd platform.UserUpdate) (*platform.User, error) {
	o, err := s.FindUserByID(ctx, id)
	if err != nil {
		return nil, err
	}

	if upd.Name != nil {
		o.Name = *upd.Name
	}

	s.userKV.Store(o.ID.String(), o)

	return o, nil
}

func (s *Service) DeleteUser(ctx context.Context, id platform.ID) error {
	if _, err := s.FindUserByID(ctx, id); err != nil {
		return err
	}
	s.userKV.Delete(id.String())
	return nil
}

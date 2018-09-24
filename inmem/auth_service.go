package inmem

import (
	"context"
	"fmt"

	"github.com/influxdata/platform"
)

var (
	errTokenNotFound = fmt.Errorf("authorization not found")
)

func (s *Service) loadAuthorization(ctx context.Context, id platform.ID) (*platform.Authorization, error) {
	i, ok := s.authorizationKV.Load(id.String())
	if !ok {
		return nil, errTokenNotFound
	}

	a, ok := i.(platform.Authorization)
	if !ok {
		return nil, fmt.Errorf("value found in map is not an authorization")
	}

	if a.Status == "" {
		a.Status = platform.Active
	}

	if err := s.setUserOnAuthorization(ctx, &a); err != nil {
		return nil, err
	}

	return &a, nil
}

func (s *Service) setUserOnAuthorization(ctx context.Context, a *platform.Authorization) error {
	u, err := s.loadUser(a.UserID)
	if err != nil {
		return err
	}

	a.User = u.Name
	return nil
}

// PutAuthorization overwrites the authorization with the contents of a.
func (s *Service) PutAuthorization(ctx context.Context, a *platform.Authorization) error {
	if a.Status == "" {
		a.Status = platform.Active
	}
	s.authorizationKV.Store(a.ID.String(), *a)
	return nil
}

// FindAuthorizationByID returns an authorization given an ID.
func (s *Service) FindAuthorizationByID(ctx context.Context, id platform.ID) (*platform.Authorization, error) {
	return s.loadAuthorization(ctx, id)
}

// FindAuthorizationByToken returns an authorization given a token.
func (s *Service) FindAuthorizationByToken(ctx context.Context, t string) (*platform.Authorization, error) {
	as, n, err := s.FindAuthorizations(ctx, platform.AuthorizationFilter{Token: &t})
	if err != nil {
		return nil, err
	}
	if n < 1 {
		return nil, errTokenNotFound
	}
	return as[0], nil
}

func filterAuthorizationsFn(filter platform.AuthorizationFilter) func(a *platform.Authorization) bool {
	if filter.ID != nil {
		return func(a *platform.Authorization) bool {
			return a.ID == *filter.ID
		}
	}

	if filter.Token != nil {
		return func(a *platform.Authorization) bool {
			return a.Token == *filter.Token
		}
	}

	if filter.UserID != nil {
		return func(a *platform.Authorization) bool {
			return a.UserID == *filter.UserID
		}
	}

	return func(a *platform.Authorization) bool { return true }
}

// FindAuthorizations returns all authorizations matching the filter.
func (s *Service) FindAuthorizations(ctx context.Context, filter platform.AuthorizationFilter, opt ...platform.FindOptions) ([]*platform.Authorization, int, error) {
	if filter.ID != nil {
		a, err := s.FindAuthorizationByID(ctx, *filter.ID)
		if err != nil {
			return nil, 0, err
		}

		return []*platform.Authorization{a}, 1, nil
	}

	var as []*platform.Authorization
	if filter.User != nil {
		u, err := s.findUserByName(ctx, *filter.User)
		if err != nil {
			return nil, 0, err
		}
		filter.UserID = &u.ID
	}
	var err error
	filterF := filterAuthorizationsFn(filter)
	s.authorizationKV.Range(func(k, v interface{}) bool {
		a, ok := v.(platform.Authorization)
		if !ok {
			err = fmt.Errorf("value found in map is not an authorization")
			return false
		}

		if err = s.setUserOnAuthorization(ctx, &a); err != nil {
			return false
		}

		if filterF(&a) {
			as = append(as, &a)
		}

		return true
	})

	if err != nil {
		return nil, 0, err
	}

	return as, len(as), nil
}

// CreateAuthorization sets a.Token and a.ID and creates an platform.Authorization
func (s *Service) CreateAuthorization(ctx context.Context, a *platform.Authorization) error {
	if !a.UserID.Valid() {
		u, err := s.findUserByName(ctx, a.User)
		if err != nil {
			return err
		}
		a.UserID = u.ID
	}
	var err error
	a.Token, err = s.TokenGenerator.Token()
	if err != nil {
		return err
	}
	a.ID = s.IDGenerator.ID()
	a.Status = platform.Active
	return s.PutAuthorization(ctx, a)
}

// DeleteAuthorization deletes an authorization associated with id.
func (s *Service) DeleteAuthorization(ctx context.Context, id platform.ID) error {
	if _, err := s.FindAuthorizationByID(ctx, id); err != nil {
		return err
	}

	s.authorizationKV.Delete(id.String())
	return nil
}

// SetAuthorizationStatus updates the status of an authorization associated with id.
func (s *Service) SetAuthorizationStatus(ctx context.Context, id platform.ID, status platform.Status) error {
	a, err := s.FindAuthorizationByID(ctx, id)
	if err != nil {
		return err
	}

	switch status {
	case platform.Active, platform.Inactive:
	default:
		return fmt.Errorf("unknown authorization status")
	}

	if a.Status == status {
		return nil
	}

	a.Status = status
	return s.PutAuthorization(ctx, a)
}

package organizations

import (
	"context"

	"github.com/influxdata/chronograf"
)

var _ chronograf.ServersStore = &ServersStore{}

type ServersStore struct {
	store        chronograf.ServersStore
	organization string
}

func NewServersStore(s chronograf.ServersStore, org string) *ServersStore {
	return &ServersStore{
		store:        s,
		organization: org,
	}
}

func (s *ServersStore) All(ctx context.Context) ([]chronograf.Server, error) {
	err := validOrganization(ctx)
	if err != nil {
		return nil, err
	}
	ds, err := s.store.All(ctx)
	if err != nil {
		return nil, err
	}

	dashboards := ds[:0]
	for _, d := range ds {
		if d.Organization == s.organization {
			dashboards = append(dashboards, d)
		}
	}

	return dashboards, nil
}

func (s *ServersStore) Add(ctx context.Context, d chronograf.Server) (chronograf.Server, error) {
	err := validOrganization(ctx)
	if err != nil {
		return chronograf.Server{}, err
	}

	d.Organization = s.organization
	return s.store.Add(ctx, d)
}

func (s *ServersStore) Delete(ctx context.Context, d chronograf.Server) error {
	err := validOrganization(ctx)
	if err != nil {
		return err
	}

	d, err = s.store.Get(ctx, d.ID)
	if err != nil {
		return err
	}

	return s.store.Delete(ctx, d)
}

func (s *ServersStore) Get(ctx context.Context, id int) (chronograf.Server, error) {
	err := validOrganization(ctx)
	if err != nil {
		return chronograf.Server{}, err
	}

	d, err := s.store.Get(ctx, id)
	if err != nil {
		return chronograf.Server{}, err
	}

	if d.Organization != s.organization {
		return chronograf.Server{}, chronograf.ErrServerNotFound
	}

	return d, nil
}

func (s *ServersStore) Update(ctx context.Context, d chronograf.Server) error {
	err := validOrganization(ctx)
	if err != nil {
		return err
	}

	_, err = s.store.Get(ctx, d.ID)
	if err != nil {
		return err
	}

	return s.store.Update(ctx, d)
}

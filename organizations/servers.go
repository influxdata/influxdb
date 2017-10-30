package organizations

import (
	"context"

	"github.com/influxdata/chronograf"
)

var _ chronograf.ServersStore = &ServersStore{}

type ServersStore struct {
	store chronograf.ServersStore
}

func NewServersStore(s chronograf.ServersStore) *ServersStore {
	return &ServersStore{
		store: s,
	}
}

func (s *ServersStore) All(ctx context.Context) ([]chronograf.Server, error) {
	org, err := validOrganization(ctx)
	if err != nil {
		return nil, err
	}
	ds, err := s.store.All(ctx)
	if err != nil {
		return nil, err
	}

	dashboards := ds[:0]
	for _, d := range ds {
		if d.Organization == org {
			dashboards = append(dashboards, d)
		}
	}

	return dashboards, nil
}

func (s *ServersStore) Add(ctx context.Context, d chronograf.Server) (chronograf.Server, error) {
	org, err := validOrganization(ctx)
	if err != nil {
		return chronograf.Server{}, err
	}

	d.Organization = org
	return s.store.Add(ctx, d)
}

func (s *ServersStore) Delete(ctx context.Context, d chronograf.Server) error {
	d, err := s.store.Get(ctx, d.ID)
	if err != nil {
		return err
	}

	return s.store.Delete(ctx, d)
}

func (s *ServersStore) Get(ctx context.Context, id int) (chronograf.Server, error) {
	org, err := validOrganization(ctx)
	if err != nil {
		return chronograf.Server{}, err
	}

	d, err := s.store.Get(ctx, id)
	if err != nil {
		return chronograf.Server{}, err
	}

	if d.Organization != org {
		return chronograf.Server{}, chronograf.ErrServerNotFound
	}

	return d, nil
}

func (s *ServersStore) Update(ctx context.Context, d chronograf.Server) error {
	_, err := s.store.Get(ctx, d.ID)
	if err != nil {
		return err
	}

	return s.store.Update(ctx, d)
}

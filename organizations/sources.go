package organizations

import (
	"context"

	"github.com/influxdata/chronograf"
)

var _ chronograf.SourcesStore = &SourcesStore{}

type SourcesStore struct {
	store chronograf.SourcesStore
}

func NewSourcesStore(s chronograf.SourcesStore) *SourcesStore {
	return &SourcesStore{
		store: s,
	}
}

func (s *SourcesStore) All(ctx context.Context) ([]chronograf.Source, error) {
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

func (s *SourcesStore) Add(ctx context.Context, d chronograf.Source) (chronograf.Source, error) {
	org, err := validOrganization(ctx)
	if err != nil {
		return chronograf.Source{}, err
	}

	d.Organization = org
	return s.store.Add(ctx, d)
}

func (s *SourcesStore) Delete(ctx context.Context, d chronograf.Source) error {
	d, err := s.store.Get(ctx, d.ID)
	if err != nil {
		return err
	}

	return s.store.Delete(ctx, d)
}

func (s *SourcesStore) Get(ctx context.Context, id int) (chronograf.Source, error) {
	org, err := validOrganization(ctx)
	if err != nil {
		return chronograf.Source{}, err
	}

	d, err := s.store.Get(ctx, id)
	if err != nil {
		return chronograf.Source{}, err
	}

	if d.Organization != org {
		return chronograf.Source{}, chronograf.ErrSourceNotFound
	}

	return d, nil
}

func (s *SourcesStore) Update(ctx context.Context, d chronograf.Source) error {
	_, err := s.store.Get(ctx, d.ID)
	if err != nil {
		return err
	}

	return s.store.Update(ctx, d)
}

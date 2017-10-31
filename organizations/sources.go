package organizations

import (
	"context"

	"github.com/influxdata/chronograf"
)

var _ chronograf.SourcesStore = &SourcesStore{}

type SourcesStore struct {
	store        chronograf.SourcesStore
	organization string
}

func NewSourcesStore(s chronograf.SourcesStore, org string) *SourcesStore {
	return &SourcesStore{
		store:        s,
		organization: org,
	}
}

func (s *SourcesStore) All(ctx context.Context) ([]chronograf.Source, error) {
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

func (s *SourcesStore) Add(ctx context.Context, d chronograf.Source) (chronograf.Source, error) {
	err := validOrganization(ctx)
	if err != nil {
		return chronograf.Source{}, err
	}

	d.Organization = s.organization
	return s.store.Add(ctx, d)
}

func (s *SourcesStore) Delete(ctx context.Context, d chronograf.Source) error {
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

func (s *SourcesStore) Get(ctx context.Context, id int) (chronograf.Source, error) {
	err := validOrganization(ctx)
	if err != nil {
		return chronograf.Source{}, err
	}

	d, err := s.store.Get(ctx, id)
	if err != nil {
		return chronograf.Source{}, err
	}

	if d.Organization != s.organization {
		return chronograf.Source{}, chronograf.ErrSourceNotFound
	}

	return d, nil
}

func (s *SourcesStore) Update(ctx context.Context, d chronograf.Source) error {
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

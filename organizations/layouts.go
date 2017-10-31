package organizations

import (
	"context"

	"github.com/influxdata/chronograf"
)

var _ chronograf.LayoutsStore = &LayoutsStore{}

type LayoutsStore struct {
	store        chronograf.LayoutsStore
	organization string
}

func NewLayoutsStore(s chronograf.LayoutsStore, org string) *LayoutsStore {
	return &LayoutsStore{
		store:        s,
		organization: org,
	}
}

func (s *LayoutsStore) All(ctx context.Context) ([]chronograf.Layout, error) {
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

func (s *LayoutsStore) Add(ctx context.Context, d chronograf.Layout) (chronograf.Layout, error) {
	err := validOrganization(ctx)
	if err != nil {
		return chronograf.Layout{}, err
	}

	d.Organization = s.organization
	return s.store.Add(ctx, d)
}

func (s *LayoutsStore) Delete(ctx context.Context, d chronograf.Layout) error {
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

func (s *LayoutsStore) Get(ctx context.Context, id string) (chronograf.Layout, error) {
	err := validOrganization(ctx)
	if err != nil {
		return chronograf.Layout{}, err
	}

	d, err := s.store.Get(ctx, id)
	if err != nil {
		return chronograf.Layout{}, err
	}

	if d.Organization != s.organization {
		return chronograf.Layout{}, chronograf.ErrLayoutNotFound
	}

	return d, nil
}

func (s *LayoutsStore) Update(ctx context.Context, d chronograf.Layout) error {
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

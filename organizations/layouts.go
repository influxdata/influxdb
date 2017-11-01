package organizations

import (
	"context"

	"github.com/influxdata/chronograf"
)

// ensure that LayoutsStore implements chronograf.LayoutStore
var _ chronograf.LayoutsStore = &LayoutsStore{}

// LayoutsStore facade on a LayoutStore that filters layouts
// by organization.
type LayoutsStore struct {
	store        chronograf.LayoutsStore
	organization string
}

// NewLayoutsStore creates a new LayoutsStore from an existing
// chronograf.LayoutStore and an organization string
func NewLayoutsStore(s chronograf.LayoutsStore, org string) *LayoutsStore {
	return &LayoutsStore{
		store:        s,
		organization: org,
	}
}

// All retrieves all layouts from the underlying LayoutStore and filters them
// by organization.
func (s *LayoutsStore) All(ctx context.Context) ([]chronograf.Layout, error) {
	err := validOrganization(ctx)
	if err != nil {
		return nil, err
	}

	ds, err := s.store.All(ctx)
	if err != nil {
		return nil, err
	}

	// This filters layouts without allocating
	// https://github.com/golang/go/wiki/SliceTricks#filtering-without-allocating
	layouts := ds[:0]
	for _, d := range ds {
		if d.Organization == s.organization {
			layouts = append(layouts, d)
		}
	}

	return layouts, nil
}

// Add creates a new Layout in the LayoutsStore with layout.Organization set to be the
// organization from the layout store.
func (s *LayoutsStore) Add(ctx context.Context, d chronograf.Layout) (chronograf.Layout, error) {
	err := validOrganization(ctx)
	if err != nil {
		return chronograf.Layout{}, err
	}

	d.Organization = s.organization
	return s.store.Add(ctx, d)
}

// Delete the layout from LayoutsStore
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

// Get returns a Layout if the id exists and belongs to the organization that is set.
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

// Update the layout in LayoutsStore.
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

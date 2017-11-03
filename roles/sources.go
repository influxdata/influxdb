package roles

import (
	"context"

	"github.com/influxdata/chronograf"
)

// ensure that SourcesStore implements chronograf.SourceStore
var _ chronograf.SourcesStore = &SourcesStore{}

// SourcesStore facade on a SourceStore that filters sources
// by role.
type SourcesStore struct {
	store chronograf.SourcesStore
	role  string
}

// NewSourcesStore creates a new SourcesStore from an existing
// chronograf.SourceStore and an role string
func NewSourcesStore(s chronograf.SourcesStore, role string) *SourcesStore {
	return &SourcesStore{
		store: s,
		role:  role,
	}
}

// All retrieves all sources from the underlying SourceStore and filters them
// by role.
func (s *SourcesStore) All(ctx context.Context) ([]chronograf.Source, error) {
	err := validRole(ctx)
	if err != nil {
		return nil, err
	}

	ds, err := s.store.All(ctx)
	if err != nil {
		return nil, err
	}

	// This filters sources without allocating
	// https://github.com/golang/go/wiki/SliceTricks#filtering-without-allocating
	sources := ds[:0]
	for _, d := range ds {
		if hasAuthorizedRole(d.Role, s.role) {
			sources = append(sources, d)
		}
	}

	return sources, nil
}

// Add creates a new Source in the SourcesStore with source.Role set to be the
// role from the source store.
func (s *SourcesStore) Add(ctx context.Context, d chronograf.Source) (chronograf.Source, error) {
	err := validRole(ctx)
	if err != nil {
		return chronograf.Source{}, err
	}

	return s.store.Add(ctx, d)
}

// Delete the source from SourcesStore
func (s *SourcesStore) Delete(ctx context.Context, d chronograf.Source) error {
	err := validRole(ctx)
	if err != nil {
		return err
	}

	d, err = s.store.Get(ctx, d.ID)
	if err != nil {
		return err
	}

	return s.store.Delete(ctx, d)
}

// Get returns a Source if the id exists and belongs to the role that is set.
func (s *SourcesStore) Get(ctx context.Context, id int) (chronograf.Source, error) {
	err := validRole(ctx)
	if err != nil {
		return chronograf.Source{}, err
	}

	d, err := s.store.Get(ctx, id)
	if err != nil {
		return chronograf.Source{}, err
	}

	if !hasAuthorizedRole(d.Role, s.role) {
		return chronograf.Source{}, chronograf.ErrSourceNotFound
	}

	return d, nil
}

// Update the source in SourcesStore.
func (s *SourcesStore) Update(ctx context.Context, d chronograf.Source) error {
	err := validRole(ctx)
	if err != nil {
		return err
	}

	_, err = s.store.Get(ctx, d.ID)
	if err != nil {
		return err
	}

	return s.store.Update(ctx, d)
}

func hasAuthorizedRole(sourceRole, providedRole string) bool {
	switch sourceRole {
	case ViewerRoleName:
		switch providedRole {
		case ViewerRoleName, EditorRoleName, AdminRoleName:
			return true
		}
	case EditorRoleName:
		switch providedRole {
		case EditorRoleName, AdminRoleName:
			return true
		}
	case AdminRoleName:
		switch providedRole {
		case AdminRoleName:
			return true
		}
	}

	return false
}

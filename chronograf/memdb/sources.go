package memdb

import (
	"context"
	"fmt"

	"github.com/influxdata/platform/chronograf"
)

// Ensure SourcesStore implements chronograf.SourcesStore.
var _ chronograf.SourcesStore = &SourcesStore{}

// SourcesStore implements the chronograf.SourcesStore interface
type SourcesStore struct {
	Source *chronograf.Source
}

// Add does not have any effect
func (store *SourcesStore) Add(ctx context.Context, src chronograf.Source) (chronograf.Source, error) {
	return chronograf.Source{}, fmt.Errorf("In-memory SourcesStore does not support adding a Source")
}

// All will return a slice containing a configured source
func (store *SourcesStore) All(ctx context.Context) ([]chronograf.Source, error) {
	if store.Source != nil {
		return []chronograf.Source{*store.Source}, nil
	}
	return nil, nil
}

// Delete removes the SourcesStore.Soruce if it matches the provided Source
func (store *SourcesStore) Delete(ctx context.Context, src chronograf.Source) error {
	if store.Source == nil || store.Source.ID != src.ID {
		return fmt.Errorf("Unable to find Source with id %d", src.ID)
	}
	store.Source = nil
	return nil
}

// Get returns the configured source if the id matches
func (store *SourcesStore) Get(ctx context.Context, id int) (chronograf.Source, error) {
	if store.Source == nil || store.Source.ID != id {
		return chronograf.Source{}, fmt.Errorf("Unable to find Source with id %d", id)
	}
	return *store.Source, nil
}

// Update does nothing
func (store *SourcesStore) Update(ctx context.Context, src chronograf.Source) error {
	if store.Source == nil || store.Source.ID != src.ID {
		return fmt.Errorf("Unable to find Source with id %d", src.ID)
	}
	store.Source = &src
	return nil
}

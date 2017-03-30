package memdb

import (
	"context"
	"fmt"

	"github.com/influxdata/chronograf"
)

// Ensure MultiSourcesStore and SourcesStore implements chronograf.SourcesStore.
var _ chronograf.SourcesStore = &SourcesStore{}
var _ chronograf.SourcesStore = &MultiSourcesStore{}

// MultiSourcesStore delegates to the SourcesStores that compose it
type MultiSourcesStore struct {
	Stores []chronograf.SourcesStore
}

// All concatenates the Sources of all contained Stores
func (multi *MultiSourcesStore) All(ctx context.Context) ([]chronograf.Source, error) {
	all := []chronograf.Source{}
	sourceSet := map[int]struct{}{}

	ok := false
	var err error
	for _, store := range multi.Stores {
		var sources []chronograf.Source
		sources, err = store.All(ctx)
		if err != nil {
			// If this Store is unable to return an array of sources, skip to the
			// next Store.
			continue
		}
		ok = true // We've received a response from at least one Store
		for _, s := range sources {
			// Enforce that the source has a unique ID
			// If the source has been seen before, don't override what we already have
			if _, okay := sourceSet[s.ID]; !okay { // We have a new Source!
				sourceSet[s.ID] = struct{}{} // We just care that the ID is unique
				all = append(all, s)
			}
		}
	}
	if !ok {
		return nil, err
	}
	return all, nil
}

// Add the src to the first Store to respond successfully
func (multi *MultiSourcesStore) Add(ctx context.Context, src chronograf.Source) (chronograf.Source, error) {
	var err error
	for _, store := range multi.Stores {
		var s chronograf.Source
		s, err = store.Add(ctx, src)
		if err == nil {
			return s, nil
		}
	}
	return chronograf.Source{}, nil
}

// Delete delegates to all stores, returns success if one Store is successful
func (multi *MultiSourcesStore) Delete(ctx context.Context, src chronograf.Source) error {
	var err error
	for _, store := range multi.Stores {
		err = store.Delete(ctx, src)
		if err == nil {
			return nil
		}
	}
	return err
}

// Get finds the Source by id among all contained Stores
func (multi *MultiSourcesStore) Get(ctx context.Context, id int) (chronograf.Source, error) {
	var err error
	for _, store := range multi.Stores {
		var s chronograf.Source
		s, err = store.Get(ctx, id)
		if err == nil {
			return s, nil
		}
	}
	return chronograf.Source{}, err
}

// Update the first store to return a successful response
func (multi *MultiSourcesStore) Update(ctx context.Context, src chronograf.Source) error {
	var err error
	for _, store := range multi.Stores {
		err = store.Update(ctx, src)
		if err == nil {
			return nil
		}
	}
	return err
}

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

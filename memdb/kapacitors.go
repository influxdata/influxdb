package memdb

import (
	"context"
	"fmt"

	"github.com/influxdata/chronograf"
)

// Ensure KapacitorStore and MultiKapacitorStore implements chronograf.ServersStore.
var _ chronograf.ServersStore = &KapacitorStore{}
var _ chronograf.ServersStore = &MultiKapacitorStore{}

// KapacitorStore implements the chronograf.ServersStore interface, and keeps
// an in-memory Kapacitor according to startup configuration
type KapacitorStore struct {
	Kapacitor *chronograf.Server
}

// All will return a slice containing a configured source
func (store *KapacitorStore) All(ctx context.Context) ([]chronograf.Server, error) {
	if store.Kapacitor != nil {
		return []chronograf.Server{*store.Kapacitor}, nil
	}
	return nil, nil
}

// Add does not have any effect
func (store *KapacitorStore) Add(ctx context.Context, kap chronograf.Server) (chronograf.Server, error) {
	return chronograf.Server{}, fmt.Errorf("In-memory KapacitorStore does not support adding a Kapacitor")
}

// Delete removes the in-memory configured Kapacitor if its ID matches what's provided
func (store *KapacitorStore) Delete(ctx context.Context, kap chronograf.Server) error {
	if store.Kapacitor == nil || store.Kapacitor.ID != kap.ID {
		return fmt.Errorf("Unable to find Kapacitor with id %d", kap.ID)
	}
	store.Kapacitor = nil
	return nil
}

// Get returns the in-memory Kapacitor if its ID matches what's provided
func (store *KapacitorStore) Get(ctx context.Context, id int) (chronograf.Server, error) {
	if store.Kapacitor == nil || store.Kapacitor.ID != id {
		return chronograf.Server{}, fmt.Errorf("Unable to find Kapacitor with id %d", id)
	}
	return *store.Kapacitor, nil
}

// Update overwrites the in-memory configured Kapacitor if its ID matches what's provided
func (store *KapacitorStore) Update(ctx context.Context, kap chronograf.Server) error {
	if store.Kapacitor == nil || store.Kapacitor.ID != kap.ID {
		return fmt.Errorf("Unable to find Kapacitor with id %d", kap.ID)
	}
	store.Kapacitor = &kap
	return nil
}

// MultiKapacitorStore implements the chronograf.ServersStore interface, and
// delegates to all contained KapacitorStores
type MultiKapacitorStore struct {
	Stores []chronograf.ServersStore
}

// All concatenates the Kapacitors of all contained Stores
func (multi *MultiKapacitorStore) All(ctx context.Context) ([]chronograf.Server, error) {
	all := []chronograf.Server{}
	kapSet := map[int]struct{}{}

	ok := false
	var err error
	for _, store := range multi.Stores {
		var kaps []chronograf.Server
		kaps, err = store.All(ctx)
		if err != nil {
			// If this Store is unable to return an array of kapacitors, skip to the
			// next Store.
			continue
		}
		ok = true // We've received a response from at least one Store
		for _, kap := range kaps {
			// Enforce that the kapacitor has a unique ID
			// If the ID has been seen before, ignore the kapacitor
			if _, okay := kapSet[kap.ID]; !okay { // We have a new kapacitor
				kapSet[kap.ID] = struct{}{} // We just care that the ID is unique
				all = append(all, kap)
			}
		}
	}
	if !ok {
		return nil, err
	}
	return all, nil
}

// Add the kap to the first responsive Store
func (multi *MultiKapacitorStore) Add(ctx context.Context, kap chronograf.Server) (chronograf.Server, error) {
	var err error
	for _, store := range multi.Stores {
		var k chronograf.Server
		k, err = store.Add(ctx, kap)
		if err == nil {
			return k, nil
		}
	}
	return chronograf.Server{}, nil
}

// Delete delegates to all Stores, returns success if one Store is successful
func (multi *MultiKapacitorStore) Delete(ctx context.Context, kap chronograf.Server) error {
	var err error
	for _, store := range multi.Stores {
		err = store.Delete(ctx, kap)
		if err == nil {
			return nil
		}
	}
	return err
}

// Get finds the Source by id among all contained Stores
func (multi *MultiKapacitorStore) Get(ctx context.Context, id int) (chronograf.Server, error) {
	var err error
	for _, store := range multi.Stores {
		var k chronograf.Server
		k, err = store.Get(ctx, id)
		if err == nil {
			return k, nil
		}
	}
	return chronograf.Server{}, nil
}

// Update the first responsive Store
func (multi *MultiKapacitorStore) Update(ctx context.Context, kap chronograf.Server) error {
	var err error
	for _, store := range multi.Stores {
		err = store.Update(ctx, kap)
		if err == nil {
			return nil
		}
	}
	return err
}

package multistore

import (
	"context"

	"github.com/influxdata/chronograf"
)

// Ensure KapacitorStore implements chronograf.ServersStore.
var _ chronograf.ServersStore = &KapacitorStore{}

// KapacitorStore implements the chronograf.ServersStore interface, and
// delegates to all contained KapacitorStores
type KapacitorStore struct {
	Stores []chronograf.ServersStore
}

// All concatenates the Kapacitors of all contained Stores
func (multi *KapacitorStore) All(ctx context.Context) ([]chronograf.Server, error) {
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
func (multi *KapacitorStore) Add(ctx context.Context, kap chronograf.Server) (chronograf.Server, error) {
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
func (multi *KapacitorStore) Delete(ctx context.Context, kap chronograf.Server) error {
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
func (multi *KapacitorStore) Get(ctx context.Context, id int) (chronograf.Server, error) {
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
func (multi *KapacitorStore) Update(ctx context.Context, kap chronograf.Server) error {
	var err error
	for _, store := range multi.Stores {
		err = store.Update(ctx, kap)
		if err == nil {
			return nil
		}
	}
	return err
}

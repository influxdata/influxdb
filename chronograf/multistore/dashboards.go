package multistore

import (
	"context"

	"github.com/influxdata/platform/chronograf"
)

// Ensure DashboardsStore implements chronograf.DashboardsStore.
var _ chronograf.DashboardsStore = &DashboardsStore{}

// DashboardsStore implements the chronograf.DashboardsStore interface, and
// delegates to all contained DashboardsStores
type DashboardsStore struct {
	Stores []chronograf.DashboardsStore
}

// All concatenates the Dashboards of all contained Stores
func (multi *DashboardsStore) All(ctx context.Context) ([]chronograf.Dashboard, error) {
	all := []chronograf.Dashboard{}
	boardSet := map[chronograf.DashboardID]struct{}{}

	ok := false
	var err error
	for _, store := range multi.Stores {
		var boards []chronograf.Dashboard
		boards, err = store.All(ctx)
		if err != nil {
			// If this Store is unable to return an array of dashboards, skip to the
			// next Store.
			continue
		}
		ok = true // We've received a response from at least one Store
		for _, board := range boards {
			// Enforce that the dashboard has a unique ID
			// If the ID has been seen before, ignore the dashboard
			if _, okay := boardSet[board.ID]; !okay { // We have a new dashboard
				boardSet[board.ID] = struct{}{} // We just care that the ID is unique
				all = append(all, board)
			}
		}
	}
	if !ok {
		return nil, err
	}
	return all, nil
}

// Add the dashboard to the first responsive Store
func (multi *DashboardsStore) Add(ctx context.Context, dashboard chronograf.Dashboard) (chronograf.Dashboard, error) {
	var err error
	for _, store := range multi.Stores {
		var d chronograf.Dashboard
		d, err = store.Add(ctx, dashboard)
		if err == nil {
			return d, nil
		}
	}
	return chronograf.Dashboard{}, nil
}

// Delete delegates to all Stores, returns success if one Store is successful
func (multi *DashboardsStore) Delete(ctx context.Context, dashboard chronograf.Dashboard) error {
	var err error
	for _, store := range multi.Stores {
		err = store.Delete(ctx, dashboard)
		if err == nil {
			return nil
		}
	}
	return err
}

// Get finds the Dashboard by id among all contained Stores
func (multi *DashboardsStore) Get(ctx context.Context, id chronograf.DashboardID) (chronograf.Dashboard, error) {
	var err error
	for _, store := range multi.Stores {
		var d chronograf.Dashboard
		d, err = store.Get(ctx, id)
		if err == nil {
			return d, nil
		}
	}
	return chronograf.Dashboard{}, nil
}

// Update the first responsive Store
func (multi *DashboardsStore) Update(ctx context.Context, dashboard chronograf.Dashboard) error {
	var err error
	for _, store := range multi.Stores {
		err = store.Update(ctx, dashboard)
		if err == nil {
			return nil
		}
	}
	return err
}

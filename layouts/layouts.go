package layouts

import (
	"github.com/influxdata/mrfusion"
	"golang.org/x/net/context"
)

// MultiLayoutStore is a Layoutstore that contains multiple LayoutStores
// The All method will return the set of all Layouts.
// Each method will be tried against the Stores slice serially.
type MultiLayoutStore struct {
	Stores []mrfusion.LayoutStore
}

// All returns the set of all layouts
func (s *MultiLayoutStore) All(ctx context.Context) ([]mrfusion.Layout, error) {
	all := []mrfusion.Layout{}
	ok := false
	var err error
	for _, store := range s.Stores {
		layouts, err := store.All(ctx)
		if err != nil {
			// Try to load as many layouts as possible
			continue
		}
		ok = true
		all = append(all, layouts...)
	}
	if !ok {
		return nil, err
	}
	return all, nil
}

// Add creates a new dashboard in the LayoutStore.  Tries each store sequentially until success.
func (s *MultiLayoutStore) Add(ctx context.Context, layout mrfusion.Layout) (mrfusion.Layout, error) {
	var err error
	for _, store := range s.Stores {
		var l mrfusion.Layout
		l, err = store.Add(ctx, layout)
		if err == nil {
			return l, nil
		}
	}
	return mrfusion.Layout{}, err
}

// Delete the dashboard from the store.  Searches through all stores to find Layout and
// then deletes from that store.
func (s *MultiLayoutStore) Delete(ctx context.Context, layout mrfusion.Layout) error {
	var err error
	for _, store := range s.Stores {
		err = store.Delete(ctx, layout)
		if err == nil {
			return nil
		}
	}
	return err
}

// Get retrieves Layout if `ID` exists.  Searches through each store sequentially until success.
func (s *MultiLayoutStore) Get(ctx context.Context, ID string) (mrfusion.Layout, error) {
	var err error
	for _, store := range s.Stores {
		var l mrfusion.Layout
		l, err = store.Get(ctx, ID)
		if err == nil {
			return l, nil
		}
	}
	return mrfusion.Layout{}, err
}

// Update the dashboard in the store.  Searches through each store sequentially until success.
func (s *MultiLayoutStore) Update(ctx context.Context, layout mrfusion.Layout) error {
	var err error
	for _, store := range s.Stores {
		err = store.Update(ctx, layout)
		if err == nil {
			return nil
		}
	}
	return err
}

package layouts

import (
	"context"

	"github.com/influxdata/mrfusion"
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
		ok := true
		all = append(all, layouts...)
	}
	if !ok {
		return nil, err
	}
	return all, nil
}

// Add creates a new dashboard in the LayoutStore.  Tries each store sequentially until success.
func (s *MultiLayoutStore) Add(context.Context, mrfusion.Layout) (mrfusion.Layout, error) {
}

// Delete the dashboard from the store.  Searches through all stores to find Layout and
// then deletes from that store.
func (s *MultiLayoutStore) Delete(context.Context, mrfusion.Layout) error {
}

// Get retrieves Layout if `ID` exists.  Searches through each store sequentially until success.
func (s *MultiLayoutStore) Get(ctx context.Context, ID int) (mrfusion.Layout, error) {
}

// Update the dashboard in the store.  Searches through each store sequentially until success.
func (s *MultiLayoutStore) Update(context.Context, mrfusion.Layout) error {
}

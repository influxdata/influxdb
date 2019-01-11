package noop

import (
	"context"
	"fmt"

	"github.com/influxdata/influxdb/chronograf"
)

// ensure LayoutsStore implements chronograf.LayoutsStore
var _ chronograf.LayoutsStore = &LayoutsStore{}

type LayoutsStore struct{}

func (s *LayoutsStore) All(context.Context) ([]chronograf.Layout, error) {
	return nil, fmt.Errorf("no layouts found")
}

func (s *LayoutsStore) Add(context.Context, chronograf.Layout) (chronograf.Layout, error) {
	return chronograf.Layout{}, fmt.Errorf("failed to add layout")
}

func (s *LayoutsStore) Delete(context.Context, chronograf.Layout) error {
	return fmt.Errorf("failed to delete layout")
}

func (s *LayoutsStore) Get(ctx context.Context, ID string) (chronograf.Layout, error) {
	return chronograf.Layout{}, chronograf.ErrLayoutNotFound
}

func (s *LayoutsStore) Update(context.Context, chronograf.Layout) error {
	return fmt.Errorf("failed to update layout")
}

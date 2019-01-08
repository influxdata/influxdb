package noop

import (
	"context"
	"fmt"

	"github.com/influxdata/influxdb/chronograf"
)

// ensure MappingsStore implements chronograf.MappingsStore
var _ chronograf.MappingsStore = &MappingsStore{}

type MappingsStore struct{}

func (s *MappingsStore) All(context.Context) ([]chronograf.Mapping, error) {
	return nil, fmt.Errorf("no mappings found")
}

func (s *MappingsStore) Add(context.Context, *chronograf.Mapping) (*chronograf.Mapping, error) {
	return nil, fmt.Errorf("failed to add mapping")
}

func (s *MappingsStore) Delete(context.Context, *chronograf.Mapping) error {
	return fmt.Errorf("failed to delete mapping")
}

func (s *MappingsStore) Get(ctx context.Context, ID string) (*chronograf.Mapping, error) {
	return nil, chronograf.ErrMappingNotFound
}

func (s *MappingsStore) Update(context.Context, *chronograf.Mapping) error {
	return fmt.Errorf("failed to update mapping")
}

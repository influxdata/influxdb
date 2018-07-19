package noop

import (
	"context"
	"fmt"

	"github.com/influxdata/platform/chronograf"
)

// ensure SourcesStore implements chronograf.SourcesStore
var _ chronograf.SourcesStore = &SourcesStore{}

type SourcesStore struct{}

func (s *SourcesStore) All(context.Context) ([]chronograf.Source, error) {
	return nil, fmt.Errorf("no sources found")
}

func (s *SourcesStore) Add(context.Context, chronograf.Source) (chronograf.Source, error) {
	return chronograf.Source{}, fmt.Errorf("failed to add source")
}

func (s *SourcesStore) Delete(context.Context, chronograf.Source) error {
	return fmt.Errorf("failed to delete source")
}

func (s *SourcesStore) Get(ctx context.Context, ID int) (chronograf.Source, error) {
	return chronograf.Source{}, chronograf.ErrSourceNotFound
}

func (s *SourcesStore) Update(context.Context, chronograf.Source) error {
	return fmt.Errorf("failed to update source")
}

package mocks

import (
	"context"

	"github.com/influxdata/chronograf"
)

var _ chronograf.OrganizationConfigStore = &OrganizationConfigStore{}

type OrganizationConfigStore struct {
	FindOrCreateF func(ctx context.Context, id string) (*chronograf.OrganizationConfig, error)
	UpdateF       func(ctx context.Context, target *chronograf.OrganizationConfig) error
}

func (s *OrganizationConfigStore) FindOrCreate(ctx context.Context, id string) (*chronograf.OrganizationConfig, error) {
	return s.FindOrCreateF(ctx, id)
}

func (s *OrganizationConfigStore) Update(ctx context.Context, target *chronograf.OrganizationConfig) error {
	return s.UpdateF(ctx, target)
}

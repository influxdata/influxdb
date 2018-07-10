package noop

import (
	"context"
	"fmt"

	"github.com/influxdata/chronograf"
)

// ensure OrganizationConfigStore implements chronograf.OrganizationConfigStore
var _ chronograf.OrganizationConfigStore = &OrganizationConfigStore{}

type OrganizationConfigStore struct{}

func (s *OrganizationConfigStore) FindOrCreate(context.Context, string) (*chronograf.OrganizationConfig, error) {
	return nil, chronograf.ErrOrganizationConfigNotFound
}

func (s *OrganizationConfigStore) Update(context.Context, *chronograf.OrganizationConfig) error {
	return fmt.Errorf("cannot update conifg")
}

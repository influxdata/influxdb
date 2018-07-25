package noop

import (
	"context"
	"fmt"

	"github.com/influxdata/platform/chronograf"
)

// ensure OrganizationsStore implements chronograf.OrganizationsStore
var _ chronograf.OrganizationsStore = &OrganizationsStore{}

type OrganizationsStore struct{}

func (s *OrganizationsStore) CreateDefault(context.Context) error {
	return fmt.Errorf("failed to add organization")
}

func (s *OrganizationsStore) DefaultOrganization(context.Context) (*chronograf.Organization, error) {
	return nil, fmt.Errorf("failed to retrieve default organization")
}

func (s *OrganizationsStore) All(context.Context) ([]chronograf.Organization, error) {
	return nil, fmt.Errorf("no organizations found")
}

func (s *OrganizationsStore) Add(context.Context, *chronograf.Organization) (*chronograf.Organization, error) {
	return nil, fmt.Errorf("failed to add organization")
}

func (s *OrganizationsStore) Delete(context.Context, *chronograf.Organization) error {
	return fmt.Errorf("failed to delete organization")
}

func (s *OrganizationsStore) Get(ctx context.Context, q chronograf.OrganizationQuery) (*chronograf.Organization, error) {
	return nil, chronograf.ErrOrganizationNotFound
}

func (s *OrganizationsStore) Update(context.Context, *chronograf.Organization) error {
	return fmt.Errorf("failed to update organization")
}

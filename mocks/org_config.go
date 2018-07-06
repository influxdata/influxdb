package mocks

import (
	"context"

	"github.com/influxdata/chronograf"
)

var _ chronograf.OrganizationConfigStore = &OrganizationConfigStore{}

type OrganizationConfigStore struct {
	//InitializeF func(ctx context.Context) error
	//GetF        func(ctx context.Context, id chronograf.OrganizationID) (*chronograf.OrganizationConfig, error)
	FindOrCreateF func(ctx context.Context, id string) (*chronograf.OrganizationConfig, error)
	UpdateF       func(ctx context.Context, target *chronograf.OrganizationConfig) error
}

func (oc *OrganizationConfigStore) FindOrCreate(ctx context.Context, id string) (*chronograf.OrganizationConfig, error) {
	return oc.FindOrCreateF(ctx, id)
}

func (oc *OrganizationConfigStore) Update(ctx context.Context, target *chronograf.OrganizationConfig) error {
	return oc.UpdateF(ctx, target)
}

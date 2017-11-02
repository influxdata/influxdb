package mocks

import (
	"context"

	"github.com/influxdata/chronograf"
)

var _ chronograf.OrganizationsStore = &OrganizationsStore{}

type OrganizationsStore struct {
	AllF    func(context.Context) ([]chronograf.Organization, error)
	AddF    func(context.Context, *chronograf.Organization) (*chronograf.Organization, error)
	DeleteF func(context.Context, *chronograf.Organization) error
	GetF    func(ctx context.Context, q chronograf.OrganizationQuery) (*chronograf.Organization, error)
	UpdateF func(context.Context, *chronograf.Organization) error
}

func (s *OrganizationsStore) Add(ctx context.Context, o *chronograf.Organization) (*chronograf.Organization, error) {
	return s.AddF(ctx, o)
}

func (s *OrganizationsStore) All(ctx context.Context) ([]chronograf.Organization, error) {
	return s.AllF(ctx)
}

func (s *OrganizationsStore) Delete(ctx context.Context, o *chronograf.Organization) error {
	return s.DeleteF(ctx, o)
}

func (s *OrganizationsStore) Get(ctx context.Context, q chronograf.OrganizationQuery) (*chronograf.Organization, error) {
	return s.GetF(ctx, q)
}

func (s *OrganizationsStore) Update(ctx context.Context, o *chronograf.Organization) error {
	return s.UpdateF(ctx, o)
}

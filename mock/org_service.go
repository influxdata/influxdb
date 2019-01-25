package mock

import (
	"context"

	"github.com/influxdata/influxdb"
)

var _ influxdb.OrganizationService = &OrganizationService{}

// OrganizationService is a mock organization server.
type OrganizationService struct {
	FindOrganizationByIDF func(ctx context.Context, id influxdb.ID) (*influxdb.Organization, error)
	FindOrganizationF     func(ctx context.Context, filter influxdb.OrganizationFilter) (*influxdb.Organization, error)
	FindOrganizationsF    func(ctx context.Context, filter influxdb.OrganizationFilter, opt ...influxdb.FindOptions) ([]*influxdb.Organization, int, error)
	CreateOrganizationF   func(ctx context.Context, b *influxdb.Organization) error
	UpdateOrganizationF   func(ctx context.Context, id influxdb.ID, upd influxdb.OrganizationUpdate) (*influxdb.Organization, error)
	DeleteOrganizationF   func(ctx context.Context, id influxdb.ID) error
}

//FindOrganizationByID calls FindOrganizationByIDF.
func (s *OrganizationService) FindOrganizationByID(ctx context.Context, id influxdb.ID) (*influxdb.Organization, error) {
	return s.FindOrganizationByIDF(ctx, id)
}

//FindOrganization calls FindOrganizationF.
func (s *OrganizationService) FindOrganization(ctx context.Context, filter influxdb.OrganizationFilter) (*influxdb.Organization, error) {
	return s.FindOrganizationF(ctx, filter)
}

//FindOrganizations calls FindOrganizationsF.
func (s *OrganizationService) FindOrganizations(ctx context.Context, filter influxdb.OrganizationFilter, opts influxdb.FindOptions) ([]*influxdb.Organization, int, error) {
	return s.FindOrganizationsF(ctx, filter, opts)
}

// CreateOrganization calls CreateOrganizationF.
func (s *OrganizationService) CreateOrganization(ctx context.Context, b *influxdb.Organization) error {
	return s.CreateOrganizationF(ctx, b)
}

// UpdateOrganization calls UpdateOrganizationF.
func (s *OrganizationService) UpdateOrganization(ctx context.Context, id influxdb.ID, upd influxdb.OrganizationUpdate) (*influxdb.Organization, error) {
	return s.UpdateOrganizationF(ctx, id, upd)
}

// DeleteOrganization calls DeleteOrganizationF.
func (s *OrganizationService) DeleteOrganization(ctx context.Context, id influxdb.ID) error {
	return s.DeleteOrganizationF(ctx, id)
}

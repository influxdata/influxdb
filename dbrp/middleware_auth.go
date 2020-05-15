package dbrp

import (
	"context"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/authorizer"
)

var _ influxdb.DBRPMappingServiceV2 = (*AuthorizedService)(nil)

type AuthorizedService struct {
	influxdb.DBRPMappingServiceV2
}

func NewAuthorizedService(s influxdb.DBRPMappingServiceV2) *AuthorizedService {
	return &AuthorizedService{DBRPMappingServiceV2: s}
}

func (svc AuthorizedService) FindByID(ctx context.Context, orgID, id influxdb.ID) (*influxdb.DBRPMappingV2, error) {
	if _, _, err := authorizer.AuthorizeRead(ctx, influxdb.DBRPResourceType, id, orgID); err != nil {
		return nil, err
	}
	return svc.DBRPMappingServiceV2.FindByID(ctx, orgID, id)
}

func (svc AuthorizedService) FindMany(ctx context.Context, filter influxdb.DBRPMappingFilterV2, opts ...influxdb.FindOptions) ([]*influxdb.DBRPMappingV2, int, error) {
	dbrps, _, err := svc.DBRPMappingServiceV2.FindMany(ctx, filter, opts...)
	if err != nil {
		return nil, 0, err
	}
	return authorizer.AuthorizeFindDBRPs(ctx, dbrps)
}

func (svc AuthorizedService) Create(ctx context.Context, t *influxdb.DBRPMappingV2) error {
	if _, _, err := authorizer.AuthorizeCreate(ctx, influxdb.DBRPResourceType, t.OrganizationID); err != nil {
		return err
	}
	return svc.DBRPMappingServiceV2.Create(ctx, t)
}

func (svc AuthorizedService) Update(ctx context.Context, u *influxdb.DBRPMappingV2) error {
	if _, _, err := authorizer.AuthorizeWrite(ctx, influxdb.DBRPResourceType, u.ID, u.OrganizationID); err != nil {
		return err
	}
	return svc.DBRPMappingServiceV2.Update(ctx, u)
}

func (svc AuthorizedService) Delete(ctx context.Context, orgID, id influxdb.ID) error {
	if _, _, err := authorizer.AuthorizeWrite(ctx, influxdb.DBRPResourceType, id, orgID); err != nil {
		return err
	}
	return svc.DBRPMappingServiceV2.Delete(ctx, orgID, id)
}

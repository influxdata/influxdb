package dbrp

import (
	"context"

	"github.com/influxdata/influxdb/v2/kit/platform"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/authorizer"
)

var _ influxdb.DBRPMappingService = (*AuthorizedService)(nil)

type AuthorizedService struct {
	influxdb.DBRPMappingService
}

func NewAuthorizedService(s influxdb.DBRPMappingService) *AuthorizedService {
	return &AuthorizedService{DBRPMappingService: s}
}

func (svc AuthorizedService) FindByID(ctx context.Context, orgID, id platform.ID) (*influxdb.DBRPMapping, error) {
	mapping, err := svc.DBRPMappingService.FindByID(ctx, orgID, id)
	if err != nil {
		return nil, err
	}
	if _, _, err := authorizer.AuthorizeRead(ctx, influxdb.BucketsResourceType, mapping.BucketID, orgID); err != nil {
		return nil, err
	}
	return mapping, nil
}

func (svc AuthorizedService) FindMany(ctx context.Context, filter influxdb.DBRPMappingFilter, opts ...influxdb.FindOptions) ([]*influxdb.DBRPMapping, int, error) {
	dbrps, _, err := svc.DBRPMappingService.FindMany(ctx, filter, opts...)
	if err != nil {
		return nil, 0, err
	}
	return authorizer.AuthorizeFindDBRPs(ctx, dbrps)
}

func (svc AuthorizedService) Create(ctx context.Context, t *influxdb.DBRPMapping) error {
	if _, _, err := authorizer.AuthorizeWrite(ctx, influxdb.BucketsResourceType, t.BucketID, t.OrganizationID); err != nil {
		return err
	}
	return svc.DBRPMappingService.Create(ctx, t)
}

func (svc AuthorizedService) Update(ctx context.Context, u *influxdb.DBRPMapping) error {
	if _, _, err := authorizer.AuthorizeWrite(ctx, influxdb.BucketsResourceType, u.BucketID, u.OrganizationID); err != nil {
		return err
	}
	return svc.DBRPMappingService.Update(ctx, u)
}

func (svc AuthorizedService) Delete(ctx context.Context, orgID, id platform.ID) error {
	mapping, err := svc.DBRPMappingService.FindByID(ctx, orgID, id)
	if err != nil {
		return err
	}
	if _, _, err := authorizer.AuthorizeWrite(ctx, influxdb.BucketsResourceType, mapping.BucketID, orgID); err != nil {
		return err
	}
	return svc.DBRPMappingService.Delete(ctx, orgID, id)
}

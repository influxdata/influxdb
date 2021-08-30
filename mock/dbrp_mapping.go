package mock

import (
	"context"

	"github.com/influxdata/influxdb/v2/kit/platform"

	"github.com/influxdata/influxdb/v2"
)

var _ influxdb.DBRPMappingServiceV2 = (*DBRPMappingServiceV2)(nil)

type DBRPMappingServiceV2 struct {
	FindByIDFn func(ctx context.Context, orgID, id platform.ID) (*influxdb.DBRPMappingV2, error)
	FindManyFn func(ctx context.Context, dbrp influxdb.DBRPMappingFilterV2, opts ...influxdb.FindOptions) ([]*influxdb.DBRPMappingV2, int, error)
	CreateFn   func(ctx context.Context, dbrp *influxdb.DBRPMappingV2) error
	UpdateFn   func(ctx context.Context, dbrp *influxdb.DBRPMappingV2) error
	DeleteFn   func(ctx context.Context, orgID, id platform.ID) error
}

func (s *DBRPMappingServiceV2) FindByID(ctx context.Context, orgID, id platform.ID) (*influxdb.DBRPMappingV2, error) {
	if s.FindByIDFn == nil {
		return nil, nil
	}
	return s.FindByIDFn(ctx, orgID, id)
}

func (s *DBRPMappingServiceV2) FindMany(ctx context.Context, dbrp influxdb.DBRPMappingFilterV2, opts ...influxdb.FindOptions) ([]*influxdb.DBRPMappingV2, int, error) {
	if s.FindManyFn == nil {
		return nil, 0, nil
	}
	return s.FindManyFn(ctx, dbrp, opts...)
}

func (s *DBRPMappingServiceV2) Create(ctx context.Context, dbrp *influxdb.DBRPMappingV2) error {
	if s.CreateFn == nil {
		return nil
	}
	return s.CreateFn(ctx, dbrp)
}

func (s *DBRPMappingServiceV2) Update(ctx context.Context, dbrp *influxdb.DBRPMappingV2) error {
	if s.UpdateFn == nil {
		return nil
	}
	return s.UpdateFn(ctx, dbrp)
}

func (s *DBRPMappingServiceV2) Delete(ctx context.Context, orgID, id platform.ID) error {
	if s.DeleteFn == nil {
		return nil
	}
	return s.DeleteFn(ctx, orgID, id)
}

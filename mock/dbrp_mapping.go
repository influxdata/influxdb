package mock

import (
	"context"

	"github.com/influxdata/influxdb/v2/kit/platform"

	"github.com/influxdata/influxdb/v2"
)

var _ influxdb.DBRPMappingService = (*DBRPMappingService)(nil)

type DBRPMappingService struct {
	FindByIDFn func(ctx context.Context, orgID, id platform.ID) (*influxdb.DBRPMapping, error)
	FindManyFn func(ctx context.Context, dbrp influxdb.DBRPMappingFilter, opts ...influxdb.FindOptions) ([]*influxdb.DBRPMapping, int, error)
	CreateFn   func(ctx context.Context, dbrp *influxdb.DBRPMapping) error
	UpdateFn   func(ctx context.Context, dbrp *influxdb.DBRPMapping) error
	DeleteFn   func(ctx context.Context, orgID, id platform.ID) error
}

func (s *DBRPMappingService) FindByID(ctx context.Context, orgID, id platform.ID) (*influxdb.DBRPMapping, error) {
	if s.FindByIDFn == nil {
		return nil, nil
	}
	return s.FindByIDFn(ctx, orgID, id)
}

func (s *DBRPMappingService) FindMany(ctx context.Context, dbrp influxdb.DBRPMappingFilter, opts ...influxdb.FindOptions) ([]*influxdb.DBRPMapping, int, error) {
	if s.FindManyFn == nil {
		return nil, 0, nil
	}
	return s.FindManyFn(ctx, dbrp, opts...)
}

func (s *DBRPMappingService) Create(ctx context.Context, dbrp *influxdb.DBRPMapping) error {
	if s.CreateFn == nil {
		return nil
	}
	return s.CreateFn(ctx, dbrp)
}

func (s *DBRPMappingService) Update(ctx context.Context, dbrp *influxdb.DBRPMapping) error {
	if s.UpdateFn == nil {
		return nil
	}
	return s.UpdateFn(ctx, dbrp)
}

func (s *DBRPMappingService) Delete(ctx context.Context, orgID, id platform.ID) error {
	if s.DeleteFn == nil {
		return nil
	}
	return s.DeleteFn(ctx, orgID, id)
}

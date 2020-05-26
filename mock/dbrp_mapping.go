package mock

import (
	"context"

	"github.com/influxdata/influxdb/v2"
)

var _ influxdb.DBRPMappingServiceV2 = (*DBRPMappingServiceV2)(nil)

type DBRPMappingServiceV2 struct {
	FindByIDFn func(ctx context.Context, orgID, id influxdb.ID) (*influxdb.DBRPMappingV2, error)
	FindManyFn func(ctx context.Context, dbrp influxdb.DBRPMappingFilterV2, opts ...influxdb.FindOptions) ([]*influxdb.DBRPMappingV2, int, error)
	CreateFn   func(ctx context.Context, dbrp *influxdb.DBRPMappingV2) error
	UpdateFn   func(ctx context.Context, dbrp *influxdb.DBRPMappingV2) error
	DeleteFn   func(ctx context.Context, orgID, id influxdb.ID) error
}

func (s *DBRPMappingServiceV2) FindByID(ctx context.Context, orgID, id influxdb.ID) (*influxdb.DBRPMappingV2, error) {
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

func (s *DBRPMappingServiceV2) Delete(ctx context.Context, orgID, id influxdb.ID) error {
	if s.DeleteFn == nil {
		return nil
	}
	return s.DeleteFn(ctx, orgID, id)
}

type DBRPMappingService struct {
	FindByFn   func(ctx context.Context, cluster string, db string, rp string) (*influxdb.DBRPMapping, error)
	FindFn     func(ctx context.Context, filter influxdb.DBRPMappingFilter) (*influxdb.DBRPMapping, error)
	FindManyFn func(ctx context.Context, filter influxdb.DBRPMappingFilter, opt ...influxdb.FindOptions) ([]*influxdb.DBRPMapping, int, error)
	CreateFn   func(ctx context.Context, dbrpMap *influxdb.DBRPMapping) error
	DeleteFn   func(ctx context.Context, cluster string, db string, rp string) error
}

func NewDBRPMappingService() *DBRPMappingService {
	return &DBRPMappingService{
		FindByFn: func(ctx context.Context, cluster string, db string, rp string) (*influxdb.DBRPMapping, error) {
			return nil, nil
		},
		FindFn: func(ctx context.Context, filter influxdb.DBRPMappingFilter) (*influxdb.DBRPMapping, error) {
			return nil, nil
		},
		FindManyFn: func(ctx context.Context, filter influxdb.DBRPMappingFilter, opt ...influxdb.FindOptions) ([]*influxdb.DBRPMapping, int, error) {
			return nil, 0, nil
		},
		CreateFn: func(ctx context.Context, dbrpMap *influxdb.DBRPMapping) error { return nil },
		DeleteFn: func(ctx context.Context, cluster string, db string, rp string) error { return nil },
	}
}

func (s *DBRPMappingService) FindBy(ctx context.Context, cluster string, db string, rp string) (*influxdb.DBRPMapping, error) {
	return s.FindByFn(ctx, cluster, db, rp)
}

func (s *DBRPMappingService) Find(ctx context.Context, filter influxdb.DBRPMappingFilter) (*influxdb.DBRPMapping, error) {
	return s.FindFn(ctx, filter)
}

func (s *DBRPMappingService) FindMany(ctx context.Context, filter influxdb.DBRPMappingFilter, opt ...influxdb.FindOptions) ([]*influxdb.DBRPMapping, int, error) {
	return s.FindManyFn(ctx, filter, opt...)
}

func (s *DBRPMappingService) Create(ctx context.Context, dbrpMap *influxdb.DBRPMapping) error {
	return s.CreateFn(ctx, dbrpMap)
}

func (s *DBRPMappingService) Delete(ctx context.Context, cluster string, db string, rp string) error {
	return s.DeleteFn(ctx, cluster, db, rp)
}

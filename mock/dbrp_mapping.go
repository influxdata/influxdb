package mock

import (
	"context"

	platform "github.com/influxdata/influxdb"
)

type DBRPMappingService struct {
	FindByFn   func(ctx context.Context, cluster string, db string, rp string) (*platform.DBRPMapping, error)
	FindFn     func(ctx context.Context, filter platform.DBRPMappingFilter) (*platform.DBRPMapping, error)
	FindManyFn func(ctx context.Context, filter platform.DBRPMappingFilter, opt ...platform.FindOptions) ([]*platform.DBRPMapping, int, error)
	CreateFn   func(ctx context.Context, dbrpMap *platform.DBRPMapping) error
	DeleteFn   func(ctx context.Context, cluster string, db string, rp string) error
}

func NewDBRPMappingService() *DBRPMappingService {
	return &DBRPMappingService{
		FindByFn: func(ctx context.Context, cluster string, db string, rp string) (*platform.DBRPMapping, error) {
			return nil, nil
		},
		FindFn: func(ctx context.Context, filter platform.DBRPMappingFilter) (*platform.DBRPMapping, error) {
			return nil, nil
		},
		FindManyFn: func(ctx context.Context, filter platform.DBRPMappingFilter, opt ...platform.FindOptions) ([]*platform.DBRPMapping, int, error) {
			return nil, 0, nil
		},
		CreateFn: func(ctx context.Context, dbrpMap *platform.DBRPMapping) error { return nil },
		DeleteFn: func(ctx context.Context, cluster string, db string, rp string) error { return nil },
	}
}

func (s *DBRPMappingService) FindBy(ctx context.Context, cluster string, db string, rp string) (*platform.DBRPMapping, error) {
	return s.FindByFn(ctx, cluster, db, rp)
}

func (s *DBRPMappingService) Find(ctx context.Context, filter platform.DBRPMappingFilter) (*platform.DBRPMapping, error) {
	return s.FindFn(ctx, filter)
}

func (s *DBRPMappingService) FindMany(ctx context.Context, filter platform.DBRPMappingFilter, opt ...platform.FindOptions) ([]*platform.DBRPMapping, int, error) {
	return s.FindManyFn(ctx, filter, opt...)
}

func (s *DBRPMappingService) Create(ctx context.Context, dbrpMap *platform.DBRPMapping) error {
	return s.CreateFn(ctx, dbrpMap)
}

func (s *DBRPMappingService) Delete(ctx context.Context, cluster string, db string, rp string) error {
	return s.DeleteFn(ctx, cluster, db, rp)
}

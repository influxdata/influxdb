package mock

import (
	"context"

	platform2 "github.com/influxdata/influxdb/v2/kit/platform"

	platform "github.com/influxdata/influxdb/v2"
)

var _ platform.UserResourceMappingService = &UserResourceMappingService{}

// UserResourceMappingService is a mock implementation of platform.UserResourceMappingService
type UserResourceMappingService struct {
	FindMappingsFn  func(context.Context, platform.UserResourceMappingFilter) ([]*platform.UserResourceMapping, int, error)
	CreateMappingFn func(context.Context, *platform.UserResourceMapping) error
	DeleteMappingFn func(context.Context, platform2.ID, platform2.ID) error
}

// NewUserResourceMappingService returns a mock of UserResourceMappingService
// where its methods will return zero values.
func NewUserResourceMappingService() *UserResourceMappingService {
	return &UserResourceMappingService{
		FindMappingsFn: func(context.Context, platform.UserResourceMappingFilter) ([]*platform.UserResourceMapping, int, error) {
			return nil, 0, nil
		},
		CreateMappingFn: func(context.Context, *platform.UserResourceMapping) error { return nil },
		DeleteMappingFn: func(context.Context, platform2.ID, platform2.ID) error { return nil },
	}
}

// FindUserResourceMappings finds mappings that match a given filter.
func (s *UserResourceMappingService) FindUserResourceMappings(ctx context.Context, filter platform.UserResourceMappingFilter, opt ...platform.FindOptions) ([]*platform.UserResourceMapping, int, error) {
	return s.FindMappingsFn(ctx, filter)
}

// CreateUserResourceMapping creates a new UserResourceMapping.
func (s *UserResourceMappingService) CreateUserResourceMapping(ctx context.Context, m *platform.UserResourceMapping) error {
	return s.CreateMappingFn(ctx, m)
}

// DeleteUserResourceMapping removes a UserResourceMapping.
func (s *UserResourceMappingService) DeleteUserResourceMapping(ctx context.Context, resourceID platform2.ID, userID platform2.ID) error {
	return s.DeleteMappingFn(ctx, resourceID, userID)
}

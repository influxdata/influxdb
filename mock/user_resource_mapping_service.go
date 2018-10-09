package mock

import (
	"context"

	"github.com/influxdata/platform"
)

var _ platform.UserResourceMappingService = &UserResourceMappingService{}

type UserResourceMappingService struct {
	FindMappingsF  func(context.Context, platform.UserResourceMappingFilter) ([]*platform.UserResourceMapping, int, error)
	CreateMappingF func(context.Context, *platform.UserResourceMapping) error
	DeleteMappingF func(context.Context, platform.ID, platform.ID) error
}

func (s *UserResourceMappingService) FindUserResourceMappings(ctx context.Context, filter platform.UserResourceMappingFilter, opt ...platform.FindOptions) ([]*platform.UserResourceMapping, int, error) {
	return s.FindMappingsF(ctx, filter)
}

func (s *UserResourceMappingService) CreateUserResourceMapping(ctx context.Context, m *platform.UserResourceMapping) error {
	return s.CreateMappingF(ctx, m)
}

func (s *UserResourceMappingService) DeleteUserResourceMapping(ctx context.Context, resourceID platform.ID, userID platform.ID) error {
	return s.DeleteMappingF(ctx, resourceID, userID)
}

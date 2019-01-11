package mock

import (
	"context"

	platform "github.com/influxdata/influxdb"
)

var _ platform.TelegrafConfigStore = &TelegrafConfigStore{}

// TelegrafConfigStore represents a service for managing telegraf config data.
type TelegrafConfigStore struct {
	FindUserResourceMappingsF  func(ctx context.Context, filter platform.UserResourceMappingFilter, opt ...platform.FindOptions) ([]*platform.UserResourceMapping, int, error)
	CreateUserResourceMappingF func(ctx context.Context, m *platform.UserResourceMapping) error
	DeleteUserResourceMappingF func(ctx context.Context, resourceID platform.ID, userID platform.ID) error
	FindTelegrafConfigByIDF    func(ctx context.Context, id platform.ID) (*platform.TelegrafConfig, error)
	FindTelegrafConfigF        func(ctx context.Context, filter platform.TelegrafConfigFilter) (*platform.TelegrafConfig, error)
	FindTelegrafConfigsF       func(ctx context.Context, filter platform.TelegrafConfigFilter, opt ...platform.FindOptions) ([]*platform.TelegrafConfig, int, error)
	CreateTelegrafConfigF      func(ctx context.Context, tc *platform.TelegrafConfig, userID platform.ID) error
	UpdateTelegrafConfigF      func(ctx context.Context, id platform.ID, tc *platform.TelegrafConfig, userID platform.ID) (*platform.TelegrafConfig, error)
	DeleteTelegrafConfigF      func(ctx context.Context, id platform.ID) error
}

// FindUserResourceMappings returns a list of UserResourceMappings that match filter and the total count of matching mappings.
func (s *TelegrafConfigStore) FindUserResourceMappings(ctx context.Context, filter platform.UserResourceMappingFilter, opt ...platform.FindOptions) ([]*platform.UserResourceMapping, int, error) {
	return s.FindUserResourceMappingsF(ctx, filter, opt...)
}

// CreateUserResourceMapping creates a user resource mapping
func (s *TelegrafConfigStore) CreateUserResourceMapping(ctx context.Context, m *platform.UserResourceMapping) error {
	return s.CreateUserResourceMappingF(ctx, m)
}

// DeleteUserResourceMapping deletes a user resource mapping
func (s *TelegrafConfigStore) DeleteUserResourceMapping(ctx context.Context, resourceID platform.ID, userID platform.ID) error {
	return s.DeleteUserResourceMappingF(ctx, resourceID, userID)
}

// FindTelegrafConfigByID returns a single telegraf config by ID.
func (s *TelegrafConfigStore) FindTelegrafConfigByID(ctx context.Context, id platform.ID) (*platform.TelegrafConfig, error) {
	return s.FindTelegrafConfigByIDF(ctx, id)
}

// FindTelegrafConfig returns the first telegraf config that matches filter.
func (s *TelegrafConfigStore) FindTelegrafConfig(ctx context.Context, filter platform.TelegrafConfigFilter) (*platform.TelegrafConfig, error) {
	return s.FindTelegrafConfigF(ctx, filter)
}

// FindTelegrafConfigs returns a list of telegraf configs that match filter and the total count of matching telegraf configs.
// Additional options provide pagination & sorting.
func (s *TelegrafConfigStore) FindTelegrafConfigs(ctx context.Context, filter platform.TelegrafConfigFilter, opt ...platform.FindOptions) ([]*platform.TelegrafConfig, int, error) {
	return s.FindTelegrafConfigsF(ctx, filter, opt...)
}

// CreateTelegrafConfig creates a new telegraf config and sets b.ID with the new identifier.
func (s *TelegrafConfigStore) CreateTelegrafConfig(ctx context.Context, tc *platform.TelegrafConfig, userID platform.ID) error {
	return s.CreateTelegrafConfigF(ctx, tc, userID)
}

// UpdateTelegrafConfig updates a single telegraf config.
// Returns the new telegraf config after update.
func (s *TelegrafConfigStore) UpdateTelegrafConfig(ctx context.Context, id platform.ID, tc *platform.TelegrafConfig, userID platform.ID) (*platform.TelegrafConfig, error) {
	return s.UpdateTelegrafConfigF(ctx, id, tc, userID)
}

// DeleteTelegrafConfig removes a telegraf config by ID.
func (s *TelegrafConfigStore) DeleteTelegrafConfig(ctx context.Context, id platform.ID) error {
	return s.DeleteTelegrafConfigF(ctx, id)
}

package mock

import (
	"context"

	"github.com/influxdata/influxdb"
)

var _ influxdb.NotificationEndpointService = &NotificationEndpointService{}

// NotificationEndpointService represents a service for managing notification rule data.
type NotificationEndpointService struct {
	OrganizationService
	UserResourceMappingService
	SecretService
	FindNotificationEndpointByIDF func(ctx context.Context, id influxdb.ID) (influxdb.NotificationEndpoint, error)
	FindNotificationEndpointsF    func(ctx context.Context, filter influxdb.NotificationEndpointFilter, opt ...influxdb.FindOptions) ([]influxdb.NotificationEndpoint, int, error)
	CreateNotificationEndpointF   func(ctx context.Context, nr influxdb.NotificationEndpoint, userID influxdb.ID) error
	UpdateNotificationEndpointF   func(ctx context.Context, id influxdb.ID, nr influxdb.NotificationEndpoint, userID influxdb.ID) (influxdb.NotificationEndpoint, error)
	PatchNotificationEndpointF    func(ctx context.Context, id influxdb.ID, upd influxdb.NotificationEndpointUpdate) (influxdb.NotificationEndpoint, error)
	DeleteNotificationEndpointF   func(ctx context.Context, id influxdb.ID) error
}

// FindNotificationEndpointByID returns a single telegraf config by ID.
func (s *NotificationEndpointService) FindNotificationEndpointByID(ctx context.Context, id influxdb.ID) (influxdb.NotificationEndpoint, error) {
	return s.FindNotificationEndpointByIDF(ctx, id)
}

// FindNotificationEndpoints returns a list of notification rules that match filter and the total count of matching notification rules.
// Additional options provide pagination & sorting.
func (s *NotificationEndpointService) FindNotificationEndpoints(ctx context.Context, filter influxdb.NotificationEndpointFilter, opt ...influxdb.FindOptions) ([]influxdb.NotificationEndpoint, int, error) {
	return s.FindNotificationEndpointsF(ctx, filter, opt...)
}

// CreateNotificationEndpoint creates a new notification rule and sets ID with the new identifier.
func (s *NotificationEndpointService) CreateNotificationEndpoint(ctx context.Context, nr influxdb.NotificationEndpoint, userID influxdb.ID) error {
	return s.CreateNotificationEndpointF(ctx, nr, userID)
}

// UpdateNotificationEndpoint updates a single notification rule.
// Returns the new notification rule after update.
func (s *NotificationEndpointService) UpdateNotificationEndpoint(ctx context.Context, id influxdb.ID, nr influxdb.NotificationEndpoint, userID influxdb.ID) (influxdb.NotificationEndpoint, error) {
	return s.UpdateNotificationEndpointF(ctx, id, nr, userID)
}

// PatchNotificationEndpoint updates a single  notification rule with changeset.
// Returns the new notification rule after update.
func (s *NotificationEndpointService) PatchNotificationEndpoint(ctx context.Context, id influxdb.ID, upd influxdb.NotificationEndpointUpdate) (influxdb.NotificationEndpoint, error) {
	return s.PatchNotificationEndpointF(ctx, id, upd)
}

// DeleteNotificationEndpoint removes a notification rule by ID.
func (s *NotificationEndpointService) DeleteNotificationEndpoint(ctx context.Context, id influxdb.ID) error {
	return s.DeleteNotificationEndpointF(ctx, id)
}

package mock

import (
	"context"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kit/platform"
)

var _ influxdb.NotificationEndpointService = &NotificationEndpointService{}

// NotificationEndpointService represents a service for managing notification rule data.
type NotificationEndpointService struct {
	*OrganizationService
	*UserResourceMappingService
	FindNotificationEndpointByIDF     func(ctx context.Context, id platform.ID) (influxdb.NotificationEndpoint, error)
	FindNotificationEndpointByIDCalls SafeCount
	FindNotificationEndpointsF        func(ctx context.Context, filter influxdb.NotificationEndpointFilter, opt ...influxdb.FindOptions) ([]influxdb.NotificationEndpoint, int, error)
	FindNotificationEndpointsCalls    SafeCount
	CreateNotificationEndpointF       func(ctx context.Context, nr influxdb.NotificationEndpoint, userID platform.ID) error
	CreateNotificationEndpointCalls   SafeCount
	UpdateNotificationEndpointF       func(ctx context.Context, id platform.ID, nr influxdb.NotificationEndpoint, userID platform.ID) (influxdb.NotificationEndpoint, error)
	UpdateNotificationEndpointCalls   SafeCount
	PatchNotificationEndpointF        func(ctx context.Context, id platform.ID, upd influxdb.NotificationEndpointUpdate) (influxdb.NotificationEndpoint, error)
	PatchNotificationEndpointCalls    SafeCount
	DeleteNotificationEndpointF       func(ctx context.Context, id platform.ID) ([]influxdb.SecretField, platform.ID, error)
	DeleteNotificationEndpointCalls   SafeCount
}

func NewNotificationEndpointService() *NotificationEndpointService {
	return &NotificationEndpointService{
		OrganizationService:        NewOrganizationService(),
		UserResourceMappingService: NewUserResourceMappingService(),
		FindNotificationEndpointByIDF: func(ctx context.Context, id platform.ID) (influxdb.NotificationEndpoint, error) {
			return nil, nil
		},
		FindNotificationEndpointsF: func(ctx context.Context, filter influxdb.NotificationEndpointFilter, opt ...influxdb.FindOptions) ([]influxdb.NotificationEndpoint, int, error) {
			return nil, 0, nil
		},
		CreateNotificationEndpointF: func(ctx context.Context, nr influxdb.NotificationEndpoint, userID platform.ID) error {
			return nil
		},
		UpdateNotificationEndpointF: func(ctx context.Context, id platform.ID, nr influxdb.NotificationEndpoint, userID platform.ID) (influxdb.NotificationEndpoint, error) {
			return nil, nil
		},
		PatchNotificationEndpointF: func(ctx context.Context, id platform.ID, upd influxdb.NotificationEndpointUpdate) (influxdb.NotificationEndpoint, error) {
			return nil, nil
		},
		DeleteNotificationEndpointF: func(ctx context.Context, id platform.ID) ([]influxdb.SecretField, platform.ID, error) {
			return nil, 0, nil
		},
	}
}

// FindNotificationEndpointByID returns a single telegraf config by ID.
func (s *NotificationEndpointService) FindNotificationEndpointByID(ctx context.Context, id platform.ID) (influxdb.NotificationEndpoint, error) {
	defer s.FindNotificationEndpointByIDCalls.IncrFn()()
	return s.FindNotificationEndpointByIDF(ctx, id)
}

// FindNotificationEndpoints returns a list of notification rules that match filter and the total count of matching notification rules.
// Additional options provide pagination & sorting.
func (s *NotificationEndpointService) FindNotificationEndpoints(ctx context.Context, filter influxdb.NotificationEndpointFilter, opt ...influxdb.FindOptions) ([]influxdb.NotificationEndpoint, int, error) {
	defer s.FindNotificationEndpointsCalls.IncrFn()()
	return s.FindNotificationEndpointsF(ctx, filter, opt...)
}

// CreateNotificationEndpoint creates a new notification rule and sets ID with the new identifier.
func (s *NotificationEndpointService) CreateNotificationEndpoint(ctx context.Context, nr influxdb.NotificationEndpoint, userID platform.ID) error {
	defer s.CreateNotificationEndpointCalls.IncrFn()()
	return s.CreateNotificationEndpointF(ctx, nr, userID)
}

// UpdateNotificationEndpoint updates a single notification rule.
// Returns the new notification rule after update.
func (s *NotificationEndpointService) UpdateNotificationEndpoint(ctx context.Context, id platform.ID, nr influxdb.NotificationEndpoint, userID platform.ID) (influxdb.NotificationEndpoint, error) {
	defer s.UpdateNotificationEndpointCalls.IncrFn()()
	return s.UpdateNotificationEndpointF(ctx, id, nr, userID)
}

// PatchNotificationEndpoint updates a single  notification rule with changeset.
// Returns the new notification rule after update.
func (s *NotificationEndpointService) PatchNotificationEndpoint(ctx context.Context, id platform.ID, upd influxdb.NotificationEndpointUpdate) (influxdb.NotificationEndpoint, error) {
	defer s.PatchNotificationEndpointCalls.IncrFn()()
	return s.PatchNotificationEndpointF(ctx, id, upd)
}

// DeleteNotificationEndpoint removes a notification rule by ID.
func (s *NotificationEndpointService) DeleteNotificationEndpoint(ctx context.Context, id platform.ID) ([]influxdb.SecretField, platform.ID, error) {
	defer s.DeleteNotificationEndpointCalls.IncrFn()()
	return s.DeleteNotificationEndpointF(ctx, id)
}

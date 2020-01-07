package mock

import (
	"context"

	"github.com/influxdata/influxdb"
)

var _ influxdb.NotificationEndpointService = &NotificationEndpointService{}

// NotificationEndpointService represents a service for managing notification rule data.
type NotificationEndpointService struct {
	*OrganizationService
	*UserResourceMappingService
	FindByIDF     func(ctx context.Context, id influxdb.ID) (influxdb.NotificationEndpoint, error)
	FindByIDCalls SafeCount
	FindF         func(ctx context.Context, filter influxdb.NotificationEndpointFilter, opt ...influxdb.FindOptions) ([]influxdb.NotificationEndpoint, error)
	FindCalls     SafeCount
	CreateF       func(ctx context.Context, userID influxdb.ID, nr influxdb.NotificationEndpoint) error
	CreateCalls   SafeCount
	UpdateF       func(ctx context.Context, update influxdb.EndpointUpdate) (influxdb.NotificationEndpoint, error)
	UpdateCalls   SafeCount
	DeleteF       func(ctx context.Context, id influxdb.ID) error
	DeleteCalls   SafeCount
}

func NewNotificationEndpointService() *NotificationEndpointService {
	return &NotificationEndpointService{
		OrganizationService:        NewOrganizationService(),
		UserResourceMappingService: NewUserResourceMappingService(),
		FindByIDF: func(ctx context.Context, id influxdb.ID) (influxdb.NotificationEndpoint, error) {
			return nil, nil
		},
		FindF: func(ctx context.Context, filter influxdb.NotificationEndpointFilter, opt ...influxdb.FindOptions) ([]influxdb.NotificationEndpoint, error) {
			return nil, nil
		},
		CreateF: func(ctx context.Context, userID influxdb.ID, nr influxdb.NotificationEndpoint) error {
			return nil
		},
		UpdateF: func(ctx context.Context, update influxdb.EndpointUpdate) (influxdb.NotificationEndpoint, error) {
			return nil, nil
		},
		DeleteF: func(ctx context.Context, id influxdb.ID) error {
			return nil
		},
	}
}

// FindByID returns a single telegraf config by ID.
func (s *NotificationEndpointService) FindByID(ctx context.Context, id influxdb.ID) (influxdb.NotificationEndpoint, error) {
	defer s.FindByIDCalls.IncrFn()()
	return s.FindByIDF(ctx, id)
}

// Find returns a list of notification rules that match filter and the total count of matching notification rules.
// Additional options provide pagination & sorting.
func (s *NotificationEndpointService) Find(ctx context.Context, filter influxdb.NotificationEndpointFilter, opt ...influxdb.FindOptions) ([]influxdb.NotificationEndpoint, error) {
	defer s.FindCalls.IncrFn()()
	return s.FindF(ctx, filter, opt...)
}

// Create creates a new notification rule and sets ID with the new identifier.
func (s *NotificationEndpointService) Create(ctx context.Context, userID influxdb.ID, nr influxdb.NotificationEndpoint) error {
	defer s.CreateCalls.IncrFn()()
	return s.CreateF(ctx, userID, nr)
}

// Update updates a single notification rule.
// Returns the new notification rule after update.
func (s *NotificationEndpointService) Update(ctx context.Context, update influxdb.EndpointUpdate) (influxdb.NotificationEndpoint, error) {
	defer s.UpdateCalls.IncrFn()()
	return s.UpdateF(ctx, update)
}

// Delete removes a notification rule by ID.
func (s *NotificationEndpointService) Delete(ctx context.Context, id influxdb.ID) error {
	defer s.DeleteCalls.IncrFn()()
	return s.DeleteF(ctx, id)
}

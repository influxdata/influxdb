package endpoints

import (
	"context"

	"github.com/influxdata/influxdb/v2"
)

// Service provides all the notification endpoint service behavior.
type Service struct {
	endpointStore influxdb.NotificationEndpointService
	secretSVC     influxdb.SecretService

	// TODO(jsteenb2): NUKE THESE 2 embedded services after fixing up the domain!
	influxdb.UserResourceMappingService
	influxdb.OrganizationService
}

// NewService constructs a new Service.
func NewService(store influxdb.NotificationEndpointService, secretSVC influxdb.SecretService, urmSVC influxdb.UserResourceMappingService, orgSVC influxdb.OrganizationService) *Service {
	return &Service{
		endpointStore:              store,
		secretSVC:                  secretSVC,
		UserResourceMappingService: urmSVC,
		OrganizationService:        orgSVC,
	}
}

var _ influxdb.NotificationEndpointService = (*Service)(nil)

// FindNotificationEndpointByID returns a single notification endpoint by ID.
func (s *Service) FindNotificationEndpointByID(ctx context.Context, id influxdb.ID) (influxdb.NotificationEndpoint, error) {
	return s.endpointStore.FindNotificationEndpointByID(ctx, id)
}

// FindNotificationEndpoints returns a list of notification endpoints that match filter and the total count of matching notification endpoints.
// Additional options provide pagination & sorting.
func (s *Service) FindNotificationEndpoints(ctx context.Context, filter influxdb.NotificationEndpointFilter, opt ...influxdb.FindOptions) ([]influxdb.NotificationEndpoint, int, error) {
	return s.endpointStore.FindNotificationEndpoints(ctx, filter, opt...)
}

// CreateNotificationEndpoint creates a new notification endpoint and sets b.ID with the new identifier.
func (s *Service) CreateNotificationEndpoint(ctx context.Context, edp influxdb.NotificationEndpoint, userID influxdb.ID) error {
	err := s.endpointStore.CreateNotificationEndpoint(ctx, edp, userID)
	if err != nil {
		return err
	}

	secrets := make(map[string]string)
	for _, fld := range edp.SecretFields() {
		if fld.Value != nil {
			secrets[fld.Key] = *fld.Value
		}
	}
	if len(secrets) == 0 {
		return nil
	}

	return s.secretSVC.PatchSecrets(ctx, edp.GetOrgID(), secrets)
}

// UpdateNotificationEndpoint updates a single notification endpoint.
// Returns the new notification endpoint after update.
func (s *Service) UpdateNotificationEndpoint(ctx context.Context, id influxdb.ID, nr influxdb.NotificationEndpoint, userID influxdb.ID) (influxdb.NotificationEndpoint, error) {
	nr.BackfillSecretKeys() // :sadpanda:
	updatedEndpoint, err := s.endpointStore.UpdateNotificationEndpoint(ctx, id, nr, userID)
	if err != nil {
		return nil, err
	}

	secrets := make(map[string]string)
	for _, fld := range updatedEndpoint.SecretFields() {
		if fld.Value != nil {
			secrets[fld.Key] = *fld.Value
		}
	}

	if len(secrets) == 0 {
		return updatedEndpoint, nil
	}

	if err := s.secretSVC.PatchSecrets(ctx, updatedEndpoint.GetOrgID(), secrets); err != nil {
		return nil, err
	}

	return updatedEndpoint, nil
}

// PatchNotificationEndpoint updates a single  notification endpoint with changeset.
// Returns the new notification endpoint state after update.
func (s *Service) PatchNotificationEndpoint(ctx context.Context, id influxdb.ID, upd influxdb.NotificationEndpointUpdate) (influxdb.NotificationEndpoint, error) {
	return s.endpointStore.PatchNotificationEndpoint(ctx, id, upd)
}

// DeleteNotificationEndpoint removes a notification endpoint by ID, returns secret fields, orgID for further deletion.
func (s *Service) DeleteNotificationEndpoint(ctx context.Context, id influxdb.ID) ([]influxdb.SecretField, influxdb.ID, error) {
	return s.endpointStore.DeleteNotificationEndpoint(ctx, id)
}

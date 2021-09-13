package authorizer

import (
	"context"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kit/platform"
)

var _ influxdb.NotificationRuleStore = (*NotificationRuleStore)(nil)

// NotificationRuleStore wraps a influxdb.NotificationRuleStore and authorizes actions
// against it appropriately.
type NotificationRuleStore struct {
	s influxdb.NotificationRuleStore
	influxdb.UserResourceMappingService
	influxdb.OrganizationService
}

// NewNotificationRuleStore constructs an instance of an authorizing notification rule service.
func NewNotificationRuleStore(s influxdb.NotificationRuleStore, urm influxdb.UserResourceMappingService, org influxdb.OrganizationService) *NotificationRuleStore {
	return &NotificationRuleStore{
		s:                          s,
		UserResourceMappingService: urm,
		OrganizationService:        org,
	}
}

// FindNotificationRuleByID checks to see if the authorizer on context has read access to the id provided.
func (s *NotificationRuleStore) FindNotificationRuleByID(ctx context.Context, id platform.ID) (influxdb.NotificationRule, error) {
	nr, err := s.s.FindNotificationRuleByID(ctx, id)
	if err != nil {
		return nil, err
	}
	if _, _, err := AuthorizeRead(ctx, influxdb.NotificationRuleResourceType, nr.GetID(), nr.GetOrgID()); err != nil {
		return nil, err
	}
	return nr, nil
}

// FindNotificationRules retrieves all notification rules that match the provided filter and then filters the list down to only the resources that are authorized.
func (s *NotificationRuleStore) FindNotificationRules(ctx context.Context, filter influxdb.NotificationRuleFilter, opt ...influxdb.FindOptions) ([]influxdb.NotificationRule, int, error) {
	// TODO: we'll likely want to push this operation into the database eventually since fetching the whole list of data
	// will likely be expensive.
	nrs, _, err := s.s.FindNotificationRules(ctx, filter, opt...)
	if err != nil {
		return nil, 0, err
	}
	return AuthorizeFindNotificationRules(ctx, nrs)
}

// CreateNotificationRule checks to see if the authorizer on context has write access to the global notification rule resource.
func (s *NotificationRuleStore) CreateNotificationRule(ctx context.Context, nr influxdb.NotificationRuleCreate, userID platform.ID) error {
	if _, _, err := AuthorizeCreate(ctx, influxdb.NotificationRuleResourceType, nr.GetOrgID()); err != nil {
		return err
	}
	return s.s.CreateNotificationRule(ctx, nr, userID)
}

// UpdateNotificationRule checks to see if the authorizer on context has write access to the notification rule provided.
func (s *NotificationRuleStore) UpdateNotificationRule(ctx context.Context, id platform.ID, upd influxdb.NotificationRuleCreate, userID platform.ID) (influxdb.NotificationRule, error) {
	nr, err := s.FindNotificationRuleByID(ctx, id)
	if err != nil {
		return nil, err
	}
	if _, _, err := AuthorizeWrite(ctx, influxdb.NotificationRuleResourceType, nr.GetID(), nr.GetOrgID()); err != nil {
		return nil, err
	}
	return s.s.UpdateNotificationRule(ctx, id, upd, userID)
}

// PatchNotificationRule checks to see if the authorizer on context has write access to the notification rule provided.
func (s *NotificationRuleStore) PatchNotificationRule(ctx context.Context, id platform.ID, upd influxdb.NotificationRuleUpdate) (influxdb.NotificationRule, error) {
	nr, err := s.s.FindNotificationRuleByID(ctx, id)
	if err != nil {
		return nil, err
	}
	if _, _, err := AuthorizeWrite(ctx, influxdb.NotificationRuleResourceType, nr.GetID(), nr.GetOrgID()); err != nil {
		return nil, err
	}
	return s.s.PatchNotificationRule(ctx, id, upd)
}

// DeleteNotificationRule checks to see if the authorizer on context has write access to the notification rule provided.
func (s *NotificationRuleStore) DeleteNotificationRule(ctx context.Context, id platform.ID) error {
	nr, err := s.s.FindNotificationRuleByID(ctx, id)
	if err != nil {
		return err
	}
	if _, _, err := AuthorizeWrite(ctx, influxdb.NotificationRuleResourceType, nr.GetID(), nr.GetOrgID()); err != nil {
		return err
	}
	return s.s.DeleteNotificationRule(ctx, id)
}

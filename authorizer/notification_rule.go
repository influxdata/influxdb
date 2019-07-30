package authorizer

import (
	"context"

	"github.com/influxdata/influxdb"
)

var _ influxdb.NotificationRuleStore = (*NotificationRuleStore)(nil)

// NotificationRuleStore wraps a influxdb.NotificationRuleStore and authorizes actions
// against it appropriately.
type NotificationRuleStore struct {
	s influxdb.NotificationRuleStore
	influxdb.UserResourceMappingService
	influxdb.OrganizationService
}

// NewNotificationRuleStore constructs an instance of an authorizing notification rule serivce.
func NewNotificationRuleStore(s influxdb.NotificationRuleStore, urm influxdb.UserResourceMappingService, org influxdb.OrganizationService) *NotificationRuleStore {
	return &NotificationRuleStore{
		s:                          s,
		UserResourceMappingService: urm,
		OrganizationService:        org,
	}
}

func newNotificationRulePermission(a influxdb.Action, orgID, id influxdb.ID) (*influxdb.Permission, error) {
	return influxdb.NewPermissionAtID(id, a, influxdb.NotificationRuleResourceType, orgID)
}

func authorizeReadNotificationRule(ctx context.Context, orgID, id influxdb.ID) error {
	p, err := newNotificationRulePermission(influxdb.ReadAction, orgID, id)
	if err != nil {
		return err
	}

	if err := IsAllowed(ctx, *p); err != nil {
		return err
	}

	return nil
}

func authorizeWriteNotificationRule(ctx context.Context, orgID, id influxdb.ID) error {
	p, err := newNotificationRulePermission(influxdb.WriteAction, orgID, id)
	if err != nil {
		return err
	}

	if err := IsAllowed(ctx, *p); err != nil {
		return err
	}

	return nil
}

// FindNotificationRuleByID checks to see if the authorizer on context has read access to the id provided.
func (s *NotificationRuleStore) FindNotificationRuleByID(ctx context.Context, id influxdb.ID) (influxdb.NotificationRule, error) {
	nr, err := s.s.FindNotificationRuleByID(ctx, id)
	if err != nil {
		return nil, err
	}

	if err := authorizeReadNotificationRule(ctx, nr.GetOrgID(), nr.GetID()); err != nil {
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

	// This filters without allocating
	// https://github.com/golang/go/wiki/SliceTricks#filtering-without-allocating
	rules := nrs[:0]
	for _, nr := range nrs {
		err := authorizeReadNotificationRule(ctx, nr.GetOrgID(), nr.GetID())
		if err != nil && influxdb.ErrorCode(err) != influxdb.EUnauthorized {
			return nil, 0, err
		}

		if influxdb.ErrorCode(err) == influxdb.EUnauthorized {
			continue
		}

		rules = append(rules, nr)
	}

	return rules, len(rules), nil
}

// CreateNotificationRule checks to see if the authorizer on context has write access to the global notification rule resource.
func (s *NotificationRuleStore) CreateNotificationRule(ctx context.Context, nr influxdb.NotificationRule, userID influxdb.ID) error {
	p, err := influxdb.NewPermission(influxdb.WriteAction, influxdb.NotificationRuleResourceType, nr.GetOrgID())
	if err != nil {
		return err
	}

	if err := IsAllowed(ctx, *p); err != nil {
		return err
	}

	return s.s.CreateNotificationRule(ctx, nr, userID)
}

// UpdateNotificationRule checks to see if the authorizer on context has write access to the notification rule provided.
func (s *NotificationRuleStore) UpdateNotificationRule(ctx context.Context, id influxdb.ID, upd influxdb.NotificationRule, userID influxdb.ID) (influxdb.NotificationRule, error) {
	nr, err := s.FindNotificationRuleByID(ctx, id)
	if err != nil {
		return nil, err
	}

	if err := authorizeWriteNotificationRule(ctx, nr.GetOrgID(), id); err != nil {
		return nil, err
	}

	return s.s.UpdateNotificationRule(ctx, id, upd, userID)
}

// PatchNotificationRule checks to see if the authorizer on context has write access to the notification rule provided.
func (s *NotificationRuleStore) PatchNotificationRule(ctx context.Context, id influxdb.ID, upd influxdb.NotificationRuleUpdate) (influxdb.NotificationRule, error) {
	nr, err := s.FindNotificationRuleByID(ctx, id)
	if err != nil {
		return nil, err
	}

	if err := authorizeWriteNotificationRule(ctx, nr.GetOrgID(), id); err != nil {
		return nil, err
	}

	return s.s.PatchNotificationRule(ctx, id, upd)
}

// DeleteNotificationRule checks to see if the authorizer on context has write access to the notification rule provided.
func (s *NotificationRuleStore) DeleteNotificationRule(ctx context.Context, id influxdb.ID) error {
	nr, err := s.FindNotificationRuleByID(ctx, id)
	if err != nil {
		return err
	}

	if err := authorizeWriteNotificationRule(ctx, nr.GetOrgID(), id); err != nil {
		return err
	}

	return s.s.DeleteNotificationRule(ctx, id)
}

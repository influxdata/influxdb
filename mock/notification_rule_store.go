package mock

import (
	"context"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kit/platform"
)

var _ influxdb.NotificationRuleStore = &NotificationRuleStore{}

// NotificationRuleStore represents a service for managing notification rule data.
type NotificationRuleStore struct {
	*OrganizationService
	*UserResourceMappingService
	FindNotificationRuleByIDF     func(ctx context.Context, id platform.ID) (influxdb.NotificationRule, error)
	FindNotificationRuleByIDCalls SafeCount
	FindNotificationRulesF        func(ctx context.Context, filter influxdb.NotificationRuleFilter, opt ...influxdb.FindOptions) ([]influxdb.NotificationRule, int, error)
	FindNotificationRulesCalls    SafeCount
	CreateNotificationRuleF       func(ctx context.Context, nr influxdb.NotificationRuleCreate, userID platform.ID) error
	CreateNotificationRuleCalls   SafeCount
	UpdateNotificationRuleF       func(ctx context.Context, id platform.ID, nr influxdb.NotificationRuleCreate, userID platform.ID) (influxdb.NotificationRule, error)
	UpdateNotificationRuleCalls   SafeCount
	PatchNotificationRuleF        func(ctx context.Context, id platform.ID, upd influxdb.NotificationRuleUpdate) (influxdb.NotificationRule, error)
	PatchNotificationRuleCalls    SafeCount
	DeleteNotificationRuleF       func(ctx context.Context, id platform.ID) error
	DeleteNotificationRuleCalls   SafeCount
}

// NewNotificationRuleStore creats a fake notification rules tore.
func NewNotificationRuleStore() *NotificationRuleStore {
	return &NotificationRuleStore{
		OrganizationService:        NewOrganizationService(),
		UserResourceMappingService: NewUserResourceMappingService(),
		FindNotificationRuleByIDF: func(ctx context.Context, id platform.ID) (influxdb.NotificationRule, error) {
			return nil, nil
		},
		FindNotificationRulesF: func(ctx context.Context, f influxdb.NotificationRuleFilter, _ ...influxdb.FindOptions) ([]influxdb.NotificationRule, int, error) {
			return nil, 0, nil
		},
		CreateNotificationRuleF: func(ctx context.Context, nr influxdb.NotificationRuleCreate, userID platform.ID) error {
			return nil
		},
		UpdateNotificationRuleF: func(ctx context.Context, id platform.ID, nr influxdb.NotificationRuleCreate, userID platform.ID) (influxdb.NotificationRule, error) {
			return nil, nil
		},
		PatchNotificationRuleF: func(ctx context.Context, id platform.ID, upd influxdb.NotificationRuleUpdate) (influxdb.NotificationRule, error) {
			return nil, nil
		},
		DeleteNotificationRuleF: func(ctx context.Context, id platform.ID) error {
			return nil
		},
	}
}

// FindNotificationRuleByID returns a single telegraf config by ID.
func (s *NotificationRuleStore) FindNotificationRuleByID(ctx context.Context, id platform.ID) (influxdb.NotificationRule, error) {
	defer s.FindNotificationRuleByIDCalls.IncrFn()()
	return s.FindNotificationRuleByIDF(ctx, id)
}

// FindNotificationRules returns a list of notification rules that match filter and the total count of matching notification rules.
// Additional options provide pagination & sorting.
func (s *NotificationRuleStore) FindNotificationRules(ctx context.Context, filter influxdb.NotificationRuleFilter, opt ...influxdb.FindOptions) ([]influxdb.NotificationRule, int, error) {
	defer s.FindNotificationRulesCalls.IncrFn()()
	return s.FindNotificationRulesF(ctx, filter, opt...)
}

// CreateNotificationRule creates a new notification rule and sets ID with the new identifier.
func (s *NotificationRuleStore) CreateNotificationRule(ctx context.Context, nr influxdb.NotificationRuleCreate, userID platform.ID) error {
	defer s.CreateNotificationRuleCalls.IncrFn()()
	return s.CreateNotificationRuleF(ctx, nr, userID)
}

// UpdateNotificationRule updates a single notification rule.
// Returns the new notification rule after update.
func (s *NotificationRuleStore) UpdateNotificationRule(ctx context.Context, id platform.ID, nr influxdb.NotificationRuleCreate, userID platform.ID) (influxdb.NotificationRule, error) {
	defer s.UpdateNotificationRuleCalls.IncrFn()()
	return s.UpdateNotificationRuleF(ctx, id, nr, userID)
}

// PatchNotificationRule updates a single  notification rule with changeset.
// Returns the new notification rule after update.
func (s *NotificationRuleStore) PatchNotificationRule(ctx context.Context, id platform.ID, upd influxdb.NotificationRuleUpdate) (influxdb.NotificationRule, error) {
	defer s.PatchNotificationRuleCalls.IncrFn()()
	return s.PatchNotificationRuleF(ctx, id, upd)
}

// DeleteNotificationRule removes a notification rule by ID.
func (s *NotificationRuleStore) DeleteNotificationRule(ctx context.Context, id platform.ID) error {
	defer s.DeleteNotificationRuleCalls.IncrFn()()
	return s.DeleteNotificationRuleF(ctx, id)
}

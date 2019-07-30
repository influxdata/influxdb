package mock

import (
	"context"

	"github.com/influxdata/influxdb"
)

var _ influxdb.NotificationRuleStore = &NotificationRuleStore{}

// NotificationRuleStore represents a service for managing notification rule data.
type NotificationRuleStore struct {
	OrganizationService
	UserResourceMappingService
	FindNotificationRuleByIDF func(ctx context.Context, id influxdb.ID) (influxdb.NotificationRule, error)
	FindNotificationRulesF    func(ctx context.Context, filter influxdb.NotificationRuleFilter, opt ...influxdb.FindOptions) ([]influxdb.NotificationRule, int, error)
	CreateNotificationRuleF   func(ctx context.Context, nr influxdb.NotificationRule, userID influxdb.ID) error
	UpdateNotificationRuleF   func(ctx context.Context, id influxdb.ID, nr influxdb.NotificationRule, userID influxdb.ID) (influxdb.NotificationRule, error)
	PatchNotificationRuleF    func(ctx context.Context, id influxdb.ID, upd influxdb.NotificationRuleUpdate) (influxdb.NotificationRule, error)
	DeleteNotificationRuleF   func(ctx context.Context, id influxdb.ID) error
}

// FindNotificationRuleByID returns a single telegraf config by ID.
func (s *NotificationRuleStore) FindNotificationRuleByID(ctx context.Context, id influxdb.ID) (influxdb.NotificationRule, error) {
	return s.FindNotificationRuleByIDF(ctx, id)
}

// FindNotificationRules returns a list of notification rules that match filter and the total count of matching notification rules.
// Additional options provide pagination & sorting.
func (s *NotificationRuleStore) FindNotificationRules(ctx context.Context, filter influxdb.NotificationRuleFilter, opt ...influxdb.FindOptions) ([]influxdb.NotificationRule, int, error) {
	return s.FindNotificationRulesF(ctx, filter, opt...)
}

// CreateNotificationRule creates a new notification rule and sets ID with the new identifier.
func (s *NotificationRuleStore) CreateNotificationRule(ctx context.Context, nr influxdb.NotificationRule, userID influxdb.ID) error {
	return s.CreateNotificationRuleF(ctx, nr, userID)
}

// UpdateNotificationRule updates a single notification rule.
// Returns the new notification rule after update.
func (s *NotificationRuleStore) UpdateNotificationRule(ctx context.Context, id influxdb.ID, nr influxdb.NotificationRule, userID influxdb.ID) (influxdb.NotificationRule, error) {
	return s.UpdateNotificationRuleF(ctx, id, nr, userID)
}

// PatchNotificationRule updates a single  notification rule with changeset.
// Returns the new notification rule after update.
func (s *NotificationRuleStore) PatchNotificationRule(ctx context.Context, id influxdb.ID, upd influxdb.NotificationRuleUpdate) (influxdb.NotificationRule, error) {
	return s.PatchNotificationRuleF(ctx, id, upd)
}

// DeleteNotificationRule removes a notification rule by ID.
func (s *NotificationRuleStore) DeleteNotificationRule(ctx context.Context, id influxdb.ID) error {
	return s.DeleteNotificationRuleF(ctx, id)
}

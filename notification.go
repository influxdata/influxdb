package influxdb

import (
	"context"
	"encoding/json"
)

// Updator is general interface to embed
// with any domain level interface to do crud related ops.
type Updator interface {
	CRUDLogSetter
	SetID(id ID)
	SetOrgID(id ID)
}

// Getter is a general getter interface
// to return id, orgID...
type Getter interface {
	GetID() ID
	GetCRUDLog() CRUDLog
	GetOrgID() ID
}

// NotificationRule is a *Query* of a *Status Bucket* that returns the *Status*.
// When warranted by the rules, sends a *Message* to a 3rd Party
// using the *Notification Endpoint* and stores a receipt in the *Notifications Bucket*.
type NotificationRule interface {
	Valid() error
	Type() string
	json.Marshaler
	Updator
	Getter
	GetLimit() *Limit
}

// Limit don't notify me more than <limit> times every <limitEvery> seconds.
// If set, limit cannot be empty.
type Limit struct {
	Rate int `json:"limit,omitempty"`
	// every seconds.
	Every int `json:"limitEvery,omitempty"`
}

// NotificationRuleFilter represents a set of filter that restrict the returned notification rules.
type NotificationRuleFilter struct {
	OrgID        *ID
	Organization *string
	UserResourceMappingFilter
}

// QueryParams Converts NotificationRuleFilter fields to url query params.
func (f NotificationRuleFilter) QueryParams() map[string][]string {
	qp := map[string][]string{}

	if f.OrgID != nil {
		qp["orgID"] = []string{f.OrgID.String()}
	}

	if f.Organization != nil {
		qp["org"] = []string{*f.Organization}
	}

	return qp
}

// NotificationRuleStore represents a service for managing notification rule.
type NotificationRuleStore interface {
	// UserResourceMappingService must be part of all NotificationRuleStore service,
	// for create, search, delete.
	UserResourceMappingService
	// OrganizationService is needed for search filter
	OrganizationService

	// FindNotificationRuleByID returns a single notification rule by ID.
	FindNotificationRuleByID(ctx context.Context, id ID) (NotificationRule, error)

	// FindNotificationRules returns a list of notification rules that match filter and the total count of matching notification rules.
	// Additional options provide pagination & sorting.
	FindNotificationRules(ctx context.Context, filter NotificationRuleFilter, opt ...FindOptions) ([]NotificationRule, int, error)

	// CreateNotificationRule creates a new notification rule and sets b.ID with the new identifier.
	CreateNotificationRule(ctx context.Context, nr NotificationRule, userID ID) error

	// UpdateNotificationRuleUpdateNotificationRule updates a single notification rule.
	// Returns the new notification rule after update.
	UpdateNotificationRule(ctx context.Context, id ID, nr NotificationRule, userID ID) (NotificationRule, error)

	// DeleteNotificationRule removes a notification rule by ID.
	DeleteNotificationRule(ctx context.Context, id ID) error
}

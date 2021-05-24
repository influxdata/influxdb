package influxdb

import (
	"context"
	"encoding/json"

	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/kit/platform/errors"
)

// NotificationRule is a *Query* of a *Status Bucket* that returns the *Status*.
// When warranted by the rules, sends a *Message* to a 3rd Party
// using the *Notification Endpoint* and stores a receipt in the *Notifications Bucket*.
type NotificationRule interface {
	Valid() error
	Type() string
	json.Marshaler
	CRUDLogSetter
	SetID(id platform.ID)
	SetOrgID(id platform.ID)
	SetName(name string)
	SetDescription(description string)
	GetID() platform.ID
	GetCRUDLog() CRUDLog
	GetOrgID() platform.ID
	GetName() string
	GetDescription() string
	SetOwnerID(id platform.ID)
	ClearPrivateData()
	GetOwnerID() platform.ID
	SetTaskID(id platform.ID)
	GetTaskID() platform.ID
	GetEndpointID() platform.ID
	GetLimit() *Limit
	GenerateFlux(NotificationEndpoint) (string, error)
	MatchesTags(tags []Tag) bool
}

// NotificationRuleStore represents a service for managing notification rule.
type NotificationRuleStore interface {
	// FindNotificationRuleByID returns a single notification rule by ID.
	FindNotificationRuleByID(ctx context.Context, id platform.ID) (NotificationRule, error)

	// FindNotificationRules returns a list of notification rules that match filter and the total count of matching notification rules.
	// Additional options provide pagination & sorting.
	FindNotificationRules(ctx context.Context, filter NotificationRuleFilter, opt ...FindOptions) ([]NotificationRule, int, error)

	// CreateNotificationRule creates a new notification rule and sets b.ID with the new identifier.
	CreateNotificationRule(ctx context.Context, nr NotificationRuleCreate, userID platform.ID) error

	// UpdateNotificationRuleUpdateNotificationRule updates a single notification rule.
	// Returns the new notification rule after update.
	UpdateNotificationRule(ctx context.Context, id platform.ID, nr NotificationRuleCreate, userID platform.ID) (NotificationRule, error)

	// PatchNotificationRule updates a single  notification rule with changeset.
	// Returns the new notification rule state after update.
	PatchNotificationRule(ctx context.Context, id platform.ID, upd NotificationRuleUpdate) (NotificationRule, error)

	// DeleteNotificationRule removes a notification rule by ID.
	DeleteNotificationRule(ctx context.Context, id platform.ID) error
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
	OrgID        *platform.ID
	Organization *string
	Tags         []Tag
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

	qp["tag"] = []string{}
	for _, tp := range f.Tags {
		qp["tag"] = append(qp["tag"], tp.QueryParam())
	}

	return qp
}

// NotificationRuleCreate is the struct providing data to create a Notification Rule.
type NotificationRuleCreate struct {
	NotificationRule
	Status Status `json:"status"`
}

// NotificationRuleUpdate is the set of upgrade fields for patch request.
type NotificationRuleUpdate struct {
	Name        *string `json:"name,omitempty"`
	Description *string `json:"description,omitempty"`
	Status      *Status `json:"status,omitempty"`
}

// Valid will verify if the NotificationRuleUpdate is valid.
func (n *NotificationRuleUpdate) Valid() error {
	if n.Name != nil && *n.Name == "" {
		return &errors.Error{
			Code: errors.EInvalid,
			Msg:  "Notification Rule Name can't be empty",
		}
	}

	if n.Description != nil && *n.Description == "" {
		return &errors.Error{
			Code: errors.EInvalid,
			Msg:  "Notification Rule Description can't be empty",
		}
	}

	if n.Status != nil {
		if err := n.Status.Valid(); err != nil {
			return err
		}
	}

	return nil
}

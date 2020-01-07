package influxdb

import (
	"context"
	"encoding/json"
	"time"
)

// NotificationEndpointService represents a service for managing notification endpoints.
type NotificationEndpointService interface {
	// Create creates a new notification endpoint and sets b.ID with the new identifier.
	Create(ctx context.Context, userID ID, ne NotificationEndpoint) error

	// Delete removes a notification endpoint by ID, returns secret fields, orgID for further deletion.
	Delete(ctx context.Context, id ID) error

	// FindByID returns a single notification endpoint by ID.
	FindByID(ctx context.Context, id ID) (NotificationEndpoint, error)

	// Find returns a list of notification endpoints that match filter and the total count of matching notification endpoints.
	// Additional options provide pagination & sorting.
	Find(ctx context.Context, filter NotificationEndpointFilter, opt ...FindOptions) ([]NotificationEndpoint, error)

	// Update updates a single notification endpoint.
	// Returns the new notification endpoint after update.
	Update(ctx context.Context, update EndpointUpdate) (NotificationEndpoint, error)
}

type EndpointUpdate struct {
	UpdateType string
	ID         ID
	Fn         func(now time.Time, existing NotificationEndpoint) (NotificationEndpoint, error)
}

// NotificationEndpoint is the configuration describing
// how to call a 3rd party service. E.g. Slack, Pagerduty
type NotificationEndpoint interface {
	Valid() error
	Type() string
	json.Marshaler
	CRUDLogSetter
	SetID(id ID)
	SetOrgID(id ID)
	SetName(name string)
	SetDescription(description string)
	SetStatus(status Status)

	GetID() ID
	GetCRUDLog() CRUDLog
	GetOrgID() ID
	GetName() string
	GetDescription() string
	GetStatus() Status
	// SecretFields return available secret fields.
	SecretFields() []SecretField
	// BackfillSecretKeys fill back fill the secret field key during the unmarshalling
	// if value of that secret field is not nil.
	BackfillSecretKeys()
}

// ops for checks error
var (
	OpFindNotificationEndpointByID = "FindNotificationEndpointByID"
	OpFindNotificationEndpoint     = "FindNotificationEndpoint"
	OpFindNotificationEndpoints    = "FindNotificationEndpoints"
	OpCreateNotificationEndpoint   = "Create"
	OpUpdateNotificationEndpoint   = "Update"
	OpDeleteNotificationEndpoint   = "Delete"
)

// NotificationEndpointFilter represents a set of filter that restrict the returned notification endpoints.
type NotificationEndpointFilter struct {
	OrgID *ID
	Org   *string
	UserResourceMappingFilter
}

// QueryParams Converts NotificationEndpointFilter fields to url query params.
func (f NotificationEndpointFilter) QueryParams() map[string][]string {
	qp := map[string][]string{}

	if f.OrgID != nil {
		qp["orgID"] = []string{f.OrgID.String()}
	}

	if f.Org != nil {
		qp["org"] = []string{*f.Org}
	}

	return qp
}

// NotificationEndpointUpdate is the set of upgrade fields for patch request.
type NotificationEndpointUpdate struct {
	Name        *string `json:"name,omitempty"`
	Description *string `json:"description,omitempty"`
	Status      *Status `json:"status,omitempty"`
}

// Valid will verify if the NotificationEndpointUpdate is valid.
func (n *NotificationEndpointUpdate) Valid() error {
	if n.Name != nil && *n.Name == "" {
		return &Error{
			Code: EInvalid,
			Msg:  "Notification Endpoint Name can't be empty",
		}
	}

	if n.Description != nil && *n.Description == "" {
		return &Error{
			Code: EInvalid,
			Msg:  "Notification Endpoint Description can't be empty",
		}
	}

	if n.Status != nil {
		if err := n.Status.Valid(); err != nil {
			return err
		}
	}

	return nil
}

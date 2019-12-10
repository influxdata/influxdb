package influxdb

import (
	"context"
	"encoding/json"
	"errors"
)

var (
	// ErrInvalidNotificationEndpointType denotes that the provided NotificationEndpoint is not a valid type
	ErrInvalidNotificationEndpointType = errors.New("unknown notification endpoint type")
)

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
	OpCreateNotificationEndpoint   = "CreateNotificationEndpoint"
	OpUpdateNotificationEndpoint   = "UpdateNotificationEndpoint"
	OpDeleteNotificationEndpoint   = "DeleteNotificationEndpoint"
)

// NotificationEndpointFilter represents a set of filter that restrict the returned notification endpoints.
type NotificationEndpointFilter struct {
	ID    *ID
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

// NotificationEndpointService represents a service for managing notification endpoints.
type NotificationEndpointService interface {
	// UserResourceMappingService must be part of all NotificationEndpointStore service,
	// for create, delete.
	UserResourceMappingService
	// OrganizationService is needed for search filter
	OrganizationService

	// FindNotificationEndpointByID returns a single notification endpoint by ID.
	FindNotificationEndpointByID(ctx context.Context, id ID) (NotificationEndpoint, error)

	// FindNotificationEndpoints returns a list of notification endpoints that match filter and the total count of matching notification endpoints.
	// Additional options provide pagination & sorting.
	FindNotificationEndpoints(ctx context.Context, filter NotificationEndpointFilter, opt ...FindOptions) ([]NotificationEndpoint, int, error)

	// CreateNotificationEndpoint creates a new notification endpoint and sets b.ID with the new identifier.
	CreateNotificationEndpoint(ctx context.Context, ne NotificationEndpoint, userID ID) error

	// UpdateNotificationEndpoint updates a single notification endpoint.
	// Returns the new notification endpoint after update.
	UpdateNotificationEndpoint(ctx context.Context, id ID, nr NotificationEndpoint, userID ID) (NotificationEndpoint, error)

	// PatchNotificationEndpoint updates a single  notification endpoint with changeset.
	// Returns the new notification endpoint state after update.
	PatchNotificationEndpoint(ctx context.Context, id ID, upd NotificationEndpointUpdate) (NotificationEndpoint, error)

	// DeleteNotificationEndpoint removes a notification endpoint by ID, returns secret fields, orgID for further deletion.
	DeleteNotificationEndpoint(ctx context.Context, id ID) (flds []SecretField, orgID ID, err error)
}

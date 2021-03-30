package influxdb

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/influxdata/influxdb/v2/kit/platform"
	errors2 "github.com/influxdata/influxdb/v2/kit/platform/errors"
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
	SetID(id platform.ID)
	SetOrgID(id platform.ID)
	SetName(name string)
	SetDescription(description string)
	SetStatus(status Status)

	GetID() platform.ID
	GetCRUDLog() CRUDLog
	GetOrgID() platform.ID
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
	ID    *platform.ID
	OrgID *platform.ID
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
		return &errors2.Error{
			Code: errors2.EInvalid,
			Msg:  "Notification Endpoint Name can't be empty",
		}
	}

	if n.Description != nil && *n.Description == "" {
		return &errors2.Error{
			Code: errors2.EInvalid,
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
	// FindNotificationEndpointByID returns a single notification endpoint by ID.
	FindNotificationEndpointByID(ctx context.Context, id platform.ID) (NotificationEndpoint, error)

	// FindNotificationEndpoints returns a list of notification endpoints that match filter and the total count of matching notification endpoints.
	// Additional options provide pagination & sorting.
	FindNotificationEndpoints(ctx context.Context, filter NotificationEndpointFilter, opt ...FindOptions) ([]NotificationEndpoint, int, error)

	// CreateNotificationEndpoint creates a new notification endpoint and sets b.ID with the new identifier.
	CreateNotificationEndpoint(ctx context.Context, ne NotificationEndpoint, userID platform.ID) error

	// UpdateNotificationEndpoint updates a single notification endpoint.
	// Returns the new notification endpoint after update.
	UpdateNotificationEndpoint(ctx context.Context, id platform.ID, nr NotificationEndpoint, userID platform.ID) (NotificationEndpoint, error)

	// PatchNotificationEndpoint updates a single  notification endpoint with changeset.
	// Returns the new notification endpoint state after update.
	PatchNotificationEndpoint(ctx context.Context, id platform.ID, upd NotificationEndpointUpdate) (NotificationEndpoint, error)

	// DeleteNotificationEndpoint removes a notification endpoint by ID, returns secret fields, orgID for further deletion.
	DeleteNotificationEndpoint(ctx context.Context, id platform.ID) (flds []SecretField, orgID platform.ID, err error)
}

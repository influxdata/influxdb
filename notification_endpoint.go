package influxdb

import (
	"context"
	"encoding/json"
	"time"
)

type (
	// NotificationEndpointService represents a service for managing notification endpoints.
	NotificationEndpointService interface {
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

	// NotificationEndpoint is the configuration describing
	// how to call a 3rd party service. E.g. Slack, Pagerduty
	NotificationEndpoint interface {
		Valid() error
		Type() string
		Base() *EndpointBase
		// SecretFields return available secret fields.
		SecretFields() []SecretField
		// BackfillSecretKeys fill back fill the secret field key during the unmarshalling
		// if value of that secret field is not nil.
		BackfillSecretKeys()
		json.Marshaler
	}

	// EndpointUpdate provides a means of updating an endpoint in different ways while
	// also informing the service of what type of update it is via the UpdateType.
	EndpointUpdate struct {
		UpdateType string
		ID         ID
		Fn         func(now time.Time, existing NotificationEndpoint) (NotificationEndpoint, error)
	}
)

// EndpointBase is the of every notification endpoint.
type EndpointBase struct {
	ID          ID     `json:"id,omitempty"`
	Name        string `json:"name"`
	Description string `json:"description,omitempty"`
	OrgID       ID     `json:"orgID,omitempty"`
	Status      Status `json:"status"`
	CRUDLog
}

func (b EndpointBase) Valid() error {
	if !b.ID.Valid() {
		return &Error{
			Code: EInvalid,
			Msg:  "Notification Endpoint ID is invalid",
		}
	}
	if b.Name == "" {
		return &Error{
			Code: EInvalid,
			Msg:  "Notification Endpoint Name can't be empty",
		}
	}
	if b.Status != Active && b.Status != Inactive {
		return &Error{
			Code: EInvalid,
			Msg:  "invalid status",
		}
	}
	return nil
}

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
		// TODO(jsteenb2): why can't description be empty string?
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

package influxdb

import (
	"context"
	"fmt"

	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/kit/platform/errors"
)

// Organization is an organization. 🎉
type Organization struct {
	ID          platform.ID `json:"id,omitempty"`
	Name        string      `json:"name"`
	Description string      `json:"description"`
	CRUDLog
}

// errors of org
var (
	// ErrOrgNameisEmpty is error when org name is empty
	ErrOrgNameisEmpty = &errors.Error{
		Code: errors.EInvalid,
		Msg:  "org name is empty",
	}
)

// ops for orgs error and orgs op logs.
const (
	OpFindOrganizationByID = "FindOrganizationByID"
	OpFindOrganization     = "FindOrganization"
	OpFindOrganizations    = "FindOrganizations"
	OpCreateOrganization   = "CreateOrganization"
	OpPutOrganization      = "PutOrganization"
	OpUpdateOrganization   = "UpdateOrganization"
	OpDeleteOrganization   = "DeleteOrganization"
)

// OrganizationService represents a service for managing organization data.
type OrganizationService interface {
	// Returns a single organization by ID.
	FindOrganizationByID(ctx context.Context, id platform.ID) (*Organization, error)

	// Returns the first organization that matches filter.
	FindOrganization(ctx context.Context, filter OrganizationFilter) (*Organization, error)

	// Returns a list of organizations that match filter and the total count of matching organizations.
	// Additional options provide pagination & sorting.
	FindOrganizations(ctx context.Context, filter OrganizationFilter, opt ...FindOptions) ([]*Organization, int, error)

	// Creates a new organization and sets b.ID with the new identifier.
	CreateOrganization(ctx context.Context, b *Organization) error

	// Updates a single organization with changeset.
	// Returns the new organization state after update.
	UpdateOrganization(ctx context.Context, id platform.ID, upd OrganizationUpdate) (*Organization, error)

	// Removes a organization by ID.
	DeleteOrganization(ctx context.Context, id platform.ID) error
}

// OrganizationUpdate represents updates to a organization.
// Only fields which are set are updated.
type OrganizationUpdate struct {
	Name        *string
	Description *string `json:"description,omitempty"`
}

// ErrInvalidOrgFilter is the error indicate org filter is empty
var ErrInvalidOrgFilter = &errors.Error{
	Code: errors.EInvalid,
	Msg:  "Please provide either orgID or org",
}

// OrganizationFilter represents a set of filter that restrict the returned results.
type OrganizationFilter struct {
	Name   *string
	ID     *platform.ID
	UserID *platform.ID
}

func ErrInternalOrgServiceError(op string, err error) *errors.Error {
	return &errors.Error{
		Code: errors.EInternal,
		Msg:  fmt.Sprintf("unexpected error in organizations; Err: %v", err),
		Op:   op,
		Err:  err,
	}
}

func (f OrganizationFilter) QueryParams() map[string][]string {
	queryParams := make(map[string][]string)
	if f.Name != nil {
		queryParams["org"] = []string{*f.Name}
	}
	if f.ID != nil {
		queryParams["orgID"] = []string{f.ID.String()}
	}
	if f.UserID != nil {
		queryParams["userID"] = []string{f.UserID.String()}
	}
	return queryParams
}

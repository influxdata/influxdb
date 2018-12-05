package platform

import "context"

// Organization is an organization. 🎉
type Organization struct {
	ID   ID     `json:"id,omitempty"`
	Name string `json:"name"`
}

// ops for orgs error and orgs op logs.
const (
	OpFindOrganizationByID = "FindOrganizationByID"
	OpFindOrganization     = "FindOrganization"
	OpFindOrganizations    = "FindOrganizations"
	OpCreateOrganization   = "CreateOrganization"
	OpUpdateOrganization   = "UpdateOrganization"
	OpDeleteOrganization   = "DeleteOrganization"
)

// OrganizationService represents a service for managing organization data.
type OrganizationService interface {
	// Returns a single organization by ID.
	FindOrganizationByID(ctx context.Context, id ID) (*Organization, error)

	// Returns the first organization that matches filter.
	FindOrganization(ctx context.Context, filter OrganizationFilter) (*Organization, error)

	// Returns a list of organizations that match filter and the total count of matching organizations.
	// Additional options provide pagination & sorting.
	FindOrganizations(ctx context.Context, filter OrganizationFilter, opt ...FindOptions) ([]*Organization, int, error)

	// Creates a new organization and sets b.ID with the new identifier.
	CreateOrganization(ctx context.Context, b *Organization) error

	// Updates a single organization with changeset.
	// Returns the new organization state after update.
	UpdateOrganization(ctx context.Context, id ID, upd OrganizationUpdate) (*Organization, error)

	// Removes a organization by ID.
	DeleteOrganization(ctx context.Context, id ID) error
}

// OrganizationUpdate represents updates to a organization.
// Only fields which are set are updated.
type OrganizationUpdate struct {
	Name *string
}

// OrganizationFilter represents a set of filter that restrict the returned results.
type OrganizationFilter struct {
	Name *string
	ID   *ID
}

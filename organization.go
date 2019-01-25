package influxdb

import (
	"context"
	"net/url"
	"sort"
)

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
	FindOrganizations(ctx context.Context, filter OrganizationFilter, opts FindOptions) ([]*Organization, int, error)

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

// QueryParams turns a user filter into query params
//
// It implements PagingFilter.
func (f OrganizationFilter) QueryParams() map[string][]string {
	qp := url.Values{}
	if f.ID != nil {
		qp.Add("id", f.ID.String())
	}

	if f.Name != nil {
		qp.Add("name", *f.Name)
	}

	return qp
}

// SortOrganizations sorts a slice of organizations by a field.
func SortOrganizations(opts FindOptions, orgs []*Organization) {
	var sorter func(i, j int) bool
	switch opts.SortBy {
	case "Name":
		sorter = func(i, j int) bool {
			if opts.Descending {
				return orgs[i].Name > orgs[j].Name
			}
			return orgs[i].Name < orgs[j].Name
		}
	default:
		sorter = func(i, j int) bool {
			if opts.Descending {
				return orgs[i].ID > orgs[j].ID
			}
			return orgs[i].ID < orgs[j].ID
		}
	}

	sort.Slice(orgs, sorter)
}

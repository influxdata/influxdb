package http

import (
	"context"
	"net/http"

	platform "github.com/influxdata/influxdb"
)

const (
	// OrgID is the http query parameter to specify an organization by ID.
	OrgID = "orgID"
	// Org is the http query parameter that take either the ID or Name interchangeably
	Org = "org"
)

// queryOrganization returns the organization for any http request.
func queryOrganization(ctx context.Context, r *http.Request, svc platform.OrganizationService) (o *platform.Organization, err error) {

	filter := platform.OrganizationFilter{}

	if organization := r.URL.Query().Get(Org); organization != "" {
		if id, err := platform.IDFromString(organization); err == nil {
			filter.ID = id
		} else {
			filter.Name = &organization
		}
	}

	if reqID := r.URL.Query().Get(OrgID); reqID != "" {
		filter.ID, err = platform.IDFromString(reqID)
		if err != nil {
			return nil, err
		}
	}
	return svc.FindOrganization(ctx, filter)
}

package http

import (
	"context"
	"net/http"

	platform "github.com/influxdata/influxdb"
)

const (
	// OrgName is the http query parameter to specify an organization by name.
	OrgName = "org"
	// OrgID is the http query parameter to specify an organization by ID.
	OrgID = "orgID"
	// Org is the http query parameter that take either the ID or Name interchangeably
	Org = "org"
)

// queryOrganization returns the organization for any http request.
func queryOrganization(ctx context.Context, r *http.Request, svc platform.OrganizationService) (o *platform.Organization, err error) {

	if organization := r.URL.Query().Get(Org); organization != "" {
		if id, err := platform.IDFromString(organization); err == nil {
			// Decoded ID successfully. Make sure it's a real org.
			o, err := svc.FindOrganization(ctx, platform.OrganizationFilter{ID: id})
			if err == nil {
				return o, err
			} else if platform.ErrorCode(err) != platform.ENotFound {
				return nil, err
			}
		}

		o, err := svc.FindOrganization(ctx, platform.OrganizationFilter{Name: &organization})
		if err != nil {
			return nil, err
		}
		return o, nil
	}

	filter := platform.OrganizationFilter{}
	if reqID := r.URL.Query().Get(OrgID); reqID != "" {
		filter.ID, err = platform.IDFromString(reqID)
		if err != nil {
			return nil, err
		}
	}
	return svc.FindOrganization(ctx, filter)
}

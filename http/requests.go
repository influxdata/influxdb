package http

import (
	"context"
	"net/http"

	"github.com/influxdata/platform"
)

const (
	// OrgName is the http query parameter to specify an organization by name.
	OrgName = "organization"
	// OrgID is the http query parameter to specify an organization by ID.
	OrgID = "organizationID"
)

// queryOrganization returns the organization for any http request.
func queryOrganization(ctx context.Context, r *http.Request, svc platform.OrganizationService) (o *platform.Organization, err error) {
	filter := platform.OrganizationFilter{}
	if reqID := r.URL.Query().Get(OrgID); reqID != "" {
		filter.ID, err = platform.IDFromString(reqID)
		if err != nil {
			return nil, err
		}
	}

	if name := r.URL.Query().Get(OrgName); name != "" {
		filter.Name = &name
	}

	return svc.FindOrganization(ctx, filter)
}

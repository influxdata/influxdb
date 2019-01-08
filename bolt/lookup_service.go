package bolt

import (
	"context"

	platform "github.com/influxdata/influxdb"
)

var _ platform.LookupService = (*Client)(nil)

// Name returns the name for the resource and ID.
func (c *Client) Name(ctx context.Context, resource platform.Resource, id platform.ID) (string, error) {
	if err := resource.Valid(); err != nil {
		return "", err
	}

	if ok := id.Valid(); !ok {
		return "", platform.ErrInvalidID
	}

	switch resource {
	case platform.TasksResource: // 5 // TODO(goller): unify task bolt storage here so we can lookup names
	case platform.AuthorizationsResource: // 0 TODO(goller): authorizations should also have optional names
	case platform.BucketsResource: // 1
		r, err := c.FindBucketByID(ctx, id)
		if err != nil {
			return "", err
		}
		return r.Name, nil
	case platform.DashboardsResource: // 2
		r, err := c.FindDashboardByID(ctx, id)
		if err != nil {
			return "", err
		}
		return r.Name, nil
	case platform.OrgsResource: // 3
		r, err := c.FindOrganizationByID(ctx, id)
		if err != nil {
			return "", err
		}
		return r.Name, nil
	case platform.SourcesResource: // 4
		r, err := c.FindSourceByID(ctx, id)
		if err != nil {
			return "", err
		}
		return r.Name, nil
	case platform.TelegrafsResource: // 6
		r, err := c.FindTelegrafConfigByID(ctx, id)
		if err != nil {
			return "", err
		}
		return r.Name, nil
	case platform.UsersResource: // 7
		r, err := c.FindUserByID(ctx, id)
		if err != nil {
			return "", err
		}
		return r.Name, nil
	}

	return "", nil
}

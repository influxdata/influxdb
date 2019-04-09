package inmem

import (
	"context"

	platform "github.com/influxdata/influxdb"
)

var _ platform.LookupService = (*Service)(nil)

// Name returns the name for the resource and ID.
func (s *Service) Name(ctx context.Context, resource platform.ResourceType, id platform.ID) (string, error) {
	if err := resource.Valid(); err != nil {
		return "", err
	}

	if ok := id.Valid(); !ok {
		return "", platform.ErrInvalidID
	}

	switch resource {
	case platform.TasksResourceType: // 5 // TODO(goller): unify task bolt storage here so we can lookup names
	case platform.AuthorizationsResourceType: // 0 TODO(goller): authorizations should also have optional names
	case platform.SourcesResourceType: // 4 TODO(goller): no inmen version of sources service: https://github.com/influxdata/platform/issues/2145
	case platform.BucketsResourceType: // 1
		r, err := s.FindBucketByID(ctx, id)
		if err != nil {
			return "", err
		}
		return r.Name, nil
	case platform.DashboardsResourceType: // 2
		r, err := s.FindDashboardByID(ctx, id)
		if err != nil {
			return "", err
		}
		return r.Name, nil
	case platform.OrgsResourceType: // 3
		r, err := s.FindOrganizationByID(ctx, id)
		if err != nil {
			return "", err
		}
		return r.Name, nil
	case platform.TelegrafsResourceType: // 6
		r, err := s.FindTelegrafConfigByID(ctx, id)
		if err != nil {
			return "", err
		}
		return r.Name, nil
	case platform.UsersResourceType: // 7
		r, err := s.FindUserByID(ctx, id)
		if err != nil {
			return "", err
		}
		return r.Name, nil
	}

	return "", nil
}

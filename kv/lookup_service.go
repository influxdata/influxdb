package kv

import (
	"context"

	"github.com/influxdata/influxdb/v2"
)

var _ influxdb.LookupService = (*Service)(nil)

// Name returns the name for the resource and ID.
func (s *Service) Name(ctx context.Context, resource influxdb.ResourceType, id influxdb.ID) (string, error) {
	if err := resource.Valid(); err != nil {
		return "", err
	}

	if ok := id.Valid(); !ok {
		return "", influxdb.ErrInvalidID
	}

	switch resource {
	case influxdb.TasksResourceType: // 5 // TODO(goller): unify task storage here so we can lookup names
	case influxdb.AuthorizationsResourceType: // 0 TODO(goller): authorizations should also have optional names
	case influxdb.BucketsResourceType: // 1
		r, err := s.FindBucketByID(ctx, id)
		if err != nil {
			return "", err
		}
		return r.Name, nil
	case influxdb.DashboardsResourceType: // 2
		r, err := s.FindDashboardByID(ctx, id)
		if err != nil {
			return "", err
		}
		return r.Name, nil
	case influxdb.OrgsResourceType: // 3
		r, err := s.FindOrganizationByID(ctx, id)
		if err != nil {
			return "", err
		}
		return r.Name, nil
	case influxdb.SourcesResourceType: // 4
		r, err := s.FindSourceByID(ctx, id)
		if err != nil {
			return "", err
		}
		return r.Name, nil
	case influxdb.TelegrafsResourceType: // 6
		r, err := s.FindTelegrafConfigByID(ctx, id)
		if err != nil {
			return "", err
		}
		return r.Name, nil
	case influxdb.UsersResourceType: // 7
		r, err := s.FindUserByID(ctx, id)
		if err != nil {
			return "", err
		}
		return r.Name, nil
	}

	return "", nil
}

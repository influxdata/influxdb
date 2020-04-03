package tenant

import (
	"context"

	"github.com/influxdata/influxdb/v2"
	icontext "github.com/influxdata/influxdb/v2/context"
	"github.com/influxdata/influxdb/v2/kv"
)

// Returns a single organization by ID.
func (s *Service) FindOrganizationByID(ctx context.Context, id influxdb.ID) (*influxdb.Organization, error) {
	var org *influxdb.Organization
	err := s.store.View(ctx, func(tx kv.Tx) error {
		o, err := s.store.GetOrg(ctx, tx, id)
		if err != nil {
			return err
		}
		org = o
		return nil
	})

	if err != nil {
		return nil, err
	}

	return org, nil
}

// Returns the first organization that matches filter.
func (s *Service) FindOrganization(ctx context.Context, filter influxdb.OrganizationFilter) (*influxdb.Organization, error) {
	if filter.ID != nil {
		return s.FindOrganizationByID(ctx, *filter.ID)
	}

	if filter.Name == nil {
		return nil, influxdb.ErrInvalidOrgFilter
	}

	var org *influxdb.Organization
	err := s.store.View(ctx, func(tx kv.Tx) error {
		o, err := s.store.GetOrgByName(ctx, tx, *filter.Name)
		if err != nil {
			return err
		}
		org = o
		return nil
	})

	if err != nil {
		return nil, err
	}

	return org, nil
}

// Returns a list of organizations that match filter and the total count of matching organizations.
// Additional options provide pagination & sorting.
func (s *Service) FindOrganizations(ctx context.Context, filter influxdb.OrganizationFilter, opt ...influxdb.FindOptions) ([]*influxdb.Organization, int, error) {
	// if im given a id or a name I know I can only return 1
	if filter.ID != nil || filter.Name != nil {
		org, err := s.FindOrganization(ctx, filter)
		if err != nil {
			return nil, 0, err
		}
		return []*influxdb.Organization{org}, 1, nil
	}

	var orgs []*influxdb.Organization
	err := s.store.View(ctx, func(tx kv.Tx) error {
		os, err := s.store.ListOrgs(ctx, tx, opt...)
		if err != nil {
			return err
		}
		orgs = os
		return nil
	})

	if err != nil {
		return nil, 0, err
	}

	return orgs, len(orgs), err
}

// Creates a new organization and sets b.ID with the new identifier.
func (s *Service) CreateOrganization(ctx context.Context, o *influxdb.Organization) error {
	err := s.store.Update(ctx, func(tx kv.Tx) error {
		err := s.store.CreateOrg(ctx, tx, o)
		if err != nil {
			return err
		}

		tb := &influxdb.Bucket{
			OrgID:           o.ID,
			Type:            influxdb.BucketTypeSystem,
			Name:            influxdb.TasksSystemBucketName,
			RetentionPeriod: influxdb.TasksSystemBucketRetention,
			Description:     "System bucket for task logs",
		}

		if err := s.store.CreateBucket(ctx, tx, tb); err != nil {
			return err
		}

		mb := &influxdb.Bucket{
			OrgID:           o.ID,
			Type:            influxdb.BucketTypeSystem,
			Name:            influxdb.MonitoringSystemBucketName,
			RetentionPeriod: influxdb.MonitoringSystemBucketRetention,
			Description:     "System bucket for monitoring logs",
		}

		if err := s.store.CreateBucket(ctx, tx, mb); err != nil {
			return err
		}

		// create assiciated URM
		userID, err := icontext.GetUserID(ctx)
		if err == nil {
			// if I am given a userid i can associate the user as the org owner
			err = s.store.CreateURM(ctx, tx, &influxdb.UserResourceMapping{
				UserID:       userID,
				UserType:     influxdb.Owner,
				MappingType:  influxdb.UserMappingType,
				ResourceType: influxdb.OrgsResourceType,
				ResourceID:   o.ID,
			})
			if err != nil {
				return err
			}
			err = s.store.CreateURM(ctx, tx, &influxdb.UserResourceMapping{
				UserID:       userID,
				UserType:     influxdb.Owner,
				MappingType:  influxdb.UserMappingType,
				ResourceType: influxdb.BucketsResourceType,
				ResourceID:   tb.ID,
			})
			if err != nil {
				return err
			}
			err = s.store.CreateURM(ctx, tx, &influxdb.UserResourceMapping{
				UserID:       userID,
				UserType:     influxdb.Owner,
				MappingType:  influxdb.UserMappingType,
				ResourceType: influxdb.BucketsResourceType,
				ResourceID:   mb.ID,
			})

			if err != nil {
				return err
			}

		}
		return nil
	})
	return err
}

// Updates a single organization with changeset.
// Returns the new organization state after update.
func (s *Service) UpdateOrganization(ctx context.Context, id influxdb.ID, upd influxdb.OrganizationUpdate) (*influxdb.Organization, error) {
	var org *influxdb.Organization
	err := s.store.Update(ctx, func(tx kv.Tx) error {
		o, err := s.store.UpdateOrg(ctx, tx, id, upd)
		if err != nil {
			return err
		}
		org = o
		return nil
	})
	if err != nil {
		return nil, err
	}
	return org, nil
}

// Removes a organization by ID.
func (s *Service) DeleteOrganization(ctx context.Context, id influxdb.ID) error {
	err := s.store.Update(ctx, func(tx kv.Tx) error {
		// clean up the buckets for this organization
		filter := BucketFilter{
			OrganizationID: &id,
		}
		bs, err := s.store.ListBuckets(ctx, tx, filter)
		if err != nil {
			return err
		}
		for _, b := range bs {
			if err := s.store.DeleteBucket(ctx, tx, b.ID); err != nil {
				if err != ErrBucketNotFound {
					return err
				}
			}
			if err := s.removeResourceRelations(ctx, tx, b.ID); err != nil {
				return err
			}
		}

		if err := s.removeResourceRelations(ctx, tx, id); err != nil {
			return err
		}

		return s.store.DeleteOrg(ctx, tx, id)
	})
	return err
}

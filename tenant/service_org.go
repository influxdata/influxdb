package tenant

import (
	"context"

	"github.com/influxdata/influxdb/v2"
	icontext "github.com/influxdata/influxdb/v2/context"
	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/kv"
)

type OrgSvc struct {
	store *Store
	svc   *Service
}

func NewOrganizationSvc(st *Store, svc *Service) *OrgSvc {
	return &OrgSvc{
		store: st,
		svc:   svc,
	}
}

// Returns a single organization by ID.
func (s *OrgSvc) FindOrganizationByID(ctx context.Context, id platform.ID) (*influxdb.Organization, error) {
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
func (s *OrgSvc) FindOrganization(ctx context.Context, filter influxdb.OrganizationFilter) (*influxdb.Organization, error) {
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
func (s *OrgSvc) FindOrganizations(ctx context.Context, filter influxdb.OrganizationFilter, opt ...influxdb.FindOptions) ([]*influxdb.Organization, int, error) {
	// if im given a id or a name I know I can only return 1
	if filter.ID != nil || filter.Name != nil {
		org, err := s.FindOrganization(ctx, filter)
		if err != nil {
			return nil, 0, err
		}
		return []*influxdb.Organization{org}, 1, nil
	}

	var orgs []*influxdb.Organization

	if filter.UserID != nil {
		// find urms for orgs with this user
		urms, _, err := s.svc.FindUserResourceMappings(ctx, influxdb.UserResourceMappingFilter{
			UserID:       *filter.UserID,
			ResourceType: influxdb.OrgsResourceType,
		}, opt...)
		if err != nil {
			return nil, 0, err
		}
		// find orgs by the urm's resource ids.
		for _, urm := range urms {
			o, err := s.FindOrganizationByID(ctx, urm.ResourceID)
			if err == nil {
				// if there is an error then this is a crufty urm and we should just move on
				orgs = append(orgs, o)
			}
		}

		return orgs, len(orgs), nil
	}

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
func (s *OrgSvc) CreateOrganization(ctx context.Context, o *influxdb.Organization) error {
	err := s.store.Update(ctx, func(tx kv.Tx) error {
		return s.store.CreateOrg(ctx, tx, o)
	})
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

	if err := s.svc.CreateBucket(ctx, tb); err != nil {
		return err
	}

	mb := &influxdb.Bucket{
		OrgID:           o.ID,
		Type:            influxdb.BucketTypeSystem,
		Name:            influxdb.MonitoringSystemBucketName,
		RetentionPeriod: influxdb.MonitoringSystemBucketRetention,
		Description:     "System bucket for monitoring logs",
	}

	if err := s.svc.CreateBucket(ctx, mb); err != nil {
		return err
	}

	// create associated URM
	userID, err := icontext.GetUserID(ctx)
	if err == nil && userID.Valid() {
		// if I am given a userid i can associate the user as the org owner
		err = s.svc.CreateUserResourceMapping(ctx, &influxdb.UserResourceMapping{
			UserID:       userID,
			UserType:     influxdb.Owner,
			MappingType:  influxdb.UserMappingType,
			ResourceType: influxdb.OrgsResourceType,
			ResourceID:   o.ID,
		})
		if err != nil {
			return err
		}
	}
	return nil
}

// Updates a single organization with changeset.
// Returns the new organization state after update.
func (s *OrgSvc) UpdateOrganization(ctx context.Context, id platform.ID, upd influxdb.OrganizationUpdate) (*influxdb.Organization, error) {
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

// DeleteOrganization removes a organization by ID and its dependent resources.
func (s *OrgSvc) DeleteOrganization(ctx context.Context, id platform.ID) error {
	// clean up the buckets for this organization
	filter := influxdb.BucketFilter{
		OrganizationID: &id,
	}
	bs, _, err := s.svc.FindBuckets(ctx, filter)
	if err != nil {
		return err
	}
	for _, b := range bs {
		if err := s.svc.DeleteBucket(internalCtx(ctx), b.ID); err != nil {
			if err != ErrBucketNotFound {
				return err
			}
		}
	}

	err = s.store.Update(ctx, func(tx kv.Tx) error {
		return s.store.DeleteOrg(ctx, tx, id)
	})
	if err != nil {
		return err
	}

	return s.removeResourceRelations(ctx, id)
}

// removeResourceRelations allows us to clean up any resource relationship that would have normally been left over after a delete action of a resource.
func (s *OrgSvc) removeResourceRelations(ctx context.Context, resourceID platform.ID) error {
	urms, _, err := s.svc.FindUserResourceMappings(ctx, influxdb.UserResourceMappingFilter{
		ResourceID: resourceID,
	})
	if err != nil {
		return err
	}
	for _, urm := range urms {
		err := s.svc.DeleteUserResourceMapping(ctx, urm.ResourceID, urm.UserID)
		if err != nil && err != ErrURMNotFound {
			return err
		}
	}
	return nil
}

package kv

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/influxdata/influxdb/v2/resource"
	"go.uber.org/zap"

	"github.com/influxdata/influxdb/v2"
	icontext "github.com/influxdata/influxdb/v2/context"
	"github.com/influxdata/influxdb/v2/kit/tracing"
)

const (
	// MaxIDGenerationN is the maximum number of times an ID generation is done before failing.
	MaxIDGenerationN = 100
)

var (
	organizationBucket = []byte("organizationsv1")
	organizationIndex  = []byte("organizationindexv1")
)

// ErrFailureGeneratingID occurs ony when the random number generator
// cannot generate an ID in MaxIDGenerationN times.
var ErrFailureGeneratingID = &influxdb.Error{
	Code: influxdb.EInternal,
	Msg:  "unable to generate valid id",
}

var _ influxdb.OrganizationService = (*Service)(nil)
var _ influxdb.OrganizationOperationLogService = (*Service)(nil)

// FindOrganizationByID retrieves a organization by id.
func (s *Service) FindOrganizationByID(ctx context.Context, id influxdb.ID) (*influxdb.Organization, error) {
	var o *influxdb.Organization
	err := s.kv.View(ctx, func(tx Tx) error {
		org, pe := s.findOrganizationByID(ctx, tx, id)
		if pe != nil {
			return &influxdb.Error{
				Err: pe,
			}
		}
		o = org
		return nil
	})

	if err != nil {
		return nil, err
	}

	return o, nil
}

func (s *Service) findOrganizationByID(ctx context.Context, tx Tx, id influxdb.ID) (*influxdb.Organization, error) {
	span, _ := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	encodedID, err := id.Encode()
	if err != nil {
		return nil, &influxdb.Error{
			Code: influxdb.EInvalid,
			Err:  err,
		}
	}

	b, err := tx.Bucket(organizationBucket)
	if err != nil {
		return nil, err
	}

	v, err := b.Get(encodedID)
	if IsNotFound(err) {
		return nil, &influxdb.Error{
			Code: influxdb.ENotFound,
			Msg:  "organization not found",
		}
	}

	if err != nil {
		return nil, err
	}

	var o influxdb.Organization
	if err := json.Unmarshal(v, &o); err != nil {
		return nil, &influxdb.Error{
			Err: err,
		}
	}

	return &o, nil
}

// FindOrganizationByName returns a organization by name for a particular organization.
func (s *Service) FindOrganizationByName(ctx context.Context, n string) (*influxdb.Organization, error) {
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	var o *influxdb.Organization

	err := s.kv.View(ctx, func(tx Tx) error {
		org, err := s.findOrganizationByName(ctx, tx, n)
		if err != nil {
			return err
		}
		o = org
		return nil
	})

	return o, err
}

func (s *Service) findOrganizationByName(ctx context.Context, tx Tx, n string) (*influxdb.Organization, error) {
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	b, err := tx.Bucket(organizationIndex)
	if err != nil {
		return nil, err
	}

	o, err := b.Get(organizationIndexKey(n))
	if IsNotFound(err) {
		return nil, &influxdb.Error{
			Code: influxdb.ENotFound,
			Msg:  fmt.Sprintf("organization name \"%s\" not found", n),
		}
	}

	if err != nil {
		return nil, err
	}

	var id influxdb.ID
	if err := id.Decode(o); err != nil {
		return nil, &influxdb.Error{
			Code: influxdb.EInvalid,
			Err:  err,
		}
	}
	return s.findOrganizationByID(ctx, tx, id)
}

// FindOrganization retrives a organization using an arbitrary organization filter.
// Filters using ID, or Name should be efficient.
// Other filters will do a linear scan across organizations until it finds a match.
func (s *Service) FindOrganization(ctx context.Context, filter influxdb.OrganizationFilter) (*influxdb.Organization, error) {
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	var o *influxdb.Organization

	err := s.kv.View(ctx, func(tx Tx) error {
		org, err := s.findOrganization(ctx, tx, filter)
		if err != nil {
			return err
		}
		o = org
		return nil
	})

	return o, err

}

func (s *Service) findOrganization(ctx context.Context, tx Tx, filter influxdb.OrganizationFilter) (*influxdb.Organization, error) {
	if filter.ID != nil {
		return s.findOrganizationByID(ctx, tx, *filter.ID)
	}

	if filter.Name != nil {
		return s.findOrganizationByName(ctx, tx, *filter.Name)
	}

	// If name and ID are not set, then, this is an invalid usage of the API.
	return nil, influxdb.ErrInvalidOrgFilter
}

func filterOrganizationsFn(filter influxdb.OrganizationFilter) func(o *influxdb.Organization) bool {
	if filter.ID != nil {
		return func(o *influxdb.Organization) bool {
			return o.ID == *filter.ID
		}
	}

	if filter.Name != nil {
		return func(o *influxdb.Organization) bool {
			return o.Name == *filter.Name
		}
	}

	return func(o *influxdb.Organization) bool { return true }
}

// FindOrganizations retrives all organizations that match an arbitrary organization filter.
// Filters using ID, or Name should be efficient.
// Other filters will do a linear scan across all organizations searching for a match.
func (s *Service) FindOrganizations(ctx context.Context, filter influxdb.OrganizationFilter, opt ...influxdb.FindOptions) ([]*influxdb.Organization, int, error) {
	if filter.ID != nil {
		o, err := s.FindOrganizationByID(ctx, *filter.ID)
		if err != nil {
			return nil, 0, &influxdb.Error{
				Err: err,
			}
		}

		return []*influxdb.Organization{o}, 1, nil
	}

	if filter.Name != nil {
		o, err := s.FindOrganizationByName(ctx, *filter.Name)
		if err != nil {
			return nil, 0, &influxdb.Error{
				Err: err,
			}
		}

		return []*influxdb.Organization{o}, 1, nil
	}

	os := []*influxdb.Organization{}

	if filter.UserID != nil {
		// find urms for orgs with this user
		urms, _, err := s.FindUserResourceMappings(ctx, influxdb.UserResourceMappingFilter{
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
				os = append(os, o)
			}
		}

		return os, len(os), nil
	}

	filterFn := filterOrganizationsFn(filter)
	err := s.kv.View(ctx, func(tx Tx) error {
		return forEachOrganization(ctx, tx, func(o *influxdb.Organization) bool {
			if filterFn(o) {
				os = append(os, o)
			}
			return true
		})
	})

	if err != nil {
		return nil, 0, &influxdb.Error{
			Err: err,
		}
	}

	return os, len(os), nil
}

// CreateOrganization creates a influxdb organization and sets b.ID.
func (s *Service) CreateOrganization(ctx context.Context, o *influxdb.Organization) error {
	return s.kv.Update(ctx, func(tx Tx) error {
		if err := s.createOrganization(ctx, tx, o); err != nil {
			return err
		}

		// Attempt to add user as owner of organization, if that is not possible allow the
		// organization to be created anyways.
		if err := s.addOrgOwner(ctx, tx, o.ID); err != nil {
			s.log.Info("Failed to make user owner of organization", zap.Error(err))
		}

		return s.createSystemBuckets(ctx, tx, o)
	})
}

// addOrgOwner attempts to create a user resource mapping for the user on the
// authorizer found on context. If no authorizer is found on context if returns an error.
func (s *Service) addOrgOwner(ctx context.Context, tx Tx, orgID influxdb.ID) error {
	return s.addResourceOwner(ctx, tx, influxdb.OrgsResourceType, orgID)
}

// CreateOrganizationTx is used when importing kv as a library
func (s *Service) CreateOrganizationTx(ctx context.Context, tx Tx, o *influxdb.Organization) (err error) {
	return s.createOrganization(ctx, tx, o)
}

func (s *Service) createOrganization(ctx context.Context, tx Tx, o *influxdb.Organization) (err error) {
	if err := s.validOrganizationName(ctx, tx, o); err != nil {
		return err
	}

	if o.ID, err = s.generateOrgID(ctx, tx); err != nil {
		return err
	}
	o.CreatedAt = s.Now()
	o.UpdatedAt = s.Now()
	if err := s.appendOrganizationEventToLog(ctx, tx, o.ID, organizationCreatedEvent); err != nil {
		return &influxdb.Error{
			Err: err,
		}
	}

	v, err := json.Marshal(o)
	if err != nil {
		return influxdb.ErrInternalOrgServiceError(influxdb.OpCreateOrganization, err)
	}
	if err := s.putOrganization(ctx, tx, o, v); err != nil {
		return &influxdb.Error{
			Err: err,
		}
	}

	uid, _ := icontext.GetUserID(ctx)
	return s.audit.Log(resource.Change{
		Type:           resource.Create,
		ResourceID:     o.ID,
		ResourceType:   influxdb.OrgsResourceType,
		OrganizationID: o.ID,
		UserID:         uid,
		ResourceBody:   v,
		Time:           time.Now(),
	})
}

func (s *Service) generateOrgID(ctx context.Context, tx Tx) (influxdb.ID, error) {
	return s.generateSafeID(ctx, tx, organizationBucket, s.OrgIDs)
}

// PutOrganization will put a organization without setting an ID.
func (s *Service) PutOrganization(ctx context.Context, o *influxdb.Organization) error {
	return s.kv.Update(ctx, func(tx Tx) error {
		v, err := json.Marshal(o)
		if err != nil {
			return influxdb.ErrInternalOrgServiceError(influxdb.OpPutOrganization, err)
		}

		if err := s.putOrganization(ctx, tx, o, v); err != nil {
			return err
		}

		uid, _ := icontext.GetUserID(ctx)
		return s.audit.Log(resource.Change{
			Type:           resource.Put,
			ResourceID:     o.ID,
			ResourceType:   influxdb.OrgsResourceType,
			OrganizationID: o.ID,
			UserID:         uid,
			ResourceBody:   v,
			Time:           time.Now(),
		})
	})
}

func (s *Service) putOrganization(ctx context.Context, tx Tx, o *influxdb.Organization, v []byte) error {
	encodedID, err := o.ID.Encode()
	if err != nil {
		return &influxdb.Error{
			Code: influxdb.EInvalid,
			Err:  err,
		}
	}

	idx, err := tx.Bucket(organizationIndex)
	if err != nil {
		return err
	}

	if err := idx.Put(organizationIndexKey(o.Name), encodedID); err != nil {
		return &influxdb.Error{
			Err: err,
		}
	}

	b, err := tx.Bucket(organizationBucket)
	if err != nil {
		return err
	}

	if err = b.Put(encodedID, v); err != nil {
		return &influxdb.Error{
			Err: err,
		}
	}
	return nil
}

func organizationIndexKey(n string) []byte {
	return []byte(n)
}

// forEachOrganization will iterate through all organizations while fn returns true.
func forEachOrganization(ctx context.Context, tx Tx, fn func(*influxdb.Organization) bool) error {
	b, err := tx.Bucket(organizationBucket)
	if err != nil {
		return err
	}

	cur, err := b.ForwardCursor(nil)
	if err != nil {
		return err
	}

	for k, v := cur.Next(); k != nil; k, v = cur.Next() {
		o := &influxdb.Organization{}
		if err := json.Unmarshal(v, o); err != nil {
			return err
		}
		if !fn(o) {
			break
		}
	}

	return nil
}

func (s *Service) validOrganizationName(ctx context.Context, tx Tx, o *influxdb.Organization) error {
	if o.Name = strings.TrimSpace(o.Name); o.Name == "" {
		return influxdb.ErrOrgNameisEmpty
	}
	key := organizationIndexKey(o.Name)

	// if the name is not unique across all organizations, then, do not
	// allow creation.
	err := s.unique(ctx, tx, organizationIndex, key)
	if err == NotUniqueError {
		return OrgAlreadyExistsError(o)
	}
	return err
}

// UpdateOrganization updates a organization according the parameters set on upd.
func (s *Service) UpdateOrganization(ctx context.Context, id influxdb.ID, upd influxdb.OrganizationUpdate) (*influxdb.Organization, error) {
	var o *influxdb.Organization
	err := s.kv.Update(ctx, func(tx Tx) error {
		org, pe := s.updateOrganization(ctx, tx, id, upd)
		if pe != nil {
			return &influxdb.Error{
				Err: pe,
			}
		}
		o = org
		return nil
	})

	return o, err
}

func (s *Service) updateOrganization(ctx context.Context, tx Tx, id influxdb.ID, upd influxdb.OrganizationUpdate) (*influxdb.Organization, error) {
	o, pe := s.findOrganizationByID(ctx, tx, id)
	if pe != nil {
		return nil, pe
	}

	if upd.Name != nil {
		// Organizations are indexed by name and so the organization index must be pruned
		// when name is modified.
		idx, err := tx.Bucket(organizationIndex)
		if err != nil {
			return nil, err
		}
		if err := idx.Delete(organizationIndexKey(o.Name)); err != nil {
			return nil, &influxdb.Error{
				Err: err,
			}
		}
		o.Name = *upd.Name
		if err := s.validOrganizationName(ctx, tx, o); err != nil {
			return nil, err
		}
	}

	if upd.Description != nil {
		o.Description = *upd.Description
	}

	o.UpdatedAt = s.Now()

	if err := s.appendOrganizationEventToLog(ctx, tx, o.ID, organizationUpdatedEvent); err != nil {
		return nil, &influxdb.Error{
			Err: err,
		}
	}

	v, err := json.Marshal(o)
	if err != nil {
		return nil, influxdb.ErrInternalOrgServiceError(influxdb.OpUpdateOrganization, err)
	}
	if pe := s.putOrganization(ctx, tx, o, v); pe != nil {
		return nil, pe
	}

	uid, _ := icontext.GetUserID(ctx)
	if err := s.audit.Log(resource.Change{
		Type:           resource.Update,
		ResourceID:     o.ID,
		ResourceType:   influxdb.OrgsResourceType,
		OrganizationID: o.ID,
		UserID:         uid,
		ResourceBody:   v,
		Time:           time.Now(),
	}); err != nil {
		return nil, &influxdb.Error{
			Err: err,
		}
	}

	return o, nil
}

func (s *Service) deleteOrganizationsBuckets(ctx context.Context, tx Tx, id influxdb.ID) error {
	filter := influxdb.BucketFilter{
		OrganizationID: &id,
	}
	bs, err := s.findBuckets(ctx, tx, filter)
	if err != nil {
		return err
	}
	for _, b := range bs {
		if err := s.deleteBucket(ctx, tx, b.ID); err != nil {
			s.log.Warn("Bucket was not deleted", zap.Stringer("bucketID", b.ID), zap.Stringer("orgID", b.OrgID))
		}
	}
	return nil
}

// DeleteOrganization deletes a organization and prunes it from the index.
func (s *Service) DeleteOrganization(ctx context.Context, id influxdb.ID) error {
	err := s.kv.Update(ctx, func(tx Tx) error {
		if err := s.deleteOrganizationsBuckets(ctx, tx, id); err != nil {
			return err
		}
		if pe := s.deleteOrganization(ctx, tx, id); pe != nil {
			return pe
		}

		uid, _ := icontext.GetUserID(ctx)
		return s.audit.Log(resource.Change{
			Type:           resource.Delete,
			ResourceID:     id,
			ResourceType:   influxdb.OrgsResourceType,
			OrganizationID: id,
			UserID:         uid,
			Time:           time.Now(),
		})
	})
	if err != nil {
		return &influxdb.Error{
			Err: err,
		}
	}
	return nil
}

func (s *Service) deleteOrganization(ctx context.Context, tx Tx, id influxdb.ID) error {
	o, pe := s.findOrganizationByID(ctx, tx, id)
	if pe != nil {
		return pe
	}

	idx, err := tx.Bucket(organizationIndex)
	if err != nil {
		return err
	}

	if err := idx.Delete(organizationIndexKey(o.Name)); err != nil {
		return &influxdb.Error{
			Err: err,
		}
	}
	encodedID, err := id.Encode()
	if err != nil {
		return &influxdb.Error{
			Code: influxdb.EInvalid,
			Err:  err,
		}
	}

	b, err := tx.Bucket(organizationBucket)
	if err != nil {
		return err
	}

	if err = b.Delete(encodedID); err != nil {
		return &influxdb.Error{
			Err: err,
		}
	}

	return nil
}

//func (s *Service) deleteOrganizationsBuckets(ctx context.Context, tx Tx, id influxdb.ID) error {
//	filter := influxdb.BucketFilter{
//		OrganizationID: &id,
//	}
//	bs, pe := s.findBuckets(ctx, tx, filter)
//	if pe != nil {
//		return pe
//	}
//	for _, b := range bs {
//		if pe := s.deleteBucket(ctx, tx, b.ID); pe != nil {
//			return pe
//		}
//	}
//	return nil
//}

// GetOrganizationOperationLog retrieves a organization operation log.
func (s *Service) GetOrganizationOperationLog(ctx context.Context, id influxdb.ID, opts influxdb.FindOptions) ([]*influxdb.OperationLogEntry, int, error) {
	// TODO(desa): might be worthwhile to allocate a slice of size opts.Limit
	log := []*influxdb.OperationLogEntry{}

	err := s.kv.View(ctx, func(tx Tx) error {
		key, err := encodeOrganizationOperationLogKey(id)
		if err != nil {
			return err
		}

		return s.forEachLogEntry(ctx, tx, key, opts, func(v []byte, t time.Time) error {
			e := &influxdb.OperationLogEntry{}
			if err := json.Unmarshal(v, e); err != nil {
				return err
			}
			e.Time = t

			log = append(log, e)

			return nil
		})
	})

	if err != nil && err != errKeyValueLogBoundsNotFound {
		return nil, 0, err
	}

	return log, len(log), nil
}

// TODO(desa): what do we want these to be?
const (
	organizationCreatedEvent = "Organization Created"
	organizationUpdatedEvent = "Organization Updated"
)

const orgOperationLogKeyPrefix = "org"

func encodeOrganizationOperationLogKey(id influxdb.ID) ([]byte, error) {
	buf, err := id.Encode()
	if err != nil {
		return nil, err
	}
	return append([]byte(orgOperationLogKeyPrefix), buf...), nil
}

func (s *Service) appendOrganizationEventToLog(ctx context.Context, tx Tx, id influxdb.ID, st string) error {
	e := &influxdb.OperationLogEntry{
		Description: st,
	}
	// TODO(desa): this is fragile and non explicit since it requires an authorizer to be on context. It should be
	//             replaced with a higher level transaction so that adding to the log can take place in the http handler
	//             where the organizationID will exist explicitly.
	a, err := icontext.GetAuthorizer(ctx)
	if err == nil {
		// Add the organization to the log if you can, but don't error if its not there.
		e.UserID = a.GetUserID()
	}

	v, err := json.Marshal(e)
	if err != nil {
		return err
	}

	k, err := encodeOrganizationOperationLogKey(id)
	if err != nil {
		return err
	}

	return s.addLogEntry(ctx, tx, k, v, s.Now())
}

// FindResourceOrganizationID is used to find the organization that a resource belongs to five the id of a resource and a resource type.
func (s *Service) FindResourceOrganizationID(ctx context.Context, rt influxdb.ResourceType, id influxdb.ID) (influxdb.ID, error) {
	switch rt {
	case influxdb.AuthorizationsResourceType:
		r, err := s.FindAuthorizationByID(ctx, id)
		if err != nil {
			return influxdb.InvalidID(), err
		}
		return r.OrgID, nil
	case influxdb.BucketsResourceType:
		r, err := s.FindBucketByID(ctx, id)
		if err != nil {
			return influxdb.InvalidID(), err
		}
		return r.OrgID, nil
	case influxdb.OrgsResourceType:
		r, err := s.FindOrganizationByID(ctx, id)
		if err != nil {
			return influxdb.InvalidID(), err
		}
		return r.ID, nil
	case influxdb.DashboardsResourceType:
		r, err := s.FindDashboardByID(ctx, id)
		if err != nil {
			return influxdb.InvalidID(), err
		}
		return r.OrganizationID, nil
	case influxdb.SourcesResourceType:
		r, err := s.FindSourceByID(ctx, id)
		if err != nil {
			return influxdb.InvalidID(), err
		}
		return r.OrganizationID, nil
	case influxdb.TasksResourceType:
		r, err := s.FindTaskByID(ctx, id)
		if err != nil {
			return influxdb.InvalidID(), err
		}
		return r.OrganizationID, nil
	case influxdb.TelegrafsResourceType:
		r, err := s.FindTelegrafConfigByID(ctx, id)
		if err != nil {
			return influxdb.InvalidID(), err
		}
		return r.OrgID, nil
	case influxdb.VariablesResourceType:
		r, err := s.FindVariableByID(ctx, id)
		if err != nil {
			return influxdb.InvalidID(), err
		}
		return r.OrganizationID, nil
	case influxdb.ScraperResourceType:
		r, err := s.GetTargetByID(ctx, id)
		if err != nil {
			return influxdb.InvalidID(), err
		}
		return r.OrgID, nil
	case influxdb.ChecksResourceType:
		r, err := s.FindCheckByID(ctx, id)
		if err != nil {
			return influxdb.InvalidID(), err
		}
		return r.GetOrgID(), nil
	case influxdb.NotificationEndpointResourceType:
		r, err := s.FindNotificationEndpointByID(ctx, id)
		if err != nil {
			return influxdb.InvalidID(), err
		}
		return r.GetOrgID(), nil
	case influxdb.NotificationRuleResourceType:
		r, err := s.FindNotificationRuleByID(ctx, id)
		if err != nil {
			return influxdb.InvalidID(), err
		}
		return r.GetOrgID(), nil
	}

	return influxdb.InvalidID(), &influxdb.Error{
		Msg: fmt.Sprintf("unsupported resource type %s", rt),
	}
}

// OrgAlreadyExistsError is used when creating a new organization with
// a name that has already been used. Organization names must be unique.
func OrgAlreadyExistsError(o *influxdb.Organization) error {
	return &influxdb.Error{
		Code: influxdb.EConflict,
		Msg:  fmt.Sprintf("organization with name %s already exists", o.Name),
	}
}

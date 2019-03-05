package kv

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/opentracing/opentracing-go"
	"go.uber.org/zap"

	"github.com/influxdata/influxdb"
	icontext "github.com/influxdata/influxdb/context"
)

var (
	organizationBucket = []byte("organizationsv1")
	organizationIndex  = []byte("organizationindexv1")
)

var _ influxdb.OrganizationService = (*Service)(nil)
var _ influxdb.OrganizationOperationLogService = (*Service)(nil)

func (s *Service) initializeOrgs(ctx context.Context, tx Tx) error {
	if _, err := tx.Bucket(organizationBucket); err != nil {
		return err
	}
	if _, err := tx.Bucket(organizationIndex); err != nil {
		return err
	}
	return nil
}

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
	span, _ := opentracing.StartSpanFromContext(ctx, "Service.findOrganizationByID")
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
	span, ctx := opentracing.StartSpanFromContext(ctx, "Service.FindOrganizationByName")
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
	span, ctx := opentracing.StartSpanFromContext(ctx, "Service.findOrganizationByName")
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
	if filter.ID != nil {
		return s.FindOrganizationByID(ctx, *filter.ID)
	}

	if filter.Name != nil {
		return s.FindOrganizationByName(ctx, *filter.Name)
	}

	// If name and ID are not set, then, this is an invalid usage of the API.
	return nil, &influxdb.Error{
		Code: influxdb.EInvalid,
		Msg:  "no filter parameters provided",
	}
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
			s.Logger.Info("failed to make user owner of organization", zap.Error(err))
		}

		return nil
	})
}

// addOrgOwner attempts to create a user resource mapping for the user on the
// authorizer found on context. If no authorizer is found on context if returns an error.
func (s *Service) addOrgOwner(ctx context.Context, tx Tx, orgID influxdb.ID) error {
	return s.addResourceOwner(ctx, tx, influxdb.OrgsResourceType, orgID)
}

func (s *Service) createOrganization(ctx context.Context, tx Tx, o *influxdb.Organization) error {
	if err := s.uniqueOrganizationName(ctx, tx, o); err != nil {
		return err
	}

	o.ID = s.IDGenerator.ID()
	if err := s.appendOrganizationEventToLog(ctx, tx, o.ID, organizationCreatedEvent); err != nil {
		return &influxdb.Error{
			Err: err,
		}
	}

	if err := s.putOrganization(ctx, tx, o); err != nil {
		return &influxdb.Error{
			Err: err,
		}
	}
	return nil
}

// PutOrganization will put a organization without setting an ID.
func (s *Service) PutOrganization(ctx context.Context, o *influxdb.Organization) error {
	var err error
	return s.kv.Update(ctx, func(tx Tx) error {
		if pe := s.putOrganization(ctx, tx, o); pe != nil {
			err = pe
		}
		return err
	})
}

func (s *Service) putOrganization(ctx context.Context, tx Tx, o *influxdb.Organization) error {
	v, err := json.Marshal(o)
	if err != nil {
		return &influxdb.Error{
			Err: err,
		}
	}
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

	cur, err := b.Cursor()
	if err != nil {
		return err
	}

	for k, v := cur.First(); k != nil; k, v = cur.Next() {
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

func (s *Service) uniqueOrganizationName(ctx context.Context, tx Tx, o *influxdb.Organization) error {
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
	}

	if err := s.appendOrganizationEventToLog(ctx, tx, o.ID, organizationUpdatedEvent); err != nil {
		return nil, &influxdb.Error{
			Err: err,
		}
	}

	if pe := s.putOrganization(ctx, tx, o); pe != nil {
		return nil, pe
	}

	return o, nil
}

// DeleteOrganization deletes a organization and prunes it from the index.
func (s *Service) DeleteOrganization(ctx context.Context, id influxdb.ID) error {
	err := s.kv.Update(ctx, func(tx Tx) error {
		//if pe := s.deleteOrganizationsBuckets(ctx, tx, id); pe != nil {
		//	return pe
		//}
		if pe := s.deleteOrganization(ctx, tx, id); pe != nil {
			return pe
		}
		return nil
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

	return s.addLogEntry(ctx, tx, k, v, s.time())
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
		return r.OrganizationID, nil
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
	case influxdb.TelegrafsResourceType:
		r, err := s.FindTelegrafConfigByID(ctx, id)
		if err != nil {
			return influxdb.InvalidID(), err
		}
		return r.OrganizationID, nil
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

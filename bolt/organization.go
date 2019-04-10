package bolt

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	bolt "github.com/coreos/bbolt"
	"github.com/influxdata/influxdb"
	influxdbcontext "github.com/influxdata/influxdb/context"
	"github.com/influxdata/influxdb/kit/tracing"
)

var (
	organizationBucket = []byte("organizationsv1")
	organizationIndex  = []byte("organizationindexv1")
)

var _ influxdb.OrganizationService = (*Client)(nil)
var _ influxdb.OrganizationOperationLogService = (*Client)(nil)

func (c *Client) initializeOrganizations(ctx context.Context, tx *bolt.Tx) error {
	if _, err := tx.CreateBucketIfNotExists([]byte(organizationBucket)); err != nil {
		return err
	}
	if _, err := tx.CreateBucketIfNotExists([]byte(organizationIndex)); err != nil {
		return err
	}
	return nil
}

// FindOrganizationByID retrieves a organization by id.
func (c *Client) FindOrganizationByID(ctx context.Context, id influxdb.ID) (*influxdb.Organization, error) {
	var o *influxdb.Organization
	err := c.db.View(func(tx *bolt.Tx) error {
		org, pe := c.findOrganizationByID(ctx, tx, id)
		if pe != nil {
			return &influxdb.Error{
				Op:  getOp(influxdb.OpFindOrganizationByID),
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

func (c *Client) findOrganizationByID(ctx context.Context, tx *bolt.Tx, id influxdb.ID) (*influxdb.Organization, *influxdb.Error) {
	span, _ := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	encodedID, err := id.Encode()
	if err != nil {
		return nil, &influxdb.Error{
			Code: influxdb.EInvalid,
			Err:  err,
		}
	}

	v := tx.Bucket(organizationBucket).Get(encodedID)
	if len(v) == 0 {
		return nil, &influxdb.Error{
			Code: influxdb.ENotFound,
			Msg:  "organization not found",
		}
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
func (c *Client) FindOrganizationByName(ctx context.Context, n string) (*influxdb.Organization, error) {
	var o *influxdb.Organization

	err := c.db.View(func(tx *bolt.Tx) error {
		org, pe := c.findOrganizationByName(ctx, tx, n)
		if pe != nil {
			return pe
		}
		o = org
		return nil
	})

	return o, err
}

func (c *Client) findOrganizationByName(ctx context.Context, tx *bolt.Tx, n string) (*influxdb.Organization, *influxdb.Error) {
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	o := tx.Bucket(organizationIndex).Get(organizationIndexKey(n))
	if o == nil {
		return nil, &influxdb.Error{
			Code: influxdb.ENotFound,
			Msg:  fmt.Sprintf("organization name \"%s\" not found", n),
		}
	}

	var id influxdb.ID
	if err := id.Decode(o); err != nil {
		return nil, &influxdb.Error{
			Code: influxdb.EInvalid,
			Err:  err,
		}
	}
	return c.findOrganizationByID(ctx, tx, id)
}

// FindOrganization retrives a organization using an arbitrary organization filter.
// Filters using ID, or Name should be efficient.
// Other filters will do a linear scan across organizations until it finds a match.
func (c *Client) FindOrganization(ctx context.Context, filter influxdb.OrganizationFilter) (*influxdb.Organization, error) {
	op := getOp(influxdb.OpFindOrganization)
	if filter.ID != nil {
		o, err := c.FindOrganizationByID(ctx, *filter.ID)
		if err != nil {
			return nil, &influxdb.Error{
				Err: err,
				Op:  op,
			}
		}
		return o, nil
	}

	if filter.Name != nil {
		o, err := c.FindOrganizationByName(ctx, *filter.Name)
		if err != nil {
			return nil, &influxdb.Error{
				Err: err,
				Op:  op,
			}
		}
		return o, nil
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
func (c *Client) FindOrganizations(ctx context.Context, filter influxdb.OrganizationFilter, opt ...influxdb.FindOptions) ([]*influxdb.Organization, int, error) {
	op := getOp(influxdb.OpFindOrganizations)
	if filter.ID != nil {
		o, err := c.FindOrganizationByID(ctx, *filter.ID)
		if err != nil {
			return nil, 0, &influxdb.Error{
				Err: err,
				Op:  op,
			}
		}

		return []*influxdb.Organization{o}, 1, nil
	}

	if filter.Name != nil {
		o, err := c.FindOrganizationByName(ctx, *filter.Name)
		if err != nil {
			return nil, 0, &influxdb.Error{
				Err: err,
				Op:  op,
			}
		}

		return []*influxdb.Organization{o}, 1, nil
	}

	os := []*influxdb.Organization{}
	filterFn := filterOrganizationsFn(filter)
	err := c.db.View(func(tx *bolt.Tx) error {
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
			Op:  op,
		}
	}

	return os, len(os), nil
}

// CreateOrganization creates a influxdb organization and sets b.ID.
func (c *Client) CreateOrganization(ctx context.Context, o *influxdb.Organization) error {
	op := getOp(influxdb.OpCreateOrganization)
	return c.db.Update(func(tx *bolt.Tx) error {
		unique := c.uniqueOrganizationName(ctx, tx, o)
		if !unique {
			return &influxdb.Error{
				Code: influxdb.EConflict,
				Op:   op,
				Msg:  fmt.Sprintf("organization with name %s already exists", o.Name),
			}
		}

		o.ID = c.IDGenerator.ID()
		if err := c.appendOrganizationEventToLog(ctx, tx, o.ID, organizationCreatedEvent); err != nil {
			return &influxdb.Error{
				Err: err,
				Op:  op,
			}
		}

		if err := c.putOrganization(ctx, tx, o); err != nil {
			return &influxdb.Error{
				Err: err,
				Op:  op,
			}
		}
		return nil
	})
}

// PutOrganization will put a organization without setting an ID.
func (c *Client) PutOrganization(ctx context.Context, o *influxdb.Organization) error {
	var err error
	return c.db.Update(func(tx *bolt.Tx) error {
		if pe := c.putOrganization(ctx, tx, o); pe != nil {
			err = pe
		}
		return err
	})
}

func (c *Client) putOrganization(ctx context.Context, tx *bolt.Tx, o *influxdb.Organization) *influxdb.Error {
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
	if err := tx.Bucket(organizationIndex).Put(organizationIndexKey(o.Name), encodedID); err != nil {
		return &influxdb.Error{
			Err: err,
		}
	}
	if err = tx.Bucket(organizationBucket).Put(encodedID, v); err != nil {
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
func forEachOrganization(ctx context.Context, tx *bolt.Tx, fn func(*influxdb.Organization) bool) error {
	cur := tx.Bucket(organizationBucket).Cursor()
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

func (c *Client) uniqueOrganizationName(ctx context.Context, tx *bolt.Tx, o *influxdb.Organization) bool {
	v := tx.Bucket(organizationIndex).Get(organizationIndexKey(o.Name))
	return len(v) == 0
}

// UpdateOrganization updates a organization according the parameters set on upd.
func (c *Client) UpdateOrganization(ctx context.Context, id influxdb.ID, upd influxdb.OrganizationUpdate) (*influxdb.Organization, error) {
	var o *influxdb.Organization
	err := c.db.Update(func(tx *bolt.Tx) error {
		org, pe := c.updateOrganization(ctx, tx, id, upd)
		if pe != nil {
			return &influxdb.Error{
				Err: pe,
				Op:  getOp(influxdb.OpUpdateOrganization),
			}
		}
		o = org
		return nil
	})

	return o, err
}

func (c *Client) updateOrganization(ctx context.Context, tx *bolt.Tx, id influxdb.ID, upd influxdb.OrganizationUpdate) (*influxdb.Organization, *influxdb.Error) {
	o, pe := c.findOrganizationByID(ctx, tx, id)
	if pe != nil {
		return nil, pe
	}

	if upd.Name != nil {
		// Organizations are indexed by name and so the organization index must be pruned
		// when name is modified.
		if err := tx.Bucket(organizationIndex).Delete(organizationIndexKey(o.Name)); err != nil {
			return nil, &influxdb.Error{
				Err: err,
			}
		}
		o.Name = *upd.Name
	}

	if err := c.appendOrganizationEventToLog(ctx, tx, o.ID, organizationUpdatedEvent); err != nil {
		return nil, &influxdb.Error{
			Err: err,
		}
	}

	if pe := c.putOrganization(ctx, tx, o); pe != nil {
		return nil, pe
	}

	return o, nil
}

// DeleteOrganization deletes a organization and prunes it from the index.
func (c *Client) DeleteOrganization(ctx context.Context, id influxdb.ID) error {
	err := c.db.Update(func(tx *bolt.Tx) error {
		if pe := c.deleteOrganizationsBuckets(ctx, tx, id); pe != nil {
			return pe
		}
		if pe := c.deleteOrganization(ctx, tx, id); pe != nil {
			return pe
		}
		return nil
	})
	if err != nil {
		return &influxdb.Error{
			Op:  getOp(influxdb.OpDeleteOrganization),
			Err: err,
		}
	}
	return nil
}

func (c *Client) deleteOrganization(ctx context.Context, tx *bolt.Tx, id influxdb.ID) *influxdb.Error {
	o, pe := c.findOrganizationByID(ctx, tx, id)
	if pe != nil {
		return pe
	}
	if err := tx.Bucket(organizationIndex).Delete(organizationIndexKey(o.Name)); err != nil {
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
	if err = tx.Bucket(organizationBucket).Delete(encodedID); err != nil {
		return &influxdb.Error{
			Err: err,
		}
	}
	return nil
}

func (c *Client) deleteOrganizationsBuckets(ctx context.Context, tx *bolt.Tx, id influxdb.ID) *influxdb.Error {
	filter := influxdb.BucketFilter{
		OrganizationID: &id,
	}
	bs, pe := c.findBuckets(ctx, tx, filter)
	if pe != nil {
		return pe
	}
	for _, b := range bs {
		if pe := c.deleteBucket(ctx, tx, b.ID); pe != nil {
			return pe
		}
	}
	return nil
}

// GeOrganizationOperationLog retrieves a organization operation log.
func (c *Client) GetOrganizationOperationLog(ctx context.Context, id influxdb.ID, opts influxdb.FindOptions) ([]*influxdb.OperationLogEntry, int, error) {
	// TODO(desa): might be worthwhile to allocate a slice of size opts.Limit
	log := []*influxdb.OperationLogEntry{}

	err := c.db.View(func(tx *bolt.Tx) error {
		key, err := encodeBucketOperationLogKey(id)
		if err != nil {
			return err
		}

		return c.forEachLogEntry(ctx, tx, key, opts, func(v []byte, t time.Time) error {
			e := &influxdb.OperationLogEntry{}
			if err := json.Unmarshal(v, e); err != nil {
				return err
			}
			e.Time = t

			log = append(log, e)

			return nil
		})
	})

	if err != nil {
		return nil, 0, err
	}

	return log, len(log), nil
}

// TODO(desa): what do we want these to be?
const (
	organizationCreatedEvent = "Organization Created"
	organizationUpdatedEvent = "Organization Updated"
)

func encodeOrganizationOperationLogKey(id influxdb.ID) ([]byte, error) {
	buf, err := id.Encode()
	if err != nil {
		return nil, err
	}
	return append([]byte(bucketOperationLogKeyPrefix), buf...), nil
}

func (c *Client) appendOrganizationEventToLog(ctx context.Context, tx *bolt.Tx, id influxdb.ID, s string) error {
	e := &influxdb.OperationLogEntry{
		Description: s,
	}
	// TODO(desa): this is fragile and non explicit since it requires an authorizer to be on context. It should be
	//             replaced with a higher level transaction so that adding to the log can take place in the http handler
	//             where the organizationID will exist explicitly.
	a, err := influxdbcontext.GetAuthorizer(ctx)
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

	return c.addLogEntry(ctx, tx, k, v, c.time())
}

func (c *Client) FindResourceOrganizationID(ctx context.Context, rt influxdb.ResourceType, id influxdb.ID) (influxdb.ID, error) {
	switch rt {
	case influxdb.AuthorizationsResourceType:
		r, err := c.FindAuthorizationByID(ctx, id)
		if err != nil {
			return influxdb.InvalidID(), err
		}
		return r.OrgID, nil
	case influxdb.BucketsResourceType:
		r, err := c.FindBucketByID(ctx, id)
		if err != nil {
			return influxdb.InvalidID(), err
		}
		return r.OrgID, nil
	case influxdb.DashboardsResourceType:
		r, err := c.FindDashboardByID(ctx, id)
		if err != nil {
			return influxdb.InvalidID(), err
		}
		return r.OrganizationID, nil
	case influxdb.OrgsResourceType:
		r, err := c.FindOrganizationByID(ctx, id)
		if err != nil {
			return influxdb.InvalidID(), err
		}
		return r.ID, nil
	case influxdb.SourcesResourceType:
		r, err := c.FindSourceByID(ctx, id)
		if err != nil {
			return influxdb.InvalidID(), err
		}
		return r.OrganizationID, nil
	case influxdb.TelegrafsResourceType:
		r, err := c.FindTelegrafConfigByID(ctx, id)
		if err != nil {
			return influxdb.InvalidID(), err
		}
		return r.OrganizationID, nil
	case influxdb.VariablesResourceType:
		r, err := c.FindVariableByID(ctx, id)
		if err != nil {
			return influxdb.InvalidID(), err
		}
		return r.OrganizationID, nil
	case influxdb.ScraperResourceType:
		r, err := c.GetTargetByID(ctx, id)
		if err != nil {
			return influxdb.InvalidID(), err
		}
		return r.OrgID, nil
	}

	return influxdb.InvalidID(), &influxdb.Error{
		Msg: fmt.Sprintf("unsupported resource type %s", rt),
	}
}

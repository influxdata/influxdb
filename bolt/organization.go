package bolt

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/coreos/bbolt"
	"github.com/influxdata/platform"
	platformcontext "github.com/influxdata/platform/context"
)

var (
	organizationBucket = []byte("organizationsv1")
	organizationIndex  = []byte("organizationindexv1")
)

var _ platform.OrganizationService = (*Client)(nil)
var _ platform.OrganizationOperationLogService = (*Client)(nil)

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
func (c *Client) FindOrganizationByID(ctx context.Context, id platform.ID) (*platform.Organization, error) {
	var o *platform.Organization

	err := c.db.View(func(tx *bolt.Tx) error {
		org, err := c.findOrganizationByID(ctx, tx, id)
		if err != nil {
			return err
		}
		o = org
		return nil
	})

	if err != nil {
		return nil, err
	}

	return o, nil
}

func (c *Client) findOrganizationByID(ctx context.Context, tx *bolt.Tx, id platform.ID) (*platform.Organization, error) {
	encodedID, err := id.Encode()
	if err != nil {
		return nil, err
	}

	v := tx.Bucket(organizationBucket).Get(encodedID)
	if len(v) == 0 {
		// TODO: Make standard error
		return nil, fmt.Errorf("organization not found")
	}

	var o platform.Organization
	if err := json.Unmarshal(v, &o); err != nil {
		return nil, err
	}

	return &o, nil
}

// FindOrganizationByName returns a organization by name for a particular organization.
func (c *Client) FindOrganizationByName(ctx context.Context, n string) (*platform.Organization, error) {
	var o *platform.Organization

	err := c.db.View(func(tx *bolt.Tx) error {
		org, err := c.findOrganizationByName(ctx, tx, n)
		if err != nil {
			return err
		}
		o = org
		return nil
	})

	return o, err
}

func (c *Client) findOrganizationByName(ctx context.Context, tx *bolt.Tx, n string) (*platform.Organization, error) {
	o := tx.Bucket(organizationIndex).Get(organizationIndexKey(n))
	if o == nil {
		// TODO: Make standard error
		return nil, fmt.Errorf("organization not found")
	}

	var id platform.ID
	if err := id.Decode(o); err != nil {
		return nil, err
	}
	return c.findOrganizationByID(ctx, tx, id)
}

// FindOrganization retrives a organization using an arbitrary organization filter.
// Filters using ID, or Name should be efficient.
// Other filters will do a linear scan across organizations until it finds a match.
func (c *Client) FindOrganization(ctx context.Context, filter platform.OrganizationFilter) (*platform.Organization, error) {
	if filter.ID != nil {
		return c.FindOrganizationByID(ctx, *filter.ID)
	}

	if filter.Name != nil {
		return c.FindOrganizationByName(ctx, *filter.Name)
	}

	filterFn := filterOrganizationsFn(filter)

	var o *platform.Organization
	err := c.db.View(func(tx *bolt.Tx) error {
		return forEachOrganization(ctx, tx, func(org *platform.Organization) bool {
			if filterFn(org) {
				o = org
				return false
			}
			return true
		})
	})

	if err != nil {
		return nil, err
	}

	if o == nil {
		return nil, fmt.Errorf("organization not found")
	}

	return o, nil
}

func filterOrganizationsFn(filter platform.OrganizationFilter) func(o *platform.Organization) bool {
	if filter.ID != nil {
		return func(o *platform.Organization) bool {
			return o.ID == *filter.ID
		}
	}

	if filter.Name != nil {
		return func(o *platform.Organization) bool {
			return o.Name == *filter.Name
		}
	}

	return func(o *platform.Organization) bool { return true }
}

// FindOrganizations retrives all organizations that match an arbitrary organization filter.
// Filters using ID, or Name should be efficient.
// Other filters will do a linear scan across all organizations searching for a match.
func (c *Client) FindOrganizations(ctx context.Context, filter platform.OrganizationFilter, opt ...platform.FindOptions) ([]*platform.Organization, int, error) {
	if filter.ID != nil {
		o, err := c.FindOrganizationByID(ctx, *filter.ID)
		if err != nil {
			return nil, 0, err
		}

		return []*platform.Organization{o}, 1, nil
	}

	if filter.Name != nil {
		o, err := c.FindOrganizationByName(ctx, *filter.Name)
		if err != nil {
			return nil, 0, err
		}

		return []*platform.Organization{o}, 1, nil
	}

	os := []*platform.Organization{}
	filterFn := filterOrganizationsFn(filter)
	err := c.db.View(func(tx *bolt.Tx) error {
		return forEachOrganization(ctx, tx, func(o *platform.Organization) bool {
			if filterFn(o) {
				os = append(os, o)
			}
			return true
		})
	})

	if err != nil {
		return nil, 0, err
	}

	return os, len(os), nil
}

// CreateOrganization creates a platform organization and sets b.ID.
func (c *Client) CreateOrganization(ctx context.Context, o *platform.Organization) error {
	return c.db.Update(func(tx *bolt.Tx) error {
		unique := c.uniqueOrganizationName(ctx, tx, o)

		if !unique {
			// TODO: make standard error
			return fmt.Errorf("organization with name %s already exists", o.Name)
		}

		o.ID = c.IDGenerator.ID()
		if err := c.appendOrganizationEventToLog(ctx, tx, o.ID, organizationCreatedEvent); err != nil {
			return err
		}

		return c.putOrganization(ctx, tx, o)
	})
}

// PutOrganization will put a organization without setting an ID.
func (c *Client) PutOrganization(ctx context.Context, o *platform.Organization) error {
	return c.db.Update(func(tx *bolt.Tx) error {
		return c.putOrganization(ctx, tx, o)
	})
}

func (c *Client) putOrganization(ctx context.Context, tx *bolt.Tx, o *platform.Organization) error {
	v, err := json.Marshal(o)
	if err != nil {
		return err
	}
	encodedID, err := o.ID.Encode()
	if err != nil {
		return err
	}
	if err := tx.Bucket(organizationIndex).Put(organizationIndexKey(o.Name), encodedID); err != nil {
		return err
	}
	return tx.Bucket(organizationBucket).Put(encodedID, v)
}

func organizationIndexKey(n string) []byte {
	return []byte(n)
}

// forEachOrganization will iterate through all organizations while fn returns true.
func forEachOrganization(ctx context.Context, tx *bolt.Tx, fn func(*platform.Organization) bool) error {
	cur := tx.Bucket(organizationBucket).Cursor()
	for k, v := cur.First(); k != nil; k, v = cur.Next() {
		o := &platform.Organization{}
		if err := json.Unmarshal(v, o); err != nil {
			return err
		}
		if !fn(o) {
			break
		}
	}

	return nil
}

func (c *Client) uniqueOrganizationName(ctx context.Context, tx *bolt.Tx, o *platform.Organization) bool {
	v := tx.Bucket(organizationIndex).Get(organizationIndexKey(o.Name))
	return len(v) == 0
}

// UpdateOrganization updates a organization according the parameters set on upd.
func (c *Client) UpdateOrganization(ctx context.Context, id platform.ID, upd platform.OrganizationUpdate) (*platform.Organization, error) {
	var o *platform.Organization
	err := c.db.Update(func(tx *bolt.Tx) error {
		org, err := c.updateOrganization(ctx, tx, id, upd)
		if err != nil {
			return err
		}
		o = org
		return nil
	})

	return o, err
}

func (c *Client) updateOrganization(ctx context.Context, tx *bolt.Tx, id platform.ID, upd platform.OrganizationUpdate) (*platform.Organization, error) {
	o, err := c.findOrganizationByID(ctx, tx, id)
	if err != nil {
		return nil, err
	}

	if upd.Name != nil {
		// Organizations are indexed by name and so the organization index must be pruned
		// when name is modified.
		if err := tx.Bucket(organizationIndex).Delete(organizationIndexKey(o.Name)); err != nil {
			return nil, err
		}
		o.Name = *upd.Name
	}

	if err := c.appendOrganizationEventToLog(ctx, tx, o.ID, organizationUpdatedEvent); err != nil {
		return nil, err
	}

	if err := c.putOrganization(ctx, tx, o); err != nil {
		return nil, err
	}

	return o, nil
}

// DeleteOrganization deletes a organization and prunes it from the index.
func (c *Client) DeleteOrganization(ctx context.Context, id platform.ID) error {
	return c.db.Update(func(tx *bolt.Tx) error {
		if err := c.deleteOrganizationsBuckets(ctx, tx, id); err != nil {
			return err
		}
		return c.deleteOrganization(ctx, tx, id)
	})
}

func (c *Client) deleteOrganization(ctx context.Context, tx *bolt.Tx, id platform.ID) error {
	o, err := c.findOrganizationByID(ctx, tx, id)
	if err != nil {
		return err
	}
	if err := tx.Bucket(organizationIndex).Delete(organizationIndexKey(o.Name)); err != nil {
		return err
	}
	encodedID, err := id.Encode()
	if err != nil {
		return err
	}
	return tx.Bucket(organizationBucket).Delete(encodedID)
}

func (c *Client) deleteOrganizationsBuckets(ctx context.Context, tx *bolt.Tx, id platform.ID) error {
	filter := platform.BucketFilter{
		OrganizationID: &id,
	}
	bs, err := c.findBuckets(ctx, tx, filter)
	if err != nil {
		return err
	}
	for _, b := range bs {
		if err := c.deleteBucket(ctx, tx, b.ID); err != nil {
			return err
		}
	}
	return nil
}

// GeOrganizationOperationLog retrieves a organization operation log.
func (c *Client) GetOrganizationOperationLog(ctx context.Context, id platform.ID, opts platform.FindOptions) ([]*platform.OperationLogEntry, int, error) {
	// TODO(desa): might be worthwhile to allocate a slice of size opts.Limit
	log := []*platform.OperationLogEntry{}

	err := c.db.View(func(tx *bolt.Tx) error {
		key, err := encodeBucketOperationLogKey(id)
		if err != nil {
			return err
		}

		return c.forEachLogEntry(ctx, tx, key, opts, func(v []byte, t time.Time) error {
			e := &platform.OperationLogEntry{}
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

func encodeOrganizationOperationLogKey(id platform.ID) ([]byte, error) {
	buf, err := id.Encode()
	if err != nil {
		return nil, err
	}
	return append([]byte(bucketOperationLogKeyPrefix), buf...), nil
}

func (c *Client) appendOrganizationEventToLog(ctx context.Context, tx *bolt.Tx, id platform.ID, s string) error {
	e := &platform.OperationLogEntry{
		Description: s,
	}
	// TODO(desa): this is fragile and non explicit since it requires an authorizer to be on context. It should be
	//             replaced with a higher level transaction so that adding to the log can take place in the http handler
	//             where the organizationID will exist explicitly.
	a, err := platformcontext.GetAuthorizer(ctx)
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

package bolt

import (
	"context"
	"encoding/json"

	bolt "github.com/coreos/bbolt"
	influxdb "github.com/influxdata/influxdb"
)

var (
	scraperBucket = []byte("scraperv2")
)

var _ influxdb.ScraperTargetStoreService = (*Client)(nil)

func (c *Client) initializeScraperTargets(ctx context.Context, tx *bolt.Tx) error {
	if _, err := tx.CreateBucketIfNotExists([]byte(scraperBucket)); err != nil {
		return err
	}
	return nil
}

// ListTargets will list all scrape targets.
func (c *Client) ListTargets(ctx context.Context, filter influxdb.ScraperTargetFilter) (list []influxdb.ScraperTarget, err error) {
	list = make([]influxdb.ScraperTarget, 0)
	err = c.db.View(func(tx *bolt.Tx) (err error) {
		cur := tx.Bucket(scraperBucket).Cursor()
		for k, v := cur.First(); k != nil; k, v = cur.Next() {
			target := new(influxdb.ScraperTarget)
			if err = json.Unmarshal(v, target); err != nil {
				return err
			}
			if err != nil {
				return err
			}
			if filter.IDs != nil {
				if _, ok := filter.IDs[target.ID]; !ok {
					continue
				}
			}
			if filter.Name != nil && target.Name != *filter.Name {
				continue
			}
			if filter.Org != nil {
				o, err := c.findOrganizationByName(ctx, tx, *filter.Org)
				if err != nil {
					return err
				}
				if target.OrgID != o.ID {
					continue
				}
			}
			if filter.OrgID != nil {
				o, err := c.findOrganizationByID(ctx, tx, *filter.OrgID)
				if err != nil {
					return err
				}
				if target.OrgID != o.ID {
					continue
				}
			}
			list = append(list, *target)
		}
		return err
	})
	if err != nil {
		return nil, &influxdb.Error{
			Op:  getOp(influxdb.OpListTargets),
			Err: err,
		}
	}
	return list, err
}

// AddTarget add a new scraper target into storage.
func (c *Client) AddTarget(ctx context.Context, target *influxdb.ScraperTarget, userID influxdb.ID) (err error) {
	if !target.OrgID.Valid() {
		return &influxdb.Error{
			Code: influxdb.EInvalid,
			Msg:  "provided organization ID has invalid format",
			Op:   OpPrefix + influxdb.OpAddTarget,
		}
	}
	if !target.BucketID.Valid() {
		return &influxdb.Error{
			Code: influxdb.EInvalid,
			Msg:  "provided bucket ID has invalid format",
			Op:   OpPrefix + influxdb.OpAddTarget,
		}
	}
	err = c.db.Update(func(tx *bolt.Tx) error {
		target.ID = c.IDGenerator.ID()
		if err := c.putTarget(ctx, tx, target); err != nil {
			return err
		}
		urm := &influxdb.UserResourceMapping{
			ResourceID:   target.ID,
			UserID:       userID,
			UserType:     influxdb.Owner,
			ResourceType: influxdb.ScraperResourceType,
		}
		return c.createUserResourceMapping(ctx, tx, urm)
	})
	if err != nil {
		return &influxdb.Error{
			Err: err,
			Op:  OpPrefix + influxdb.OpAddTarget,
		}
	}
	return nil
}

// RemoveTarget removes a scraper target from the bucket.
func (c *Client) RemoveTarget(ctx context.Context, id influxdb.ID) error {
	err := c.db.Update(func(tx *bolt.Tx) error {
		_, pe := c.findTargetByID(ctx, tx, id)
		if pe != nil {
			return pe
		}
		encID, err := id.Encode()
		if err != nil {
			return &influxdb.Error{
				Code: influxdb.EInvalid,
				Err:  err,
			}
		}
		if err = tx.Bucket(scraperBucket).Delete(encID); err != nil {
			return nil
		}
		return c.deleteUserResourceMappings(ctx, tx, influxdb.UserResourceMappingFilter{
			ResourceID:   id,
			ResourceType: influxdb.ScraperResourceType,
		})
	})
	if err != nil {
		return &influxdb.Error{
			Err: err,
			Op:  OpPrefix + influxdb.OpRemoveTarget,
		}
	}
	return nil
}

// UpdateTarget updates a scraper target.
func (c *Client) UpdateTarget(ctx context.Context, update *influxdb.ScraperTarget, userID influxdb.ID) (target *influxdb.ScraperTarget, err error) {
	op := getOp(influxdb.OpUpdateTarget)
	var pe *influxdb.Error
	if !update.ID.Valid() {
		return nil, &influxdb.Error{
			Code: influxdb.EInvalid,
			Op:   op,
			Msg:  "provided scraper target ID has invalid format",
		}
	}
	err = c.db.Update(func(tx *bolt.Tx) error {
		target, pe = c.findTargetByID(ctx, tx, update.ID)
		if pe != nil {
			return pe
		}
		if !update.BucketID.Valid() {
			update.BucketID = target.BucketID
		}
		if !update.OrgID.Valid() {
			update.OrgID = target.OrgID
		}
		target = update
		return c.putTarget(ctx, tx, target)
	})

	if err != nil {
		return nil, &influxdb.Error{
			Op:  op,
			Err: err,
		}
	}

	return target, nil
}

// GetTargetByID retrieves a scraper target by id.
func (c *Client) GetTargetByID(ctx context.Context, id influxdb.ID) (target *influxdb.ScraperTarget, err error) {
	var pe *influxdb.Error
	err = c.db.View(func(tx *bolt.Tx) error {
		target, pe = c.findTargetByID(ctx, tx, id)
		if pe != nil {
			return &influxdb.Error{
				Op:  getOp(influxdb.OpGetTargetByID),
				Err: pe,
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return target, nil
}

func (c *Client) findTargetByID(ctx context.Context, tx *bolt.Tx, id influxdb.ID) (target *influxdb.ScraperTarget, pe *influxdb.Error) {
	target = new(influxdb.ScraperTarget)
	encID, err := id.Encode()
	if err != nil {
		return nil, &influxdb.Error{
			Err: err,
		}
	}
	v := tx.Bucket(scraperBucket).Get(encID)
	if len(v) == 0 {
		return nil, &influxdb.Error{
			Code: influxdb.ENotFound,
			Msg:  "scraper target is not found",
		}
	}

	if err := json.Unmarshal(v, target); err != nil {
		return nil, &influxdb.Error{
			Err: err,
		}
	}
	return target, nil
}

func (c *Client) putTarget(ctx context.Context, tx *bolt.Tx, target *influxdb.ScraperTarget) (err error) {
	v, err := json.Marshal(target)
	if err != nil {
		return err
	}
	encID, err := target.ID.Encode()
	if err != nil {
		return err
	}
	return tx.Bucket(scraperBucket).Put(encID, v)
}

// PutTarget will put a scraper target without setting an ID.
func (c *Client) PutTarget(ctx context.Context, target *influxdb.ScraperTarget) error {
	return c.db.Update(func(tx *bolt.Tx) error {
		return c.putTarget(ctx, tx, target)
	})
}

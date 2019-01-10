package bolt

import (
	"context"
	"encoding/json"

	bolt "github.com/coreos/bbolt"
	platform "github.com/influxdata/influxdb"
)

var (
	scraperBucket = []byte("scraperv2")
)

var _ platform.ScraperTargetStoreService = (*Client)(nil)

func (c *Client) initializeScraperTargets(ctx context.Context, tx *bolt.Tx) error {
	if _, err := tx.CreateBucketIfNotExists([]byte(scraperBucket)); err != nil {
		return err
	}
	return nil
}

// ListTargets will list all scrape targets.
func (c *Client) ListTargets(ctx context.Context) (list []platform.ScraperTarget, err error) {
	list = make([]platform.ScraperTarget, 0)
	err = c.db.View(func(tx *bolt.Tx) (err error) {
		cur := tx.Bucket(scraperBucket).Cursor()
		for k, v := cur.First(); k != nil; k, v = cur.Next() {
			target := new(platform.ScraperTarget)
			if err = json.Unmarshal(v, target); err != nil {
				return err
			}
			list = append(list, *target)
		}
		return err
	})
	if err != nil {
		return nil, &platform.Error{
			Op:  getOp(platform.OpListTargets),
			Err: err,
		}
	}
	return list, err
}

// AddTarget add a new scraper target into storage.
func (c *Client) AddTarget(ctx context.Context, target *platform.ScraperTarget) (err error) {
	if !target.OrgID.Valid() {
		return &platform.Error{
			Code: platform.EInvalid,
			Msg:  "org id is invalid",
			Op:   OpPrefix + platform.OpAddTarget,
		}
	}
	if !target.BucketID.Valid() {
		return &platform.Error{
			Code: platform.EInvalid,
			Msg:  "bucket id is invalid",
			Op:   OpPrefix + platform.OpAddTarget,
		}
	}
	err = c.db.Update(func(tx *bolt.Tx) error {
		target.ID = c.IDGenerator.ID()
		return c.putTarget(ctx, tx, target)
	})
	if err != nil {
		return &platform.Error{
			Err: err,
			Op:  OpPrefix + platform.OpAddTarget,
		}
	}
	return nil
}

// RemoveTarget removes a scraper target from the bucket.
func (c *Client) RemoveTarget(ctx context.Context, id platform.ID) error {
	err := c.db.Update(func(tx *bolt.Tx) error {
		_, pe := c.findTargetByID(ctx, tx, id)
		if pe != nil {
			return pe
		}
		encID, err := id.Encode()
		if err != nil {
			return &platform.Error{
				Code: platform.EInvalid,
				Err:  err,
			}
		}
		return tx.Bucket(scraperBucket).Delete(encID)
	})
	if err != nil {
		return &platform.Error{
			Err: err,
			Op:  OpPrefix + platform.OpRemoveTarget,
		}
	}
	return nil
}

// UpdateTarget updates a scraper target.
func (c *Client) UpdateTarget(ctx context.Context, update *platform.ScraperTarget) (target *platform.ScraperTarget, err error) {
	op := getOp(platform.OpUpdateTarget)
	var pe *platform.Error
	if !update.ID.Valid() {
		return nil, &platform.Error{
			Code: platform.EInvalid,
			Op:   op,
			Msg:  "id is invalid",
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
		return nil, &platform.Error{
			Op:  op,
			Err: err,
		}
	}

	return target, nil
}

// GetTargetByID retrieves a scraper target by id.
func (c *Client) GetTargetByID(ctx context.Context, id platform.ID) (target *platform.ScraperTarget, err error) {
	var pe *platform.Error
	err = c.db.View(func(tx *bolt.Tx) error {
		target, pe = c.findTargetByID(ctx, tx, id)
		if pe != nil {
			return &platform.Error{
				Op:  getOp(platform.OpGetTargetByID),
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

func (c *Client) findTargetByID(ctx context.Context, tx *bolt.Tx, id platform.ID) (target *platform.ScraperTarget, pe *platform.Error) {
	target = new(platform.ScraperTarget)
	encID, err := id.Encode()
	if err != nil {
		return nil, &platform.Error{
			Err: err,
		}
	}
	v := tx.Bucket(scraperBucket).Get(encID)
	if len(v) == 0 {
		return nil, &platform.Error{
			Code: platform.ENotFound,
			Msg:  "scraper target is not found",
		}
	}

	if err := json.Unmarshal(v, target); err != nil {
		return nil, &platform.Error{
			Err: err,
		}
	}
	return target, nil
}

func (c *Client) putTarget(ctx context.Context, tx *bolt.Tx, target *platform.ScraperTarget) (err error) {
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
func (c *Client) PutTarget(ctx context.Context, target *platform.ScraperTarget) error {
	return c.db.Update(func(tx *bolt.Tx) error {
		return c.putTarget(ctx, tx, target)
	})
}

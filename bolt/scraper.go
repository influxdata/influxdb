package bolt

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	bolt "github.com/coreos/bbolt"
	"github.com/influxdata/platform"
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
	return list, err
}

// AddTarget add a new scraper target into storage.
func (c *Client) AddTarget(ctx context.Context, target *platform.ScraperTarget) (err error) {
	return c.db.Update(func(tx *bolt.Tx) error {
		target.ID = c.IDGenerator.ID()
		return c.putTarget(ctx, tx, target)
	})
}

// RemoveTarget removes a scraper target from the bucket.
func (c *Client) RemoveTarget(ctx context.Context, id platform.ID) error {
	return c.db.Update(func(tx *bolt.Tx) error {
		_, err := c.findTargetByID(ctx, tx, id)
		if err != nil {
			return err
		}
		encID, err := id.Encode()
		if err != nil {
			return err
		}
		return tx.Bucket(scraperBucket).Delete(encID)
	})
}

// UpdateTarget updates a scraper target.
func (c *Client) UpdateTarget(ctx context.Context, update *platform.ScraperTarget) (target *platform.ScraperTarget, err error) {
	if !update.ID.Valid() {
		return nil, errors.New("update scraper: id is invalid")
	}
	err = c.db.Update(func(tx *bolt.Tx) error {
		target, err = c.findTargetByID(ctx, tx, update.ID)
		if err != nil {
			return err
		}
		target = update
		return c.putTarget(ctx, tx, target)
	})

	return target, err
}

// GetTargetByID retrieves a scraper target by id.
func (c *Client) GetTargetByID(ctx context.Context, id platform.ID) (target *platform.ScraperTarget, err error) {
	err = c.db.View(func(tx *bolt.Tx) error {
		target, err = c.findTargetByID(ctx, tx, id)
		return err
	})
	return target, err
}

func (c *Client) findTargetByID(ctx context.Context, tx *bolt.Tx, id platform.ID) (target *platform.ScraperTarget, err error) {
	target = new(platform.ScraperTarget)
	encID, err := id.Encode()
	if err != nil {
		return nil, err
	}
	v := tx.Bucket(scraperBucket).Get(encID)
	if len(v) == 0 {
		return nil, fmt.Errorf("scraper target is not found")
	}

	if err := json.Unmarshal(v, target); err != nil {
		return nil, err
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

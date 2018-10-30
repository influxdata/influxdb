package bolt

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	bolt "github.com/coreos/bbolt"
	"github.com/influxdata/platform"
)

var (
	telegrafBucket = []byte("telegrafv1")
)

var _ platform.TelegrafConfigStore = new(Client)

func (c *Client) initializeTelegraf(ctx context.Context, tx *bolt.Tx) error {
	if _, err := tx.CreateBucketIfNotExists(telegrafBucket); err != nil {
		return err
	}
	return nil
}

// FindTelegrafConfigByID returns a single telegraf config by ID.
func (c *Client) FindTelegrafConfigByID(ctx context.Context, id platform.ID) (tc *platform.TelegrafConfig, err error) {
	op := "bolt/find telegraf config by id"
	err = c.db.View(func(tx *bolt.Tx) error {
		var pErr *platform.Error
		tc, pErr = c.findTelegrafConfigByID(ctx, tx, id)
		if pErr != nil {
			pErr.Op = op
			err = pErr
		}
		return err
	})
	return tc, err
}

func (c *Client) findTelegrafConfigByID(ctx context.Context, tx *bolt.Tx, id platform.ID) (*platform.TelegrafConfig, *platform.Error) {
	encID, err := id.Encode()
	if err != nil {
		return nil, &platform.Error{
			Code: platform.EEmptyValue,
			Err:  err,
		}
	}
	d := tx.Bucket(telegrafBucket).Get(encID)
	if d == nil {
		return nil, &platform.Error{
			Code: platform.ENotFound,
			Msg:  fmt.Sprintf("telegraf config with ID %v not found", id),
		}
	}
	tc := new(platform.TelegrafConfig)
	err = json.Unmarshal(d, tc)
	if err != nil {
		return nil, &platform.Error{
			Err: err,
		}
	}
	return tc, nil
}

// FindTelegrafConfig returns the first telegraf config that matches filter.
func (c *Client) FindTelegrafConfig(ctx context.Context, filter platform.UserResourceMappingFilter) (*platform.TelegrafConfig, error) {
	op := "bolt/find telegraf config"
	tcs, n, err := c.FindTelegrafConfigs(ctx, filter)
	if err != nil {
		return nil, err
	}
	if n > 0 {
		return tcs[0], nil
	}
	return nil, &platform.Error{
		Code: platform.ENotFound,
		Op:   op,
	}
}

func (c *Client) findTelegrafConfigs(ctx context.Context, tx *bolt.Tx, filter platform.UserResourceMappingFilter, opt ...platform.FindOptions) ([]*platform.TelegrafConfig, int, *platform.Error) {
	tcs := make([]*platform.TelegrafConfig, 0)
	m, err := c.findUserResourceMappings(ctx, tx, filter)
	if err != nil {
		return nil, 0, &platform.Error{
			Err: err,
		}
	}
	if len(m) == 0 {
		return nil, 0, &platform.Error{
			Code: platform.ENotFound,
		}
	}
	for _, item := range m {
		tc, err := c.findTelegrafConfigByID(ctx, tx, item.ResourceID)
		if err != nil {
			return nil, 0, &platform.Error{
				// return internal error, for any mapping issue
				Err: err,
			}
		}
		tcs = append(tcs, tc)
	}
	if len(tcs) == 0 {
		return nil, 0, &platform.Error{
			Msg: "inconsistent user resource mapping and telegraf config",
		}
	}
	return tcs, len(tcs), nil
}

// FindTelegrafConfigs returns a list of telegraf configs that match filter and the total count of matching telegraf configs.
// Additional options provide pagination & sorting.
func (c *Client) FindTelegrafConfigs(ctx context.Context, filter platform.UserResourceMappingFilter, opt ...platform.FindOptions) (tcs []*platform.TelegrafConfig, n int, err error) {
	op := "bolt/find telegraf configs"
	err = c.db.View(func(tx *bolt.Tx) error {
		var pErr *platform.Error
		tcs, n, pErr = c.findTelegrafConfigs(ctx, tx, filter)
		if pErr != nil {
			pErr.Op = op
			return pErr
		}
		return nil
	})
	return tcs, len(tcs), err
}

func (c *Client) putTelegrafConfig(ctx context.Context, tx *bolt.Tx, tc *platform.TelegrafConfig) *platform.Error {
	v, err := json.Marshal(tc)
	if err != nil {
		return &platform.Error{
			Err: err,
		}
	}
	encodedID, err := tc.ID.Encode()
	if err != nil {
		return &platform.Error{
			Code: platform.EEmptyValue,
			Err:  err,
		}
	}
	err = tx.Bucket(telegrafBucket).Put(encodedID, v)
	if err != nil {
		return &platform.Error{
			Err: err,
		}
	}
	return nil
}

// CreateTelegrafConfig creates a new telegraf config and sets b.ID with the new identifier.
func (c *Client) CreateTelegrafConfig(ctx context.Context, tc *platform.TelegrafConfig, userID platform.ID, now time.Time) error {
	op := "bolt/create telegraf config"
	return c.db.Update(func(tx *bolt.Tx) error {
		tc.ID = c.IDGenerator.ID()
		err := c.createUserResourceMapping(ctx, tx, &platform.UserResourceMapping{
			ResourceID:   tc.ID,
			UserID:       userID,
			UserType:     platform.Owner,
			ResourceType: platform.TelegrafResourceType,
		})
		if err != nil {
			return err
		}
		tc.Created = now
		tc.LastMod = now
		tc.LastModBy = userID
		pErr := c.putTelegrafConfig(ctx, tx, tc)
		if pErr != nil {
			pErr.Op = op
			err = pErr
		}
		return err
	})
}

// UpdateTelegrafConfig updates a single telegraf config.
// Returns the new telegraf config after update.
func (c *Client) UpdateTelegrafConfig(ctx context.Context, id platform.ID, tc *platform.TelegrafConfig, userID platform.ID, now time.Time) (*platform.TelegrafConfig, error) {
	op := "bolt/update telegraf config"
	err := c.db.Update(func(tx *bolt.Tx) (err error) {
		oldTc, pErr := c.findTelegrafConfigByID(ctx, tx, id)
		if pErr != nil {
			pErr.Op = op
			err = pErr
			return err
		}
		tc.ID = id
		tc.Created = oldTc.Created
		tc.LastMod = now
		tc.LastModBy = userID
		pErr = c.putTelegrafConfig(ctx, tx, tc)
		if pErr != nil {
			pErr.Op = op
			err = pErr
		}
		return err
	})
	return tc, err
}

// DeleteTelegrafConfig removes a telegraf config by ID.
func (c *Client) DeleteTelegrafConfig(ctx context.Context, id platform.ID) error {
	op := "bolt/delete telegraf config"
	err := c.db.Update(func(tx *bolt.Tx) error {
		encodedID, err := id.Encode()
		if err != nil {
			return &platform.Error{
				Code: platform.EEmptyValue,
				Err:  err,
			}
		}
		err = tx.Bucket(telegrafBucket).Delete(encodedID)
		if err != nil {
			return err
		}
		return c.deleteUserResourceMappings(ctx, tx, platform.UserResourceMappingFilter{
			ResourceID:   id,
			ResourceType: platform.TelegrafResourceType,
		})
	})
	if err != nil {
		err = &platform.Error{
			Code: platform.ErrorCode(err),
			Op:   op,
			Err:  err,
		}
	}
	return err
}

// PutTelegrafConfig put a telegraf config to storage
func (c *Client) PutTelegrafConfig(ctx context.Context, tc *platform.TelegrafConfig) error {
	return c.db.Update(func(tx *bolt.Tx) (err error) {
		pErr := c.putTelegrafConfig(ctx, tx, tc)
		if pErr != nil {
			err = pErr
		}
		return nil
	})
}

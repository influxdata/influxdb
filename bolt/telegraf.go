package bolt

import (
	"context"
	"encoding/json"

	bolt "github.com/coreos/bbolt"
	platform "github.com/influxdata/influxdb"
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
	err = c.db.View(func(tx *bolt.Tx) error {
		var pe *platform.Error
		tc, pe = c.findTelegrafConfigByID(ctx, tx, id)
		if pe != nil {
			return pe
		}
		return nil
	})
	return tc, err
}

func (c *Client) findTelegrafConfigByID(ctx context.Context, tx *bolt.Tx, id platform.ID) (*platform.TelegrafConfig, *platform.Error) {
	encID, err := id.Encode()
	if err != nil {
		return nil, &platform.Error{
			Code: platform.EInvalid,
			Msg:  "provided telegraf configuration ID has invalid format",
		}
	}
	d := tx.Bucket(telegrafBucket).Get(encID)
	if d == nil {
		return nil, &platform.Error{
			Code: platform.ENotFound,
			Msg:  platform.ErrTelegrafConfigNotFound,
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

func (c *Client) findTelegrafConfigs(ctx context.Context, tx *bolt.Tx, filter platform.TelegrafConfigFilter, opt ...platform.FindOptions) ([]*platform.TelegrafConfig, int, *platform.Error) {
	tcs := make([]*platform.TelegrafConfig, 0)
	m, err := c.findUserResourceMappings(ctx, tx, filter.UserResourceMappingFilter)
	if err != nil {
		return nil, 0, &platform.Error{
			Err: err,
		}
	}
	if len(m) == 0 {
		return tcs, 0, nil
	}
	for _, item := range m {
		tc, err := c.findTelegrafConfigByID(ctx, tx, item.ResourceID)
		if err != nil && platform.ErrorCode(err) != platform.ENotFound {
			return nil, 0, &platform.Error{
				// return internal error, for any mapping issue
				Err: err,
			}
		}
		if tc != nil {
			// Restrict results by organization ID, if it has been provided
			if filter.OrganizationID != nil && filter.OrganizationID.Valid() && tc.OrganizationID != *filter.OrganizationID {
				continue
			}
			tcs = append(tcs, tc)
		}
	}
	return tcs, len(tcs), nil
}

// FindTelegrafConfigs returns a list of telegraf configs that match filter and the total count of matching telegraf configs.
// Additional options provide pagination & sorting.
func (c *Client) FindTelegrafConfigs(ctx context.Context, filter platform.TelegrafConfigFilter, opt ...platform.FindOptions) (tcs []*platform.TelegrafConfig, n int, err error) {
	op := OpPrefix + platform.OpFindTelegrafConfigs
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
	if !tc.OrganizationID.Valid() {
		return &platform.Error{
			Code: platform.EEmptyValue,
			Msg:  platform.ErrTelegrafConfigInvalidOrganizationID,
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
func (c *Client) CreateTelegrafConfig(ctx context.Context, tc *platform.TelegrafConfig, userID platform.ID) error {
	op := OpPrefix + platform.OpCreateTelegrafConfig
	return c.db.Update(func(tx *bolt.Tx) error {
		tc.ID = c.IDGenerator.ID()

		pErr := c.putTelegrafConfig(ctx, tx, tc)
		if pErr != nil {
			pErr.Op = op
			return pErr
		}

		urm := &platform.UserResourceMapping{
			ResourceID:   tc.ID,
			UserID:       userID,
			UserType:     platform.Owner,
			ResourceType: platform.TelegrafsResourceType,
		}
		if err := c.createUserResourceMapping(ctx, tx, urm); err != nil {
			return err
		}

		return nil
	})
}

// UpdateTelegrafConfig updates a single telegraf config.
// Returns the new telegraf config after update.
func (c *Client) UpdateTelegrafConfig(ctx context.Context, id platform.ID, tc *platform.TelegrafConfig, userID platform.ID) (*platform.TelegrafConfig, error) {
	op := OpPrefix + platform.OpUpdateTelegrafConfig
	err := c.db.Update(func(tx *bolt.Tx) (err error) {
		current, pErr := c.findTelegrafConfigByID(ctx, tx, id)
		if pErr != nil {
			pErr.Op = op
			err = pErr
			return err
		}
		tc.ID = id
		// OrganizationID can not be updated
		tc.OrganizationID = current.OrganizationID
		pErr = c.putTelegrafConfig(ctx, tx, tc)
		if pErr != nil {
			return &platform.Error{
				Err: pErr,
			}
		}
		return nil
	})
	return tc, err
}

// DeleteTelegrafConfig removes a telegraf config by ID.
func (c *Client) DeleteTelegrafConfig(ctx context.Context, id platform.ID) error {
	err := c.db.Update(func(tx *bolt.Tx) error {
		encodedID, err := id.Encode()
		if err != nil {
			return &platform.Error{
				Code: platform.EInvalid,
				Msg:  "provided telegraf configuration ID has invalid format",
			}
		}
		err = tx.Bucket(telegrafBucket).Delete(encodedID)
		if err != nil {
			return err
		}
		return c.deleteUserResourceMappings(ctx, tx, platform.UserResourceMappingFilter{
			ResourceID:   id,
			ResourceType: platform.TelegrafsResourceType,
		})
	})
	if err != nil {
		err = &platform.Error{
			Code: platform.ErrorCode(err),
			Err:  err,
		}
	}
	return err
}

// PutTelegrafConfig put a telegraf config to storage.
func (c *Client) PutTelegrafConfig(ctx context.Context, tc *platform.TelegrafConfig) error {
	return c.db.Update(func(tx *bolt.Tx) (err error) {
		pErr := c.putTelegrafConfig(ctx, tx, tc)
		if pErr != nil {
			err = pErr
		}
		return nil
	})
}

package kv

import (
	"context"
	"encoding/json"

	"github.com/influxdata/influxdb"
)

// UnexpectedTelegrafBucketError is used when the error comes from an internal system.
func UnexpectedTelegrafBucketError(err error) *influxdb.Error {
	return &influxdb.Error{
		Code: influxdb.EInternal,
		Msg:  "unexpected error retrieving telegraf bucket",
		Err:  err,
		Op:   "kv/telegrafBucket",
	}
}

var (
	telegrafBucket = []byte("telegrafv1")
)

var _ influxdb.TelegrafConfigStore = (*Service)(nil)

func (s *Service) initializeTelegraf(ctx context.Context, tx Tx) error {
	if _, err := s.telegrafBucket(tx); err != nil {
		return err
	}
	return nil
}

func (s *Service) telegrafBucket(tx Tx) (Bucket, error) {
	b, err := tx.Bucket([]byte(telegrafBucket))
	if err != nil {
		return nil, UnexpectedTelegrafBucketError(err)
	}
	return b, nil
}

// FindTelegrafConfigByID returns a single telegraf config by ID.
func (s *Service) FindTelegrafConfigByID(ctx context.Context, id influxdb.ID) (tc *influxdb.TelegrafConfig, err error) {
	op := OpPrefix + influxdb.OpFindTelegrafConfigByID
	err = s.kv.View(func(tx Tx) error {
		var pErr *influxdb.Error
		tc, pErr = s.findTelegrafConfigByID(ctx, tx, id)
		if pErr != nil {
			pErr.Op = op
			err = pErr
		}
		return err
	})
	return tc, err
}

func (s *Service) findTelegrafConfigByID(ctx context.Context, tx Tx, id influxdb.ID) (*influxdb.TelegrafConfig, *influxdb.Error) {
	encID, err := id.Encode()
	if err != nil {
		return nil, &influxdb.Error{
			Code: influxdb.EEmptyValue,
			Err:  err,
		}
	}
	// TODO(goller): handle errors
	bucket, err := tx.Bucket(telegrafBucket)
	d, err := bucket.Get(encID)
	if d == nil {
		return nil, &influxdb.Error{
			Code: influxdb.ENotFound,
			Msg:  influxdb.ErrTelegrafConfigNotFound,
		}
	}
	tc := new(influxdb.TelegrafConfig)
	err = json.Unmarshal(d, tc)
	if err != nil {
		return nil, &influxdb.Error{
			Err: err,
		}
	}
	return tc, nil
}

// FindTelegrafConfig returns the first telegraf config that matches filter.
func (s *Service) FindTelegrafConfig(ctx context.Context, filter influxdb.TelegrafConfigFilter) (*influxdb.TelegrafConfig, error) {
	op := OpPrefix + influxdb.OpFindTelegrafConfig
	tcs, n, err := s.FindTelegrafConfigs(ctx, filter, influxdb.FindOptions{Limit: 1})
	if err != nil {
		return nil, err
	}
	if n > 0 {
		return tcs[0], nil
	}
	return nil, &influxdb.Error{
		Code: influxdb.ENotFound,
		Op:   op,
	}
}

func (s *Service) findTelegrafConfigs(ctx context.Context, tx Tx, filter influxdb.TelegrafConfigFilter, opt ...influxdb.FindOptions) ([]*influxdb.TelegrafConfig, int, *influxdb.Error) {
	return nil, 0, nil
	/* no idea here
		tcs := make([]*influxdb.TelegrafConfig, 0)
		m, err := s.findUserResourceMappings(ctx, tx, filter.UserResourceMappingFilter)
		if err != nil {
			return nil, 0, &influxdb.Error{
				Err: err,
			}
		}
		if len(m) == 0 {
			return tcs, 0, nil
		}
		for _, item := range m {
			tc, err := s.findTelegrafConfigByID(ctx, tx, item.ResourceID)
			if err != nil && influxdb.ErrorCode(err) != influxdb.ENotFound {
				return nil, 0, &influxdb.Error{
					// return internal error, for any mapping issue
					Err: err,
				}
			}
			if tc != nil {
				// Restrict results by organization ID, if it has been provided
				if filter.OrganizationID != nil && filter.OrganizationID.Valid() && ts.OrganizationID != *filter.OrganizationID {
					continue
				}
				tcs = append(tcs, tc)
			}
		}
	    return tcs, len(tcs), nil
	*/
}

// FindTelegrafConfigs returns a list of telegraf configs that match filter and the total count of matching telegraf configs.
// Additional options provide pagination & sorting.
func (s *Service) FindTelegrafConfigs(ctx context.Context, filter influxdb.TelegrafConfigFilter, opt ...influxdb.FindOptions) (tcs []*influxdb.TelegrafConfig, n int, err error) {
	op := OpPrefix + influxdb.OpFindTelegrafConfigs
	err = s.kv.View(func(tx Tx) error {
		var pErr *influxdb.Error
		tcs, n, pErr = s.findTelegrafConfigs(ctx, tx, filter)
		if pErr != nil {
			pErr.Op = op
			return pErr
		}
		return nil
	})
	return tcs, len(tcs), err
}

func (s *Service) putTelegrafConfig(ctx context.Context, tx Tx, tc *influxdb.TelegrafConfig) *influxdb.Error {
	v, err := json.Marshal(tc)
	if err != nil {
		return &influxdb.Error{
			Err: err,
		}
	}
	encodedID, err := tc.ID.Encode()
	if err != nil {
		return &influxdb.Error{
			Code: influxdb.EEmptyValue,
			Err:  err,
		}
	}
	if !tc.OrganizationID.Valid() {
		return &influxdb.Error{
			Code: influxdb.EEmptyValue,
			Msg:  influxdb.ErrTelegrafConfigInvalidOrganizationID,
		}
	}
	bucket, err := tx.Bucket(telegrafBucket)
	// TODO(goller): deal with bucket error
	if err := bucket.Put(encodedID, v); err != nil {
		return &influxdb.Error{
			Err: err,
		}
	}
	return nil
}

// CreateTelegrafConfig creates a new telegraf config and sets b.ID with the new identifier.
func (s *Service) CreateTelegrafConfig(ctx context.Context, tc *influxdb.TelegrafConfig, userID influxdb.ID) error {
	op := OpPrefix + influxdb.OpCreateTelegrafConfig
	return s.kv.Update(func(tx Tx) error {
		tc.ID = s.IDGenerator.ID()

		pErr := s.putTelegrafConfig(ctx, tx, tc)
		if pErr != nil {
			pErr.Op = op
			return pErr
		}

		/* TODO(goller): no idea here
				urm := &influxdb.UserResourceMapping{
					ResourceID:   tc.ID,
					UserID:       userID,
					UserType:     influxdb.Owner,
					ResourceType: influxdb.TelegrafsResourceType,
				}
				if err := s.createUserResourceMapping(ctx, tx, urm); err != nil {
					return err
		        }
		*/

		return nil
	})
}

// UpdateTelegrafConfig updates a single telegraf config.
// Returns the new telegraf config after update.
func (s *Service) UpdateTelegrafConfig(ctx context.Context, id influxdb.ID, tc *influxdb.TelegrafConfig, userID influxdb.ID) (*influxdb.TelegrafConfig, error) {
	op := OpPrefix + influxdb.OpUpdateTelegrafConfig
	err := s.kv.Update(func(tx Tx) (err error) {
		current, pErr := s.findTelegrafConfigByID(ctx, tx, id)
		if pErr != nil {
			pErr.Op = op
			err = pErr
			return err
		}
		tc.ID = id
		// OrganizationID can not be updated
		tc.OrganizationID = current.OrganizationID
		pErr = s.putTelegrafConfig(ctx, tx, tc)
		if pErr != nil {
			return &influxdb.Error{
				Err: pErr,
			}
		}
		return nil
	})
	return tc, err
}

// DeleteTelegrafConfig removes a telegraf config by ID.
func (s *Service) DeleteTelegrafConfig(ctx context.Context, id influxdb.ID) error {
	op := OpPrefix + influxdb.OpDeleteTelegrafConfig
	err := s.kv.Update(func(tx Tx) error {
		encodedID, err := id.Encode()
		if err != nil {
			return &influxdb.Error{
				Code: influxdb.EEmptyValue,
				Err:  err,
			}
		}
		// TODO(goller) deal with errors
		bucket, err := tx.Bucket(telegrafBucket)
		if err != nil {
			return err
		}

		if err := bucket.Delete(encodedID); err != nil {
			return err
		}

		/* TODO(goller): no idea about mappings
				return s.deleteUserResourceMappings(ctx, tx, influxdb.UserResourceMappingFilter{
					ResourceID:   id,
					ResourceType: influxdb.TelegrafsResourceType,
		        })
		*/
		return nil
	})
	if err != nil {
		err = &influxdb.Error{
			Code: influxdb.ErrorCode(err),
			Op:   op,
			Err:  err,
		}
	}
	return err
}

// PutTelegrafConfig put a telegraf config to storage.
func (s *Service) PutTelegrafConfig(ctx context.Context, tc *influxdb.TelegrafConfig) error {
	return s.kv.Update(func(tx Tx) (err error) {
		pErr := s.putTelegrafConfig(ctx, tx, tc)
		if pErr != nil {
			err = pErr
		}
		return nil
	})
}

// CreateUserResourceMapping creates a user resource mapping.
// TODO(goller): I'm not sure mappings really need to be here
func (s *Service) CreateUserResourceMapping(ctx context.Context, m *influxdb.UserResourceMapping) error {
	return nil
}

// DeleteUserResourceMapping deletes a user resource mapping.
func (s *Service) DeleteUserResourceMapping(ctx context.Context, resourceID, userID influxdb.ID) error {
	return nil
}

// FindUserResourceMappings returns a list of UserResourceMappings that match filter and the total count of matching mappings.
func (s *Service) FindUserResourceMappings(ctx context.Context, filter influxdb.UserResourceMappingFilter, opt ...influxdb.FindOptions) ([]*influxdb.UserResourceMapping, int, error) {
	return nil, 0, nil
}

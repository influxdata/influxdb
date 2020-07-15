package kv

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/influxdata/influxdb/v2"
)

var (
	// ErrTelegrafNotFound is used when the telegraf configuration is not found.
	ErrTelegrafNotFound = &influxdb.Error{
		Msg:  "telegraf configuration not found",
		Code: influxdb.ENotFound,
	}

	// ErrInvalidTelegrafID is used when the service was provided
	// an invalid ID format.
	ErrInvalidTelegrafID = &influxdb.Error{
		Code: influxdb.EInvalid,
		Msg:  "provided telegraf configuration ID has invalid format",
	}

	// ErrInvalidTelegrafOrgID is the error message for a missing or invalid organization ID.
	ErrInvalidTelegrafOrgID = &influxdb.Error{
		Code: influxdb.EEmptyValue,
		Msg:  "provided telegraf configuration organization ID is missing or invalid",
	}
)

// UnavailableTelegrafServiceError is used if we aren't able to interact with the
// store, it means the store is not available at the moment (e.g. network).
func UnavailableTelegrafServiceError(err error) *influxdb.Error {
	return &influxdb.Error{
		Code: influxdb.EInternal,
		Msg:  fmt.Sprintf("Unable to connect to telegraf service. Please try again; Err: %v", err),
		Op:   "kv/telegraf",
	}
}

// InternalTelegrafServiceError is used when the error comes from an
// internal system.
func InternalTelegrafServiceError(err error) *influxdb.Error {
	return &influxdb.Error{
		Code: influxdb.EInternal,
		Msg:  fmt.Sprintf("Unknown internal telegraf data error; Err: %v", err),
		Op:   "kv/telegraf",
	}
}

// CorruptTelegrafError is used when the config cannot be unmarshalled from the
// bytes stored in the kv.
func CorruptTelegrafError(err error) *influxdb.Error {
	return &influxdb.Error{
		Code: influxdb.EInternal,
		Msg:  fmt.Sprintf("Unknown internal telegraf data error; Err: %v", err),
		Op:   "kv/telegraf",
	}
}

// ErrUnprocessableTelegraf is used when a telegraf is not able to be converted to JSON.
func ErrUnprocessableTelegraf(err error) *influxdb.Error {
	return &influxdb.Error{
		Code: influxdb.EUnprocessableEntity,
		Msg:  fmt.Sprintf("unable to convert telegraf configuration into JSON; Err %v", err),
	}
}

var (
	telegrafBucket        = []byte("telegrafv1")
	telegrafPluginsBucket = []byte("telegrafPluginsv1")
)

var _ influxdb.TelegrafConfigStore = (*Service)(nil)

func (s *Service) telegrafBucket(tx Tx) (Bucket, error) {
	b, err := tx.Bucket(telegrafBucket)
	if err != nil {
		return nil, UnavailableTelegrafServiceError(err)
	}
	return b, nil
}

func (s *Service) telegrafPluginsBucket(tx Tx) (Bucket, error) {
	b, err := tx.Bucket(telegrafPluginsBucket)
	if err != nil {
		return nil, UnavailableTelegrafServiceError(err)
	}
	return b, nil
}

// FindTelegrafConfigByID returns a single telegraf config by ID.
func (s *Service) FindTelegrafConfigByID(ctx context.Context, id influxdb.ID) (*influxdb.TelegrafConfig, error) {
	var (
		tc  *influxdb.TelegrafConfig
		err error
	)

	err = s.kv.View(ctx, func(tx Tx) error {
		tc, err = s.findTelegrafConfigByID(ctx, tx, id)
		return err
	})

	return tc, err
}

func (s *Service) findTelegrafConfigByID(ctx context.Context, tx Tx, id influxdb.ID) (*influxdb.TelegrafConfig, error) {
	encID, err := id.Encode()
	if err != nil {
		return nil, ErrInvalidTelegrafID
	}

	bucket, err := s.telegrafBucket(tx)
	if err != nil {
		return nil, err
	}

	v, err := bucket.Get(encID)
	if IsNotFound(err) {
		return nil, ErrTelegrafNotFound
	}
	if err != nil {
		return nil, InternalTelegrafServiceError(err)
	}

	return unmarshalTelegraf(v)
}

// FindTelegrafConfigs returns a list of telegraf configs that match filter and the total count of matching telegraf configs.
// Additional options provide pagination & sorting.
func (s *Service) FindTelegrafConfigs(ctx context.Context, filter influxdb.TelegrafConfigFilter, opt ...influxdb.FindOptions) (tcs []*influxdb.TelegrafConfig, n int, err error) {
	err = s.kv.View(ctx, func(tx Tx) error {
		tcs, n, err = s.findTelegrafConfigs(ctx, tx, filter)
		return err
	})
	return tcs, n, err
}

func (s *Service) findTelegrafConfigs(ctx context.Context, tx Tx, filter influxdb.TelegrafConfigFilter, opt ...influxdb.FindOptions) ([]*influxdb.TelegrafConfig, int, error) {
	tcs := make([]*influxdb.TelegrafConfig, 0)

	m, err := s.findUserResourceMappings(ctx, tx, filter.UserResourceMappingFilter)
	if err != nil {
		return nil, 0, err
	}

	if len(m) == 0 {
		return tcs, 0, nil
	}

	for _, item := range m {
		tc, err := s.findTelegrafConfigByID(ctx, tx, item.ResourceID)
		if err == ErrTelegrafNotFound { // Stale user resource mappings are skipped
			continue
		}
		if err != nil {
			return nil, 0, InternalTelegrafServiceError(err)
		}

		// Restrict results by organization ID, if it has been provided
		if filter.OrgID != nil && filter.OrgID.Valid() && tc.OrgID != *filter.OrgID {
			continue
		}
		tcs = append(tcs, tc)
	}
	return tcs, len(tcs), nil
}

// PutTelegrafConfig put a telegraf config to storage.
func (s *Service) PutTelegrafConfig(ctx context.Context, tc *influxdb.TelegrafConfig) error {
	return s.kv.Update(ctx, func(tx Tx) (err error) {
		return s.putTelegrafConfig(ctx, tx, tc)
	})
}

func (s *Service) putTelegrafConfig(ctx context.Context, tx Tx, tc *influxdb.TelegrafConfig) error {
	encodedID, err := tc.ID.Encode()
	if err != nil {
		return ErrInvalidTelegrafID
	}

	if !tc.OrgID.Valid() {
		return ErrInvalidTelegrafOrgID
	}

	v, err := marshalTelegraf(tc)
	if err != nil {
		return err
	}

	bucket, err := s.telegrafBucket(tx)
	if err != nil {
		return err
	}

	if err := bucket.Put(encodedID, v); err != nil {
		return UnavailableTelegrafServiceError(err)
	}

	return s.putTelegrafConfigStats(encodedID, tx, tc)
}

func (s *Service) putTelegrafConfigStats(encodedID []byte, tx Tx, tc *influxdb.TelegrafConfig) error {
	bucket, err := s.telegrafPluginsBucket(tx)
	if err != nil {
		return err
	}

	v, err := marshalTelegrafPlugins(tc.CountPlugins())
	if err != nil {
		return err
	}

	if err := bucket.Put(encodedID, v); err != nil {
		return UnavailableTelegrafServiceError(err)
	}

	return nil
}

// CreateTelegrafConfig creates a new telegraf config and sets b.ID with the new identifier.
func (s *Service) CreateTelegrafConfig(ctx context.Context, tc *influxdb.TelegrafConfig, userID influxdb.ID) error {
	return s.kv.Update(ctx, func(tx Tx) error {
		return s.createTelegrafConfig(ctx, tx, tc, userID)
	})
}

func (s *Service) createTelegrafConfig(ctx context.Context, tx Tx, tc *influxdb.TelegrafConfig, userID influxdb.ID) error {
	tc.ID = s.IDGenerator.ID()
	if err := s.putTelegrafConfig(ctx, tx, tc); err != nil {
		return err
	}

	urm := &influxdb.UserResourceMapping{
		ResourceID:   tc.ID,
		UserID:       userID,
		UserType:     influxdb.Owner,
		ResourceType: influxdb.TelegrafsResourceType,
	}
	return s.createUserResourceMapping(ctx, tx, urm)
}

// UpdateTelegrafConfig updates a single telegraf config.
// Returns the new telegraf config after update.
func (s *Service) UpdateTelegrafConfig(ctx context.Context, id influxdb.ID, tc *influxdb.TelegrafConfig, userID influxdb.ID) (*influxdb.TelegrafConfig, error) {
	var err error
	err = s.kv.Update(ctx, func(tx Tx) error {
		tc, err = s.updateTelegrafConfig(ctx, tx, id, tc, userID)
		return err
	})
	return tc, err
}

func (s *Service) updateTelegrafConfig(ctx context.Context, tx Tx, id influxdb.ID, tc *influxdb.TelegrafConfig, userID influxdb.ID) (*influxdb.TelegrafConfig, error) {
	current, err := s.findTelegrafConfigByID(ctx, tx, id)
	if err != nil {
		return nil, err
	}

	// ID and OrganizationID can not be updated
	tc.ID = current.ID
	tc.OrgID = current.OrgID
	err = s.putTelegrafConfig(ctx, tx, tc)
	return tc, err
}

// DeleteTelegrafConfig removes a telegraf config by ID.
func (s *Service) DeleteTelegrafConfig(ctx context.Context, id influxdb.ID) error {
	return s.kv.Update(ctx, func(tx Tx) error {
		return s.deleteTelegrafConfig(ctx, tx, id)
	})
}

func (s *Service) deleteTelegrafConfig(ctx context.Context, tx Tx, id influxdb.ID) error {
	encodedID, err := id.Encode()
	if err != nil {
		return ErrInvalidTelegrafID
	}

	bucket, err := s.telegrafBucket(tx)
	if err != nil {
		return err
	}

	_, err = bucket.Get(encodedID)
	if IsNotFound(err) {
		return ErrTelegrafNotFound
	}
	if err != nil {
		return InternalTelegrafServiceError(err)
	}

	if err := bucket.Delete(encodedID); err != nil {
		return UnavailableTelegrafServiceError(err)
	}

	if err := s.deleteUserResourceMappings(ctx, tx, influxdb.UserResourceMappingFilter{
		ResourceID:   id,
		ResourceType: influxdb.TelegrafsResourceType,
	}); err != nil {
		return err
	}

	return s.deleteTelegrafConfigStats(encodedID, tx)
}

func (s *Service) deleteTelegrafConfigStats(encodedID []byte, tx Tx) error {
	bucket, err := s.telegrafPluginsBucket(tx)
	if err != nil {
		return err
	}

	if err := bucket.Delete(encodedID); err != nil {
		return &influxdb.Error{
			Code: influxdb.EInternal,
			Msg:  fmt.Sprintf("Unable to connect to telegraf config stats service. Please try again; Err: %v", err),
			Op:   "kv/telegraf",
		}
	}

	return nil
}

// unmarshalTelegraf turns the stored byte slice in the kv into a *influxdb.TelegrafConfig.
func unmarshalTelegraf(v []byte) (*influxdb.TelegrafConfig, error) {
	t := &influxdb.TelegrafConfig{}
	if err := json.Unmarshal(v, t); err != nil {
		return nil, CorruptTelegrafError(err)
	}
	return t, nil
}

func marshalTelegraf(tc *influxdb.TelegrafConfig) ([]byte, error) {
	v, err := json.Marshal(tc)
	if err != nil {
		return nil, ErrUnprocessableTelegraf(err)
	}
	return v, nil
}

func marshalTelegrafPlugins(plugins map[string]float64) ([]byte, error) {
	v, err := json.Marshal(plugins)
	if err != nil {
		return nil, ErrUnprocessableTelegraf(err)
	}
	return v, nil
}

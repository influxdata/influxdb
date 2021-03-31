package service

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/kit/platform/errors"

	influxdb "github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kv"
	"github.com/influxdata/influxdb/v2/snowflake"
	"github.com/influxdata/influxdb/v2/telegraf"
)

var (
	// ErrTelegrafNotFound is used when the telegraf configuration is not found.
	ErrTelegrafNotFound = &errors.Error{
		Msg:  "telegraf configuration not found",
		Code: errors.ENotFound,
	}

	// ErrInvalidTelegrafID is used when the service was provided
	// an invalid ID format.
	ErrInvalidTelegrafID = &errors.Error{
		Code: errors.EInvalid,
		Msg:  "provided telegraf configuration ID has invalid format",
	}

	// ErrInvalidTelegrafOrgID is the error message for a missing or invalid organization ID.
	ErrInvalidTelegrafOrgID = &errors.Error{
		Code: errors.EEmptyValue,
		Msg:  "provided telegraf configuration organization ID is missing or invalid",
	}
)

// UnavailableTelegrafServiceError is used if we aren't able to interact with the
// store, it means the store is not available at the moment (e.g. network).
func UnavailableTelegrafServiceError(err error) *errors.Error {
	return &errors.Error{
		Code: errors.EInternal,
		Msg:  fmt.Sprintf("Unable to connect to telegraf service. Please try again; Err: %v", err),
		Op:   "kv/telegraf",
	}
}

// InternalTelegrafServiceError is used when the error comes from an
// internal system.
func InternalTelegrafServiceError(err error) *errors.Error {
	return &errors.Error{
		Code: errors.EInternal,
		Msg:  fmt.Sprintf("Unknown internal telegraf data error; Err: %v", err),
		Op:   "kv/telegraf",
	}
}

// CorruptTelegrafError is used when the config cannot be unmarshalled from the
// bytes stored in the kv.
func CorruptTelegrafError(err error) *errors.Error {
	return &errors.Error{
		Code: errors.EInternal,
		Msg:  fmt.Sprintf("Unknown internal telegraf data error; Err: %v", err),
		Op:   "kv/telegraf",
	}
}

// ErrUnprocessableTelegraf is used when a telegraf is not able to be converted to JSON.
func ErrUnprocessableTelegraf(err error) *errors.Error {
	return &errors.Error{
		Code: errors.EUnprocessableEntity,
		Msg:  fmt.Sprintf("unable to convert telegraf configuration into JSON; Err %v", err),
	}
}

var (
	telegrafBucket        = []byte("telegrafv1")
	telegrafPluginsBucket = []byte("telegrafPluginsv1")
)

var _ influxdb.TelegrafConfigStore = (*Service)(nil)

// Service is a telegraf config service.
type Service struct {
	kv kv.Store

	byOrganisationIndex *kv.Index

	IDGenerator platform.IDGenerator
}

// New constructs and configures a new telegraf config service.
func New(store kv.Store) *Service {
	return &Service{
		kv: store,
		byOrganisationIndex: kv.NewIndex(
			telegraf.ByOrganizationIndexMapping,
			kv.WithIndexReadPathEnabled,
		),
		IDGenerator: snowflake.NewIDGenerator(),
	}
}

func (s *Service) telegrafBucket(tx kv.Tx) (kv.Bucket, error) {
	b, err := tx.Bucket(telegrafBucket)
	if err != nil {
		return nil, UnavailableTelegrafServiceError(err)
	}
	return b, nil
}

func (s *Service) telegrafPluginsBucket(tx kv.Tx) (kv.Bucket, error) {
	b, err := tx.Bucket(telegrafPluginsBucket)
	if err != nil {
		return nil, UnavailableTelegrafServiceError(err)
	}
	return b, nil
}

// FindTelegrafConfigByID returns a single telegraf config by ID.
func (s *Service) FindTelegrafConfigByID(ctx context.Context, id platform.ID) (*influxdb.TelegrafConfig, error) {
	var (
		tc  *influxdb.TelegrafConfig
		err error
	)

	err = s.kv.View(ctx, func(tx kv.Tx) error {
		tc, err = s.findTelegrafConfigByID(ctx, tx, id)
		return err
	})

	return tc, err
}

func (s *Service) findTelegrafConfigByID(ctx context.Context, tx kv.Tx, id platform.ID) (*influxdb.TelegrafConfig, error) {
	encID, err := id.Encode()
	if err != nil {
		return nil, ErrInvalidTelegrafID
	}

	bucket, err := s.telegrafBucket(tx)
	if err != nil {
		return nil, err
	}

	v, err := bucket.Get(encID)
	if kv.IsNotFound(err) {
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
	err = s.kv.View(ctx, func(tx kv.Tx) error {
		tcs, n, err = s.findTelegrafConfigs(ctx, tx, filter, opt...)
		return err
	})
	return tcs, n, err
}

func (s *Service) findTelegrafConfigs(ctx context.Context, tx kv.Tx, filter influxdb.TelegrafConfigFilter, opt ...influxdb.FindOptions) ([]*influxdb.TelegrafConfig, int, error) {
	var (
		limit  = influxdb.DefaultPageSize
		offset int
		count  int
		tcs    = make([]*influxdb.TelegrafConfig, 0)
	)

	if len(opt) > 0 {
		limit = opt[0].GetLimit()
		offset = opt[0].Offset
	}

	visit := func(k, v []byte) (bool, error) {
		var tc influxdb.TelegrafConfig
		if err := json.Unmarshal(v, &tc); err != nil {
			return false, err
		}

		// skip until offset reached
		if count >= offset {
			tcs = append(tcs, &tc)
		}

		count++

		// stop cursing when limit is reached
		return len(tcs) < limit, nil
	}

	if filter.OrgID == nil {
		// forward cursor entire bucket
		bucket, err := s.telegrafBucket(tx)
		if err != nil {
			return nil, 0, err
		}

		// cursors do not support numeric offset
		// but we can at least constrain the response
		// size by the offset + limit since we are
		// not doing any other filtering
		// REMOVE this cursor option if you do any
		// other filtering

		cursor, err := bucket.ForwardCursor(nil, kv.WithCursorLimit(offset+limit))
		if err != nil {
			return nil, 0, err
		}

		return tcs, len(tcs), kv.WalkCursor(ctx, cursor, visit)
	}

	orgID, err := filter.OrgID.Encode()
	if err != nil {
		return nil, 0, err
	}

	return tcs, len(tcs), s.byOrganisationIndex.Walk(ctx, tx, orgID, visit)
}

// PutTelegrafConfig put a telegraf config to storage.
func (s *Service) PutTelegrafConfig(ctx context.Context, tc *influxdb.TelegrafConfig) error {
	return s.kv.Update(ctx, func(tx kv.Tx) (err error) {
		return s.putTelegrafConfig(ctx, tx, tc)
	})
}

func (s *Service) putTelegrafConfig(ctx context.Context, tx kv.Tx, tc *influxdb.TelegrafConfig) error {
	encodedID, err := tc.ID.Encode()
	if err != nil {
		return ErrInvalidTelegrafID
	}

	if !tc.OrgID.Valid() {
		return ErrInvalidTelegrafOrgID
	}

	orgID, err := tc.OrgID.Encode()
	if err != nil {
		return err
	}

	// insert index entry for orgID -> id
	if err := s.byOrganisationIndex.Insert(tx, orgID, encodedID); err != nil {
		return err
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

func (s *Service) putTelegrafConfigStats(encodedID []byte, tx kv.Tx, tc *influxdb.TelegrafConfig) error {
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
func (s *Service) CreateTelegrafConfig(ctx context.Context, tc *influxdb.TelegrafConfig, userID platform.ID) error {
	return s.kv.Update(ctx, func(tx kv.Tx) error {
		return s.createTelegrafConfig(ctx, tx, tc, userID)
	})
}

func (s *Service) createTelegrafConfig(ctx context.Context, tx kv.Tx, tc *influxdb.TelegrafConfig, userID platform.ID) error {
	tc.ID = s.IDGenerator.ID()

	return s.putTelegrafConfig(ctx, tx, tc)
}

// UpdateTelegrafConfig updates a single telegraf config.
// Returns the new telegraf config after update.
func (s *Service) UpdateTelegrafConfig(ctx context.Context, id platform.ID, tc *influxdb.TelegrafConfig, userID platform.ID) (*influxdb.TelegrafConfig, error) {
	var err error
	err = s.kv.Update(ctx, func(tx kv.Tx) error {
		tc, err = s.updateTelegrafConfig(ctx, tx, id, tc, userID)
		return err
	})
	return tc, err
}

func (s *Service) updateTelegrafConfig(ctx context.Context, tx kv.Tx, id platform.ID, tc *influxdb.TelegrafConfig, userID platform.ID) (*influxdb.TelegrafConfig, error) {
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
func (s *Service) DeleteTelegrafConfig(ctx context.Context, id platform.ID) error {
	return s.kv.Update(ctx, func(tx kv.Tx) error {
		return s.deleteTelegrafConfig(ctx, tx, id)
	})
}

func (s *Service) deleteTelegrafConfig(ctx context.Context, tx kv.Tx, id platform.ID) error {
	tc, err := s.findTelegrafConfigByID(ctx, tx, id)
	if err != nil {
		return err
	}

	encodedID, err := tc.ID.Encode()
	if err != nil {
		return ErrInvalidTelegrafID
	}

	orgID, err := tc.OrgID.Encode()
	if err != nil {
		return err
	}

	// removing index entry for orgID -> id
	if err := s.byOrganisationIndex.Delete(tx, orgID, encodedID); err != nil {
		return err
	}

	bucket, err := s.telegrafBucket(tx)
	if err != nil {
		return err
	}

	_, err = bucket.Get(encodedID)
	if kv.IsNotFound(err) {
		return ErrTelegrafNotFound
	}
	if err != nil {
		return InternalTelegrafServiceError(err)
	}

	if err := bucket.Delete(encodedID); err != nil {
		return UnavailableTelegrafServiceError(err)
	}

	return s.deleteTelegrafConfigStats(encodedID, tx)
}

func (s *Service) deleteTelegrafConfigStats(encodedID []byte, tx kv.Tx) error {
	bucket, err := s.telegrafPluginsBucket(tx)
	if err != nil {
		return err
	}

	if err := bucket.Delete(encodedID); err != nil {
		return &errors.Error{
			Code: errors.EInternal,
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

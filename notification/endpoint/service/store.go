package service

import (
	"context"

	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/kit/platform/errors"

	influxdb "github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kv"
	"github.com/influxdata/influxdb/v2/notification/endpoint"
	"github.com/influxdata/influxdb/v2/snowflake"
)

var (
	// ErrNotificationEndpointNotFound is used when the notification endpoint is not found.
	ErrNotificationEndpointNotFound = &errors.Error{
		Msg:  "notification endpoint not found",
		Code: errors.ENotFound,
	}

	notificationEndpointBucket      = []byte("notificationEndpointv1")
	notificationEndpointIndexBucket = []byte("notificationEndpointIndexv1")
)

var _ influxdb.NotificationEndpointService = (*Store)(nil)

func newEndpointStore() *kv.IndexStore {
	const resource = "notification endpoint"

	var decEndpointEntFn kv.DecodeBucketValFn = func(key, val []byte) ([]byte, interface{}, error) {
		edp, err := endpoint.UnmarshalJSON(val)
		return key, edp, err
	}

	var decValToEntFn kv.ConvertValToEntFn = func(_ []byte, v interface{}) (kv.Entity, error) {
		edp, ok := v.(influxdb.NotificationEndpoint)
		if err := kv.IsErrUnexpectedDecodeVal(ok); err != nil {
			return kv.Entity{}, err
		}
		return kv.Entity{
			PK:        kv.EncID(edp.GetID()),
			UniqueKey: kv.Encode(kv.EncID(edp.GetOrgID()), kv.EncString(edp.GetName())),
			Body:      edp,
		}, nil
	}

	return &kv.IndexStore{
		Resource:   resource,
		EntStore:   kv.NewStoreBase(resource, notificationEndpointBucket, kv.EncIDKey, kv.EncBodyJSON, decEndpointEntFn, decValToEntFn),
		IndexStore: kv.NewOrgNameKeyStore(resource, notificationEndpointIndexBucket, true),
	}
}

type Store struct {
	kv kv.Store

	endpointStore *kv.IndexStore

	IDGenerator   platform.IDGenerator
	TimeGenerator influxdb.TimeGenerator
}

func NewStore(store kv.Store) *Store {
	return &Store{
		kv:            store,
		endpointStore: newEndpointStore(),
		IDGenerator:   snowflake.NewDefaultIDGenerator(),
		TimeGenerator: influxdb.RealTimeGenerator{},
	}
}

// CreateNotificationEndpoint creates a new notification endpoint and sets b.ID with the new identifier.
func (s *Store) CreateNotificationEndpoint(ctx context.Context, edp influxdb.NotificationEndpoint, userID platform.ID) error {
	return s.kv.Update(ctx, func(tx kv.Tx) error {
		return s.createNotificationEndpoint(ctx, tx, edp, userID)
	})
}

func (s *Store) createNotificationEndpoint(ctx context.Context, tx kv.Tx, edp influxdb.NotificationEndpoint, userID platform.ID) error {
	id := s.IDGenerator.ID()
	edp.SetID(id)
	now := s.TimeGenerator.Now()
	edp.SetCreatedAt(now)
	edp.SetUpdatedAt(now)
	edp.BackfillSecretKeys()

	if err := edp.Valid(); err != nil {
		return err
	}

	ent := kv.Entity{
		PK:        kv.EncID(edp.GetID()),
		UniqueKey: kv.Encode(kv.EncID(edp.GetOrgID()), kv.EncString(edp.GetName())),
		Body:      edp,
	}
	if err := s.endpointStore.Put(ctx, tx, ent, kv.PutNew()); err != nil {
		return err
	}

	return nil
}

// UpdateNotificationEndpoint updates a single notification endpoint.
// Returns the new notification endpoint after update.
func (s *Store) UpdateNotificationEndpoint(ctx context.Context, id platform.ID, edp influxdb.NotificationEndpoint, userID platform.ID) (influxdb.NotificationEndpoint, error) {
	var err error
	err = s.kv.Update(ctx, func(tx kv.Tx) error {
		edp, err = s.updateNotificationEndpoint(ctx, tx, id, edp, userID)
		return err
	})
	return edp, err
}

func (s *Store) updateNotificationEndpoint(ctx context.Context, tx kv.Tx, id platform.ID, edp influxdb.NotificationEndpoint, userID platform.ID) (influxdb.NotificationEndpoint, error) {
	current, err := s.findNotificationEndpointByID(ctx, tx, id)
	if err != nil {
		return nil, err
	}

	// ID and OrganizationID can not be updated
	edp.SetCreatedAt(current.GetCRUDLog().CreatedAt)
	edp.SetUpdatedAt(s.TimeGenerator.Now())

	if err := edp.Valid(); err != nil {
		return nil, err
	}

	ent := kv.Entity{
		PK:        kv.EncID(edp.GetID()),
		UniqueKey: kv.Encode(kv.EncID(edp.GetOrgID()), kv.EncString(edp.GetName())),
		Body:      edp,
	}
	if err := s.endpointStore.Put(ctx, tx, ent, kv.PutUpdate()); err != nil {
		return nil, err
	}

	return edp, nil
}

// PatchNotificationEndpoint updates a single  notification endpoint with changeset.
// Returns the new notification endpoint state after update.
func (s *Store) PatchNotificationEndpoint(ctx context.Context, id platform.ID, upd influxdb.NotificationEndpointUpdate) (influxdb.NotificationEndpoint, error) {
	var edp influxdb.NotificationEndpoint
	if err := s.kv.Update(ctx, func(tx kv.Tx) (err error) {
		edp, err = s.patchNotificationEndpoint(ctx, tx, id, upd)
		if err != nil {
			return err
		}
		return nil
	}); err != nil {
		return nil, err
	}

	return edp, nil
}

func (s *Store) patchNotificationEndpoint(ctx context.Context, tx kv.Tx, id platform.ID, upd influxdb.NotificationEndpointUpdate) (influxdb.NotificationEndpoint, error) {
	edp, err := s.findNotificationEndpointByID(ctx, tx, id)
	if err != nil {
		return nil, err
	}

	if upd.Name != nil {
		edp.SetName(*upd.Name)
	}
	if upd.Description != nil {
		edp.SetDescription(*upd.Description)
	}
	if upd.Status != nil {
		edp.SetStatus(*upd.Status)
	}
	edp.SetUpdatedAt(s.TimeGenerator.Now())

	if err := edp.Valid(); err != nil {
		return nil, err
	}

	// TODO(jsteenb2): every above here moves into service layer

	ent := kv.Entity{
		PK:        kv.EncID(edp.GetID()),
		UniqueKey: kv.Encode(kv.EncID(edp.GetOrgID()), kv.EncString(edp.GetName())),
		Body:      edp,
	}
	if err := s.endpointStore.Put(ctx, tx, ent, kv.PutUpdate()); err != nil {
		return nil, err
	}

	return edp, nil
}

// PutNotificationEndpoint put a notification endpoint to storage.
func (s *Store) PutNotificationEndpoint(ctx context.Context, edp influxdb.NotificationEndpoint) error {
	// TODO(jsteenb2): all the stuffs before the update should be moved up into the
	//  service layer as well as all the id/time setting items
	if err := edp.Valid(); err != nil {
		return err
	}

	return s.kv.Update(ctx, func(tx kv.Tx) (err error) {
		ent := kv.Entity{
			PK:        kv.EncID(edp.GetID()),
			UniqueKey: kv.Encode(kv.EncID(edp.GetOrgID()), kv.EncString(edp.GetName())),
			Body:      edp,
		}
		return s.endpointStore.Put(ctx, tx, ent)
	})
}

// FindNotificationEndpointByID returns a single notification endpoint by ID.
func (s *Store) FindNotificationEndpointByID(ctx context.Context, id platform.ID) (influxdb.NotificationEndpoint, error) {
	var (
		edp influxdb.NotificationEndpoint
		err error
	)

	err = s.kv.View(ctx, func(tx kv.Tx) error {
		edp, err = s.findNotificationEndpointByID(ctx, tx, id)
		return err
	})

	return edp, err
}

func (s *Store) findNotificationEndpointByID(ctx context.Context, tx kv.Tx, id platform.ID) (influxdb.NotificationEndpoint, error) {
	decodedEnt, err := s.endpointStore.FindEnt(ctx, tx, kv.Entity{PK: kv.EncID(id)})
	if err != nil {
		return nil, err
	}
	edp, ok := decodedEnt.(influxdb.NotificationEndpoint)
	return edp, kv.IsErrUnexpectedDecodeVal(ok)
}

// FindNotificationEndpoints returns a list of notification endpoints that match isNext and the total count of matching notification endpoints.
// Additional options provide pagination & sorting.
func (s *Store) FindNotificationEndpoints(ctx context.Context, filter influxdb.NotificationEndpointFilter, opt ...influxdb.FindOptions) (edps []influxdb.NotificationEndpoint, n int, err error) {
	err = s.kv.View(ctx, func(tx kv.Tx) error {
		edps, n, err = s.findNotificationEndpoints(ctx, tx, filter, opt...)
		return err
	})
	return edps, n, err
}

func (s *Store) findNotificationEndpoints(ctx context.Context, tx kv.Tx, filter influxdb.NotificationEndpointFilter, opt ...influxdb.FindOptions) ([]influxdb.NotificationEndpoint, int, error) {
	var o influxdb.FindOptions
	if len(opt) > 0 {
		o = opt[0]
	}

	edps := make([]influxdb.NotificationEndpoint, 0)
	err := s.endpointStore.Find(ctx, tx, kv.FindOpts{
		Descending:  o.Descending,
		Offset:      o.Offset,
		Limit:       o.Limit,
		FilterEntFn: filterEndpointsFn(filter),
		CaptureFn: func(k []byte, v interface{}) error {
			edp, ok := v.(influxdb.NotificationEndpoint)
			if err := kv.IsErrUnexpectedDecodeVal(ok); err != nil {
				return err
			}
			edps = append(edps, edp)
			return nil
		},
	})
	if err != nil {
		return nil, 0, err
	}

	return edps, len(edps), err
}

func filterEndpointsFn(filter influxdb.NotificationEndpointFilter) func([]byte, interface{}) bool {
	return func(key []byte, val interface{}) bool {
		edp := val.(influxdb.NotificationEndpoint)
		if filter.ID != nil && edp.GetID() != *filter.ID {
			return false
		}

		if filter.OrgID != nil && edp.GetOrgID() != *filter.OrgID {
			return false
		}

		return true
	}
}

// DeleteNotificationEndpoint removes a notification endpoint by ID.
func (s *Store) DeleteNotificationEndpoint(ctx context.Context, id platform.ID) (flds []influxdb.SecretField, orgID platform.ID, err error) {
	err = s.kv.Update(ctx, func(tx kv.Tx) error {
		flds, orgID, err = s.deleteNotificationEndpoint(ctx, tx, id)
		return err
	})
	return flds, orgID, err
}

func (s *Store) deleteNotificationEndpoint(ctx context.Context, tx kv.Tx, id platform.ID) (flds []influxdb.SecretField, orgID platform.ID, err error) {
	edp, err := s.findNotificationEndpointByID(ctx, tx, id)
	if err != nil {
		return nil, 0, err
	}

	if err := s.endpointStore.DeleteEnt(ctx, tx, kv.Entity{PK: kv.EncID(id)}); err != nil {
		return nil, 0, err
	}

	return edp.SecretFields(), edp.GetOrgID(), nil
}

package kv

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/influxdata/influxdb/kit/tracing"
	"github.com/influxdata/influxdb/notification/endpoint"

	"github.com/influxdata/influxdb"
)

var (
	notificationEndpointBucket = []byte("notificationEndpointv1")
	notificationEndpointIndex  = []byte("notificationEndpointIndexv1")

	// ErrNotificationEndpointNotFound is used when the notification endpoint is not found.
	ErrNotificationEndpointNotFound = &influxdb.Error{
		Msg:  "notification endpoint not found",
		Code: influxdb.ENotFound,
	}

	// ErrInvalidNotificationEndpointID is used when the service was provided
	// an invalid ID format.
	ErrInvalidNotificationEndpointID = &influxdb.Error{
		Code: influxdb.EInvalid,
		Msg:  "provided notification endpoint ID has invalid format",
	}
)

var _ influxdb.NotificationEndpointService = (*Service)(nil)

func (s *Service) initializeNotificationEndpoint(ctx context.Context, tx Tx) error {
	if _, err := s.notificationEndpointBucket(tx); err != nil {
		return err
	}
	if _, err := s.notificationEndpointIndexBucket(tx); err != nil {
		return err
	}
	return nil
}

// UnavailableNotificationEndpointStoreError is used if we aren't able to interact with the
// store, it means the store is not available at the moment (e.g. network).
func UnavailableNotificationEndpointStoreError(err error) *influxdb.Error {
	return &influxdb.Error{
		Code: influxdb.EInternal,
		Msg:  fmt.Sprintf("Unable to connect to notification endpoint store service. Please try again; Err: %v", err),
		Op:   "kv/notificationEndpoint",
	}
}

// UnavailableNotificationEndpointIndexError is used when the error comes from an internal system.
func UnavailableNotificationEndpointIndexError(err error) *influxdb.Error {
	return &influxdb.Error{
		Code: influxdb.EInternal,
		Msg:  fmt.Sprintf("unexpected error retrieving notification endpoint's index bucket; Err %v", err),
		Op:   "kv/notificationEndpointIndex",
	}
}

// InternalNotificationEndpointStoreError is used when the error comes from an
// internal system.
func InternalNotificationEndpointStoreError(err error) *influxdb.Error {
	return &influxdb.Error{
		Code: influxdb.EInternal,
		Msg:  fmt.Sprintf("Unknown internal notification endpoint data error; Err: %v", err),
		Op:   "kv/notificationEndpoint",
	}
}

func (s *Service) notificationEndpointBucket(tx Tx) (Bucket, error) {
	b, err := tx.Bucket(notificationEndpointBucket)
	if err != nil {
		return nil, UnavailableNotificationEndpointStoreError(err)
	}
	return b, nil
}

func (s *Service) notificationEndpointIndexBucket(tx Tx) (Bucket, error) {
	b, err := tx.Bucket(notificationEndpointIndex)
	if err != nil {
		return nil, UnavailableNotificationEndpointIndexError(err)
	}
	return b, nil
}

// CreateNotificationEndpoint creates a new notification endpoint and sets b.ID with the new identifier.
func (s *Service) CreateNotificationEndpoint(ctx context.Context, edp influxdb.NotificationEndpoint, userID influxdb.ID) error {
	return s.kv.Update(ctx, func(tx Tx) error {
		return s.createNotificationEndpoint(ctx, tx, edp, userID)
	})
}

func (s *Service) createNotificationEndpoint(ctx context.Context, tx Tx, edp influxdb.NotificationEndpoint, userID influxdb.ID) error {
	if edp.GetOrgID().Valid() {
		span, ctx := tracing.StartSpanFromContext(ctx)
		defer span.Finish()

		if _, err := s.findOrganizationByID(ctx, tx, edp.GetOrgID()); err != nil {
			return err
		}
	}
	// notification endpoint name unique
	if _, err := s.findNotificationEndpointByName(ctx, tx, edp.GetOrgID(), edp.GetName()); err == nil {
		return &influxdb.Error{
			Code: influxdb.EConflict,
			Msg:  fmt.Sprintf("notification endpoint with name %s already exists", edp.GetName()),
		}
	}
	id := s.IDGenerator.ID()
	edp.SetID(id)
	now := s.TimeGenerator.Now()
	edp.SetCreatedAt(now)
	edp.SetUpdatedAt(now)
	edp.BackfillSecretKeys()

	if err := s.putNotificationEndpoint(ctx, tx, edp); err != nil {
		return err
	}

	urm := &influxdb.UserResourceMapping{
		ResourceID:   edp.GetID(),
		UserID:       userID,
		UserType:     influxdb.Owner,
		ResourceType: influxdb.NotificationEndpointResourceType,
	}
	return s.createUserResourceMapping(ctx, tx, urm)
}

func (s *Service) findNotificationEndpointByName(ctx context.Context, tx Tx, orgID influxdb.ID, n string) (influxdb.NotificationEndpoint, error) {
	span, _ := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	key, err := notificationEndpointIndexKey(orgID, n)
	if err != nil {
		return nil, &influxdb.Error{
			Code: influxdb.EInvalid,
			Err:  err,
		}
	}

	idx, err := s.notificationEndpointIndexBucket(tx)
	if err != nil {
		return nil, err
	}

	buf, err := idx.Get(key)
	if IsNotFound(err) {
		return nil, &influxdb.Error{
			Code: influxdb.ENotFound,
			Msg:  fmt.Sprintf("notification endpoint %q not found", n),
		}
	}

	if err != nil {
		return nil, err
	}

	var id influxdb.ID
	if err := id.Decode(buf); err != nil {
		return nil, &influxdb.Error{
			Err: err,
		}
	}
	edp, _, _, err := s.findNotificationEndpointByID(tx, id)
	return edp, err
}

// UpdateNotificationEndpoint updates a single notification endpoint.
// Returns the new notification endpoint after update.
func (s *Service) UpdateNotificationEndpoint(ctx context.Context, id influxdb.ID, edp influxdb.NotificationEndpoint, userID influxdb.ID) (influxdb.NotificationEndpoint, error) {
	var err error
	err = s.kv.Update(ctx, func(tx Tx) error {
		edp, err = s.updateNotificationEndpoint(ctx, tx, id, edp, userID)
		return err
	})
	return edp, err
}

func (s *Service) updateNotificationEndpoint(ctx context.Context, tx Tx, id influxdb.ID, edp influxdb.NotificationEndpoint, userID influxdb.ID) (influxdb.NotificationEndpoint, error) {
	current, _, _, err := s.findNotificationEndpointByID(tx, id)
	if err != nil {
		return nil, err
	}

	if edp.GetName() != current.GetName() {
		edp0, err := s.findNotificationEndpointByName(ctx, tx, current.GetOrgID(), edp.GetName())
		if err == nil && edp0.GetID() != id {
			return nil, &influxdb.Error{
				Code: influxdb.EConflict,
				Msg:  "notification endpoint name is not unique",
			}
		}

		key, err := notificationEndpointIndexKey(current.GetOrgID(), current.GetName())
		if err != nil {
			return nil, err
		}

		idx, err := s.notificationEndpointIndexBucket(tx)
		if err != nil {
			return nil, err
		}

		if err := idx.Delete(key); err != nil {
			return nil, err
		}
	}

	// ID and OrganizationID can not be updated
	edp.SetID(current.GetID())
	edp.SetOrgID(current.GetOrgID())
	edp.SetCreatedAt(current.GetCRUDLog().CreatedAt)
	edp.SetUpdatedAt(s.TimeGenerator.Now())
	return edp, s.putNotificationEndpoint(ctx, tx, edp)
}

// PatchNotificationEndpoint updates a single  notification endpoint with changeset.
// Returns the new notification endpoint state after update.
func (s *Service) PatchNotificationEndpoint(ctx context.Context, id influxdb.ID, upd influxdb.NotificationEndpointUpdate) (influxdb.NotificationEndpoint, error) {
	var edp influxdb.NotificationEndpoint
	if err := s.kv.Update(ctx, func(tx Tx) (err error) {
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

func (s *Service) patchNotificationEndpoint(ctx context.Context, tx Tx, id influxdb.ID, upd influxdb.NotificationEndpointUpdate) (influxdb.NotificationEndpoint, error) {
	edp, _, _, err := s.findNotificationEndpointByID(tx, id)
	if err != nil {
		return nil, err
	}
	if upd.Name != nil {
		edp0, err := s.findNotificationEndpointByName(ctx, tx, edp.GetOrgID(), *upd.Name)
		if err == nil && edp0.GetID() != id {
			return nil, &influxdb.Error{
				Code: influxdb.EConflict,
				Msg:  "notification endpoint name is not unique",
			}
		}
		key, err := notificationEndpointIndexKey(edp.GetOrgID(), edp.GetName())
		if err != nil {
			return nil, err
		}
		idx, err := s.notificationEndpointIndexBucket(tx)
		if err != nil {
			return nil, err
		}
		if err := idx.Delete(key); err != nil {
			return nil, err
		}
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
	err = s.putNotificationEndpoint(ctx, tx, edp)
	if err != nil {
		return nil, err
	}

	return edp, nil
}

// PutNotificationEndpoint put a notification endpoint to storage.
func (s *Service) PutNotificationEndpoint(ctx context.Context, edp influxdb.NotificationEndpoint) error {
	return s.kv.Update(ctx, func(tx Tx) (err error) {
		return s.putNotificationEndpoint(ctx, tx, edp)
	})
}

func (s *Service) putNotificationEndpoint(ctx context.Context, tx Tx, edp influxdb.NotificationEndpoint) error {
	if err := edp.Valid(); err != nil {
		return err
	}
	encodedID, _ := edp.GetID().Encode()

	endpointBytes, err := json.Marshal(edp)
	if err != nil {
		return err
	}

	key, err := notificationEndpointIndexKey(edp.GetOrgID(), edp.GetName())
	if err != nil {
		return err
	}

	idx, err := s.notificationEndpointIndexBucket(tx)
	if err != nil {
		return err
	}

	if err := idx.Put(key, encodedID); err != nil {
		return &influxdb.Error{
			Err: err,
		}
	}

	bucket, err := s.notificationEndpointBucket(tx)
	if err != nil {
		return err
	}

	if err := bucket.Put(encodedID, endpointBytes); err != nil {
		return UnavailableNotificationEndpointStoreError(err)
	}

	return nil
}

// notificationEndpointIndexKey is a combination of the orgID and the notification endpoint name.
func notificationEndpointIndexKey(orgID influxdb.ID, name string) ([]byte, error) {
	orgIDEncoded, err := orgID.Encode()
	if err != nil {
		return nil, &influxdb.Error{
			Code: influxdb.EInvalid,
			Err:  err,
		}
	}
	k := make([]byte, influxdb.IDLength+len(name))
	copy(k, orgIDEncoded)
	copy(k[influxdb.IDLength:], name)
	return k, nil
}

// FindNotificationEndpointByID returns a single notification endpoint by ID.
func (s *Service) FindNotificationEndpointByID(ctx context.Context, id influxdb.ID) (influxdb.NotificationEndpoint, error) {
	var (
		edp influxdb.NotificationEndpoint
		err error
	)

	err = s.kv.View(ctx, func(tx Tx) error {
		edp, _, _, err = s.findNotificationEndpointByID(tx, id)
		return err
	})

	return edp, err
}

func (s *Service) findNotificationEndpointByID(tx Tx, id influxdb.ID) (influxdb.NotificationEndpoint, []byte, Bucket, error) {
	encID, err := id.Encode()
	if err != nil {
		return nil, nil, nil, ErrInvalidNotificationEndpointID
	}

	bucket, err := s.notificationEndpointBucket(tx)
	if err != nil {
		return nil, nil, nil, err
	}

	v, err := bucket.Get(encID)
	if IsNotFound(err) {
		return nil, nil, nil, ErrNotificationEndpointNotFound
	}
	if err != nil {
		return nil, nil, nil, InternalNotificationEndpointStoreError(err)
	}

	edp, err := endpoint.UnmarshalJSON(v)
	if err != nil {
		return nil, nil, nil, err
	}
	return edp, encID, bucket, err
}

// FindNotificationEndpoints returns a list of notification endpoints that match filter and the total count of matching notification endpoints.
// Additional options provide pagination & sorting.
func (s *Service) FindNotificationEndpoints(ctx context.Context, filter influxdb.NotificationEndpointFilter, opt ...influxdb.FindOptions) (edps []influxdb.NotificationEndpoint, n int, err error) {
	err = s.kv.View(ctx, func(tx Tx) error {
		edps, n, err = s.findNotificationEndpoints(ctx, tx, filter, opt...)
		return err
	})
	return edps, n, err
}

func (s *Service) findNotificationEndpoints(ctx context.Context, tx Tx, filter influxdb.NotificationEndpointFilter, opt ...influxdb.FindOptions) ([]influxdb.NotificationEndpoint, int, error) {
	edps := make([]influxdb.NotificationEndpoint, 0)
	m, err := s.findUserResourceMappings(ctx, tx, filter.UserResourceMappingFilter)
	if err != nil {
		return nil, 0, err
	}

	if len(m) == 0 {
		return edps, 0, nil
	}

	idMap := make(map[influxdb.ID]bool)
	for _, item := range m {
		idMap[item.ResourceID] = true
	}

	if filter.Org != nil {
		o, err := s.findOrganizationByName(ctx, tx, *filter.Org)
		if err != nil {
			return nil, 0, &influxdb.Error{
				Err: err,
			}
		}
		filter.OrgID = &o.ID
	}

	var offset, limit, count int
	var descending bool
	if len(opt) > 0 {
		offset = opt[0].Offset
		limit = opt[0].Limit
		descending = opt[0].Descending
	}
	filterFn := filterNotificationEndpointsFn(idMap, filter)
	err = s.forEachNotificationEndpoint(ctx, tx, descending, func(edp influxdb.NotificationEndpoint) bool {
		if filterFn(edp) {
			if count >= offset {
				edps = append(edps, edp)
			}
			count++
		}

		if limit > 0 && len(edps) >= limit {
			return false
		}

		return true
	})

	return edps, len(edps), err
}

// forEachNotificationEndpoint will iterate through all notification endpoints while fn returns true.
func (s *Service) forEachNotificationEndpoint(ctx context.Context, tx Tx, descending bool, fn func(influxdb.NotificationEndpoint) bool) error {

	bkt, err := s.notificationEndpointBucket(tx)
	if err != nil {
		return err
	}

	cur, err := bkt.Cursor()
	if err != nil {
		return err
	}

	var k, v []byte
	if descending {
		k, v = cur.Last()
	} else {
		k, v = cur.First()
	}

	for k != nil {
		edp, err := endpoint.UnmarshalJSON(v)
		if err != nil {
			return err
		}
		if !fn(edp) {
			break
		}

		if descending {
			k, v = cur.Prev()
		} else {
			k, v = cur.Next()
		}
	}

	return nil
}

func filterNotificationEndpointsFn(idMap map[influxdb.ID]bool, filter influxdb.NotificationEndpointFilter) func(edp influxdb.NotificationEndpoint) bool {
	return func(edp influxdb.NotificationEndpoint) bool {
		if filter.ID != nil {
			if edp.GetID() != *filter.ID {
				return false
			}
		}

		if filter.OrgID != nil {
			if edp.GetOrgID() != *filter.OrgID {
				return false
			}
		}
		if idMap == nil {
			return true
		}
		return idMap[edp.GetID()]
	}
}

// DeleteNotificationEndpoint removes a notification endpoint by ID.
func (s *Service) DeleteNotificationEndpoint(ctx context.Context, id influxdb.ID) (flds []influxdb.SecretField, orgID influxdb.ID, err error) {
	err = s.kv.Update(ctx, func(tx Tx) error {
		flds, orgID, err = s.deleteNotificationEndpoint(ctx, tx, id)
		return err
	})
	return flds, orgID, err
}

func (s *Service) deleteNotificationEndpoint(ctx context.Context, tx Tx, id influxdb.ID) (flds []influxdb.SecretField, orgID influxdb.ID, err error) {
	edp, encID, bucket, err := s.findNotificationEndpointByID(tx, id)
	if err != nil {
		return nil, 0, err
	}

	if err = bucket.Delete(encID); err != nil {
		return nil, 0, InternalNotificationEndpointStoreError(err)
	}

	return edp.SecretFields(), edp.GetOrgID(), s.deleteUserResourceMappings(ctx, tx, influxdb.UserResourceMappingFilter{
		ResourceID:   id,
		ResourceType: influxdb.NotificationEndpointResourceType,
	})
}

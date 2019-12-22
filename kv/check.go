package kv

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/kit/tracing"
	"github.com/influxdata/influxdb/notification/check"
)

var (
	checkBucket = []byte("checksv1")
	checkIndex  = []byte("checkindexv1")
)

var _ influxdb.CheckService = (*Service)(nil)

func (s *Service) initializeChecks(ctx context.Context, tx Tx) error {
	if _, err := s.checksBucket(tx); err != nil {
		return err
	}
	if _, err := s.checksIndexBucket(tx); err != nil {
		return err
	}
	return nil
}

// UnexpectedCheckError is used when the error comes from an internal system.
func UnexpectedCheckError(err error) *influxdb.Error {
	return &influxdb.Error{
		Code: influxdb.EInternal,
		Msg:  fmt.Sprintf("unexpected error retrieving check's bucket; Err %v", err),
		Op:   "kv/checkBucket",
	}
}

// UnexpectedCheckIndexError is used when the error comes from an internal system.
func UnexpectedCheckIndexError(err error) *influxdb.Error {
	return &influxdb.Error{
		Code: influxdb.EInternal,
		Msg:  fmt.Sprintf("unexpected error retrieving check's index bucket; Err %v", err),
		Op:   "kv/checkIndex",
	}
}

func (s *Service) checksBucket(tx Tx) (Bucket, error) {
	b, err := tx.Bucket(checkBucket)
	if err != nil {
		return nil, UnexpectedCheckError(err)
	}

	return b, nil
}

func (s *Service) checksIndexBucket(tx Tx) (Bucket, error) {
	b, err := tx.Bucket(checkIndex)
	if err != nil {
		return nil, UnexpectedCheckIndexError(err)
	}

	return b, nil
}

// FindCheckByID retrieves a check by id.
func (s *Service) FindCheckByID(ctx context.Context, id influxdb.ID) (influxdb.Check, error) {
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	var c influxdb.Check
	var err error

	err = s.kv.View(ctx, func(tx Tx) error {
		chk, pe := s.findCheckByID(ctx, tx, id)
		if pe != nil {
			err = pe
			return err
		}
		c = chk
		return nil
	})

	if err != nil {
		return nil, err
	}

	return c, nil
}

func (s *Service) findCheckByID(ctx context.Context, tx Tx, id influxdb.ID) (influxdb.Check, error) {
	span, _ := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	encodedID, err := id.Encode()
	if err != nil {
		return nil, &influxdb.Error{
			Code: influxdb.EInvalid,
			Err:  err,
		}
	}

	bkt, err := s.checksBucket(tx)
	if err != nil {
		return nil, err
	}

	v, err := bkt.Get(encodedID)
	if IsNotFound(err) {
		return nil, &influxdb.Error{
			Code: influxdb.ENotFound,
			Msg:  "check not found",
		}
	}

	if err != nil {
		return nil, err
	}

	c, err := check.UnmarshalJSON(v)
	if err != nil {
		return nil, &influxdb.Error{
			Err: err,
		}
	}

	return c, nil
}

// FindCheck retrives a check using an arbitrary check filter.
// Filters using ID, or OrganizationID and check Name should be efficient.
// Other filters will do a linear scan across checks until it finds a match.
func (s *Service) FindCheck(ctx context.Context, filter influxdb.CheckFilter) (influxdb.Check, error) {
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	var c influxdb.Check
	var err error

	if filter.ID != nil {
		c, err = s.FindCheckByID(ctx, *filter.ID)
		if err != nil {
			return nil, &influxdb.Error{
				Err: err,
			}
		}
		return c, nil
	}

	err = s.kv.View(ctx, func(tx Tx) error {
		if filter.Org != nil {
			o, err := s.findOrganizationByName(ctx, tx, *filter.Org)
			if err != nil {
				return err
			}
			filter.OrgID = &o.ID
		}

		filterFn := filterChecksFn(nil, filter)
		return s.forEachCheck(ctx, tx, false, func(chk influxdb.Check) bool {
			if filterFn(chk) {
				c = chk
				return false
			}
			return true
		})
	})

	if err != nil {
		return nil, &influxdb.Error{
			Err: err,
		}
	}

	if c == nil {
		return nil, &influxdb.Error{
			Code: influxdb.ENotFound,
			Msg:  "check not found",
		}
	}

	return c, nil
}

func filterChecksFn(idMap map[influxdb.ID]bool, filter influxdb.CheckFilter) func(c influxdb.Check) bool {
	return func(c influxdb.Check) bool {
		if filter.ID != nil {
			if c.GetID() != *filter.ID {
				return false
			}
		}
		if filter.Name != nil {
			if c.GetName() != *filter.Name {
				return false
			}
		}
		if filter.OrgID != nil {
			if c.GetOrgID() != *filter.OrgID {
				return false
			}
		}
		if idMap == nil {
			return true
		}
		return idMap[c.GetID()]
	}
}

// FindChecks retrives all checks that match an arbitrary check filter.
// Filters using ID, or OrganizationID and check Name should be efficient.
// Other filters will do a linear scan across all checks searching for a match.
func (s *Service) FindChecks(ctx context.Context, filter influxdb.CheckFilter, opts ...influxdb.FindOptions) ([]influxdb.Check, int, error) {
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	if filter.ID != nil {
		c, err := s.FindCheckByID(ctx, *filter.ID)
		if err != nil {
			return nil, 0, err
		}

		return []influxdb.Check{c}, 1, nil
	}

	cs := []influxdb.Check{}
	err := s.kv.View(ctx, func(tx Tx) error {
		chks, err := s.findChecks(ctx, tx, filter, opts...)
		if err != nil {
			return err
		}
		cs = chks
		return nil
	})

	if err != nil {
		return nil, 0, err
	}

	return cs, len(cs), nil
}

func (s *Service) findChecks(ctx context.Context, tx Tx, filter influxdb.CheckFilter, opts ...influxdb.FindOptions) ([]influxdb.Check, error) {
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	cs := []influxdb.Check{}
	m, err := s.findUserResourceMappings(ctx, tx, filter.UserResourceMappingFilter)
	if err != nil {
		return nil, err
	}

	if len(m) == 0 {
		return cs, nil
	}

	idMap := make(map[influxdb.ID]bool)
	for _, item := range m {
		idMap[item.ResourceID] = true
	}
	if filter.Org != nil {
		o, err := s.findOrganizationByName(ctx, tx, *filter.Org)
		if err != nil {
			return nil, &influxdb.Error{
				Err: err,
			}
		}
		filter.OrgID = &o.ID
	}

	var offset, limit, count int
	var descending bool
	if len(opts) > 0 {
		offset = opts[0].Offset
		limit = opts[0].Limit
		descending = opts[0].Descending
	}

	filterFn := filterChecksFn(idMap, filter)
	err = s.forEachCheck(ctx, tx, descending, func(c influxdb.Check) bool {
		if filterFn(c) {
			if count >= offset {
				cs = append(cs, c)
			}
			count++
		}

		if limit > 0 && len(cs) >= limit {
			return false
		}

		return true
	})

	if err != nil {
		return nil, &influxdb.Error{
			Err: err,
		}
	}

	return cs, nil
}

// CreateCheck creates a influxdb check and sets ID.
func (s *Service) CreateCheck(ctx context.Context, c influxdb.CheckCreate, userID influxdb.ID) error {
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	return s.kv.Update(ctx, func(tx Tx) error {
		return s.createCheck(ctx, tx, c, userID)
	})
}

func (s *Service) createCheck(ctx context.Context, tx Tx, c influxdb.CheckCreate, userID influxdb.ID) error {
	if c.GetOrgID().Valid() {
		span, ctx := tracing.StartSpanFromContext(ctx)
		defer span.Finish()

		_, pe := s.findOrganizationByID(ctx, tx, c.GetOrgID())
		if pe != nil {
			return &influxdb.Error{
				Op:  influxdb.OpCreateCheck,
				Err: pe,
			}
		}
	}
	// check name unique
	if _, err := s.findCheckByName(ctx, tx, c.GetOrgID(), c.GetName()); err == nil {
		if err == nil {
			return &influxdb.Error{
				Code: influxdb.EConflict,
				Msg:  fmt.Sprintf("check with name %s already exists", c.GetName()),
			}
		}
	}
	c.SetID(s.IDGenerator.ID())
	c.SetOwnerID(userID)
	now := s.Now()
	c.SetCreatedAt(now)
	c.SetUpdatedAt(now)
	if err := c.Valid(); err != nil {
		return err
	}
	t, err := s.createCheckTask(ctx, tx, c)
	if err != nil {
		return err
	}
	c.SetTaskID(t.ID)
	if err := c.Status.Valid(); err != nil {
		return err
	}
	if err := s.putCheck(ctx, tx, c); err != nil {
		return err
	}
	if err := s.createCheckUserResourceMappings(ctx, tx, c); err != nil {
		return err
	}
	return nil
}

func (s *Service) createCheckTask(ctx context.Context, tx Tx, c influxdb.CheckCreate) (*influxdb.Task, error) {
	script, err := c.GenerateFlux()
	if err != nil {
		return nil, err
	}

	tc := influxdb.TaskCreate{
		Type:           c.Type(),
		Flux:           script,
		OwnerID:        c.GetOwnerID(),
		OrganizationID: c.GetOrgID(),
		Status:         string(c.Status),
	}

	t, err := s.createTask(ctx, tx, tc)
	if err != nil {
		return nil, err
	}

	return t, nil
}

// PutCheck will put a check without setting an ID.
func (s *Service) PutCheck(ctx context.Context, c influxdb.Check) error {
	return s.kv.Update(ctx, func(tx Tx) error {
		if err := c.Valid(); err != nil {
			return err
		}

		return s.putCheck(ctx, tx, c)
	})
}

func (s *Service) createCheckUserResourceMappings(ctx context.Context, tx Tx, c influxdb.Check) error {
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	ms, err := s.findUserResourceMappings(ctx, tx, influxdb.UserResourceMappingFilter{
		ResourceType: influxdb.OrgsResourceType,
		ResourceID:   c.GetOrgID(),
	})
	if err != nil {
		return &influxdb.Error{
			Err: err,
		}
	}

	for _, m := range ms {
		if err := s.createUserResourceMapping(ctx, tx, &influxdb.UserResourceMapping{
			ResourceType: influxdb.ChecksResourceType,
			ResourceID:   c.GetID(),
			UserID:       m.UserID,
			UserType:     m.UserType,
		}); err != nil {
			return &influxdb.Error{
				Err: err,
			}
		}
	}

	return nil
}

func (s *Service) putCheck(ctx context.Context, tx Tx, c influxdb.Check) error {
	span, _ := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	v, err := json.Marshal(c)
	if err != nil {
		return &influxdb.Error{
			Err: err,
		}
	}

	encodedID, err := c.GetID().Encode()
	if err != nil {
		return &influxdb.Error{
			Err: err,
		}
	}
	key, pe := checkIndexKey(c.GetOrgID(), c.GetName())
	if err != nil {
		return pe
	}

	idx, err := s.checksIndexBucket(tx)
	if err != nil {
		return err
	}

	if err := idx.Put(key, encodedID); err != nil {
		return &influxdb.Error{
			Err: err,
		}
	}

	bkt, err := s.checksBucket(tx)
	if bkt.Put(encodedID, v); err != nil {
		return &influxdb.Error{
			Err: err,
		}
	}
	return nil
}

// checkIndexKey is a combination of the orgID and the check name.
func checkIndexKey(orgID influxdb.ID, name string) ([]byte, error) {
	orgIDEncoded, err := orgID.Encode()
	if err != nil {
		return nil, &influxdb.Error{
			Code: influxdb.EInvalid,
			Err:  err,
		}
	}
	k := make([]byte, influxdb.IDLength+len(name))
	copy(k, orgIDEncoded)
	copy(k[influxdb.IDLength:], []byte(name))
	return k, nil
}

// forEachCheck will iterate through all checks while fn returns true.
func (s *Service) forEachCheck(ctx context.Context, tx Tx, descending bool, fn func(influxdb.Check) bool) error {
	span, _ := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	bkt, err := s.checksBucket(tx)
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
		c, err := check.UnmarshalJSON(v)
		if err != nil {
			return err
		}
		if !fn(c) {
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

// PatchCheck updates a check according the parameters set on upd.
func (s *Service) PatchCheck(ctx context.Context, id influxdb.ID, upd influxdb.CheckUpdate) (influxdb.Check, error) {
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	var c influxdb.Check
	err := s.kv.Update(ctx, func(tx Tx) error {
		chk, err := s.patchCheck(ctx, tx, id, upd)
		if err != nil {
			return err
		}
		c = chk
		return nil
	})

	return c, err
}

// UpdateCheck updates the check.
func (s *Service) UpdateCheck(ctx context.Context, id influxdb.ID, chk influxdb.CheckCreate) (influxdb.Check, error) {
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	var c influxdb.Check
	err := s.kv.Update(ctx, func(tx Tx) error {
		chk, err := s.updateCheck(ctx, tx, id, chk)
		if err != nil {
			return err
		}
		c = chk
		return nil
	})

	return c, err
}

func (s *Service) updateCheck(ctx context.Context, tx Tx, id influxdb.ID, chk influxdb.CheckCreate) (influxdb.Check, error) {
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	current, err := s.findCheckByID(ctx, tx, id)
	if err != nil {
		return nil, err
	}

	if chk.GetName() != current.GetName() {
		c0, err := s.findCheckByName(ctx, tx, current.GetOrgID(), chk.GetName())
		if err == nil && c0.GetID() != id {
			return nil, &influxdb.Error{
				Code: influxdb.EConflict,
				Msg:  "check name is not unique",
			}
		}
		key, err := checkIndexKey(current.GetOrgID(), current.GetName())
		if err != nil {
			return nil, err
		}
		idx, err := s.checksIndexBucket(tx)
		if err != nil {
			return nil, err
		}
		if err := idx.Delete(key); err != nil {
			return nil, err
		}
	}

	chk.SetTaskID(current.GetTaskID())
	flux, err := chk.GenerateFlux()
	if err != nil {
		return nil, err
	}

	tu := influxdb.TaskUpdate{
		Flux:        &flux,
		Description: strPtr(chk.GetDescription()),
	}

	if chk.Status != "" {
		tu.Status = strPtr(string(chk.Status))
	}

	if _, err := s.updateTask(ctx, tx, chk.GetTaskID(), tu); err != nil {
		return nil, err
	}

	// ID and OrganizationID can not be updated
	chk.SetID(current.GetID())
	chk.SetOrgID(current.GetOrgID())
	chk.SetOwnerID(current.GetOwnerID())
	chk.SetCreatedAt(current.GetCRUDLog().CreatedAt)
	chk.SetUpdatedAt(s.Now())

	if err := chk.Valid(); err != nil {
		return nil, err
	}

	if err := chk.Status.Valid(); err != nil {
		return nil, err
	}

	if err := s.putCheck(ctx, tx, chk.Check); err != nil {
		return nil, err
	}

	return chk.Check, nil
}

func strPtr(s string) *string {
	ss := new(string)
	*ss = s
	return ss
}

func (s *Service) patchCheck(ctx context.Context, tx Tx, id influxdb.ID, upd influxdb.CheckUpdate) (influxdb.Check, error) {
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	c, err := s.findCheckByID(ctx, tx, id)
	if err != nil {
		return nil, err
	}

	if upd.Name != nil {
		c0, err := s.findCheckByName(ctx, tx, c.GetOrgID(), *upd.Name)
		if err == nil && c0.GetID() != id {
			return nil, &influxdb.Error{
				Code: influxdb.EConflict,
				Msg:  "check name is not unique",
			}
		}
		key, err := checkIndexKey(c.GetOrgID(), c.GetName())
		if err != nil {
			return nil, err
		}
		idx, err := s.checksIndexBucket(tx)
		if err != nil {
			return nil, err
		}
		if err := idx.Delete(key); err != nil {
			return nil, err
		}
		c.SetName(*upd.Name)
	}

	if upd.Description != nil {
		c.SetDescription(*upd.Description)
	}

	c.SetUpdatedAt(s.Now())
	tu := influxdb.TaskUpdate{
		Description: strPtr(c.GetDescription()),
	}

	if upd.Status != nil {
		tu.Status = strPtr(string(*upd.Status))
	}

	if _, err := s.updateTask(ctx, tx, c.GetTaskID(), tu); err != nil {
		return nil, err
	}

	if err := c.Valid(); err != nil {
		return nil, err
	}

	if err := s.putCheck(ctx, tx, c); err != nil {
		return nil, err
	}

	return c, nil
}

func (s *Service) findCheckByName(ctx context.Context, tx Tx, orgID influxdb.ID, n string) (influxdb.Check, error) {
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	key, err := checkIndexKey(orgID, n)
	if err != nil {
		return nil, &influxdb.Error{
			Code: influxdb.EInvalid,
			Err:  err,
		}
	}

	idx, err := s.checksIndexBucket(tx)
	if err != nil {
		return nil, err
	}

	buf, err := idx.Get(key)
	if IsNotFound(err) {
		return nil, &influxdb.Error{
			Code: influxdb.ENotFound,
			Msg:  fmt.Sprintf("check %q not found", n),
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
	return s.findCheckByID(ctx, tx, id)
}

// DeleteCheck deletes a check and prunes it from the index.
func (s *Service) DeleteCheck(ctx context.Context, id influxdb.ID) error {
	return s.kv.Update(ctx, func(tx Tx) error {
		var err error
		if pe := s.deleteCheck(ctx, tx, id); pe != nil {
			err = pe
		}
		return err
	})
}

func (s *Service) deleteCheck(ctx context.Context, tx Tx, id influxdb.ID) error {
	c, pe := s.findCheckByID(ctx, tx, id)
	if pe != nil {
		return pe
	}

	if err := s.deleteTask(ctx, tx, c.GetTaskID()); err != nil {
		return err
	}

	key, pe := checkIndexKey(c.GetOrgID(), c.GetName())
	if pe != nil {
		return pe
	}

	idx, err := s.checksIndexBucket(tx)
	if err != nil {
		return err
	}

	if err := idx.Delete(key); err != nil {
		return &influxdb.Error{
			Err: err,
		}
	}

	encodedID, err := id.Encode()
	if err != nil {
		return &influxdb.Error{
			Code: influxdb.EInvalid,
			Err:  err,
		}
	}

	bkt, err := s.checksBucket(tx)
	if err != nil {
		return err
	}

	if err := bkt.Delete(encodedID); err != nil {
		return &influxdb.Error{
			Err: err,
		}
	}

	if err := s.deleteUserResourceMappings(ctx, tx, influxdb.UserResourceMappingFilter{
		ResourceID:   id,
		ResourceType: influxdb.ChecksResourceType,
	}); err != nil {
		return err
	}

	return nil
}

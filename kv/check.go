package kv

import (
	"context"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kit/tracing"
	"github.com/influxdata/influxdb/v2/notification/check"
)

var _ influxdb.CheckService = (*Service)(nil)

func newCheckStore() *IndexStore {
	const resource = "check"

	var decEndpointEntFn DecodeBucketValFn = func(key, val []byte) ([]byte, interface{}, error) {
		ch, err := check.UnmarshalJSON(val)
		return key, ch, err
	}

	var decValToEntFn ConvertValToEntFn = func(_ []byte, v interface{}) (Entity, error) {
		ch, ok := v.(influxdb.Check)
		if err := IsErrUnexpectedDecodeVal(ok); err != nil {
			return Entity{}, err
		}
		return Entity{
			PK:        EncID(ch.GetID()),
			UniqueKey: Encode(EncID(ch.GetOrgID()), EncString(ch.GetName())),
			Body:      ch,
		}, nil
	}

	return &IndexStore{
		Resource:   resource,
		EntStore:   NewStoreBase(resource, []byte("checksv1"), EncIDKey, EncBodyJSON, decEndpointEntFn, decValToEntFn),
		IndexStore: NewOrgNameKeyStore(resource, []byte("checkindexv1"), false),
	}
}

// FindCheckByID retrieves a check by id.
func (s *Service) FindCheckByID(ctx context.Context, id influxdb.ID) (influxdb.Check, error) {
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	var c influxdb.Check
	err := s.kv.View(ctx, func(tx Tx) error {
		chkVal, err := s.findCheckByID(ctx, tx, id)
		if err != nil {
			return err
		}
		c = chkVal
		return nil
	})
	if err != nil {
		return nil, err
	}

	return c, nil
}

func (s *Service) findCheckByID(ctx context.Context, tx Tx, id influxdb.ID) (influxdb.Check, error) {
	chkVal, err := s.checkStore.FindEnt(ctx, tx, Entity{PK: EncID(id)})
	if err != nil {
		return nil, err
	}
	return chkVal.(influxdb.Check), nil
}

func (s *Service) findCheckByName(ctx context.Context, tx Tx, orgID influxdb.ID, name string) (influxdb.Check, error) {
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	chVal, err := s.checkStore.FindEnt(ctx, tx, Entity{
		UniqueKey: Encode(EncID(orgID), EncString(name)),
	})
	if IsNotFound(err) {
		return nil, &influxdb.Error{
			Code: influxdb.ENotFound,
			Err:  err,
		}
	}
	if err != nil {
		return nil, err
	}

	return chVal.(influxdb.Check), nil
}

// FindCheck retrives a check using an arbitrary check filter.
// Filters using ID, or OrganizationID and check Name should be efficient.
// Other filters will do a linear scan across checks until it finds a match.
func (s *Service) FindCheck(ctx context.Context, filter influxdb.CheckFilter) (influxdb.Check, error) {
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	if filter.ID != nil {
		return s.FindCheckByID(ctx, *filter.ID)
	}

	if filter.Org != nil {
		o, err := s.FindOrganizationByName(ctx, *filter.Org)
		if err != nil {
			return nil, err
		}
		filter.OrgID = &o.ID
	}

	var c influxdb.Check
	err := s.kv.View(ctx, func(tx Tx) error {
		if filter.OrgID != nil && filter.Name != nil {
			ch, err := s.findCheckByName(ctx, tx, *filter.OrgID, *filter.Name)
			c = ch
			return err
		}

		var prefix []byte
		if filter.OrgID != nil {
			ent := Entity{UniqueKey: EncID(*filter.OrgID)}
			prefix, _ = s.checkStore.IndexStore.EntKey(ctx, ent)
		}
		filterFn := filterChecksFn(nil, filter)

		return s.checkStore.Find(ctx, tx, FindOpts{
			Prefix: prefix,
			Limit:  1,
			FilterEntFn: func(k []byte, v interface{}) bool {
				ch, ok := v.(influxdb.Check)
				if err := IsErrUnexpectedDecodeVal(ok); err != nil {
					return false
				}
				return filterFn(ch)
			},
			CaptureFn: func(key []byte, decodedVal interface{}) error {
				c, _ = decodedVal.(influxdb.Check)
				return nil
			},
		})
	})
	if err != nil {
		return nil, err
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
		if filter.ID != nil && c.GetID() != *filter.ID {
			return false
		}
		if filter.OrgID != nil && c.GetOrgID() != *filter.OrgID {
			return false
		}
		if filter.Name != nil && c.GetName() != *filter.Name {
			return false
		}
		if idMap == nil {
			return true
		}
		return idMap[c.GetID()]
	}
}

// FindChecks retrieves all checks that match an arbitrary check filter.
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

	m, _, err := s.FindUserResourceMappings(ctx, filter.UserResourceMappingFilter)
	if err != nil || len(m) == 0 {
		return nil, 0, err
	}
	idMap := make(map[influxdb.ID]bool)
	for _, item := range m {
		idMap[item.ResourceID] = true
	}

	var checks []influxdb.Check
	err = s.kv.View(ctx, func(tx Tx) error {
		if filter.Org != nil {
			o, err := s.findOrganizationByName(ctx, tx, *filter.Org)
			if err != nil {
				return &influxdb.Error{Err: err}
			}
			filter.OrgID = &o.ID
		}

		var opt influxdb.FindOptions
		if len(opts) > 0 {
			opt = opts[0]
		}

		filterFn := filterChecksFn(idMap, filter)
		return s.checkStore.Find(ctx, tx, FindOpts{
			Descending: opt.Descending,
			Offset:     opt.Offset,
			Limit:      opt.Limit,
			FilterEntFn: func(k []byte, v interface{}) bool {
				ch, ok := v.(influxdb.Check)
				if err := IsErrUnexpectedDecodeVal(ok); err != nil {
					return false
				}
				return filterFn(ch)
			},
			CaptureFn: func(key []byte, decodedVal interface{}) error {
				c, ok := decodedVal.(influxdb.Check)
				if err := IsErrUnexpectedDecodeVal(ok); err != nil {
					return err
				}
				checks = append(checks, c)
				return nil
			},
		})
	})
	if err != nil {
		return nil, 0, err
	}

	return checks, len(checks), nil
}

// CreateCheck creates a influxdb check and sets ID.
func (s *Service) CreateCheck(ctx context.Context, c influxdb.CheckCreate, userID influxdb.ID) error {
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	if err := c.Status.Valid(); err != nil {
		return err
	}

	if c.GetOrgID().Valid() {
		span, ctx := tracing.StartSpanFromContext(ctx)
		defer span.Finish()

		if _, err := s.FindOrganizationByID(ctx, c.GetOrgID()); err != nil {
			return &influxdb.Error{
				Code: influxdb.ENotFound,
				Op:   influxdb.OpCreateCheck,
				Err:  err,
			}
		}
	}

	return s.kv.Update(ctx, func(tx Tx) error {
		return s.createCheck(ctx, tx, c, userID)
	})
}

func (s *Service) createCheck(ctx context.Context, tx Tx, c influxdb.CheckCreate, userID influxdb.ID) error {
	c.SetID(s.IDGenerator.ID())
	c.SetOwnerID(userID)
	now := s.Now()
	c.SetCreatedAt(now)
	c.SetUpdatedAt(now)

	if err := c.Valid(s.FluxLanguageService); err != nil {
		return err
	}

	t, err := s.createCheckTask(ctx, tx, c)
	if err != nil {
		return &influxdb.Error{
			Code: influxdb.EInvalid,
			Msg:  "Could not create task from check",
			Err:  err,
		}
	}
	c.SetTaskID(t.ID)

	if err := s.putCheck(ctx, tx, c, PutNew()); err != nil {
		return err
	}

	return s.createUserResourceMappingForOrg(ctx, tx, c.GetOrgID(), c.GetID(), influxdb.ChecksResourceType)
}

func (s *Service) createCheckTask(ctx context.Context, tx Tx, c influxdb.CheckCreate) (*influxdb.Task, error) {
	script, err := c.GenerateFlux(s.FluxLanguageService)
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
	if err := c.Valid(s.FluxLanguageService); err != nil {
		return err
	}
	return s.kv.Update(ctx, func(tx Tx) error {
		return s.putCheck(ctx, tx, c)
	})
}

func (s *Service) putCheck(ctx context.Context, tx Tx, c influxdb.Check, opts ...PutOptionFn) error {
	return s.checkStore.Put(ctx, tx, Entity{
		PK:        EncID(c.GetID()),
		UniqueKey: Encode(EncID(c.GetOrgID()), EncString(c.GetName())),
		Body:      c,
	}, opts...)
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

		ent := Entity{
			UniqueKey: Encode(EncID(current.GetOrgID()), EncString(current.GetName())),
		}
		if err := s.checkStore.IndexStore.DeleteEnt(ctx, tx, ent); err != nil {
			return nil, err
		}
	}

	chk.SetTaskID(current.GetTaskID())
	flux, err := chk.GenerateFlux(s.FluxLanguageService)
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

	if err := chk.Valid(s.FluxLanguageService); err != nil {
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

func (s *Service) patchCheck(ctx context.Context, tx Tx, id influxdb.ID, upd influxdb.CheckUpdate) (influxdb.Check, error) {
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	c, err := s.findCheckByID(ctx, tx, id)
	if err != nil {
		return nil, err
	}

	if upd.Name != nil {
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

	if err := c.Valid(s.FluxLanguageService); err != nil {
		return nil, err
	}

	if err := s.putCheck(ctx, tx, c, PutUpdate()); err != nil {
		return nil, err
	}

	if _, err := s.updateTask(ctx, tx, c.GetTaskID(), tu); err != nil {
		return nil, err
	}

	return c, nil
}

// DeleteCheck deletes a check and prunes it from the index.
func (s *Service) DeleteCheck(ctx context.Context, id influxdb.ID) error {
	ch, err := s.FindCheckByID(ctx, id)
	if err != nil {
		return err
	}

	return s.kv.Update(ctx, func(tx Tx) error {
		err := s.checkStore.DeleteEnt(ctx, tx, Entity{
			PK: EncID(id),
		})
		if err != nil {
			return err
		}

		if err := s.deleteTask(ctx, tx, ch.GetTaskID()); err != nil {
			return err
		}

		return s.deleteUserResourceMappings(ctx, tx, influxdb.UserResourceMappingFilter{
			ResourceID:   id,
			ResourceType: influxdb.ChecksResourceType,
		})
	})
}

func strPtr(s string) *string {
	ss := new(string)
	*ss = s
	return ss
}

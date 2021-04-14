package checks

import (
	"context"
	"fmt"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/kit/platform/errors"
	"github.com/influxdata/influxdb/v2/kit/tracing"
	"github.com/influxdata/influxdb/v2/kv"
	"github.com/influxdata/influxdb/v2/notification/check"
	"github.com/influxdata/influxdb/v2/query/fluxlang"
	"github.com/influxdata/influxdb/v2/snowflake"
	"github.com/influxdata/influxdb/v2/task/taskmodel"
	"go.uber.org/zap"
)

var _ influxdb.CheckService = (*Service)(nil)

// Service is a check service
// It provides all the operations needed to manage checks
type Service struct {
	kv kv.Store

	log *zap.Logger

	orgs  influxdb.OrganizationService
	tasks taskmodel.TaskService

	timeGenerator influxdb.TimeGenerator
	idGenerator   platform.IDGenerator

	checkStore *kv.IndexStore
}

// NewService constructs and configures a new checks.Service
func NewService(logger *zap.Logger, store kv.Store, orgs influxdb.OrganizationService, tasks taskmodel.TaskService) *Service {
	return &Service{
		kv:    store,
		log:   logger,
		orgs:  orgs,
		tasks: tasks,

		timeGenerator: influxdb.RealTimeGenerator{},
		idGenerator:   snowflake.NewIDGenerator(),
		checkStore:    newCheckStore(),
	}
}

func newCheckStore() *kv.IndexStore {
	const resource = "check"

	var decEndpointEntFn kv.DecodeBucketValFn = func(key, val []byte) ([]byte, interface{}, error) {
		ch, err := check.UnmarshalJSON(val)
		return key, ch, err
	}

	var decValToEntFn kv.ConvertValToEntFn = func(_ []byte, v interface{}) (kv.Entity, error) {
		ch, ok := v.(influxdb.Check)
		if err := kv.IsErrUnexpectedDecodeVal(ok); err != nil {
			return kv.Entity{}, err
		}
		return kv.Entity{
			PK: kv.EncID(ch.GetID()),
			UniqueKey: kv.Encode(
				kv.EncID(ch.GetOrgID()),
				kv.EncString(ch.GetName()),
			),
			Body: ch,
		}, nil
	}

	return &kv.IndexStore{
		Resource: resource,
		EntStore: kv.NewStoreBase(
			resource,
			[]byte("checksv1"),
			kv.EncIDKey,
			kv.EncBodyJSON,
			decEndpointEntFn,
			decValToEntFn,
		),
		IndexStore: kv.NewOrgNameKeyStore(resource, []byte("checkindexv1"), false),
	}
}

// FindCheckByID retrieves a check by id.
func (s *Service) FindCheckByID(ctx context.Context, id platform.ID) (influxdb.Check, error) {
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	var c influxdb.Check
	err := s.kv.View(ctx, func(tx kv.Tx) error {
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

func (s *Service) findCheckByID(ctx context.Context, tx kv.Tx, id platform.ID) (influxdb.Check, error) {
	chkVal, err := s.checkStore.FindEnt(ctx, tx, kv.Entity{PK: kv.EncID(id)})
	if err != nil {
		return nil, err
	}
	return chkVal.(influxdb.Check), nil
}

func (s *Service) findCheckByName(ctx context.Context, tx kv.Tx, orgID platform.ID, name string) (influxdb.Check, error) {
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	chVal, err := s.checkStore.FindEnt(ctx, tx, kv.Entity{
		UniqueKey: kv.Encode(kv.EncID(orgID), kv.EncString(name)),
	})
	if kv.IsNotFound(err) {
		return nil, &errors.Error{
			Code: errors.ENotFound,
			Err:  err,
		}
	}
	if err != nil {
		return nil, err
	}

	return chVal.(influxdb.Check), nil
}

// FindCheck retrieves a check using an arbitrary check filter.
// Filters using ID, or OrganizationID and check Name should be efficient.
// Other filters will do a linear scan across checks until it finds a match.
func (s *Service) FindCheck(ctx context.Context, filter influxdb.CheckFilter) (influxdb.Check, error) {
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	if filter.ID != nil {
		return s.FindCheckByID(ctx, *filter.ID)
	}

	if filter.Org != nil {
		o, err := s.orgs.FindOrganization(ctx, influxdb.OrganizationFilter{Name: filter.Org})
		if err != nil {
			return nil, err
		}
		filter.OrgID = &o.ID
	}

	var c influxdb.Check
	err := s.kv.View(ctx, func(tx kv.Tx) error {
		if filter.OrgID != nil && filter.Name != nil {
			ch, err := s.findCheckByName(ctx, tx, *filter.OrgID, *filter.Name)
			c = ch
			return err
		}

		var prefix []byte
		if filter.OrgID != nil {
			ent := kv.Entity{UniqueKey: kv.EncID(*filter.OrgID)}
			prefix, _ = s.checkStore.IndexStore.EntKey(ctx, ent)
		}

		filterFn := filterChecksFn(filter)
		return s.checkStore.Find(ctx, tx, kv.FindOpts{
			Prefix: prefix,
			Limit:  1,
			FilterEntFn: func(k []byte, v interface{}) bool {
				ch, ok := v.(influxdb.Check)
				if err := kv.IsErrUnexpectedDecodeVal(ok); err != nil {
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
		return nil, &errors.Error{
			Code: errors.ENotFound,
			Msg:  "check not found",
		}
	}
	return c, nil
}

func filterChecksFn(filter influxdb.CheckFilter) func(c influxdb.Check) bool {
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
		return true
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

	if filter.Org != nil {
		o, err := s.orgs.FindOrganization(ctx, influxdb.OrganizationFilter{Name: filter.Org})
		if err != nil {
			return nil, 0, &errors.Error{Err: err}
		}

		filter.OrgID = &o.ID
	}

	var checks []influxdb.Check
	err := s.kv.View(ctx, func(tx kv.Tx) error {
		var opt influxdb.FindOptions
		if len(opts) > 0 {
			opt = opts[0]
		}

		filterFn := filterChecksFn(filter)
		return s.checkStore.Find(ctx, tx, kv.FindOpts{
			Descending: opt.Descending,
			Offset:     opt.Offset,
			Limit:      opt.Limit,
			FilterEntFn: func(k []byte, v interface{}) bool {
				ch, ok := v.(influxdb.Check)
				if err := kv.IsErrUnexpectedDecodeVal(ok); err != nil {
					return false
				}
				return filterFn(ch)
			},
			CaptureFn: func(key []byte, decodedVal interface{}) error {
				c, ok := decodedVal.(influxdb.Check)
				if err := kv.IsErrUnexpectedDecodeVal(ok); err != nil {
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
func (s *Service) CreateCheck(ctx context.Context, c influxdb.CheckCreate, userID platform.ID) (err error) {
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	if err := c.Status.Valid(); err != nil {
		return err
	}

	if c.GetOrgID().Valid() {
		if _, err := s.orgs.FindOrganizationByID(ctx, c.GetOrgID()); err != nil {
			return &errors.Error{
				Code: errors.ENotFound,
				Op:   influxdb.OpCreateCheck,
				Err:  err,
			}
		}
	}

	c.SetID(s.idGenerator.ID())
	c.SetOwnerID(userID)
	now := s.timeGenerator.Now()
	c.SetCreatedAt(now)
	c.SetUpdatedAt(now)

	if err := c.Valid(fluxlang.DefaultService); err != nil {
		return err
	}

	// create task initially in inactive state
	t, err := s.createCheckTask(ctx, c)
	if err != nil {
		return &errors.Error{
			Code: errors.EInvalid,
			Msg:  "Could not create task from check",
			Err:  err,
		}
	}

	c.SetTaskID(t.ID)

	err = s.kv.Update(ctx, func(tx kv.Tx) error {
		return s.putCheck(ctx, tx, c, kv.PutNew())
	})

	// something went wrong persisting new check
	// so remove associated task
	if err != nil {
		if derr := s.tasks.DeleteTask(ctx, t.ID); derr != nil {
			msg := fmt.Sprintf("error removing task %q for check %q in org %q", t.ID, c.GetName(), c.GetOrgID())
			s.log.Error(msg, zap.Error(derr))
		}

		return err
	}

	// update task to be in matching state to check
	if influxdb.Status(t.Status) != c.Status {
		_, err = s.tasks.UpdateTask(ctx, t.ID, taskmodel.TaskUpdate{
			Status: strPtr(string(c.Status)),
		})
	}

	return err
}

func (s *Service) createCheckTask(ctx context.Context, c influxdb.CheckCreate) (*taskmodel.Task, error) {
	script, err := c.GenerateFlux(fluxlang.DefaultService)
	if err != nil {
		return nil, err
	}

	tc := taskmodel.TaskCreate{
		Type:           c.Type(),
		Flux:           script,
		OwnerID:        c.GetOwnerID(),
		OrganizationID: c.GetOrgID(),
		// task initially in inactive state to ensure it isn't
		// scheduled until check is persisted and active
		Status: string(influxdb.Inactive),
	}

	t, err := s.tasks.CreateTask(ctx, tc)
	if err != nil {
		return nil, err
	}

	return t, nil
}

// PutCheck will put a check without setting an ID.
func (s *Service) PutCheck(ctx context.Context, c influxdb.Check) error {
	if err := c.Valid(fluxlang.DefaultService); err != nil {
		return err
	}
	return s.kv.Update(ctx, func(tx kv.Tx) error {
		return s.putCheck(ctx, tx, c)
	})
}

func (s *Service) putCheck(ctx context.Context, tx kv.Tx, c influxdb.Check, opts ...kv.PutOptionFn) error {
	return s.checkStore.Put(ctx, tx, kv.Entity{
		PK:        kv.EncID(c.GetID()),
		UniqueKey: kv.Encode(kv.EncID(c.GetOrgID()), kv.EncString(c.GetName())),
		Body:      c,
	}, opts...)
}

// PatchCheck updates a check according the parameters set on upd.
func (s *Service) PatchCheck(ctx context.Context, id platform.ID, upd influxdb.CheckUpdate) (influxdb.Check, error) {
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	var check influxdb.Check
	if err := s.kv.Update(ctx, func(tx kv.Tx) error {
		c, err := s.findCheckByID(ctx, tx, id)
		if err != nil {
			return err
		}

		c, err = s.patchCheck(ctx, tx, c, upd)
		if err != nil {
			return err
		}

		check = c
		return nil
	}); err != nil {
		return nil, err
	}

	if err := s.patchCheckTask(ctx, check.GetTaskID(), upd); err != nil {
		return nil, err
	}

	return check, nil
}

// UpdateCheck updates the check.
func (s *Service) UpdateCheck(ctx context.Context, id platform.ID, chk influxdb.CheckCreate) (influxdb.Check, error) {
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	var check influxdb.Check
	if err := s.kv.Update(ctx, func(tx kv.Tx) error {
		c, err := s.updateCheck(ctx, tx, id, chk)
		if err != nil {
			return err
		}
		check = c
		return nil
	}); err != nil {
		return nil, err
	}

	if err := s.updateCheckTask(ctx, chk); err != nil {
		return nil, err
	}

	return check, nil
}

func (s *Service) updateCheckTask(ctx context.Context, chk influxdb.CheckCreate) error {
	flux, err := chk.GenerateFlux(fluxlang.DefaultService)
	if err != nil {
		return err
	}

	tu := taskmodel.TaskUpdate{
		Flux:        &flux,
		Description: strPtr(chk.GetDescription()),
	}

	if chk.Status != "" {
		tu.Status = strPtr(string(chk.Status))
	}

	if _, err := s.tasks.UpdateTask(ctx, chk.GetTaskID(), tu); err != nil {
		return err
	}

	return err
}

func (s *Service) patchCheckTask(ctx context.Context, taskID platform.ID, upd influxdb.CheckUpdate) error {
	tu := taskmodel.TaskUpdate{
		Description: upd.Description,
	}

	if upd.Status != nil {
		tu.Status = strPtr(string(*upd.Status))
	}

	if _, err := s.tasks.UpdateTask(ctx, taskID, tu); err != nil {
		return err
	}

	return nil
}

func (s *Service) updateCheck(ctx context.Context, tx kv.Tx, id platform.ID, chk influxdb.CheckCreate) (influxdb.Check, error) {
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	current, err := s.findCheckByID(ctx, tx, id)
	if err != nil {
		return nil, err
	}

	chk.SetTaskID(current.GetTaskID())

	if chk.GetName() != current.GetName() {
		c0, err := s.findCheckByName(ctx, tx, current.GetOrgID(), chk.GetName())
		if err == nil && c0.GetID() != id {
			return nil, &errors.Error{
				Code: errors.EConflict,
				Msg:  "check name is not unique",
			}
		}

		ent := kv.Entity{
			UniqueKey: kv.Encode(kv.EncID(current.GetOrgID()), kv.EncString(current.GetName())),
		}
		if err := s.checkStore.IndexStore.DeleteEnt(ctx, tx, ent); err != nil {
			return nil, err
		}
	}

	// ID and OrganizationID can not be updated
	chk.SetID(current.GetID())
	chk.SetOrgID(current.GetOrgID())
	chk.SetOwnerID(current.GetOwnerID())
	chk.SetCreatedAt(current.GetCRUDLog().CreatedAt)
	chk.SetUpdatedAt(s.timeGenerator.Now())

	if err := chk.Valid(fluxlang.DefaultService); err != nil {
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

func (s *Service) patchCheck(ctx context.Context, tx kv.Tx, check influxdb.Check, upd influxdb.CheckUpdate) (influxdb.Check, error) {
	if upd.Name != nil {
		check.SetName(*upd.Name)
	}

	if upd.Description != nil {
		check.SetDescription(*upd.Description)
	}

	check.SetUpdatedAt(s.timeGenerator.Now())

	if err := check.Valid(fluxlang.DefaultService); err != nil {
		return nil, err
	}

	if err := s.putCheck(ctx, tx, check, kv.PutUpdate()); err != nil {
		return nil, err
	}

	return check, nil
}

// DeleteCheck deletes a check and prunes it from the index.
func (s *Service) DeleteCheck(ctx context.Context, id platform.ID) error {
	ch, err := s.FindCheckByID(ctx, id)
	if err != nil {
		return err
	}

	if err := s.tasks.DeleteTask(ctx, ch.GetTaskID()); err != nil {
		return err
	}

	return s.kv.Update(ctx, func(tx kv.Tx) error {
		return s.checkStore.DeleteEnt(ctx, tx, kv.Entity{
			PK: kv.EncID(id),
		})
	})
}

func strPtr(s string) *string {
	ss := new(string)
	*ss = s
	return ss
}

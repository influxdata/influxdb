package service

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/kit/platform/errors"
	"github.com/influxdata/influxdb/v2/kv"
	"github.com/influxdata/influxdb/v2/notification/rule"
	"github.com/influxdata/influxdb/v2/pkg/pointer"
	"github.com/influxdata/influxdb/v2/snowflake"
	"github.com/influxdata/influxdb/v2/task/taskmodel"
	"go.uber.org/zap"
)

var (
	notificationRuleBucket = []byte("notificationRulev1")

	// ErrNotificationRuleNotFound is used when the notification rule is not found.
	ErrNotificationRuleNotFound = &errors.Error{
		Msg:  "notification rule not found",
		Code: errors.ENotFound,
	}

	// ErrInvalidNotificationRuleID is used when the service was provided
	// an invalid ID format.
	ErrInvalidNotificationRuleID = &errors.Error{
		Code: errors.EInvalid,
		Msg:  "provided notification rule ID has invalid format",
	}
)

// RuleService is an implementation of the influxdb CheckService
// It is backed by the kv store abstraction.
type RuleService struct {
	log *zap.Logger

	kv        kv.Store
	tasks     taskmodel.TaskService
	orgs      influxdb.OrganizationService
	endpoints influxdb.NotificationEndpointService

	idGenerator   platform.IDGenerator
	timeGenerator influxdb.TimeGenerator
}

// New constructs and configures a notification rule service
func New(logger *zap.Logger, store kv.Store, tasks taskmodel.TaskService, orgs influxdb.OrganizationService, endpoints influxdb.NotificationEndpointService) (*RuleService, error) {
	s := &RuleService{
		log:           logger,
		kv:            store,
		tasks:         tasks,
		orgs:          orgs,
		endpoints:     endpoints,
		timeGenerator: influxdb.RealTimeGenerator{},
		idGenerator:   snowflake.NewIDGenerator(),
	}

	ctx := context.Background()
	if err := store.Update(ctx, func(tx kv.Tx) error {
		return s.initializeNotificationRule(ctx, tx)
	}); err != nil {
		return nil, err
	}

	return s, nil
}

var _ influxdb.NotificationRuleStore = (*RuleService)(nil)

func (s *RuleService) initializeNotificationRule(ctx context.Context, tx kv.Tx) error {
	if _, err := s.notificationRuleBucket(tx); err != nil {
		return err
	}
	return nil
}

// UnavailableNotificationRuleStoreError is used if we aren't able to interact with the
// store, it means the store is not available at the moment (e.g. network).
func UnavailableNotificationRuleStoreError(err error) *errors.Error {
	return &errors.Error{
		Code: errors.EInternal,
		Msg:  fmt.Sprintf("Unable to connect to notification rule store service. Please try again; Err: %v", err),
		Op:   "kv/notificationRule",
	}
}

// InternalNotificationRuleStoreError is used when the error comes from an
// internal system.
func InternalNotificationRuleStoreError(err error) *errors.Error {
	return &errors.Error{
		Code: errors.EInternal,
		Msg:  fmt.Sprintf("Unknown internal notificationRule data error; Err: %v", err),
		Op:   "kv/notificationRule",
	}
}

func (s *RuleService) notificationRuleBucket(tx kv.Tx) (kv.Bucket, error) {
	b, err := tx.Bucket(notificationRuleBucket)
	if err != nil {
		return nil, UnavailableNotificationRuleStoreError(err)
	}
	return b, nil
}

// CreateNotificationRule creates a new notification rule and sets b.ID with the new identifier.
func (s *RuleService) CreateNotificationRule(ctx context.Context, nr influxdb.NotificationRuleCreate, userID platform.ID) error {
	// set notification rule ID
	id := s.idGenerator.ID()
	nr.SetID(id)

	// set notification rule created / updated times
	now := s.timeGenerator.Now()
	nr.SetOwnerID(userID)
	nr.SetCreatedAt(now)
	nr.SetUpdatedAt(now)

	// create backing task and set ID (in inactive state initially)
	t, err := s.createNotificationTask(ctx, nr)
	if err != nil {
		return err
	}

	nr.SetTaskID(t.ID)

	if err := s.kv.Update(ctx, func(tx kv.Tx) error {
		return s.createNotificationRule(ctx, tx, nr, userID)
	}); err != nil {
		// remove associated task
		if derr := s.tasks.DeleteTask(ctx, t.ID); derr != nil {
			s.log.Error("failed to remove task for invalid notification rule", zap.Error(derr))
		}

		return err
	}

	// set task to notification rule create status
	_, err = s.tasks.UpdateTask(ctx, t.ID, taskmodel.TaskUpdate{Status: pointer.String(string(nr.Status))})
	return err
}

func (s *RuleService) createNotificationRule(ctx context.Context, tx kv.Tx, nr influxdb.NotificationRuleCreate, userID platform.ID) error {
	if err := nr.Valid(); err != nil {
		return err
	}

	if err := nr.Status.Valid(); err != nil {
		return err
	}

	return s.putNotificationRule(ctx, tx, nr.NotificationRule)
}

func (s *RuleService) createNotificationTask(ctx context.Context, r influxdb.NotificationRuleCreate) (*taskmodel.Task, error) {
	ep, err := s.endpoints.FindNotificationEndpointByID(ctx, r.GetEndpointID())
	if err != nil {
		return nil, err
	}

	script, err := r.GenerateFlux(ep)
	if err != nil {
		return nil, err
	}

	tc := taskmodel.TaskCreate{
		Type:           r.Type(),
		Flux:           script,
		OwnerID:        r.GetOwnerID(),
		OrganizationID: r.GetOrgID(),
		// create task initially in inactive status
		Status: string(influxdb.Inactive),
	}

	t, err := s.tasks.CreateTask(ctx, tc)
	if err != nil {
		return nil, err
	}

	return t, nil
}

// UpdateNotificationRule updates a single notification rule.
// Returns the new notification rule after update.
func (s *RuleService) UpdateNotificationRule(ctx context.Context, id platform.ID, nr influxdb.NotificationRuleCreate, userID platform.ID) (influxdb.NotificationRule, error) {
	rule, err := s.FindNotificationRuleByID(ctx, id)
	if err != nil {
		return nil, err
	}

	// ID and OrganizationID can not be updated
	nr.SetID(rule.GetID())
	nr.SetOrgID(rule.GetOrgID())
	nr.SetOwnerID(rule.GetOwnerID())
	nr.SetCreatedAt(rule.GetCRUDLog().CreatedAt)
	nr.SetUpdatedAt(s.timeGenerator.Now())
	nr.SetTaskID(rule.GetTaskID())

	if err := nr.Valid(); err != nil {
		return nil, err
	}

	if err := nr.Status.Valid(); err != nil {
		return nil, err
	}

	_, err = s.updateNotificationTask(ctx, nr, pointer.String(string(nr.Status)))
	if err != nil {
		return nil, err
	}

	err = s.kv.Update(ctx, func(tx kv.Tx) error {
		return s.putNotificationRule(ctx, tx, nr.NotificationRule)
	})

	return nr.NotificationRule, err
}

func (s *RuleService) updateNotificationTask(ctx context.Context, r influxdb.NotificationRule, status *string) (*taskmodel.Task, error) {
	ep, err := s.endpoints.FindNotificationEndpointByID(ctx, r.GetEndpointID())
	if err != nil {
		return nil, err
	}

	script, err := r.GenerateFlux(ep)
	if err != nil {
		return nil, err
	}

	tu := taskmodel.TaskUpdate{
		Flux:        &script,
		Description: pointer.String(r.GetDescription()),
		Status:      status,
	}

	t, err := s.tasks.UpdateTask(ctx, r.GetTaskID(), tu)
	if err != nil {
		return nil, err
	}

	return t, nil
}

// PatchNotificationRule updates a single  notification rule with changeset.
// Returns the new notification rule state after update.
func (s *RuleService) PatchNotificationRule(ctx context.Context, id platform.ID, upd influxdb.NotificationRuleUpdate) (influxdb.NotificationRule, error) {
	nr, err := s.FindNotificationRuleByID(ctx, id)
	if err != nil {
		return nil, err
	}

	if upd.Name != nil {
		nr.SetName(*upd.Name)
	}
	if upd.Description != nil {
		nr.SetDescription(*upd.Description)
	}

	var status *string
	if upd.Status != nil {
		status = pointer.String(string(*upd.Status))
	}

	nr.SetUpdatedAt(s.timeGenerator.Now())
	if err := nr.Valid(); err != nil {
		return nil, err
	}

	_, err = s.updateNotificationTask(ctx, nr, status)
	if err != nil {
		return nil, err
	}

	if err := s.kv.Update(ctx, func(tx kv.Tx) (err error) {
		return s.putNotificationRule(ctx, tx, nr)
	}); err != nil {
		return nil, err
	}

	return nr, nil
}

// PutNotificationRule put a notification rule to storage.
func (s *RuleService) PutNotificationRule(ctx context.Context, nr influxdb.NotificationRuleCreate) error {
	return s.kv.Update(ctx, func(tx kv.Tx) (err error) {
		if err := nr.Valid(); err != nil {
			return err
		}

		if err := nr.Status.Valid(); err != nil {
			return err
		}

		return s.putNotificationRule(ctx, tx, nr)
	})
}

func (s *RuleService) putNotificationRule(ctx context.Context, tx kv.Tx, nr influxdb.NotificationRule) error {
	encodedID, _ := nr.GetID().Encode()

	v, err := json.Marshal(nr)
	if err != nil {
		return err
	}

	bucket, err := s.notificationRuleBucket(tx)
	if err != nil {
		return err
	}

	if err := bucket.Put(encodedID, v); err != nil {
		return UnavailableNotificationRuleStoreError(err)
	}
	return nil
}

// FindNotificationRuleByID returns a single notification rule by ID.
func (s *RuleService) FindNotificationRuleByID(ctx context.Context, id platform.ID) (influxdb.NotificationRule, error) {
	var (
		nr  influxdb.NotificationRule
		err error
	)

	err = s.kv.View(ctx, func(tx kv.Tx) error {
		nr, err = s.findNotificationRuleByID(ctx, tx, id)
		return err
	})

	return nr, err
}

func (s *RuleService) findNotificationRuleByID(ctx context.Context, tx kv.Tx, id platform.ID) (influxdb.NotificationRule, error) {
	encID, err := id.Encode()
	if err != nil {
		return nil, ErrInvalidNotificationRuleID
	}

	bucket, err := s.notificationRuleBucket(tx)
	if err != nil {
		return nil, err
	}

	v, err := bucket.Get(encID)
	if kv.IsNotFound(err) {
		return nil, ErrNotificationRuleNotFound
	}
	if err != nil {
		return nil, InternalNotificationRuleStoreError(err)
	}

	return rule.UnmarshalJSON(v)
}

// FindNotificationRules returns a list of notification rules that match filter and the total count of matching notification rules.
// Additional options provide pagination & sorting.
func (s *RuleService) FindNotificationRules(ctx context.Context, filter influxdb.NotificationRuleFilter, opt ...influxdb.FindOptions) (nrs []influxdb.NotificationRule, n int, err error) {
	if filter.OrgID == nil && filter.Organization != nil {
		o, err := s.orgs.FindOrganization(ctx, influxdb.OrganizationFilter{
			Name: filter.Organization,
		})

		if err != nil {
			return nrs, 0, err
		}

		filter.OrgID = &o.ID
	}

	err = s.kv.View(ctx, func(tx kv.Tx) error {
		nrs, n, err = s.findNotificationRules(ctx, tx, filter, opt...)
		return err
	})

	return nrs, n, err
}

func (s *RuleService) findNotificationRules(ctx context.Context, tx kv.Tx, filter influxdb.NotificationRuleFilter, opt ...influxdb.FindOptions) ([]influxdb.NotificationRule, int, error) {
	var (
		nrs        = make([]influxdb.NotificationRule, 0)
		offset     int
		limit      int
		count      int
		descending bool
	)

	if len(opt) > 0 {
		offset = opt[0].Offset
		limit = opt[0].Limit
		descending = opt[0].Descending
	}

	filterFn := filterNotificationRulesFn(filter)
	err := s.forEachNotificationRule(ctx, tx, descending, func(nr influxdb.NotificationRule) bool {
		if filterFn(nr) {
			if count >= offset {
				nrs = append(nrs, nr)
			}
			count++
		}

		if limit > 0 && len(nrs) >= limit {
			return false
		}

		return true
	})

	return nrs, len(nrs), err
}

// forEachNotificationRule will iterate through all notification rules while fn returns true.
func (s *RuleService) forEachNotificationRule(ctx context.Context, tx kv.Tx, descending bool, fn func(influxdb.NotificationRule) bool) error {

	bkt, err := s.notificationRuleBucket(tx)
	if err != nil {
		return err
	}

	direction := kv.CursorAscending
	if descending {
		direction = kv.CursorDescending
	}

	cur, err := bkt.ForwardCursor(nil, kv.WithCursorDirection(direction))
	if err != nil {
		return err
	}

	for k, v := cur.Next(); k != nil; k, v = cur.Next() {
		nr, err := rule.UnmarshalJSON(v)
		if err != nil {
			return err
		}
		if !fn(nr) {
			break
		}
	}

	return nil
}

func filterNotificationRulesFn(filter influxdb.NotificationRuleFilter) func(nr influxdb.NotificationRule) bool {
	if filter.OrgID != nil {
		return func(nr influxdb.NotificationRule) bool {
			if !nr.MatchesTags(filter.Tags) {
				return false
			}

			return nr.GetOrgID() == *filter.OrgID
		}
	}

	return func(nr influxdb.NotificationRule) bool {
		return nr.MatchesTags(filter.Tags)
	}
}

// DeleteNotificationRule removes a notification rule by ID.
func (s *RuleService) DeleteNotificationRule(ctx context.Context, id platform.ID) error {
	r, err := s.FindNotificationRuleByID(ctx, id)
	if err != nil {
		return err
	}

	if err := s.tasks.DeleteTask(ctx, r.GetTaskID()); err != nil {
		return err
	}

	return s.kv.Update(ctx, func(tx kv.Tx) error {
		return s.deleteNotificationRule(ctx, tx, r)
	})
}

func (s *RuleService) deleteNotificationRule(ctx context.Context, tx kv.Tx, r influxdb.NotificationRule) error {
	encodedID, err := r.GetID().Encode()
	if err != nil {
		return ErrInvalidNotificationRuleID
	}

	bucket, err := s.notificationRuleBucket(tx)
	if err != nil {
		return err
	}

	_, err = bucket.Get(encodedID)
	if kv.IsNotFound(err) {
		return ErrNotificationRuleNotFound
	}
	if err != nil {
		return InternalNotificationRuleStoreError(err)
	}

	if err := bucket.Delete(encodedID); err != nil {
		return InternalNotificationRuleStoreError(err)
	}

	return nil
}

package kv

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/influxdata/influxdb/v2"
	icontext "github.com/influxdata/influxdb/v2/context"
	"github.com/influxdata/influxdb/v2/kit/feature"
	"github.com/influxdata/influxdb/v2/resource"
	"github.com/influxdata/influxdb/v2/task/options"
)

// Task Storage Schema
// taskBucket:
//   <taskID>: task data storage
// taskRunBucket:
//   <taskID>/<runID>: run data storage
//   <taskID>/manualRuns: list of runs to run manually
//   <taskID>/latestCompleted: run data for the latest completed run of a task
// taskIndexBucket
//   <orgID>/<taskID>: index for tasks by org

// We may want to add a <taskName>/<taskID> index to allow us to look up tasks by task name.

var (
	taskBucket      = []byte("tasksv1")
	taskRunBucket   = []byte("taskRunsv1")
	taskIndexBucket = []byte("taskIndexsv1")
)

var _ influxdb.TaskService = (*Service)(nil)

type kvTask struct {
	ID              influxdb.ID            `json:"id"`
	Type            string                 `json:"type,omitempty"`
	OrganizationID  influxdb.ID            `json:"orgID"`
	Organization    string                 `json:"org"`
	OwnerID         influxdb.ID            `json:"ownerID"`
	Name            string                 `json:"name"`
	Description     string                 `json:"description,omitempty"`
	Status          string                 `json:"status"`
	Flux            string                 `json:"flux"`
	Every           string                 `json:"every,omitempty"`
	Cron            string                 `json:"cron,omitempty"`
	LastRunStatus   string                 `json:"lastRunStatus,omitempty"`
	LastRunError    string                 `json:"lastRunError,omitempty"`
	Offset          influxdb.Duration      `json:"offset,omitempty"`
	LatestCompleted time.Time              `json:"latestCompleted,omitempty"`
	LatestScheduled time.Time              `json:"latestScheduled,omitempty"`
	LatestSuccess   time.Time              `json:"latestSuccess,omitempty"`
	LatestFailure   time.Time              `json:"latestFailure,omitempty"`
	CreatedAt       time.Time              `json:"createdAt,omitempty"`
	UpdatedAt       time.Time              `json:"updatedAt,omitempty"`
	Metadata        map[string]interface{} `json:"metadata,omitempty"`
}

func kvToInfluxTask(k *kvTask) *influxdb.Task {
	return &influxdb.Task{
		ID:              k.ID,
		Type:            k.Type,
		OrganizationID:  k.OrganizationID,
		Organization:    k.Organization,
		OwnerID:         k.OwnerID,
		Name:            k.Name,
		Description:     k.Description,
		Status:          k.Status,
		Flux:            k.Flux,
		Every:           k.Every,
		Cron:            k.Cron,
		LastRunStatus:   k.LastRunStatus,
		LastRunError:    k.LastRunError,
		Offset:          k.Offset.Duration,
		LatestCompleted: k.LatestCompleted,
		LatestScheduled: k.LatestScheduled,
		LatestSuccess:   k.LatestSuccess,
		LatestFailure:   k.LatestFailure,
		CreatedAt:       k.CreatedAt,
		UpdatedAt:       k.UpdatedAt,
		Metadata:        k.Metadata,
	}
}

// FindTaskByID returns a single task
func (s *Service) FindTaskByID(ctx context.Context, id influxdb.ID) (*influxdb.Task, error) {
	var t *influxdb.Task
	err := s.kv.View(ctx, func(tx Tx) error {
		task, err := s.findTaskByID(ctx, tx, id)
		if err != nil {
			return err
		}
		t = task
		return nil
	})
	if err != nil {
		return nil, err
	}

	return t, nil
}

// findTaskByID is an internal method used to do any action with tasks internally
// that do not require authorization.
func (s *Service) findTaskByID(ctx context.Context, tx Tx, id influxdb.ID) (*influxdb.Task, error) {
	taskKey, err := taskKey(id)
	if err != nil {
		return nil, err
	}

	b, err := tx.Bucket(taskBucket)
	if err != nil {
		return nil, influxdb.ErrUnexpectedTaskBucketErr(err)
	}

	v, err := b.Get(taskKey)
	if IsNotFound(err) {
		return nil, influxdb.ErrTaskNotFound
	}
	if err != nil {
		return nil, err
	}
	kvTask := &kvTask{}
	if err := json.Unmarshal(v, kvTask); err != nil {
		return nil, influxdb.ErrInternalTaskServiceError(err)
	}

	t := kvToInfluxTask(kvTask)

	if t.LatestCompleted.IsZero() {
		t.LatestCompleted = t.CreatedAt
	}

	return t, nil
}

// FindTasks returns a list of tasks that match a filter (limit 100) and the total count
// of matching tasks.
func (s *Service) FindTasks(ctx context.Context, filter influxdb.TaskFilter) ([]*influxdb.Task, int, error) {
	if filter.Organization != "" {
		org, err := s.orgs.FindOrganization(ctx, influxdb.OrganizationFilter{
			Name: &filter.Organization,
		})
		if err != nil {
			return nil, 0, err
		}

		filter.OrganizationID = &org.ID
	}

	var ts []*influxdb.Task
	err := s.kv.View(ctx, func(tx Tx) error {
		tasks, _, err := s.findTasks(ctx, tx, filter)
		if err != nil {
			return err
		}
		ts = tasks
		return nil
	})
	if err != nil {
		return nil, 0, err
	}

	return ts, len(ts), nil
}

func (s *Service) findTasks(ctx context.Context, tx Tx, filter influxdb.TaskFilter) ([]*influxdb.Task, int, error) {
	// complain about limits
	if filter.Limit < 0 {
		return nil, 0, influxdb.ErrPageSizeTooSmall
	}
	if filter.Limit > influxdb.TaskMaxPageSize {
		return nil, 0, influxdb.ErrPageSizeTooLarge
	}
	if filter.Limit == 0 {
		filter.Limit = influxdb.TaskDefaultPageSize
	}

	// if no user or organization is passed, assume contexts auth is the user we are looking for.
	// it is possible for a  internal system to call this with no auth so we shouldnt fail if no auth is found.
	if filter.OrganizationID == nil && filter.User == nil {
		userAuth, err := icontext.GetAuthorizer(ctx)
		if err == nil {
			userID := userAuth.GetUserID()
			if userID.Valid() {
				filter.User = &userID
			}
		}
	}

	// filter by user id.
	if filter.User != nil {
		return s.findTasksByUser(ctx, tx, filter)
	} else if filter.OrganizationID != nil {
		return s.findTasksByOrg(ctx, tx, *filter.OrganizationID, filter)
	}

	return s.findAllTasks(ctx, tx, filter)
}

// findTasksByUser is a subset of the find tasks function. Used for cleanliness
func (s *Service) findTasksByUser(ctx context.Context, tx Tx, filter influxdb.TaskFilter) ([]*influxdb.Task, int, error) {
	var ts []*influxdb.Task

	taskBucket, err := tx.Bucket(taskBucket)
	if err != nil {
		return nil, 0, influxdb.ErrUnexpectedTaskBucketErr(err)
	}

	var (
		seek []byte
		opts []CursorOption
	)

	if filter.After != nil {
		seek, err = taskKey(*filter.After)
		if err != nil {
			return nil, 0, err
		}

		opts = append(opts, WithCursorSkipFirstItem())
	}

	c, err := taskBucket.ForwardCursor(seek, opts...)
	if err != nil {
		return nil, 0, influxdb.ErrUnexpectedTaskBucketErr(err)
	}

	matchFn := newTaskMatchFn(filter, nil)

	for k, v := c.Next(); k != nil; k, v = c.Next() {
		kvTask := &kvTask{}
		if err := json.Unmarshal(v, kvTask); err != nil {
			return nil, 0, influxdb.ErrInternalTaskServiceError(err)
		}

		t := kvToInfluxTask(kvTask)
		if matchFn == nil || matchFn(t) {
			ts = append(ts, t)

			if len(ts) >= filter.Limit {
				break
			}
		}
	}
	if err := c.Err(); err != nil {
		return nil, 0, err
	}

	return ts, len(ts), c.Close()
}

// findTasksByOrg is a subset of the find tasks function. Used for cleanliness
func (s *Service) findTasksByOrg(ctx context.Context, tx Tx, orgID influxdb.ID, filter influxdb.TaskFilter) ([]*influxdb.Task, int, error) {
	var err error
	if !orgID.Valid() {
		return nil, 0, fmt.Errorf("finding tasks by organization ID: %w", influxdb.ErrInvalidID)
	}

	var ts []*influxdb.Task

	indexBucket, err := tx.Bucket(taskIndexBucket)
	if err != nil {
		return nil, 0, influxdb.ErrUnexpectedTaskBucketErr(err)
	}

	prefix, err := orgID.Encode()
	if err != nil {
		return nil, 0, influxdb.ErrInvalidTaskID
	}

	var (
		key  = prefix
		opts []CursorOption
	)
	// we can filter by orgID
	if filter.After != nil {
		key, err = taskOrgKey(orgID, *filter.After)
		if err != nil {
			return nil, 0, err
		}

		opts = append(opts, WithCursorSkipFirstItem())
	}

	c, err := indexBucket.ForwardCursor(
		key,
		append(opts, WithCursorPrefix(prefix))...,
	)
	if err != nil {
		return nil, 0, influxdb.ErrUnexpectedTaskBucketErr(err)
	}

	// free cursor resources
	defer c.Close()

	matchFn := newTaskMatchFn(filter, nil)

	for k, v := c.Next(); k != nil; k, v = c.Next() {
		id, err := influxdb.IDFromString(string(v))
		if err != nil {
			return nil, 0, influxdb.ErrInvalidTaskID
		}

		t, err := s.findTaskByID(ctx, tx, *id)
		if err != nil {
			if err == influxdb.ErrTaskNotFound {
				// we might have some crufty index's
				err = nil
				continue
			}
			return nil, 0, err
		}

		// If the new task doesn't belong to the org we have looped outside the org filter
		if t.OrganizationID != orgID {
			break
		}

		if matchFn == nil || matchFn(t) {
			ts = append(ts, t)
			// Check if we are over running the limit
			if len(ts) >= filter.Limit {
				break
			}
		}
	}

	return ts, len(ts), c.Err()
}

type taskMatchFn func(*influxdb.Task) bool

// newTaskMatchFn returns a function for validating
// a task matches the filter. Will return nil if
// the filter should match all tasks.
func newTaskMatchFn(f influxdb.TaskFilter, org *influxdb.Organization) func(t *influxdb.Task) bool {
	var fn taskMatchFn

	if org != nil {
		expected := org.ID
		prevFn := fn
		fn = func(t *influxdb.Task) bool {
			res := prevFn == nil || prevFn(t)
			return res && expected == t.OrganizationID
		}
	}

	if f.Type != nil {
		expected := *f.Type
		prevFn := fn
		fn = func(t *influxdb.Task) bool {
			res := prevFn == nil || prevFn(t)
			return res &&
				((expected == influxdb.TaskSystemType && (t.Type == influxdb.TaskSystemType || t.Type == "")) || expected == t.Type)
		}
	}

	if f.Name != nil {
		expected := *f.Name
		prevFn := fn
		fn = func(t *influxdb.Task) bool {
			res := prevFn == nil || prevFn(t)
			return res && (expected == t.Name)
		}
	}

	if f.Status != nil {
		prevFn := fn
		fn = func(t *influxdb.Task) bool {
			res := prevFn == nil || prevFn(t)
			return res && (t.Status == *f.Status)
		}
	}

	if f.User != nil {
		prevFn := fn
		fn = func(t *influxdb.Task) bool {
			res := prevFn == nil || prevFn(t)
			return res && t.OwnerID == *f.User
		}
	}

	return fn
}

// findAllTasks is a subset of the find tasks function. Used for cleanliness.
// This function should only be executed internally because it doesn't force organization or user filtering.
// Enforcing filters should be done in a validation layer.
func (s *Service) findAllTasks(ctx context.Context, tx Tx, filter influxdb.TaskFilter) ([]*influxdb.Task, int, error) {
	var ts []*influxdb.Task
	taskBucket, err := tx.Bucket(taskBucket)
	if err != nil {
		return nil, 0, influxdb.ErrUnexpectedTaskBucketErr(err)
	}

	var (
		seek []byte
		opts []CursorOption
	)

	if filter.After != nil {
		seek, err = taskKey(*filter.After)
		if err != nil {
			return nil, 0, err
		}

		opts = append(opts, WithCursorSkipFirstItem())
	}

	c, err := taskBucket.ForwardCursor(seek, opts...)
	if err != nil {
		return nil, 0, influxdb.ErrUnexpectedTaskBucketErr(err)
	}

	// free cursor resources
	defer c.Close()

	matchFn := newTaskMatchFn(filter, nil)

	for k, v := c.Next(); k != nil; k, v = c.Next() {
		kvTask := &kvTask{}
		if err := json.Unmarshal(v, kvTask); err != nil {
			return nil, 0, influxdb.ErrInternalTaskServiceError(err)
		}

		t := kvToInfluxTask(kvTask)

		if matchFn == nil || matchFn(t) {
			ts = append(ts, t)

			if len(ts) >= filter.Limit {
				break
			}
		}
	}

	if err := c.Err(); err != nil {
		return nil, 0, err
	}

	return ts, len(ts), err
}

// CreateTask creates a new task.
// The owner of the task is inferred from the authorizer associated with ctx.
func (s *Service) CreateTask(ctx context.Context, tc influxdb.TaskCreate) (*influxdb.Task, error) {
	var orgFilter influxdb.OrganizationFilter

	if tc.Organization != "" {
		orgFilter.Name = &tc.Organization
	} else if tc.OrganizationID.Valid() {
		orgFilter.ID = &tc.OrganizationID

	} else {
		return nil, errors.New("organization required")
	}

	org, err := s.orgs.FindOrganization(ctx, orgFilter)
	if err != nil {
		return nil, err
	}

	var t *influxdb.Task
	err = s.kv.Update(ctx, func(tx Tx) error {
		task, err := s.createTask(ctx, tx, org, tc)
		if err != nil {
			return err
		}
		t = task
		return nil
	})

	return t, err
}

func (s *Service) createTask(ctx context.Context, tx Tx, org *influxdb.Organization, tc influxdb.TaskCreate) (*influxdb.Task, error) {
	// TODO: Uncomment this once the checks/notifications no longer create tasks in kv
	// confirm the owner is a real user.
	// if _, err = s.findUserByID(ctx, tx, tc.OwnerID); err != nil {
	// 	return nil, influxdb.ErrInvalidOwnerID
	// }

	opts, err := ExtractTaskOptions(ctx, s.FluxLanguageService, tc.Flux)
	if err != nil {
		return nil, influxdb.ErrTaskOptionParse(err)
	}

	if tc.Status == "" {
		tc.Status = string(influxdb.TaskActive)
	}

	createdAt := s.clock.Now().Truncate(time.Second).UTC()
	task := &influxdb.Task{
		ID:              s.IDGenerator.ID(),
		Type:            tc.Type,
		OrganizationID:  org.ID,
		Organization:    org.Name,
		OwnerID:         tc.OwnerID,
		Metadata:        tc.Metadata,
		Name:            opts.Name,
		Description:     tc.Description,
		Status:          tc.Status,
		Flux:            tc.Flux,
		Every:           opts.Every.String(),
		Cron:            opts.Cron,
		CreatedAt:       createdAt,
		LatestCompleted: createdAt,
		LatestScheduled: createdAt,
	}

	if opts.Offset != nil {
		off, err := time.ParseDuration(opts.Offset.String())
		if err != nil {
			return nil, influxdb.ErrTaskTimeParse(err)
		}
		task.Offset = off

	}

	taskBucket, err := tx.Bucket(taskBucket)
	if err != nil {
		return nil, influxdb.ErrUnexpectedTaskBucketErr(err)
	}

	indexBucket, err := tx.Bucket(taskIndexBucket)
	if err != nil {
		return nil, influxdb.ErrUnexpectedTaskBucketErr(err)
	}

	taskBytes, err := json.Marshal(task)
	if err != nil {
		return nil, influxdb.ErrInternalTaskServiceError(err)
	}

	taskKey, err := taskKey(task.ID)
	if err != nil {
		return nil, err
	}

	orgKey, err := taskOrgKey(task.OrganizationID, task.ID)
	if err != nil {
		return nil, err
	}

	// write the task
	err = taskBucket.Put(taskKey, taskBytes)
	if err != nil {
		return nil, influxdb.ErrUnexpectedTaskBucketErr(err)
	}

	// write the org index
	err = indexBucket.Put(orgKey, taskKey)
	if err != nil {
		return nil, influxdb.ErrUnexpectedTaskBucketErr(err)
	}

	uid, _ := icontext.GetUserID(ctx)
	if err := s.audit.Log(resource.Change{
		Type:           resource.Create,
		ResourceID:     task.ID,
		ResourceType:   influxdb.TasksResourceType,
		OrganizationID: task.OrganizationID,
		UserID:         uid,
		ResourceBody:   taskBytes,
		Time:           time.Now(),
	}); err != nil {
		return nil, err
	}

	return task, nil
}

// UpdateTask updates a single task with changeset.
func (s *Service) UpdateTask(ctx context.Context, id influxdb.ID, upd influxdb.TaskUpdate) (*influxdb.Task, error) {
	var t *influxdb.Task
	err := s.kv.Update(ctx, func(tx Tx) error {
		task, err := s.updateTask(ctx, tx, id, upd)
		if err != nil {
			return err
		}
		t = task
		return nil
	})
	if err != nil {
		return nil, err
	}

	return t, nil
}

func (s *Service) updateTask(ctx context.Context, tx Tx, id influxdb.ID, upd influxdb.TaskUpdate) (*influxdb.Task, error) {
	// retrieve the task
	task, err := s.findTaskByID(ctx, tx, id)
	if err != nil {
		return nil, err
	}

	updatedAt := s.clock.Now().UTC()

	// update the flux script
	if !upd.Options.IsZero() || upd.Flux != nil {
		if err = upd.UpdateFlux(ctx, s.FluxLanguageService, task.Flux); err != nil {
			return nil, err
		}
		task.Flux = *upd.Flux

		opts, err := ExtractTaskOptions(ctx, s.FluxLanguageService, *upd.Flux)
		if err != nil {
			return nil, influxdb.ErrTaskOptionParse(err)
		}
		task.Name = opts.Name
		task.Every = opts.Every.String()
		task.Cron = opts.Cron

		var off time.Duration
		if opts.Offset != nil {
			off, err = time.ParseDuration(opts.Offset.String())
			if err != nil {
				return nil, influxdb.ErrTaskTimeParse(err)
			}
		}
		task.Offset = off
		task.UpdatedAt = updatedAt
	}

	if upd.Description != nil {
		task.Description = *upd.Description
		task.UpdatedAt = updatedAt
	}

	if upd.Status != nil && task.Status != *upd.Status {
		task.Status = *upd.Status
		task.UpdatedAt = updatedAt

		// task is transitioning from inactive to active, ensure scheduled and completed are updated
		if task.Status == influxdb.TaskStatusActive {
			updatedAtTrunc := updatedAt.Truncate(time.Second).UTC()
			task.LatestCompleted = updatedAtTrunc
			task.LatestScheduled = updatedAtTrunc
		}
	}

	if upd.Metadata != nil {
		task.Metadata = upd.Metadata
		task.UpdatedAt = updatedAt
	}

	if upd.LatestCompleted != nil {
		// make sure we only update latest completed one way
		tlc := task.LatestCompleted
		ulc := *upd.LatestCompleted

		if !ulc.IsZero() && ulc.After(tlc) {
			task.LatestCompleted = *upd.LatestCompleted
		}
	}

	if upd.LatestScheduled != nil {
		// make sure we only update latest scheduled one way
		if upd.LatestScheduled.After(task.LatestScheduled) {
			task.LatestScheduled = *upd.LatestScheduled
		}
	}

	if upd.LatestSuccess != nil {
		// make sure we only update latest success one way
		tlc := task.LatestSuccess
		ulc := *upd.LatestSuccess

		if !ulc.IsZero() && ulc.After(tlc) {
			task.LatestSuccess = *upd.LatestSuccess
		}
	}

	if upd.LatestFailure != nil {
		// make sure we only update latest failure one way
		tlc := task.LatestFailure
		ulc := *upd.LatestFailure

		if !ulc.IsZero() && ulc.After(tlc) {
			task.LatestFailure = *upd.LatestFailure
		}
	}

	if upd.LastRunStatus != nil {
		task.LastRunStatus = *upd.LastRunStatus
		if *upd.LastRunStatus == "failed" && upd.LastRunError != nil {
			task.LastRunError = *upd.LastRunError
		} else {
			task.LastRunError = ""
		}
	}

	// save the updated task
	bucket, err := tx.Bucket(taskBucket)
	if err != nil {
		return nil, influxdb.ErrUnexpectedTaskBucketErr(err)
	}
	key, err := taskKey(id)
	if err != nil {
		return nil, err
	}

	taskBytes, err := json.Marshal(task)
	if err != nil {
		return nil, influxdb.ErrInternalTaskServiceError(err)
	}

	err = bucket.Put(key, taskBytes)
	if err != nil {
		return nil, influxdb.ErrUnexpectedTaskBucketErr(err)
	}

	uid, _ := icontext.GetUserID(ctx)
	if err := s.audit.Log(resource.Change{
		Type:           resource.Update,
		ResourceID:     task.ID,
		ResourceType:   influxdb.TasksResourceType,
		OrganizationID: task.OrganizationID,
		UserID:         uid,
		ResourceBody:   taskBytes,
		Time:           time.Now(),
	}); err != nil {
		return nil, err
	}

	return task, nil
}

// DeleteTask removes a task by ID and purges all associated data and scheduled runs.
func (s *Service) DeleteTask(ctx context.Context, id influxdb.ID) error {
	err := s.kv.Update(ctx, func(tx Tx) error {
		err := s.deleteTask(ctx, tx, id)
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return err
	}

	return nil
}

func (s *Service) deleteTask(ctx context.Context, tx Tx, id influxdb.ID) error {
	taskBucket, err := tx.Bucket(taskBucket)
	if err != nil {
		return influxdb.ErrUnexpectedTaskBucketErr(err)
	}

	runBucket, err := tx.Bucket(taskRunBucket)
	if err != nil {
		return influxdb.ErrUnexpectedTaskBucketErr(err)
	}

	indexBucket, err := tx.Bucket(taskIndexBucket)
	if err != nil {
		return influxdb.ErrUnexpectedTaskBucketErr(err)
	}

	// retrieve the task
	task, err := s.findTaskByID(ctx, tx, id)
	if err != nil {
		return err
	}

	// remove the orgs index
	orgKey, err := taskOrgKey(task.OrganizationID, task.ID)
	if err != nil {
		return err
	}

	if err := indexBucket.Delete(orgKey); err != nil {
		return influxdb.ErrUnexpectedTaskBucketErr(err)
	}

	// remove latest completed
	lastCompletedKey, err := taskLatestCompletedKey(task.ID)
	if err != nil {
		return err
	}

	if err := runBucket.Delete(lastCompletedKey); err != nil {
		return influxdb.ErrUnexpectedTaskBucketErr(err)
	}

	// remove the runs
	runs, _, err := s.findRuns(ctx, tx, influxdb.RunFilter{Task: task.ID})
	if err != nil {
		return err
	}

	for _, run := range runs {
		key, err := taskRunKey(task.ID, run.ID)
		if err != nil {
			return err
		}

		if err := runBucket.Delete(key); err != nil {
			return influxdb.ErrUnexpectedTaskBucketErr(err)
		}
	}
	// remove the task
	key, err := taskKey(task.ID)
	if err != nil {
		return err
	}

	if err := taskBucket.Delete(key); err != nil {
		return influxdb.ErrUnexpectedTaskBucketErr(err)
	}

	uid, _ := icontext.GetUserID(ctx)
	return s.audit.Log(resource.Change{
		Type:           resource.Delete,
		ResourceID:     task.ID,
		ResourceType:   influxdb.TasksResourceType,
		OrganizationID: task.OrganizationID,
		UserID:         uid,
		Time:           time.Now(),
	})
}

// FindLogs returns logs for a run.
func (s *Service) FindLogs(ctx context.Context, filter influxdb.LogFilter) ([]*influxdb.Log, int, error) {
	var logs []*influxdb.Log
	err := s.kv.View(ctx, func(tx Tx) error {
		ls, _, err := s.findLogs(ctx, tx, filter)
		if err != nil {
			return err
		}
		logs = ls
		return nil
	})
	if err != nil {
		return nil, 0, err
	}

	return logs, len(logs), nil
}

func (s *Service) findLogs(ctx context.Context, tx Tx, filter influxdb.LogFilter) ([]*influxdb.Log, int, error) {
	if filter.Run != nil {
		r, err := s.findRunByID(ctx, tx, filter.Task, *filter.Run)
		if err != nil {
			return nil, 0, err
		}
		rtn := make([]*influxdb.Log, len(r.Log))
		for i := 0; i < len(r.Log); i++ {
			rtn[i] = &r.Log[i]
		}
		return rtn, len(rtn), nil
	}

	runs, _, err := s.findRuns(ctx, tx, influxdb.RunFilter{Task: filter.Task})
	if err != nil {
		return nil, 0, err
	}
	var logs []*influxdb.Log
	for _, run := range runs {
		for i := 0; i < len(run.Log); i++ {
			logs = append(logs, &run.Log[i])

		}
	}
	return logs, len(logs), nil
}

// FindRuns returns a list of runs that match a filter and the total count of returned runs.
func (s *Service) FindRuns(ctx context.Context, filter influxdb.RunFilter) ([]*influxdb.Run, int, error) {
	var runs []*influxdb.Run
	err := s.kv.View(ctx, func(tx Tx) error {
		rs, _, err := s.findRuns(ctx, tx, filter)
		if err != nil {
			return err
		}
		runs = rs
		return nil
	})
	if err != nil {
		return nil, 0, err
	}

	return runs, len(runs), nil
}

func (s *Service) findRuns(ctx context.Context, tx Tx, filter influxdb.RunFilter) ([]*influxdb.Run, int, error) {
	if filter.Limit == 0 {
		filter.Limit = influxdb.TaskDefaultPageSize
	}

	if filter.Limit < 0 || filter.Limit > influxdb.TaskMaxPageSize {
		return nil, 0, influxdb.ErrOutOfBoundsLimit
	}

	var runs []*influxdb.Run
	// manual runs
	manualRuns, err := s.manualRuns(ctx, tx, filter.Task)
	if err != nil {
		return nil, 0, err
	}
	for _, run := range manualRuns {
		runs = append(runs, run)
		if len(runs) >= filter.Limit {
			return runs, len(runs), nil
		}
	}

	// append currently running
	currentlyRunning, err := s.currentlyRunning(ctx, tx, filter.Task)
	if err != nil {
		return nil, 0, err
	}
	for _, run := range currentlyRunning {
		runs = append(runs, run)
		if len(runs) >= filter.Limit {
			return runs, len(runs), nil
		}
	}

	return runs, len(runs), nil
}

// FindRunByID returns a single run.
func (s *Service) FindRunByID(ctx context.Context, taskID, runID influxdb.ID) (*influxdb.Run, error) {
	var run *influxdb.Run
	err := s.kv.View(ctx, func(tx Tx) error {
		r, err := s.findRunByID(ctx, tx, taskID, runID)
		if err != nil {
			return err
		}
		run = r
		return nil
	})
	if err != nil {
		return nil, err
	}

	return run, nil
}

func (s *Service) findRunByID(ctx context.Context, tx Tx, taskID, runID influxdb.ID) (*influxdb.Run, error) {
	bucket, err := tx.Bucket(taskRunBucket)
	if err != nil {
		return nil, influxdb.ErrUnexpectedTaskBucketErr(err)
	}

	key, err := taskRunKey(taskID, runID)
	if err != nil {
		return nil, err
	}
	runBytes, err := bucket.Get(key)
	if err != nil {
		if IsNotFound(err) {
			return nil, influxdb.ErrRunNotFound
		}
		return nil, influxdb.ErrUnexpectedTaskBucketErr(err)
	}
	run := &influxdb.Run{}
	err = json.Unmarshal(runBytes, run)
	if err != nil {
		return nil, influxdb.ErrInternalTaskServiceError(err)
	}

	return run, nil
}

// CancelRun cancels a currently running run.
func (s *Service) CancelRun(ctx context.Context, taskID, runID influxdb.ID) error {
	err := s.kv.Update(ctx, func(tx Tx) error {
		err := s.cancelRun(ctx, tx, taskID, runID)
		if err != nil {
			return err
		}
		return nil
	})
	return err
}

func (s *Service) cancelRun(ctx context.Context, tx Tx, taskID, runID influxdb.ID) error {
	// get the run
	run, err := s.findRunByID(ctx, tx, taskID, runID)
	if err != nil {
		return err
	}

	// set status to canceled
	run.Status = "canceled"

	// save
	bucket, err := tx.Bucket(taskRunBucket)
	if err != nil {
		return influxdb.ErrUnexpectedTaskBucketErr(err)
	}

	runBytes, err := json.Marshal(run)
	if err != nil {
		return influxdb.ErrInternalTaskServiceError(err)
	}

	runKey, err := taskRunKey(taskID, runID)
	if err != nil {
		return err
	}

	if err := bucket.Put(runKey, runBytes); err != nil {
		return influxdb.ErrUnexpectedTaskBucketErr(err)
	}

	return nil
}

// RetryRun creates and returns a new run (which is a retry of another run).
func (s *Service) RetryRun(ctx context.Context, taskID, runID influxdb.ID) (*influxdb.Run, error) {
	var r *influxdb.Run
	err := s.kv.Update(ctx, func(tx Tx) error {
		run, err := s.retryRun(ctx, tx, taskID, runID)
		if err != nil {
			return err
		}
		r = run
		return nil
	})
	return r, err
}

func (s *Service) retryRun(ctx context.Context, tx Tx, taskID, runID influxdb.ID) (*influxdb.Run, error) {
	// find the run
	r, err := s.findRunByID(ctx, tx, taskID, runID)
	if err != nil {
		return nil, err
	}

	r.ID = s.IDGenerator.ID()
	r.Status = influxdb.RunScheduled.String()
	r.StartedAt = time.Time{}
	r.FinishedAt = time.Time{}
	r.RequestedAt = time.Time{}

	// add a clean copy of the run to the manual runs
	bucket, err := tx.Bucket(taskRunBucket)
	if err != nil {
		return nil, influxdb.ErrUnexpectedTaskBucketErr(err)
	}

	key, err := taskManualRunKey(taskID)
	if err != nil {
		return nil, err
	}

	runs := []*influxdb.Run{}
	runsBytes, err := bucket.Get(key)
	if err != nil {
		if err != ErrKeyNotFound {
			return nil, influxdb.ErrRunNotFound
		}
		return nil, influxdb.ErrUnexpectedTaskBucketErr(err)

	}

	if runsBytes != nil {
		if err := json.Unmarshal(runsBytes, &runs); err != nil {
			return nil, influxdb.ErrInternalTaskServiceError(err)
		}
	}

	runs = append(runs, r)

	// save manual runs
	runsBytes, err = json.Marshal(runs)
	if err != nil {
		return nil, influxdb.ErrInternalTaskServiceError(err)
	}

	if err := bucket.Put(key, runsBytes); err != nil {
		return nil, influxdb.ErrUnexpectedTaskBucketErr(err)
	}

	return r, nil
}

// ForceRun forces a run to occur with unix timestamp scheduledFor, to be executed as soon as possible.
// The value of scheduledFor may or may not align with the task's schedule.
func (s *Service) ForceRun(ctx context.Context, taskID influxdb.ID, scheduledFor int64) (*influxdb.Run, error) {
	var r *influxdb.Run
	err := s.kv.Update(ctx, func(tx Tx) error {
		run, err := s.forceRun(ctx, tx, taskID, scheduledFor)
		if err != nil {
			return err
		}
		r = run
		return nil
	})
	return r, err
}

func (s *Service) forceRun(ctx context.Context, tx Tx, taskID influxdb.ID, scheduledFor int64) (*influxdb.Run, error) {
	// create a run
	t := time.Unix(scheduledFor, 0).UTC()
	r := &influxdb.Run{
		ID:           s.IDGenerator.ID(),
		TaskID:       taskID,
		Status:       influxdb.RunScheduled.String(),
		RequestedAt:  time.Now().UTC(),
		ScheduledFor: t,
		Log:          []influxdb.Log{},
	}

	// add a clean copy of the run to the manual runs
	bucket, err := tx.Bucket(taskRunBucket)
	if err != nil {
		return nil, influxdb.ErrUnexpectedTaskBucketErr(err)
	}

	runs, err := s.manualRuns(ctx, tx, taskID)
	if err != nil {
		return nil, err
	}

	// check to see if this run is already queued
	for _, run := range runs {
		if run.ScheduledFor == r.ScheduledFor {
			return nil, influxdb.ErrTaskRunAlreadyQueued
		}
	}
	runs = append(runs, r)

	// save manual runs
	runsBytes, err := json.Marshal(runs)
	if err != nil {
		return nil, influxdb.ErrInternalTaskServiceError(err)
	}

	key, err := taskManualRunKey(taskID)
	if err != nil {
		return nil, err
	}

	if err := bucket.Put(key, runsBytes); err != nil {
		return nil, influxdb.ErrUnexpectedTaskBucketErr(err)
	}

	return r, nil
}

// CreateRun creates a run with a scheduledFor time as now.
func (s *Service) CreateRun(ctx context.Context, taskID influxdb.ID, scheduledFor time.Time, runAt time.Time) (*influxdb.Run, error) {
	var r *influxdb.Run
	err := s.kv.Update(ctx, func(tx Tx) error {
		run, err := s.createRun(ctx, tx, taskID, scheduledFor, runAt)
		if err != nil {
			return err
		}
		r = run
		return nil
	})
	return r, err
}
func (s *Service) createRun(ctx context.Context, tx Tx, taskID influxdb.ID, scheduledFor time.Time, runAt time.Time) (*influxdb.Run, error) {
	id := s.IDGenerator.ID()
	t := time.Unix(scheduledFor.Unix(), 0).UTC()

	run := influxdb.Run{
		ID:           id,
		TaskID:       taskID,
		ScheduledFor: t,
		RunAt:        runAt,
		Status:       influxdb.RunScheduled.String(),
		Log:          []influxdb.Log{},
	}

	b, err := tx.Bucket(taskRunBucket)
	if err != nil {
		return nil, influxdb.ErrUnexpectedTaskBucketErr(err)
	}

	runBytes, err := json.Marshal(run)
	if err != nil {
		return nil, influxdb.ErrInternalTaskServiceError(err)
	}

	runKey, err := taskRunKey(taskID, run.ID)
	if err != nil {
		return nil, err
	}
	if err := b.Put(runKey, runBytes); err != nil {
		return nil, influxdb.ErrUnexpectedTaskBucketErr(err)
	}

	return &run, nil
}

func (s *Service) CurrentlyRunning(ctx context.Context, taskID influxdb.ID) ([]*influxdb.Run, error) {
	var runs []*influxdb.Run
	err := s.kv.View(ctx, func(tx Tx) error {
		rs, err := s.currentlyRunning(ctx, tx, taskID)
		if err != nil {
			return err
		}
		runs = rs
		return nil
	})
	if err != nil {
		return nil, err
	}

	return runs, nil
}

func (s *Service) currentlyRunning(ctx context.Context, tx Tx, taskID influxdb.ID) ([]*influxdb.Run, error) {
	bucket, err := tx.Bucket(taskRunBucket)
	if err != nil {
		return nil, influxdb.ErrUnexpectedTaskBucketErr(err)
	}

	c, err := bucket.Cursor(WithCursorHintPrefix(taskID.String()))
	if err != nil {
		return nil, influxdb.ErrUnexpectedTaskBucketErr(err)
	}
	var runs []*influxdb.Run

	taskKey, err := taskKey(taskID)
	if err != nil {
		return nil, err
	}

	k, v := c.Seek(taskKey)
	for {
		if k == nil || !strings.HasPrefix(string(k), string(taskKey)) {
			break
		}
		if strings.HasSuffix(string(k), "manualRuns") || strings.HasSuffix(string(k), "latestCompleted") {
			k, v = c.Next()
			continue
		}
		r := &influxdb.Run{}
		if err := json.Unmarshal(v, r); err != nil {
			return nil, influxdb.ErrInternalTaskServiceError(err)
		}

		// if the run no longer belongs to the task we are done
		if r.TaskID != taskID {
			break
		}
		runs = append(runs, r)
		k, v = c.Next()
	}
	return runs, nil
}

func (s *Service) ManualRuns(ctx context.Context, taskID influxdb.ID) ([]*influxdb.Run, error) {
	var runs []*influxdb.Run
	err := s.kv.View(ctx, func(tx Tx) error {
		rs, err := s.manualRuns(ctx, tx, taskID)
		if err != nil {
			return err
		}
		runs = rs
		return nil
	})
	if err != nil {
		return nil, err
	}

	return runs, nil
}

func (s *Service) manualRuns(ctx context.Context, tx Tx, taskID influxdb.ID) ([]*influxdb.Run, error) {
	b, err := tx.Bucket(taskRunBucket)
	if err != nil {
		return nil, influxdb.ErrUnexpectedTaskBucketErr(err)
	}
	key, err := taskManualRunKey(taskID)
	if err != nil {
		return nil, err
	}

	runs := []*influxdb.Run{}
	val, err := b.Get(key)
	if err != nil {
		if err == ErrKeyNotFound {
			return runs, nil
		}
		return nil, influxdb.ErrUnexpectedTaskBucketErr(err)
	}
	if err := json.Unmarshal(val, &runs); err != nil {
		return nil, influxdb.ErrInternalTaskServiceError(err)
	}
	return runs, nil
}

func (s *Service) StartManualRun(ctx context.Context, taskID, runID influxdb.ID) (*influxdb.Run, error) {
	var r *influxdb.Run
	err := s.kv.Update(ctx, func(tx Tx) error {
		run, err := s.startManualRun(ctx, tx, taskID, runID)
		if err != nil {
			return err
		}
		r = run
		return nil
	})
	return r, err
}

func (s *Service) startManualRun(ctx context.Context, tx Tx, taskID, runID influxdb.ID) (*influxdb.Run, error) {

	mRuns, err := s.manualRuns(ctx, tx, taskID)
	if err != nil {
		return nil, influxdb.ErrRunNotFound
	}

	if len(mRuns) < 1 {
		return nil, influxdb.ErrRunNotFound
	}

	var run *influxdb.Run
	for i, r := range mRuns {
		if r.ID == runID {
			run = r
			mRuns = append(mRuns[:i], mRuns[i+1:]...)
		}
	}
	if run == nil {
		return nil, influxdb.ErrRunNotFound
	}

	// save manual runs
	mRunsBytes, err := json.Marshal(mRuns)
	if err != nil {
		return nil, influxdb.ErrInternalTaskServiceError(err)
	}

	runsKey, err := taskManualRunKey(taskID)
	if err != nil {
		return nil, err
	}

	b, err := tx.Bucket(taskRunBucket)
	if err != nil {
		return nil, influxdb.ErrUnexpectedTaskBucketErr(err)
	}

	if err := b.Put(runsKey, mRunsBytes); err != nil {
		return nil, influxdb.ErrUnexpectedTaskBucketErr(err)
	}

	// add mRun to the list of currently running
	mRunBytes, err := json.Marshal(run)
	if err != nil {
		return nil, influxdb.ErrInternalTaskServiceError(err)
	}

	runKey, err := taskRunKey(taskID, run.ID)
	if err != nil {
		return nil, err
	}

	if err := b.Put(runKey, mRunBytes); err != nil {
		return nil, influxdb.ErrUnexpectedTaskBucketErr(err)
	}

	return run, nil
}

// FinishRun removes runID from the list of running tasks and if its `now` is later then last completed update it.
func (s *Service) FinishRun(ctx context.Context, taskID, runID influxdb.ID) (*influxdb.Run, error) {
	var run *influxdb.Run
	err := s.kv.Update(ctx, func(tx Tx) error {
		r, err := s.finishRun(ctx, tx, taskID, runID)
		if err != nil {
			return err
		}
		run = r
		return nil
	})
	return run, err
}

func (s *Service) finishRun(ctx context.Context, tx Tx, taskID, runID influxdb.ID) (*influxdb.Run, error) {
	// get the run
	r, err := s.findRunByID(ctx, tx, taskID, runID)
	if err != nil {
		return nil, err
	}

	// tell task to update latest completed
	scheduled := r.ScheduledFor

	var latestSuccess, latestFailure *time.Time

	if r.Status == "failed" {
		latestFailure = &scheduled
	} else {
		latestSuccess = &scheduled
	}

	_, err = s.updateTask(ctx, tx, taskID, influxdb.TaskUpdate{
		LatestCompleted: &scheduled,
		LatestSuccess:   latestSuccess,
		LatestFailure:   latestFailure,
		LastRunStatus:   &r.Status,
		LastRunError: func() *string {
			if r.Status == "failed" {
				// prefer the second to last log message as the error message
				// per https://github.com/influxdata/influxdb/issues/15153#issuecomment-547706005
				if len(r.Log) > 1 {
					return &r.Log[len(r.Log)-2].Message
				} else if len(r.Log) > 0 {
					return &r.Log[len(r.Log)-1].Message
				}
			}
			return nil
		}(),
	})
	if err != nil {
		return nil, err
	}

	// remove run
	bucket, err := tx.Bucket(taskRunBucket)
	if err != nil {
		return nil, influxdb.ErrUnexpectedTaskBucketErr(err)
	}
	key, err := taskRunKey(taskID, runID)
	if err != nil {
		return nil, err
	}
	if err := bucket.Delete(key); err != nil {
		return nil, influxdb.ErrUnexpectedTaskBucketErr(err)
	}

	return r, nil
}

// UpdateRunState sets the run state at the respective time.
func (s *Service) UpdateRunState(ctx context.Context, taskID, runID influxdb.ID, when time.Time, state influxdb.RunStatus) error {
	err := s.kv.Update(ctx, func(tx Tx) error {
		err := s.updateRunState(ctx, tx, taskID, runID, when, state)
		if err != nil {
			return err
		}
		return nil
	})
	return err
}

func (s *Service) updateRunState(ctx context.Context, tx Tx, taskID, runID influxdb.ID, when time.Time, state influxdb.RunStatus) error {
	// find run
	run, err := s.findRunByID(ctx, tx, taskID, runID)
	if err != nil {
		return err
	}

	// update state
	run.Status = state.String()
	switch state {
	case influxdb.RunStarted:
		run.StartedAt = when
	case influxdb.RunSuccess, influxdb.RunFail, influxdb.RunCanceled:
		run.FinishedAt = when
	}

	// save run
	b, err := tx.Bucket(taskRunBucket)
	if err != nil {
		return influxdb.ErrUnexpectedTaskBucketErr(err)
	}

	runBytes, err := json.Marshal(run)
	if err != nil {
		return influxdb.ErrInternalTaskServiceError(err)
	}

	runKey, err := taskRunKey(taskID, run.ID)
	if err != nil {
		return err
	}
	if err := b.Put(runKey, runBytes); err != nil {
		return influxdb.ErrUnexpectedTaskBucketErr(err)
	}

	return nil
}

// AddRunLog adds a log line to the run.
func (s *Service) AddRunLog(ctx context.Context, taskID, runID influxdb.ID, when time.Time, log string) error {
	err := s.kv.Update(ctx, func(tx Tx) error {
		err := s.addRunLog(ctx, tx, taskID, runID, when, log)
		if err != nil {
			return err
		}
		return nil
	})
	return err
}

func (s *Service) addRunLog(ctx context.Context, tx Tx, taskID, runID influxdb.ID, when time.Time, log string) error {
	// find run
	run, err := s.findRunByID(ctx, tx, taskID, runID)
	if err != nil {
		return err
	}
	// update log
	l := influxdb.Log{RunID: runID, Time: when.Format(time.RFC3339Nano), Message: log}
	run.Log = append(run.Log, l)
	// save run
	b, err := tx.Bucket(taskRunBucket)
	if err != nil {
		return influxdb.ErrUnexpectedTaskBucketErr(err)
	}

	runBytes, err := json.Marshal(run)
	if err != nil {
		return influxdb.ErrInternalTaskServiceError(err)
	}

	runKey, err := taskRunKey(taskID, run.ID)
	if err != nil {
		return err
	}

	if err := b.Put(runKey, runBytes); err != nil {
		return influxdb.ErrUnexpectedTaskBucketErr(err)
	}

	return nil
}

func taskKey(taskID influxdb.ID) ([]byte, error) {
	encodedID, err := taskID.Encode()
	if err != nil {
		return nil, influxdb.ErrInvalidTaskID
	}
	return encodedID, nil
}

func taskLatestCompletedKey(taskID influxdb.ID) ([]byte, error) {
	encodedID, err := taskID.Encode()
	if err != nil {
		return nil, influxdb.ErrInvalidTaskID
	}
	return []byte(string(encodedID) + "/latestCompleted"), nil
}

func taskManualRunKey(taskID influxdb.ID) ([]byte, error) {
	encodedID, err := taskID.Encode()
	if err != nil {
		return nil, influxdb.ErrInvalidTaskID
	}
	return []byte(string(encodedID) + "/manualRuns"), nil
}

func taskOrgKey(orgID, taskID influxdb.ID) ([]byte, error) {
	encodedOrgID, err := orgID.Encode()
	if err != nil {
		return nil, influxdb.ErrInvalidTaskID
	}
	encodedID, err := taskID.Encode()
	if err != nil {
		return nil, influxdb.ErrInvalidTaskID
	}

	return []byte(string(encodedOrgID) + "/" + string(encodedID)), nil
}

func taskRunKey(taskID, runID influxdb.ID) ([]byte, error) {
	encodedID, err := taskID.Encode()
	if err != nil {
		return nil, influxdb.ErrInvalidTaskID
	}
	encodedRunID, err := runID.Encode()
	if err != nil {
		return nil, influxdb.ErrInvalidTaskID
	}

	return []byte(string(encodedID) + "/" + string(encodedRunID)), nil
}

// ExtractTaskOptions is a feature-flag driven switch between normal options
// parsing and a more simplified variant.
//
// The simplified variant extracts the options assignment and passes only that
// content through the parser. This allows us to allow scenarios like [1] to
// pass through options validation. One clear drawback of this is that it
// requires constant values for the parameter assignments. However, most people
// are doing that anyway.
//
// [1]: https://github.com/influxdata/influxdb/issues/17666
func ExtractTaskOptions(ctx context.Context, lang influxdb.FluxLanguageService, flux string) (options.Options, error) {
	if feature.SimpleTaskOptionsExtraction().Enabled(ctx) {
		return options.FromScriptAST(lang, flux)
	}
	return options.FromScript(lang, flux)
}

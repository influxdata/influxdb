package kv

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"github.com/influxdata/influxdb"
	icontext "github.com/influxdata/influxdb/context"
	"github.com/influxdata/influxdb/task/backend"
	"github.com/influxdata/influxdb/task/options"
	cron "gopkg.in/robfig/cron.v2"
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
var _ backend.TaskControlService = (*Service)(nil)

func (s *Service) initializeTasks(ctx context.Context, tx Tx) error {
	if _, err := tx.Bucket(taskBucket); err != nil {
		return err
	}
	if _, err := tx.Bucket(taskRunBucket); err != nil {
		return err
	}
	if _, err := tx.Bucket(taskIndexBucket); err != nil {
		return err
	}
	return nil
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
	t := &influxdb.Task{}
	if err := json.Unmarshal(v, t); err != nil {
		return nil, influxdb.ErrInternalTaskServiceError(err)
	}
	latestCompleted, err := s.findLatestCompletedTime(ctx, tx, t.ID)
	if err != nil {
		return nil, err
	}
	if !latestCompleted.IsZero() {
		if t.LatestCompleted != "" {
			tlc, err := time.Parse(time.RFC3339, t.LatestCompleted)
			if err == nil && latestCompleted.After(tlc) {
				t.LatestCompleted = latestCompleted.Format(time.RFC3339)

			}
		} else {
			t.LatestCompleted = latestCompleted.Format(time.RFC3339)
		}
	}

	if t.LatestCompleted == "" {
		t.LatestCompleted = t.CreatedAt
	}
	latestCompleted, err = time.Parse(time.RFC3339, t.LatestCompleted)
	if err != nil {
		return nil, err
	}

	return t, nil
}

// FindTasks returns a list of tasks that match a filter (limit 100) and the total count
// of matching tasks.
func (s *Service) FindTasks(ctx context.Context, filter influxdb.TaskFilter) ([]*influxdb.Task, int, error) {
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

	var org *influxdb.Organization
	var err error
	if filter.OrganizationID != nil {
		org, err = s.findOrganizationByID(ctx, tx, *filter.OrganizationID)
		if err != nil {
			return nil, 0, err
		}
	} else if filter.Organization != "" {
		org, err = s.findOrganizationByName(ctx, tx, filter.Organization)
		if err != nil {
			return nil, 0, err
		}
	}

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
	if org == nil && filter.User == nil {
		userAuth, err := icontext.GetAuthorizer(ctx)
		if err == nil {
			userID := userAuth.GetUserID()
			filter.User = &userID
		}
	}

	// filter by user id.
	if filter.User != nil {
		return s.findTasksByUser(ctx, tx, filter)
	} else if org != nil {
		return s.findTaskByOrg(ctx, tx, filter)
	}

	return s.findAllTasks(ctx, tx, filter)
}

// findTasksByUser is a subset of the find tasks function. Used for cleanliness
func (s *Service) findTasksByUser(ctx context.Context, tx Tx, filter influxdb.TaskFilter) ([]*influxdb.Task, int, error) {
	if filter.User == nil {
		return nil, 0, influxdb.ErrTaskNotFound
	}
	var org *influxdb.Organization
	var err error
	if filter.OrganizationID != nil {
		org, err = s.findOrganizationByID(ctx, tx, *filter.OrganizationID)
		if err != nil {
			return nil, 0, err
		}
	} else if filter.Organization != "" {
		org, err = s.findOrganizationByName(ctx, tx, filter.Organization)
		if err != nil {
			return nil, 0, err
		}
	}

	var ts []*influxdb.Task

	maps, err := s.findUserResourceMappings(
		ctx,
		tx,
		influxdb.UserResourceMappingFilter{
			ResourceType: influxdb.TasksResourceType,
			UserID:       *filter.User,
			UserType:     influxdb.Owner,
		},
	)
	if err != nil {
		return nil, 0, err
	}

	for _, m := range maps {
		task, err := s.findTaskByID(ctx, tx, m.ResourceID)
		if err != nil && err != influxdb.ErrTaskNotFound {
			return nil, 0, err
		}
		if err == influxdb.ErrTaskNotFound {
			continue
		}

		if org != nil && task.OrganizationID != org.ID {
			continue
		}

		if filter.Type == nil {
			ft := ""
			filter.Type = &ft
		}
		if *filter.Type != influxdb.TaskTypeWildcard && *filter.Type != task.Type {
			continue
		}

		ts = append(ts, task)

		if len(ts) >= filter.Limit {
			break
		}
	}
	return ts, len(ts), nil
}

// findTaskByOrg is a subset of the find tasks function. Used for cleanliness
func (s *Service) findTaskByOrg(ctx context.Context, tx Tx, filter influxdb.TaskFilter) ([]*influxdb.Task, int, error) {
	var org *influxdb.Organization
	var err error
	if filter.OrganizationID != nil {
		org, err = s.findOrganizationByID(ctx, tx, *filter.OrganizationID)
		if err != nil {
			return nil, 0, err
		}
	} else if filter.Organization != "" {
		org, err = s.findOrganizationByName(ctx, tx, filter.Organization)
		if err != nil {
			return nil, 0, err
		}
	}

	if org == nil {
		return nil, 0, influxdb.ErrTaskNotFound
	}

	var ts []*influxdb.Task

	indexBucket, err := tx.Bucket(taskIndexBucket)
	if err != nil {
		return nil, 0, influxdb.ErrUnexpectedTaskBucketErr(err)
	}

	c, err := indexBucket.Cursor()
	if err != nil {
		return nil, 0, influxdb.ErrUnexpectedTaskBucketErr(err)
	}
	// we can filter by orgID
	if filter.After != nil {
		key, err := taskOrgKey(org.ID, *filter.After)
		if err != nil {
			return nil, 0, err
		}
		// ignore the key:val returned in this seek because we are starting "after"
		// this key
		c.Seek(key)
	} else {
		// if we dont have an after we just move the cursor to the first instance of the
		// orgID
		key, err := org.ID.Encode()
		if err != nil {
			return nil, 0, influxdb.ErrInvalidTaskID
		}
		k, v := c.Seek(key)
		if k != nil {
			id, err := influxdb.IDFromString(string(v))
			if err != nil {
				return nil, 0, influxdb.ErrInvalidTaskID
			}

			t, err := s.findTaskByID(ctx, tx, *id)
			if err != nil && err != influxdb.ErrTaskNotFound {
				// we might have some crufty index's
				return nil, 0, err
			}

			if t != nil {
				typ := ""
				if filter.Type != nil {
					typ = *filter.Type
				}

				// if the filter type matches task type or filter type is a wildcard
				if typ == t.Type || typ == influxdb.TaskTypeWildcard {
					ts = append(ts, t)
				}
			}
		}
	}

	// if someone has a limit of 1
	if len(ts) >= filter.Limit {
		return ts, len(ts), nil
	}

	for {
		k, v := c.Next()
		if k == nil {
			break
		}

		id, err := influxdb.IDFromString(string(v))
		if err != nil {
			return nil, 0, influxdb.ErrInvalidTaskID
		}

		t, err := s.findTaskByID(ctx, tx, *id)
		if err != nil {
			if err == influxdb.ErrTaskNotFound {
				// we might have some crufty index's
				continue
			}
			return nil, 0, err
		}

		// If the new task doesn't belong to the org we have looped outside the org filter
		if org != nil && t.OrganizationID != org.ID {
			break
		}

		if filter.Type == nil {
			ft := ""
			filter.Type = &ft
		}
		if *filter.Type != influxdb.TaskTypeWildcard && *filter.Type != t.Type {
			continue
		}

		// insert the new task into the list
		ts = append(ts, t)

		// Check if we are over running the limit
		if len(ts) >= filter.Limit {
			break
		}
	}
	return ts, len(ts), err
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

	c, err := taskBucket.Cursor()
	if err != nil {
		return nil, 0, influxdb.ErrUnexpectedTaskBucketErr(err)
	}
	// we can filter by orgID
	if filter.After != nil {

		key, err := taskKey(*filter.After)
		if err != nil {
			return nil, 0, err
		}
		// ignore the key:val returned in this seek because we are starting "after"
		// this key
		c.Seek(key)
	} else {
		k, v := c.First()
		if k == nil {
			return ts, len(ts), nil
		}

		t := &influxdb.Task{}
		if err := json.Unmarshal(v, t); err != nil {
			return nil, 0, influxdb.ErrInternalTaskServiceError(err)
		}
		latestCompleted, err := s.findLatestCompletedTime(ctx, tx, t.ID)
		if err != nil {
			return nil, 0, err
		}
		if !latestCompleted.IsZero() {
			t.LatestCompleted = latestCompleted.Format(time.RFC3339)
		} else {
			t.LatestCompleted = t.CreatedAt
		}
		// insert the new task into the list
		ts = append(ts, t)
	}

	// if someone has a limit of 1
	if len(ts) >= filter.Limit {
		return ts, len(ts), nil
	}

	for {
		k, v := c.Next()
		if k == nil {
			break
		}
		t := &influxdb.Task{}
		if err := json.Unmarshal(v, t); err != nil {
			return nil, 0, influxdb.ErrInternalTaskServiceError(err)
		}
		latestCompleted, err := s.findLatestCompletedTime(ctx, tx, t.ID)
		if err != nil {
			return nil, 0, err
		}
		if !latestCompleted.IsZero() {
			t.LatestCompleted = latestCompleted.Format(time.RFC3339)
		} else {
			t.LatestCompleted = t.CreatedAt
		}
		// insert the new task into the list
		ts = append(ts, t)

		// Check if we are over running the limit
		if len(ts) >= filter.Limit {
			break
		}
	}
	return ts, len(ts), err
}

// CreateTask creates a new task.
// The owner of the task is inferred from the authorizer associated with ctx.
func (s *Service) CreateTask(ctx context.Context, tc influxdb.TaskCreate) (*influxdb.Task, error) {
	var t *influxdb.Task
	err := s.kv.Update(ctx, func(tx Tx) error {
		task, err := s.createTask(ctx, tx, tc)
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

func (s *Service) createTask(ctx context.Context, tx Tx, tc influxdb.TaskCreate) (*influxdb.Task, error) {
	userAuth, err := icontext.GetAuthorizer(ctx)
	if err != nil {
		return nil, err
	}

	if tc.Token == "" {
		return nil, influxdb.ErrMissingToken
	}

	auth, err := s.findAuthorizationByToken(ctx, tx, tc.Token)
	if err != nil {
		if err.Error() != "<not found> authorization not found" {
			return nil, err
		}
	}

	var org *influxdb.Organization
	if tc.OrganizationID.Valid() {
		org, err = s.findOrganizationByID(ctx, tx, tc.OrganizationID)
		if err != nil {
			return nil, err
		}
	} else if tc.Organization != "" {
		org, err = s.findOrganizationByName(ctx, tx, tc.Organization)
		if err != nil {
			return nil, err
		}
	}
	if org == nil {
		return nil, influxdb.ErrOrgNotFound
	}

	opt, err := options.FromScript(tc.Flux)
	if err != nil {
		return nil, influxdb.ErrTaskOptionParse(err)
	}

	if tc.Status == "" {
		tc.Status = string(backend.TaskActive)
	}

	createdAt := time.Now().UTC().Format(time.RFC3339)
	task := &influxdb.Task{
		ID:              s.IDGenerator.ID(),
		Type:            tc.Type,
		OrganizationID:  org.ID,
		Organization:    org.Name,
		AuthorizationID: auth.Identifier(),
		Name:            opt.Name,
		Description:     tc.Description,
		Status:          tc.Status,
		Flux:            tc.Flux,
		Every:           opt.Every.String(),
		Cron:            opt.Cron,
		CreatedAt:       createdAt,
		LatestCompleted: createdAt,
	}
	if opt.Offset != nil {
		task.Offset = opt.Offset.String()
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
	if err := s.createUserResourceMapping(ctx, tx, &influxdb.UserResourceMapping{
		ResourceType: influxdb.TasksResourceType,
		ResourceID:   task.ID,
		UserID:       userAuth.GetUserID(),
		UserType:     influxdb.Owner,
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

	// update the flux script
	if !upd.Options.IsZero() || upd.Flux != nil {
		if err = upd.UpdateFlux(task.Flux); err != nil {
			return nil, err
		}
		task.Flux = *upd.Flux

		options, err := options.FromScript(*upd.Flux)
		if err != nil {
			return nil, influxdb.ErrTaskOptionParse(err)
		}
		task.Name = options.Name
		task.Every = options.Every.String()
		task.Cron = options.Cron
		if options.Offset != nil {
			task.Offset = options.Offset.String()
		}
	}

	// update the Token
	if upd.Token != "" {
		auth, err := s.findAuthorizationByToken(ctx, tx, upd.Token)
		if err != nil {
			return nil, err
		}
		task.AuthorizationID = auth.ID
	}

	if upd.Description != nil {
		task.Description = *upd.Description
	}

	if upd.Status != nil {
		task.Status = *upd.Status
	}

	if upd.LatestCompleted != nil {
		task.LatestCompleted = *upd.LatestCompleted
	}

	task.UpdatedAt = time.Now().UTC().Format(time.RFC3339)
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

	return task, bucket.Put(key, taskBytes)
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

	return s.deleteUserResourceMapping(ctx, tx, influxdb.UserResourceMappingFilter{
		ResourceID: task.ID,
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
	bucket, err := tx.Bucket(taskBucket)
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
	r.Status = backend.RunScheduled.String()
	r.StartedAt = ""
	r.FinishedAt = ""
	r.RequestedAt = ""

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
		Status:       backend.RunScheduled.String(),
		RequestedAt:  time.Now().UTC().Format(time.RFC3339),
		ScheduledFor: t.Format(time.RFC3339),
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

// CreateNextRun creates the earliest needed run scheduled no later than the given Unix timestamp now.
// Internally, the Store should rely on the underlying task's StoreTaskMeta to create the next run.
func (s *Service) CreateNextRun(ctx context.Context, taskID influxdb.ID, now int64) (backend.RunCreation, error) {
	var rc backend.RunCreation
	err := s.kv.Update(ctx, func(tx Tx) error {
		runCreate, err := s.createNextRun(ctx, tx, taskID, now)
		if err != nil {
			return err
		}
		rc = runCreate
		return nil
	})
	return rc, err
}

func (s *Service) createNextRun(ctx context.Context, tx Tx, taskID influxdb.ID, now int64) (backend.RunCreation, error) {
	// pull the scheduler for the task
	task, err := s.findTaskByID(ctx, tx, taskID)
	if err != nil {
		return backend.RunCreation{}, err
	}

	// check if we have any manual runs queued
	mRuns, err := s.manualRuns(ctx, tx, taskID)
	if err != nil {
		return backend.RunCreation{}, err
	}

	if len(mRuns) > 0 {
		mRun := mRuns[0]
		mRuns := mRuns[1:]
		// save manual runs
		b, err := tx.Bucket(taskRunBucket)
		if err != nil {
			return backend.RunCreation{}, influxdb.ErrUnexpectedTaskBucketErr(err)
		}
		mRunsBytes, err := json.Marshal(mRuns)
		if err != nil {
			return backend.RunCreation{}, influxdb.ErrInternalTaskServiceError(err)
		}

		runsKey, err := taskManualRunKey(taskID)
		if err != nil {
			return backend.RunCreation{}, err
		}

		if err := b.Put(runsKey, mRunsBytes); err != nil {
			return backend.RunCreation{}, influxdb.ErrUnexpectedTaskBucketErr(err)
		}
		// add mRun to the list of currently running
		mRunBytes, err := json.Marshal(mRun)
		if err != nil {
			return backend.RunCreation{}, influxdb.ErrInternalTaskServiceError(err)
		}

		runKey, err := taskRunKey(taskID, mRun.ID)
		if err != nil {
			return backend.RunCreation{}, err
		}

		if err := b.Put(runKey, mRunBytes); err != nil {
			return backend.RunCreation{}, influxdb.ErrUnexpectedTaskBucketErr(err)
		}

		// return mRun
		schedFor, err := mRun.ScheduledForTime()
		if err != nil {
			return backend.RunCreation{}, err
		}

		reqAt, err := mRun.RequestedAtTime()
		if err != nil {
			return backend.RunCreation{}, err
		}

		nextDue, err := s.nextDueRun(ctx, tx, taskID)
		if err != nil {
			return backend.RunCreation{}, err
		}

		rc := backend.RunCreation{
			Created: backend.QueuedRun{
				TaskID: taskID,
				RunID:  mRun.ID,
				DueAt:  time.Now().UTC().Unix(),
				Now:    schedFor.Unix(),
			},
			NextDue:  nextDue,
			HasQueue: len(mRuns) > 0,
		}
		if !reqAt.IsZero() {
			rc.Created.RequestedAt = reqAt.Unix()
		}
		return rc, nil
	}

	// get the latest completed and the latest currently running run's time
	// the earliest it could have been completed is "created at"
	latestCompleted, err := time.Parse(time.RFC3339, task.CreatedAt)
	if err != nil {
		return backend.RunCreation{}, influxdb.ErrTaskTimeParse(err)
	}

	// we could have a latest completed newer then the created at time.
	if task.LatestCompleted != "" {
		lc, err := time.Parse(time.RFC3339, task.LatestCompleted)
		if err == nil && lc.After(latestCompleted) {
			latestCompleted = lc
		}
	}

	lRun, err := s.findLatestCompleted(ctx, tx, taskID)
	if err != nil {
		return backend.RunCreation{}, err
	}

	if lRun != nil {
		runTime, err := lRun.ScheduledForTime()
		if err != nil {
			return backend.RunCreation{}, err
		}
		if runTime.After(latestCompleted) {
			latestCompleted = runTime
		}
	}
	// Align create to the hour/minute
	// If we decide we no longer want to do this we can just remove the code block below
	{
		if strings.HasPrefix(task.EffectiveCron(), "@every ") {
			everyString := strings.TrimPrefix(task.EffectiveCron(), "@every ")
			every := options.Duration{}
			err := every.Parse(everyString)
			if err != nil {
				// We cannot align a invalid time
				goto NoChange
			}
			t := time.Unix(latestCompleted.Unix(), 0)
			everyDur, err := every.DurationFrom(t)
			if err != nil {
				goto NoChange
			}
			t = t.Truncate(everyDur)
			latestCompleted = t.Truncate(time.Second)
		}
	NoChange:
	}

	// create a run if possible
	sch, err := cron.Parse(task.EffectiveCron())
	if err != nil {
		return backend.RunCreation{}, influxdb.ErrTaskTimeParse(err)
	}
	nowTime := time.Unix(now, 0)
	nextScheduled := sch.Next(latestCompleted).UTC()
	nextScheduledUnix := nextScheduled.Unix()
	offset := &options.Duration{}
	if err := offset.Parse(task.Offset); err != nil {
		return backend.RunCreation{}, influxdb.ErrTaskTimeParse(err)
	}
	dueAt, err := offset.Add(nextScheduled)
	if err != nil {
		return backend.RunCreation{}, influxdb.ErrTaskTimeParse(err)
	}
	if dueAt.After(nowTime) {
		return backend.RunCreation{}, influxdb.ErrRunNotDueYet(dueAt.Unix())
	}

	id := s.IDGenerator.ID()

	run := influxdb.Run{
		ID:           id,
		TaskID:       task.ID,
		ScheduledFor: nextScheduled.Format(time.RFC3339),
		Status:       backend.RunScheduled.String(),
		Log:          []influxdb.Log{},
	}
	b, err := tx.Bucket(taskRunBucket)
	if err != nil {
		return backend.RunCreation{}, influxdb.ErrUnexpectedTaskBucketErr(err)
	}

	runBytes, err := json.Marshal(run)
	if err != nil {
		return backend.RunCreation{}, influxdb.ErrInternalTaskServiceError(err)
	}

	runKey, err := taskRunKey(taskID, run.ID)
	if err != nil {
		return backend.RunCreation{}, err
	}
	if err := b.Put(runKey, runBytes); err != nil {
		return backend.RunCreation{}, influxdb.ErrUnexpectedTaskBucketErr(err)
	}

	nextDue, err := offset.Add(sch.Next(nextScheduled))
	if err != nil {
		return backend.RunCreation{}, influxdb.ErrTaskTimeParse(err)
	}
	// populate RunCreation
	return backend.RunCreation{
		Created: backend.QueuedRun{
			TaskID: taskID,
			RunID:  id,
			DueAt:  dueAt.Unix(),
			Now:    nextScheduledUnix,
		},
		NextDue:  nextDue.Unix(),
		HasQueue: false,
	}, nil
}

// CreateRun creates a run with a scheduledFor time as now.
func (s *Service) CreateRun(ctx context.Context, taskID influxdb.ID, scheduledFor time.Time) (*influxdb.Run, error) {
	var r *influxdb.Run
	err := s.kv.Update(ctx, func(tx Tx) error {
		run, err := s.createRun(ctx, tx, taskID, scheduledFor)
		if err != nil {
			return err
		}
		r = run
		return nil
	})
	return r, err
}
func (s *Service) createRun(ctx context.Context, tx Tx, taskID influxdb.ID, scheduledFor time.Time) (*influxdb.Run, error) {
	id := s.IDGenerator.ID()

	run := influxdb.Run{
		ID:           id,
		TaskID:       taskID,
		ScheduledFor: scheduledFor.Format(time.RFC3339),
		Status:       backend.RunScheduled.String(),
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

	c, err := bucket.Cursor()
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
	rTime, err := r.ScheduledForTime()
	if err != nil {
		return nil, err
	}
	// check if its latest
	latestRun, err := s.findLatestCompleted(ctx, tx, taskID)
	if err != nil {
		return nil, err
	}

	var lTime time.Time
	if latestRun != nil {
		lTime, err = latestRun.ScheduledForTime()
		if err != nil {
			return nil, err
		}
	}
	bucket, err := tx.Bucket(taskRunBucket)
	if err != nil {
		return nil, influxdb.ErrUnexpectedTaskBucketErr(err)
	}

	if rTime.After(lTime) {
		rb, err := json.Marshal(r)
		if err != nil {
			return nil, influxdb.ErrInternalTaskServiceError(err)
		}
		lKey, err := taskLatestCompletedKey(taskID)
		if err != nil {
			return nil, err
		}

		if err := bucket.Put(lKey, rb); err != nil {
			return nil, influxdb.ErrUnexpectedTaskBucketErr(err)
		}
	}

	// remove run
	key, err := taskRunKey(taskID, runID)
	if err != nil {
		return nil, err
	}
	if err := bucket.Delete(key); err != nil {
		return nil, influxdb.ErrUnexpectedTaskBucketErr(err)
	}

	return r, nil
}

// NextDueRun returns the Unix timestamp of when the next call to CreateNextRun will be ready.
// The returned timestamp reflects the task's offset, so it does not necessarily exactly match the schedule time.
func (s *Service) NextDueRun(ctx context.Context, taskID influxdb.ID) (int64, error) {
	var nextDue int64
	err := s.kv.View(ctx, func(tx Tx) error {
		due, err := s.nextDueRun(ctx, tx, taskID)
		if err != nil {
			return err
		}
		nextDue = due
		return nil
	})
	if err != nil {
		return 0, err
	}

	return nextDue, nil
}

func (s *Service) nextDueRun(ctx context.Context, tx Tx, taskID influxdb.ID) (int64, error) {
	task, err := s.findTaskByID(ctx, tx, taskID)
	if err != nil {
		return 0, err
	}

	latestCompleted, err := time.Parse(time.RFC3339, task.LatestCompleted)
	if err != nil {
		return 0, err
	}

	// create a run if possible
	sch, err := cron.Parse(task.EffectiveCron())
	if err != nil {
		return 0, influxdb.ErrTaskTimeParse(err)
	}
	nextScheduled := sch.Next(latestCompleted).UTC()

	return nextScheduled.Unix(), nil
}

// UpdateRunState sets the run state at the respective time.
func (s *Service) UpdateRunState(ctx context.Context, taskID, runID influxdb.ID, when time.Time, state backend.RunStatus) error {
	err := s.kv.Update(ctx, func(tx Tx) error {
		err := s.updateRunState(ctx, tx, taskID, runID, when, state)
		if err != nil {
			return err
		}
		return nil
	})
	return err
}

func (s *Service) updateRunState(ctx context.Context, tx Tx, taskID, runID influxdb.ID, when time.Time, state backend.RunStatus) error {
	// find run
	run, err := s.findRunByID(ctx, tx, taskID, runID)
	if err != nil {
		return err
	}

	// update state
	run.Status = state.String()
	switch state {
	case backend.RunStarted:
		run.StartedAt = when.UTC().Format(time.RFC3339Nano)
	case backend.RunSuccess, backend.RunFail, backend.RunCanceled:
		run.FinishedAt = when.UTC().Format(time.RFC3339Nano)
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

func (s *Service) findLatestCompleted(ctx context.Context, tx Tx, id influxdb.ID) (*influxdb.Run, error) {
	bucket, err := tx.Bucket(taskRunBucket)
	if err != nil {
		return nil, influxdb.ErrUnexpectedTaskBucketErr(err)
	}
	key, err := taskLatestCompletedKey(id)
	if err != nil {
		return nil, err
	}

	bytes, err := bucket.Get(key)
	if err != nil {
		if err == ErrKeyNotFound {
			return nil, nil
		}
		return nil, influxdb.ErrUnexpectedTaskBucketErr(err)
	}

	run := &influxdb.Run{}
	if err = json.Unmarshal(bytes, run); err != nil {
		return nil, influxdb.ErrInternalTaskServiceError(err)
	}

	return run, nil
}

func (s *Service) findLatestCompletedTime(ctx context.Context, tx Tx, id influxdb.ID) (time.Time, error) {
	run, err := s.findLatestCompleted(ctx, tx, id)
	if err != nil {
		return time.Time{}, err
	}
	if run == nil {
		return time.Time{}, nil
	}

	return run.ScheduledForTime()
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

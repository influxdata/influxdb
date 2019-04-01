package kv

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/task/backend"
	"github.com/influxdata/influxdb/task/options"
	// icontext "github.com/influxdata/influxdb/context"
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
			return &influxdb.Error{
				Err: err,
			}
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
	encodedID, err := id.Encode()
	if err != nil {
		return nil, &influxdb.Error{
			Code: influxdb.EInvalid,
			Err:  err,
		}
	}

	b, err := tx.Bucket(taskBucket)
	if err != nil {
		return nil, err
	}

	v, err := b.Get(encodedID)
	if IsNotFound(err) {
		return nil, &influxdb.Error{
			Code: influxdb.ENotFound,
			Msg:  "task not found",
		}
	}

	if err != nil {
		return nil, err
	}

	var t *influxdb.Task
	if err := json.Unmarshal(v, t); err != nil {
		return nil, &influxdb.Error{
			Err: err,
		}
	}

	latestCompleted, err := s.findLatestCompletedTime(ctx, tx, t.ID)
	if err != nil {
		return nil, &influxdb.Error{
			Err: err,
		}
	}
	t.LatestCompleted = latestCompleted

	return t, nil
}

// FindTasks returns a list of tasks that match a filter (limit 100) and the total count
// of matching tasks.
func (s *Service) FindTasks(ctx context.Context, filter influxdb.TaskFilter) ([]*influxdb.Task, int, error) {
	var ts []*influxdb.Task
	err := s.kv.View(ctx, func(tx Tx) error {
		tasks, _, err := s.findTasks(ctx, tx, filter)
		if err != nil {
			return &influxdb.Error{
				Err: err,
			}
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
	if filter.User == nil && filter.OrganizationID == nil && filter.Organization == "" {
		return nil, 0, errors.New("find tasks requires filtering by org or user")
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

	if filter.Limit < 0 {
		return nil, 0, errors.New("ListTasks: PageSize must be positive")
	}
	if filter.Limit > influxdb.TaskMaxPageSize {
		return nil, 0, fmt.Errorf("ListTasks: PageSize exceeds maximum of %d", influxdb.TaskMaxPageSize)
	}
	if filter.Limit == 0 {
		filter.Limit = influxdb.TaskDefaultPageSize
	}

	var ts []*influxdb.Task

	// filter by user id.
	if filter.User != nil {
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
			if err != nil {
				return nil, 0, err
			}

			if org != nil && task.OrganizationID != org.ID {
				continue
			}

			latestCompleted, err := s.findLatestCompletedTime(ctx, tx, task.ID)
			if err != nil {
				return nil, 0, &influxdb.Error{
					Err: err,
				}
			}
			task.LatestCompleted = latestCompleted

			ts = append(ts, task)

			if len(ts) >= filter.Limit {
				break
			}
		}
	} else {
		indexBucket, err := tx.Bucket(taskIndexBucket)

		c, err := indexBucket.Cursor()
		if err != nil {
			return nil, 0, err
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
				return nil, 0, err
			}
			k, v := c.Seek(key)
			if k != nil {
				id, err := influxdb.IDFromString(string(v))
				if err != nil {
					return nil, 0, err
				}

				t, err := s.findTaskByID(ctx, tx, *id)
				if err != nil {
					return nil, 0, err
				}

				// insert the new task into the list
				ts = append(ts, t)
			}

		}

		for {
			k, v := c.Next()
			if k != nil {
				break
			}

			id, err := influxdb.IDFromString(string(v))
			if err != nil {
				return nil, 0, err
			}

			t, err := s.findTaskByID(ctx, tx, *id)
			if err != nil {
				return nil, 0, err
			}

			// If the new task doesn't belong to the org we have looped outside the org filter
			if org != nil && t.OrganizationID != org.ID {
				break
			}

			// insert the new task into the list
			ts = append(ts, t)

			// Check if we are over running the limit
			if len(ts) >= filter.Limit {
				break
			}
		}
	}

	return ts, len(ts), nil
}

// CreateTask creates a new task.
// The owner of the task is inferred from the authorizer associated with ctx.
func (s *Service) CreateTask(ctx context.Context, tc influxdb.TaskCreate) (*influxdb.Task, error) {
	var t *influxdb.Task
	err := s.kv.Update(ctx, func(tx Tx) error {
		task, err := s.createTask(ctx, tx, tc)
		if err != nil {
			return &influxdb.Error{
				Err: err,
			}
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
	auth, err := s.findAuthorizationByToken(ctx, tx, tc.Token)
	if err != nil {
		return nil, err
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
		return nil, errors.New("organization not found")
	}

	opt, err := options.FromScript(tc.Flux)
	if err != nil {
		return nil, err
	}

	task := &influxdb.Task{
		ID:              s.IDGenerator.ID(),
		OrganizationID:  org.ID,
		Organization:    org.Name,
		AuthorizationID: auth.Identifier(),
		Name:            opt.Name,
		Status:          tc.Status,
		Flux:            tc.Flux,
		Every:           opt.Every.String(),
		Cron:            opt.Cron,
		Offset:          opt.Offset.String(),
		CreatedAt:       time.Now().Format(time.RFC3339),
	}

	taskBucket, err := tx.Bucket(taskBucket)
	if err != nil {
		return nil, err
	}

	indexBucket, err := tx.Bucket(taskIndexBucket)
	if err != nil {
		return nil, err
	}

	taskBytes, err := json.Marshal(task)
	if err != nil {
		return nil, err
	}

	taskKey, err := taskKey(task.ID)
	if err != nil {
		return nil, err
	}

	orgKey, err := taskOrgKey(task.ID, task.OrganizationID)
	// write the task
	err = taskBucket.Put(taskKey, taskBytes)
	if err != nil {
		return nil, err
	}

	// write the org index
	err = indexBucket.Put(orgKey, taskKey)
	if err != nil {
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
			return &influxdb.Error{
				Err: err,
			}
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
	}

	// update the existing task
	if upd.Token != "" {
		auth, err := s.findAuthorizationByToken(ctx, tx, upd.Token)
		if err != nil {
			return nil, err
		}
		task.AuthorizationID = auth.ID
	}

	if !upd.Options.IsZero() {
		task.Name = upd.Options.Name
		task.Every = upd.Options.Every.String()
		task.Cron = upd.Options.Cron
		task.Offset = upd.Options.Offset.String()
	}

	// save the updated task
	bucket, err := tx.Bucket(taskBucket)
	if err != nil {
		return nil, err
	}
	key, err := taskKey(id)
	if err != nil {
		return nil, err
	}

	taskBytes, err := json.Marshal(task)
	if err != nil {
		return nil, err
	}

	return task, bucket.Put(key, taskBytes)
}

// DeleteTask removes a task by ID and purges all associated data and scheduled runs.
func (s *Service) DeleteTask(ctx context.Context, id influxdb.ID) error {
	err := s.kv.Update(ctx, func(tx Tx) error {
		err := s.deleteTask(ctx, tx, id)
		if err != nil {
			return &influxdb.Error{
				Err: err,
			}
		}
		return nil
	})
	if err != nil {
		return err
	}

	return nil
}

func (s *Service) deleteTask(ctx context.Context, tx Tx, id influxdb.ID) error {
	bucket, err := tx.Bucket(taskBucket)
	if err != nil {
		return err
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

	if err := bucket.Delete(orgKey); err != nil {
		return err
	}

	// remove latest completed
	lastCompletedKey, err := taskLatestCompletedKey(task.ID)
	if err != nil {
		return err
	}

	if err := bucket.Delete(lastCompletedKey); err != nil {
		return err
	}

	// remove the runs
	runs, _, err := s.findRuns(ctx, tx, influxdb.RunFilter{Task: task.ID})
	if err != nil {
		return err
	}

	runBucket, err := tx.Bucket(taskRunBucket)
	if err != nil {
		return err
	}

	for _, run := range runs {
		key, err := taskRunKey(task.ID, run.ID)
		if err != nil {
			return err
		}

		if err := runBucket.Delete(key); err != nil {
			return err
		}
	}
	// remove the task
	key, err := taskKey(task.ID)
	if err != nil {
		return err
	}

	return bucket.Delete(key)
}

// FindLogs returns logs for a run.
func (s *Service) FindLogs(ctx context.Context, filter influxdb.LogFilter) ([]*influxdb.Log, int, error) {
	var logs []*influxdb.Log
	err := s.kv.View(ctx, func(tx Tx) error {
		ls, _, err := s.findLogs(ctx, tx, filter)
		if err != nil {
			return &influxdb.Error{
				Err: err,
			}
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
		for i, log := range r.Log {
			rtn[i] = &log
		}
		return rtn, len(rtn), nil
	}

	runs, _, err := s.findRuns(ctx, tx, influxdb.RunFilter{Task: filter.Task})
	if err != nil {
		return nil, 0, err
	}

	var logs []*influxdb.Log
	for _, run := range runs {
		for _, log := range run.Log {
			logs = append(logs, &log)
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
			return &influxdb.Error{
				Err: err,
			}
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
	bucket, err := tx.Bucket(taskRunBucket)
	if err != nil {
		return nil, 0, err
	}

	c, err := bucket.Cursor()

	if filter.After != nil {
		afterKey, err := taskRunKey(filter.Task, *filter.After)
		if err != nil {
			return nil, 0, err
		}
		c.Seek(afterKey)
	}

	var runs []*influxdb.Run
	for k, v := c.Next(); k != nil; k, v = c.Next() {
		r := &influxdb.Run{}

		if err := json.Unmarshal(v, r); err != nil {
			return nil, 0, err
		}

		// if the run no longer belongs to the task we are done
		if r.TaskID != filter.Task {
			break
		}

		runs = append(runs, r)

		if len(runs) >= filter.Limit {
			break
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
			return &influxdb.Error{
				Err: err,
			}
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
	bucket, err := tx.Bucket(taskBucket)
	if err != nil {
		return nil, err
	}

	key, err := taskRunKey(taskID, runID)
	if err != nil {
		return nil, err
	}

	runBytes, err := bucket.Get(key)
	if err != nil {
		return nil, err
	}

	run := &influxdb.Run{}
	err = json.Unmarshal(runBytes, run)
	if err != nil {
		return nil, err
	}

	return run, nil
}

// CancelRun cancels a currently running run.
func (s *Service) CancelRun(ctx context.Context, taskID, runID influxdb.ID) error {
	err := s.kv.Update(ctx, func(tx Tx) error {
		err := s.cancelRun(ctx, tx, taskID, runID)
		if err != nil {
			return &influxdb.Error{
				Err: err,
			}
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
		return err
	}

	runBytes, err := json.Marshal(run)
	if err != nil {
		return err
	}

	runKey, err := taskRunKey(taskID, runID)
	if err != nil {
		return err
	}

	return bucket.Put(runKey, runBytes)
}

// RetryRun creates and returns a new run (which is a retry of another run).
func (s *Service) RetryRun(ctx context.Context, taskID, runID influxdb.ID) (*influxdb.Run, error) {
	var r *influxdb.Run
	err := s.kv.Update(ctx, func(tx Tx) error {
		run, err := s.retryRun(ctx, tx, taskID, runID)
		if err != nil {
			return &influxdb.Error{
				Err: err,
			}
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
	r.Status = ""
	r.StartedAt = ""
	r.FinishedAt = ""
	r.RequestedAt = ""

	// add a clean copy of the run to the manual runs
	bucket, err := tx.Bucket(taskRunBucket)
	if err != nil {
		return nil, err
	}

	key, err := taskManualRunKey(taskID)
	if err != nil {
		return nil, err
	}

	runs := []*influxdb.Run{}
	runsBytes, err := bucket.Get(key)
	if err != nil {
		if err != ErrKeyNotFound {
			return nil, err
		}
	}

	if runsBytes != nil {
		if err := json.Unmarshal(runsBytes, runs); err != nil {
			return nil, err
		}
	}

	runs = append(runs, r)

	// save manual runs
	runsBytes, err = json.Marshal(runs)
	if err != nil {
		return nil, err
	}

	if err := bucket.Put(key, runsBytes); err != nil {
		return nil, err
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
			return &influxdb.Error{
				Err: err,
			}
		}
		r = run
		return nil
	})
	return r, err
}

func (s *Service) forceRun(ctx context.Context, tx Tx, taskID influxdb.ID, scheduledFor int64) (*influxdb.Run, error) {
	// create a run
	t := time.Unix(scheduledFor, 0)
	r := &influxdb.Run{
		ID:           s.IDGenerator.ID(),
		TaskID:       taskID,
		RequestedAt:  time.Now().Format(time.RFC3339),
		ScheduledFor: t.Format(time.RFC3339),
	}

	// add a clean copy of the run to the manual runs
	bucket, err := tx.Bucket(taskRunBucket)

	key, err := taskManualRunKey(taskID)

	runs := []*influxdb.Run{}
	runsBytes, err := bucket.Get(key)
	if err != nil {
		if err != ErrKeyNotFound {
			return nil, err
		}
	}

	if runsBytes != nil {
		if err := json.Unmarshal(runsBytes, runs); err != nil {
			return nil, err
		}
	}

	runs = append(runs, r)

	// save manual runs
	runsBytes, err = json.Marshal(runs)

	if err := bucket.Put(key, runsBytes); err != nil {
		return nil, err
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
			return &influxdb.Error{
				Err: err,
			}
		}
		rc = runCreate
		return nil
	})
	return rc, err
}

func (s *Service) createNextRun(ctx context.Context, tx Tx, taskID influxdb.ID, now int64) (backend.RunCreation, error) {
	// pull the scheduler for the task
	task, err := s.findTaskByID(ctx, tx, taskID)

	schedule := task.EffectiveCron()
	// get the latest completed and the latest currently running run's time
	var latestCompleted time.Time
	lRun, err := s.findLatestCompleted(ctx, tx, taskID)

	if lRun != nil {

	}
	runs, _, err := s.findRuns(ctx, tx, influxdb.RunFilter{Task: taskID})

	for _, run := range runs {
		runTime, err := run.ScheduledForTime()
		if runTime.After(latestCompleted) {
			latestCompleted = runTime
		}
	}

	// create a run if possible

	// check if we have any manual runs queued

	// populate RunCreation
}

func (s *Service) CurrentlyRunning(ctx context.Context, taskID influxdb.ID) ([]*influxdb.Run, error) {
	// TODO(docmerlin): fill in logic
	return nil, nil
}

func (s *Service) ManualRuns(ctx context.Context, taskID influxdb.ID) ([]*influxdb.Run, error) {
	// TODO(docmerlin): fill in logic
	return nil, nil

}

// FinishRun removes runID from the list of running tasks and if its `now` is later then last completed update it.
func (s *Service) FinishRun(ctx context.Context, taskID, runID influxdb.ID) error {
	err := s.kv.Update(ctx, func(tx Tx) error {
		err := s.finishRun(ctx, tx, taskID, runID)
		if err != nil {
			return &influxdb.Error{
				Err: err,
			}
		}
		return nil
	})
	return err
}

func (s *Service) finishRun(ctx context.Context, tx Tx, taskID, runID influxdb.ID) error {
	// get the run
	r, err := s.findRunByID(ctx, tx, taskID, runID)
	rTime, err := r.ScheduledForTime()
	if err != nil {
		return err
	}
	// check if its latest
	latestRun, err := s.findLatestCompleted(ctx, tx, taskID)
	lTime, err := r.ScheduledForTime()
	if err != nil {
		return err
	}

	bucket, err := tx.Bucket(taskRunBucket)

	if rTime.After(lTime) {
		rb, err := json.Marshal(r)
		if err != nil {
			return err
		}
		lKey, err := taskLatestCompletedKey(taskID)
		if err != nil {
			return err
		}

		if err := bucket.Put(lKey, rb); err != nil {
			return err
		}
	}
	// remove run
	key, err := taskRunKey(taskID, runID)
	if err != nil {
		return err
	}
	return bucket.Delete(key)
}

func (s *Service) findLatestCompleted(ctx context.Context, tx Tx, id influxdb.ID) (*influxdb.Run, error) {
	bucket, err := tx.Bucket(taskBucket)
	if err != nil {
		return nil, err
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
		return nil, err
	}

	run := &influxdb.Run{}
	if err = json.Unmarshal(bytes, run); err != nil {
		return nil, err
	}

	return run, nil
}

func (s *Service) findLatestCompletedTime(ctx context.Context, tx Tx, id influxdb.ID) (string, error) {
	run, err := s.findLatestCompleted(ctx, tx, id)
	if err != nil {
		return "", err
	}
	return run.ScheduledFor, nil
}

func taskKey(taskID influxdb.ID) ([]byte, error) {
	encodedID, err := taskID.Encode()
	if err != nil {
		return nil, err
	}
	return encodedID, nil
}

func taskLatestCompletedKey(taskID influxdb.ID) ([]byte, error) {
	encodedID, err := taskID.Encode()
	if err != nil {
		return nil, err
	}
	return []byte(string(encodedID) + "/latestCreated"), nil
}

func taskManualRunKey(taskID influxdb.ID) ([]byte, error) {
	encodedID, err := taskID.Encode()
	if err != nil {
		return nil, err
	}
	return []byte(string(encodedID) + "/manualRuns"), nil
}

func taskOrgKey(orgID, taskID influxdb.ID) ([]byte, error) {
	encodedOrgID, err := orgID.Encode()
	if err != nil {
		return nil, err
	}
	encodedID, err := taskID.Encode()
	if err != nil {
		return nil, err
	}

	return []byte(string(encodedOrgID) + "/" + string(encodedID)), nil
}

func taskRunKey(taskID, runID influxdb.ID) ([]byte, error) {
	encodedID, err := taskID.Encode()
	if err != nil {
		return nil, err
	}
	encodedRunID, err := runID.Encode()
	if err != nil {
		return nil, err
	}

	return []byte(string(encodedID) + "/" + string(encodedRunID)), nil
}

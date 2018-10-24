package backend

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/influxdata/platform"
	"github.com/influxdata/platform/snowflake"
	"github.com/influxdata/platform/task/options"
)

var _ Store = (*inmem)(nil)

// inmem is an in-memory task store.
type inmem struct {
	idgen platform.IDGenerator

	mu sync.RWMutex

	// It might be more natural to use a map of ID to task,
	// but then we wouldn't have guaranteed ordering for paging.
	tasks []StoreTask

	runners map[string]StoreTaskMeta
}

// NewInMemStore returns a new in-memory store.
// This store is not designed to be efficient, it is here for testing purposes.
func NewInMemStore() Store {
	return &inmem{
		idgen:   snowflake.NewIDGenerator(),
		runners: map[string]StoreTaskMeta{},
	}
}

func (s *inmem) CreateTask(_ context.Context, req CreateTaskRequest) (platform.ID, error) {
	o, err := StoreValidator.CreateArgs(req)
	if err != nil {
		return platform.InvalidID(), err
	}

	id := s.idgen.ID()

	task := StoreTask{
		ID: id,

		Org:  req.Org,
		User: req.User,

		Name: o.Name,

		Script: req.Script,
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.tasks = append(s.tasks, task)

	stm := StoreTaskMeta{
		MaxConcurrency:  int32(o.Concurrency),
		Status:          string(req.Status),
		LatestCompleted: req.ScheduleAfter,
		EffectiveCron:   o.EffectiveCronString(),
		Delay:           int32(o.Delay / time.Second),
	}
	if stm.Status == "" {
		stm.Status = string(DefaultTaskStatus)
	}
	s.runners[id.String()] = stm

	return id, nil
}

func (s *inmem) UpdateTask(_ context.Context, req UpdateTaskRequest) (UpdateTaskResult, error) {
	var res UpdateTaskResult
	op, err := StoreValidator.UpdateArgs(req)
	if err != nil {
		return res, err
	}

	idStr := req.ID.String()

	s.mu.Lock()
	defer s.mu.Unlock()

	found := false
	for n, t := range s.tasks {
		if t.ID != req.ID {
			continue
		}
		found = true

		res.OldScript = t.Script
		if req.Script == "" {
			op, err = options.FromScript(t.Script)
			if err != nil {
				return res, err
			}
		} else {
			t.Script = req.Script
		}
		t.Name = op.Name

		s.tasks[n] = t
		res.NewTask = t
		break
	}
	if !found {
		return res, fmt.Errorf("ModifyTask: record not found for %s", idStr)
	}

	stm, ok := s.runners[idStr]
	if !ok {
		panic("inmem store: had task without runner for task ID " + idStr)
	}
	res.OldStatus = TaskStatus(stm.Status)

	if req.Status != "" {
		// Changing the status.
		stm.Status = string(req.Status)
		s.runners[idStr] = stm
	}
	res.NewMeta = stm

	return res, nil
}

func (s *inmem) ListTasks(_ context.Context, params TaskSearchParams) ([]StoreTaskWithMeta, error) {
	if params.Org.Valid() && params.User.Valid() {
		return nil, errors.New("ListTasks: org and user filters are mutually exclusive")
	}

	const (
		defaultPageSize = 100
		maxPageSize     = 500
	)

	if params.PageSize < 0 {
		return nil, errors.New("ListTasks: PageSize must be positive")
	}
	if params.PageSize > maxPageSize {
		return nil, fmt.Errorf("ListTasks: PageSize exceeds maximum of %d", maxPageSize)
	}

	lim := params.PageSize
	if lim == 0 {
		lim = defaultPageSize
	}

	out := make([]StoreTaskWithMeta, 0, lim)

	org := params.Org
	user := params.User

	var after platform.ID
	if !params.After.Valid() {
		after = platform.ID(1)
	} else {
		after = params.After
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	for _, t := range s.tasks {
		if after >= t.ID {
			continue
		}
		if org.Valid() && org != t.Org {
			continue
		}
		if user.Valid() && user != t.User {
			continue
		}

		out = append(out, StoreTaskWithMeta{Task: t})
		if len(out) >= lim {
			break
		}
	}

	for i := range out {
		id := out[i].Task.ID
		out[i].Meta = s.runners[id.String()]
	}

	return out, nil
}

func (s *inmem) FindTaskByID(_ context.Context, id platform.ID) (*StoreTask, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	for _, t := range s.tasks {
		if t.ID == id {
			// Return a copy of the task.
			task := new(StoreTask)
			*task = t
			return task, nil
		}
	}

	return nil, ErrTaskNotFound
}

func (s *inmem) FindTaskByIDWithMeta(_ context.Context, id platform.ID) (*StoreTask, *StoreTaskMeta, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var task *StoreTask
	for _, t := range s.tasks {
		if t.ID == id {
			// Return a copy of the task.
			task = new(StoreTask)
			*task = t
			break
		}
	}
	if task == nil {
		return nil, nil, ErrTaskNotFound
	}

	meta, ok := s.runners[id.String()]
	if !ok {
		return nil, nil, errors.New("task meta not found")
	}

	return task, &meta, nil
}

func (s *inmem) FindTaskMetaByID(ctx context.Context, id platform.ID) (*StoreTaskMeta, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	meta, ok := s.runners[id.String()]
	if !ok {
		return nil, ErrTaskNotFound
	}

	return &meta, nil
}

func (s *inmem) DeleteTask(_ context.Context, id platform.ID) (deleted bool, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	idx := -1
	for i, t := range s.tasks {
		if t.ID == id {
			idx = i
			break
		}
	}

	if idx < 0 {
		return false, nil
	}

	// Delete entry from slice.
	s.tasks = append(s.tasks[:idx], s.tasks[idx+1:]...)
	return true, nil
}

func (s *inmem) Close() error {
	return nil
}

func (s *inmem) CreateNextRun(ctx context.Context, taskID platform.ID, now int64) (RunCreation, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	stm, ok := s.runners[taskID.String()]
	if !ok {
		return RunCreation{}, errors.New("task not found")
	}

	makeID := func() (platform.ID, error) {
		return s.idgen.ID(), nil
	}
	rc, err := stm.CreateNextRun(now, makeID)
	if err != nil {
		return RunCreation{}, err
	}
	rc.Created.TaskID = taskID

	s.runners[taskID.String()] = stm
	return rc, nil
}

// FinishRun removes runID from the list of running tasks and if its `now` is later then last completed update it.
func (s *inmem) FinishRun(ctx context.Context, taskID, runID platform.ID) error {
	s.mu.RLock()
	stm, ok := s.runners[taskID.String()]
	s.mu.RUnlock()

	if !ok {
		return errors.New("taskRunner not found")
	}

	if !stm.FinishRun(runID) {
		return errors.New("run not found")
	}

	s.mu.Lock()
	s.runners[taskID.String()] = stm
	s.mu.Unlock()

	return nil
}

func (s *inmem) ManuallyRunTimeRange(_ context.Context, taskID platform.ID, start, end, requestedAt int64) error {
	tid := taskID.String()

	s.mu.Lock()
	defer s.mu.Unlock()

	stm, ok := s.runners[tid]
	if !ok {
		return errors.New("task not found")
	}

	if err := stm.ManuallyRunTimeRange(start, end, requestedAt); err != nil {
		return err
	}

	s.runners[tid] = stm
	return nil
}

func (s *inmem) delete(ctx context.Context, id platform.ID, f func(StoreTask) platform.ID) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	newTasks := []StoreTask{}
	deletingTasks := []platform.ID{}
	for i := range s.tasks {
		if f(s.tasks[i]) != id {
			newTasks = append(newTasks, s.tasks[i])
		} else {
			deletingTasks = append(deletingTasks, s.tasks[i].ID)
		}
		//check for cancelations
		if i&(1024-1) == 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
		}
	}
	//last check for cancelations
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	for i := range deletingTasks {
		delete(s.runners, s.tasks[i].ID.String())
	}
	s.tasks = newTasks
	return nil
}

func getUser(st StoreTask) platform.ID {
	return st.User
}

func getOrg(st StoreTask) platform.ID {
	return st.Org
}

// DeleteOrg synchronously deletes an org and all their tasks from a from an in-mem store store.
func (s *inmem) DeleteOrg(ctx context.Context, id platform.ID) error {
	return s.delete(ctx, id, getOrg)
}

// DeleteUser synchronously deletes a user and all their tasks from a from an in-mem store store.
func (s *inmem) DeleteUser(ctx context.Context, id platform.ID) error {
	return s.delete(ctx, id, getUser)
}

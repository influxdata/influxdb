package backend

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"

	"github.com/influxdata/platform"
	"github.com/influxdata/platform/snowflake"
	"github.com/influxdata/platform/task/backend/pb"
)

var _ Store = (*inmem)(nil)

// inmem is an in-memory task store.
type inmem struct {
	idgen platform.IDGenerator

	mu sync.RWMutex

	// It might be more natural to use a map of ID to task,
	// but then we wouldn't have guaranteed ordering for paging.
	tasks []StoreTask

	runners map[string]pb.StoredTaskInternalMeta
}

// NewInMemStore returns a new in-memory store.
func NewInMemStore() Store {
	return &inmem{
		idgen:   snowflake.NewIDGenerator(),
		runners: map[string]pb.StoredTaskInternalMeta{},
	}
}

func (s *inmem) CreateTask(_ context.Context, org, user platform.ID, script string) (platform.ID, error) {
	o, err := StoreValidator.CreateArgs(org, user, script)
	if err != nil {
		return nil, err
	}

	id := s.idgen.ID()

	task := StoreTask{
		ID: id,

		Org:  org,
		User: user,

		Name: o.Name,

		Script: script,
	}

	s.mu.Lock()
	s.tasks = append(s.tasks, task)
	s.runners[id.String()] = pb.StoredTaskInternalMeta{MaxConcurrency: int32(o.Concurrency)} // TODO:  make settable in the create
	s.mu.Unlock()

	return id, nil
}

func (s *inmem) ModifyTask(_ context.Context, id platform.ID, script string) error {
	if _, err := StoreValidator.ModifyArgs(id, script); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	for n, t := range s.tasks {
		if bytes.Equal(t.ID, id) {
			t.Script = script
			s.tasks[n] = t
			return nil
		}
	}
	return fmt.Errorf("ModifyTask: record not found for %s", id)
}

func (s *inmem) ListTasks(_ context.Context, params TaskSearchParams) ([]StoreTask, error) {
	if len(params.Org) > 0 && len(params.User) > 0 {
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

	out := make([]StoreTask, 0, lim)

	org := params.Org
	user := params.User
	after := params.After

	s.mu.RLock()
	defer s.mu.RUnlock()

	for _, t := range s.tasks {
		if len(after) > 0 && bytes.Compare(after, t.ID) >= 0 {
			continue
		}
		if len(org) > 0 && !bytes.Equal(org, t.Org) {
			continue
		}
		if len(user) > 0 && !bytes.Equal(user, t.User) {
			continue
		}

		out = append(out, t)
		if len(out) >= lim {
			break
		}
	}

	return out, nil
}

func (s *inmem) FindTaskByID(_ context.Context, id platform.ID) (*StoreTask, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	for _, t := range s.tasks {
		if bytes.Equal(t.ID, id) {
			// Return a copy of the task.
			task := new(StoreTask)
			*task = t
			return task, nil
		}
	}

	return nil, nil
}
func (s *inmem) FindTaskMetaByID(ctx context.Context, id platform.ID) (*pb.StoredTaskInternalMeta, error) {
	meta, ok := s.runners[id.String()]
	if !ok {
		return nil, errors.New("task meta not found")
	}

	return &meta, nil
}

func (s *inmem) DeleteTask(_ context.Context, id platform.ID) (deleted bool, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	idx := -1
	for i, t := range s.tasks {
		if bytes.Equal(t.ID, id) {
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

// CreateRun adds `now` to the task's metaData if we have not exceeded 'max_concurrency'.
func (s *inmem) CreateRun(ctx context.Context, taskID platform.ID, now int64) (QueuedRun, error) {
	queuedRun := QueuedRun{}

	stm, ok := s.runners[taskID.String()]
	if !ok {
		return queuedRun, errors.New("taskRunner not found")
	}

	if len(stm.CurrentlyRunning) >= int(stm.MaxConcurrency) {
		return queuedRun, errors.New("MaxConcurrency reached")
	}

	runID := s.idgen.ID()

	running := &pb.StoredTaskInternalMeta_RunningList{
		NowTimestampUnix: now,
		Try:              1,
		RunID:            binary.BigEndian.Uint64(runID),
	}

	stm.CurrentlyRunning = append(stm.CurrentlyRunning, running)
	s.mu.Lock()
	s.runners[taskID.String()] = stm
	s.mu.Unlock()

	queuedRun.TaskID = taskID
	queuedRun.RunID = runID
	queuedRun.Now = now
	return queuedRun, nil
}

// FinishRun removes runID from the list of running tasks and if its `now` is later then last completed update it.
func (s *inmem) FinishRun(ctx context.Context, taskID, runID platform.ID) error {
	intID := binary.BigEndian.Uint64(runID)

	stm, ok := s.runners[taskID.String()]
	if !ok {
		return errors.New("taskRunner not found")
	}

	found := false
	for i, runner := range stm.CurrentlyRunning {
		if runner.RunID == intID {
			found = true
			stm.CurrentlyRunning = append(stm.CurrentlyRunning[:i], stm.CurrentlyRunning[i+1:]...)
			if runner.NowTimestampUnix > stm.LastCompletedTimestampUnix {
				stm.LastCompletedTimestampUnix = runner.NowTimestampUnix
				break
			}
		}
	}

	if !found {
		return errors.New("run not found")
	}

	s.mu.Lock()
	s.runners[taskID.String()] = stm
	s.mu.Unlock()

	return nil
}

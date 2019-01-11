// Package mock contains mock implementations of different task interfaces.
package mock

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	platform "github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/task/backend"
	scheduler "github.com/influxdata/influxdb/task/backend"
	"go.uber.org/zap"
)

// Scheduler is a mock implementation of a task scheduler.
type Scheduler struct {
	sync.Mutex

	lastTick int64

	claims map[string]*Task
	meta   map[string]backend.StoreTaskMeta

	createChan  chan *Task
	releaseChan chan *Task
	updateChan  chan *Task

	claimError   error
	releaseError error
}

// Task is a mock implementation of a task.
type Task struct {
	Script           string
	StartExecution   int64
	ConcurrencyLimit uint8
}

func NewScheduler() *Scheduler {
	return &Scheduler{
		claims: map[string]*Task{},
		meta:   map[string]backend.StoreTaskMeta{},
	}
}

func (s *Scheduler) Tick(now int64) {
	s.Lock()
	defer s.Unlock()

	s.lastTick = now
}

func (s *Scheduler) WithLogger(l *zap.Logger) {}

func (s *Scheduler) Start(context.Context) {}

func (s *Scheduler) Stop() {}

func (s *Scheduler) ClaimTask(task *backend.StoreTask, meta *backend.StoreTaskMeta) error {
	if s.claimError != nil {
		return s.claimError
	}

	s.Lock()
	defer s.Unlock()

	_, ok := s.claims[task.ID.String()]
	if ok {
		return backend.ErrTaskAlreadyClaimed
	}
	s.meta[task.ID.String()] = *meta

	t := &Task{Script: task.Script, StartExecution: meta.LatestCompleted, ConcurrencyLimit: uint8(meta.MaxConcurrency)}

	s.claims[task.ID.String()] = t

	if s.createChan != nil {
		s.createChan <- t
	}

	return nil
}

func (s *Scheduler) UpdateTask(task *backend.StoreTask, meta *backend.StoreTaskMeta) error {
	s.Lock()
	defer s.Unlock()

	_, ok := s.claims[task.ID.String()]
	if !ok {
		return backend.ErrTaskNotClaimed
	}

	s.meta[task.ID.String()] = *meta

	t := &Task{Script: task.Script, StartExecution: meta.LatestCompleted, ConcurrencyLimit: uint8(meta.MaxConcurrency)}

	s.claims[task.ID.String()] = t

	if s.updateChan != nil {
		s.updateChan <- t
	}

	return nil
}

func (s *Scheduler) ReleaseTask(taskID platform.ID) error {
	if s.releaseError != nil {
		return s.releaseError
	}

	s.Lock()
	defer s.Unlock()

	t, ok := s.claims[taskID.String()]
	if !ok {
		return backend.ErrTaskNotClaimed
	}
	if s.releaseChan != nil {
		s.releaseChan <- t
	}

	delete(s.claims, taskID.String())
	delete(s.meta, taskID.String())

	return nil
}

func (s *Scheduler) TaskFor(id platform.ID) *Task {
	s.Lock()
	defer s.Unlock()
	return s.claims[id.String()]
}

func (s *Scheduler) TaskCreateChan() <-chan *Task {
	s.createChan = make(chan *Task, 10)
	return s.createChan
}
func (s *Scheduler) TaskReleaseChan() <-chan *Task {
	s.releaseChan = make(chan *Task, 10)
	return s.releaseChan
}
func (s *Scheduler) TaskUpdateChan() <-chan *Task {
	s.updateChan = make(chan *Task, 10)
	return s.updateChan
}

// ClaimError sets an error to be returned by s.ClaimTask, if err is not nil.
func (s *Scheduler) ClaimError(err error) {
	s.claimError = err
}

// ReleaseError sets an error to be returned by s.ReleaseTask, if err is not nil.
func (s *Scheduler) ReleaseError(err error) {
	s.releaseError = err
}

func (s *Scheduler) CancelRun(_ context.Context, taskID, runID platform.ID) error {
	return nil
}

// DesiredState is a mock implementation of DesiredState (used by NewScheduler).
type DesiredState struct {
	mu sync.Mutex
	// Map of stringified task ID to last ID used for run.
	runIDs map[string]uint64

	// Map of stringified, concatenated task and platform ID, to runs that have been created.
	created map[string]backend.QueuedRun

	// Map of stringified task ID to task meta.
	meta map[string]backend.StoreTaskMeta
}

var _ backend.DesiredState = (*DesiredState)(nil)

func NewDesiredState() *DesiredState {
	return &DesiredState{
		runIDs:  make(map[string]uint64),
		created: make(map[string]backend.QueuedRun),
		meta:    make(map[string]backend.StoreTaskMeta),
	}
}

// SetTaskMeta sets the task meta for the given task ID.
// SetTaskMeta must be called before CreateNextRun, for a given task ID.
func (d *DesiredState) SetTaskMeta(taskID platform.ID, meta backend.StoreTaskMeta) {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.meta[taskID.String()] = meta
}

// CreateNextRun creates the next run for the given task.
// Refer to the documentation for SetTaskPeriod to understand how the times are determined.
func (d *DesiredState) CreateNextRun(_ context.Context, taskID platform.ID, now int64) (backend.RunCreation, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if !taskID.Valid() {
		return backend.RunCreation{}, errors.New("invalid task id")
	}
	tid := taskID.String()

	meta, ok := d.meta[tid]
	if !ok {
		panic(fmt.Sprintf("meta not set for task with ID %s", tid))
	}

	makeID := func() (platform.ID, error) {
		d.runIDs[tid]++
		runID := platform.ID(d.runIDs[tid])
		return runID, nil
	}

	rc, err := meta.CreateNextRun(now, makeID)
	if err != nil {
		return backend.RunCreation{}, err
	}
	d.meta[tid] = meta
	rc.Created.TaskID = taskID
	d.created[tid+rc.Created.RunID.String()] = rc.Created
	return rc, nil
}

func (d *DesiredState) FinishRun(_ context.Context, taskID, runID platform.ID) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	tid := taskID.String()
	rid := runID.String()
	m := d.meta[tid]
	if !m.FinishRun(runID) {
		var knownIDs []string
		for _, r := range m.CurrentlyRunning {
			knownIDs = append(knownIDs, platform.ID(r.RunID).String())
		}
		return fmt.Errorf("unknown run ID %s; known run IDs: %s", rid, strings.Join(knownIDs, ", "))
	}
	d.meta[tid] = m
	delete(d.created, tid+rid)
	return nil
}

func (d *DesiredState) CreatedFor(taskID platform.ID) []backend.QueuedRun {
	d.mu.Lock()
	defer d.mu.Unlock()

	var qrs []backend.QueuedRun
	for _, qr := range d.created {
		if qr.TaskID == taskID {
			qrs = append(qrs, qr)
		}
	}

	return qrs
}

// PollForNumberCreated blocks for a small amount of time waiting for exactly the given count of created runs for the given task ID.
// If the expected number isn't found in time, it returns an error.
//
// Because the scheduler and executor do a lot of state changes asynchronously, this is useful in test.
func (d *DesiredState) PollForNumberCreated(taskID platform.ID, count int) ([]scheduler.QueuedRun, error) {
	const numAttempts = 50
	actualCount := 0
	var created []scheduler.QueuedRun
	for i := 0; i < numAttempts; i++ {
		time.Sleep(2 * time.Millisecond) // we sleep even on first so it becomes more likely that we catch when too many are produced.
		created = d.CreatedFor(taskID)
		actualCount = len(created)
		if actualCount == count {
			return created, nil
		}
	}
	return created, fmt.Errorf("did not see count of %d created task(s) for ID %s in time, instead saw %d", count, taskID.String(), actualCount) // we return created anyways, to make it easier to debug
}

type Executor struct {
	mu         sync.Mutex
	hangingFor time.Duration

	// Map of stringified, concatenated task and run ID, to runs that have begun execution but have not finished.
	running map[string]*RunPromise

	// Map of stringified, concatenated task and run ID, to results of runs that have executed and completed.
	finished map[string]backend.RunResult

	wg sync.WaitGroup
}

var _ backend.Executor = (*Executor)(nil)

func NewExecutor() *Executor {
	return &Executor{
		running:  make(map[string]*RunPromise),
		finished: make(map[string]backend.RunResult),
	}
}

func (e *Executor) Execute(ctx context.Context, run backend.QueuedRun) (backend.RunPromise, error) {
	rp := NewRunPromise(run)
	rp.WithHanging(ctx, e.hangingFor)
	id := run.TaskID.String() + run.RunID.String()
	e.mu.Lock()
	e.running[id] = rp
	e.mu.Unlock()
	e.wg.Add(1)
	go func() {
		defer e.wg.Done()
		res, _ := rp.Wait()
		e.mu.Lock()
		delete(e.running, id)
		e.finished[id] = res
		e.mu.Unlock()
	}()
	return rp, nil
}

func (e *Executor) Wait() {
	e.wg.Wait()
}

func (e *Executor) WithHanging(dt time.Duration) {
	e.hangingFor = dt
}

// RunningFor returns the run promises for the given task.
func (e *Executor) RunningFor(taskID platform.ID) []*RunPromise {
	e.mu.Lock()
	defer e.mu.Unlock()

	var rps []*RunPromise
	for _, rp := range e.running {
		if rp.Run().TaskID == taskID {
			rps = append(rps, rp)
		}
	}

	return rps
}

// PollForNumberRunning blocks for a small amount of time waiting for exactly the given count of active runs for the given task ID.
// If the expected number isn't found in time, it returns an error.
//
// Because the scheduler and executor do a lot of state changes asynchronously, this is useful in test.
func (e *Executor) PollForNumberRunning(taskID platform.ID, count int) ([]*RunPromise, error) {
	const numAttempts = 20
	var running []*RunPromise
	for i := 0; i < numAttempts; i++ {
		if i > 0 {
			time.Sleep(10 * time.Millisecond)
		}
		running = e.RunningFor(taskID)
		if len(running) == count {
			return running, nil
		}
	}
	return nil, fmt.Errorf("did not see count of %d running task(s) for ID %s in time; last count was %d", count, taskID.String(), len(running))
}

// RunPromise is a mock RunPromise.
type RunPromise struct {
	qr backend.QueuedRun

	setResultOnce sync.Once
	hangingFor    time.Duration
	cancelFunc    context.CancelFunc
	ctx           context.Context
	mu            sync.Mutex
	res           backend.RunResult
	err           error
}

var _ backend.RunPromise = (*RunPromise)(nil)

func NewRunPromise(qr backend.QueuedRun) *RunPromise {
	p := &RunPromise{
		qr: qr,
	}
	p.mu.Lock() // Locked so calls to Wait will block until setResultOnce is called.
	return p
}

func (p *RunPromise) WithHanging(ctx context.Context, hangingFor time.Duration) {
	p.ctx, p.cancelFunc = context.WithCancel(ctx)
	p.hangingFor = hangingFor
}

func (p *RunPromise) Run() backend.QueuedRun {
	return p.qr
}

func (p *RunPromise) Wait() (backend.RunResult, error) {
	p.mu.Lock()
	// can't cancel if we haven't set it to hang.
	if p.ctx != nil {
		select {
		case <-p.ctx.Done():
		case <-time.After(p.hangingFor):
		}
		p.cancelFunc()
	}

	defer p.mu.Unlock()
	return p.res, p.err
}

func (p *RunPromise) Cancel() {
	p.cancelFunc()
	p.Finish(nil, backend.ErrRunCanceled)
}

// Finish unblocks any call to Wait, to return r and err.
// Only the first call to Finish has any effect.
func (p *RunPromise) Finish(r backend.RunResult, err error) {
	p.setResultOnce.Do(func() {
		p.res, p.err = r, err
		p.mu.Unlock()
	})
}

// RunResult is a mock implementation of RunResult.
type RunResult struct {
	err         error
	isRetryable bool
}

var _ backend.RunResult = (*RunResult)(nil)

func NewRunResult(err error, isRetryable bool) *RunResult {
	return &RunResult{err: err, isRetryable: isRetryable}
}

func (rr *RunResult) Err() error {
	return rr.err
}

func (rr *RunResult) IsRetryable() bool {
	return rr.isRetryable
}

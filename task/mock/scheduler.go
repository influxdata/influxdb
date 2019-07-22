// Package mock contains mock implementations of different task interfaces.
package mock

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/influxdata/flux"
	platform "github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/task/backend"
	"go.uber.org/zap"
)

// Scheduler is a mock implementation of a task scheduler.
type Scheduler struct {
	sync.Mutex

	lastTick int64

	claims map[platform.ID]*platform.Task

	createChan  chan *platform.Task
	releaseChan chan *platform.Task
	updateChan  chan *platform.Task

	claimError   error
	releaseError error
}

func NewScheduler() *Scheduler {
	return &Scheduler{
		claims: map[platform.ID]*platform.Task{},
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

func (s *Scheduler) Now() time.Time {
	return time.Unix(s.lastTick, 0)
}

func (s *Scheduler) ClaimTask(_ context.Context, task *platform.Task) error {
	if s.claimError != nil {
		return s.claimError
	}

	s.Lock()
	defer s.Unlock()

	_, ok := s.claims[task.ID]
	if ok {
		return platform.ErrTaskAlreadyClaimed
	}

	s.claims[task.ID] = task

	if s.createChan != nil {
		s.createChan <- task
	}

	return nil
}

func (s *Scheduler) UpdateTask(_ context.Context, task *platform.Task) error {
	s.Lock()
	defer s.Unlock()

	_, ok := s.claims[task.ID]
	if !ok {
		return platform.ErrTaskNotClaimed
	}

	s.claims[task.ID] = task

	if s.updateChan != nil {
		s.updateChan <- task
	}

	return nil
}

func (s *Scheduler) ReleaseTask(taskID platform.ID) error {
	if s.releaseError != nil {
		return s.releaseError
	}

	s.Lock()
	defer s.Unlock()

	t, ok := s.claims[taskID]
	if !ok {
		return platform.ErrTaskNotClaimed
	}
	if s.releaseChan != nil {
		s.releaseChan <- t
	}

	delete(s.claims, taskID)

	return nil
}

func (s *Scheduler) TaskFor(id platform.ID) *platform.Task {
	s.Lock()
	defer s.Unlock()
	return s.claims[id]
}

func (s *Scheduler) TaskCreateChan() <-chan *platform.Task {
	s.Lock()
	defer s.Unlock()

	s.createChan = make(chan *platform.Task, 10)
	return s.createChan
}
func (s *Scheduler) TaskReleaseChan() <-chan *platform.Task {
	s.Lock()
	defer s.Unlock()

	s.releaseChan = make(chan *platform.Task, 10)
	return s.releaseChan
}
func (s *Scheduler) TaskUpdateChan() <-chan *platform.Task {
	s.Lock()
	defer s.Unlock()

	s.updateChan = make(chan *platform.Task, 10)
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

type Executor struct {
	mu         sync.Mutex
	hangingFor time.Duration

	// Map of stringified, concatenated task and run ID, to runs that have begun execution but have not finished.
	running map[string]*RunPromise

	// Map of stringified, concatenated task and run ID, to results of runs that have executed and completed.
	finished map[string]backend.RunResult

	// Forced error for next call to Execute.
	nextExecuteErr error

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
	if err := e.nextExecuteErr; err != nil {
		e.nextExecuteErr = nil
		e.mu.Unlock()
		return nil, err
	}
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

// FailNextCallToExecute causes the next call to e.Execute to unconditionally return err.
func (e *Executor) FailNextCallToExecute(err error) {
	e.mu.Lock()
	e.nextExecuteErr = err
	e.mu.Unlock()
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
	return nil, fmt.Errorf("did not see count of %d running task(s) for ID %s in time; last count was %d", count, taskID, len(running))
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
	p.Finish(nil, platform.ErrRunCanceled)
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

	// Most tests don't care about statistics.
	// If your test does care, adjust it after the call to NewRunResult.
	Stats flux.Statistics
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

func (rr *RunResult) Statistics() flux.Statistics {
	return rr.Stats
}

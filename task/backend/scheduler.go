package backend

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/influxdata/platform"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

var ErrRunCanceled = errors.New("run canceled")
var ErrTaskNotClaimed = errors.New("task not claimed")

// DesiredState persists the desired state of a run.
type DesiredState interface {
	// CreateNextRun requests the next run from the desired state, delegating to (*StoreTaskMeta).CreateNextRun.
	// This allows the scheduler to be "dumb" and just tell DesiredState what time the scheduler thinks it is,
	// and the DesiredState will create the appropriate run according to the task's cron schedule,
	// and according to what's in progress and what's been finished.
	//
	// If a Run is requested and the cron schedule says the schedule isn't ready, a RunNotYetDueError is returned.
	CreateNextRun(ctx context.Context, taskID platform.ID, now int64) (RunCreation, error)

	// FinishRun indicates that the given run is no longer intended to be executed.
	// This may be called after a successful or failed execution, or upon cancellation.
	FinishRun(ctx context.Context, taskID, runID platform.ID) error
}

// Executor handles execution of a run.
type Executor interface {
	// Execute attempts to begin execution of a run.
	// If there is an error invoking execution, that error is returned and RunPromise is nil.
	// TODO(mr): this assumes you can execute a run just from a taskID and a now time.
	// We may need to include the script content in this method signature.
	Execute(ctx context.Context, run QueuedRun) (RunPromise, error)
}

// QueuedRun is a task run that has been assigned an ID,
// but whose execution has not necessarily started.
type QueuedRun struct {
	TaskID, RunID platform.ID

	// The Unix timestamp (seconds since January 1, 1970 UTC) that will be set
	// as the "now" option when executing the task.
	Now int64
}

// RunPromise represents an in-progress run whose result is not yet known.
type RunPromise interface {
	// Run returns the details about the queued run.
	Run() QueuedRun

	// Wait blocks until the run completes.
	// Wait may be called concurrently.
	// Subsequent calls to Wait will return identical values.
	Wait() (RunResult, error)

	// Cancel interrupts the RunFuture.
	// Calls to Wait() will immediately unblock and return nil, ErrRunCanceled.
	// Cancel is safe to call concurrently.
	// If Wait() has already returned, Cancel is a no-op.
	Cancel()
}

type RunResult interface {
	// If the run did not succeed, Err returns the error associated with the run.
	Err() error

	// IsRetryable returns true if the error was non-terminal and the run is eligible for retry.
	IsRetryable() bool

	// TODO(mr): add more detail here like number of points written, execution time, etc.
}

// Scheduler accepts tasks and handles their scheduling.
//
// TODO(mr): right now the methods on Scheduler are synchronous.
// We'll probably want to make them asynchronous in the near future,
// which likely means we will change the method signatures to something where
// we can wait for the result to complete and possibly inspect any relevant output.
type Scheduler interface {
	// Tick updates the time of the scheduler.
	// Any owned tasks who are due to execute and who have a free concurrency slot,
	// will begin a new execution.
	Tick(now int64)

	// ClaimTask begins control of task execution in this scheduler.
	ClaimTask(task *StoreTask, meta *StoreTaskMeta) error

	// ReleaseTask immediately cancels any in-progress runs for the given task ID,
	// and releases any resources related to management of that task.
	ReleaseTask(taskID platform.ID) error
}

type SchedulerOption func(Scheduler)

func WithTicker(ctx context.Context, d time.Duration) SchedulerOption {
	return func(s Scheduler) {
		ticker := time.NewTicker(d)

		go func() {
			for {
				select {
				case time := <-ticker.C:
					go s.Tick(time.Unix())
				case <-ctx.Done():
					ticker.Stop()
					return
				}
			}
		}()
	}
}

// WithLogger sets the logger for the scheduler.
// If not set, the scheduler will use a no-op logger.
func WithLogger(logger *zap.Logger) SchedulerOption {
	return func(s Scheduler) {
		switch sched := s.(type) {
		case *outerScheduler:
			sched.logger = logger.With(zap.String("svc", "taskd/scheduler"))
		default:
			panic(fmt.Sprintf("cannot apply WithLogger to Scheduler of type %T", s))
		}
	}
}

// NewScheduler returns a new scheduler with the given desired state and the given now UTC timestamp.
func NewScheduler(desiredState DesiredState, executor Executor, lw LogWriter, now int64, opts ...SchedulerOption) Scheduler {
	o := &outerScheduler{
		desiredState:   desiredState,
		executor:       executor,
		logWriter:      lw,
		now:            now,
		taskSchedulers: make(map[string]*taskScheduler),
		logger:         zap.NewNop(),
		metrics:        newSchedulerMetrics(),
	}

	for _, opt := range opts {
		opt(o)
	}

	return o
}

type outerScheduler struct {
	desiredState DesiredState
	executor     Executor
	logWriter    LogWriter

	now    int64
	logger *zap.Logger

	metrics *schedulerMetrics

	schedulerMu    sync.Mutex                // Protects access and modification of taskSchedulers map.
	taskSchedulers map[string]*taskScheduler // Stringified task ID -> task scheduler.
}

func (s *outerScheduler) Tick(now int64) {
	atomic.StoreInt64(&s.now, now)

	s.schedulerMu.Lock()
	defer s.schedulerMu.Unlock()

	for _, ts := range s.taskSchedulers {
		if nextDue, hasQueue := ts.NextDue(); now >= nextDue || hasQueue {
			ts.Work()
		}
	}
}

func (s *outerScheduler) ClaimTask(task *StoreTask, meta *StoreTaskMeta) (err error) {
	defer s.metrics.ClaimTask(err == nil)

	ts, err := newTaskScheduler(s, task, meta, s.metrics)
	if err != nil {
		return err
	}

	tid := task.ID.String()
	s.schedulerMu.Lock()
	_, ok := s.taskSchedulers[tid]
	if ok {
		s.schedulerMu.Unlock()
		return errors.New("task has already been claimed")
	}

	s.taskSchedulers[tid] = ts

	s.schedulerMu.Unlock()

	next, hasQueue := ts.NextDue()
	if now := atomic.LoadInt64(&s.now); now >= next || hasQueue {
		ts.Work()
	}
	return nil
}

func (s *outerScheduler) ReleaseTask(taskID platform.ID) error {
	s.schedulerMu.Lock()
	defer s.schedulerMu.Unlock()

	tid := taskID.String()
	t, ok := s.taskSchedulers[tid]
	if !ok {
		return ErrTaskNotClaimed
	}

	t.Cancel()
	delete(s.taskSchedulers, tid)

	s.metrics.ReleaseTask(tid)

	return nil
}

func (s *outerScheduler) PrometheusCollectors() []prometheus.Collector {
	return s.metrics.PrometheusCollectors()
}

// taskScheduler is a lightweight wrapper around a collection of runners.
type taskScheduler struct {
	// Reference to outerScheduler.now. Must be accessed atomically.
	now *int64

	// Task we are scheduling for.
	task *StoreTask

	// CancelFunc for context passed to runners, to enable Cancel method.
	cancel context.CancelFunc

	// Fixed-length slice of runners.
	runners []*runner

	logger *zap.Logger

	metrics *schedulerMetrics

	nextDueMu     sync.RWMutex // Protects following fields.
	nextDue       int64        // Unix timestamp of next due.
	nextDueSource int64        // Run time that produced nextDue.
	hasQueue      bool         // Whether there is a queue of manual runs.
}

func newTaskScheduler(
	s *outerScheduler,
	task *StoreTask,
	meta *StoreTaskMeta,
	metrics *schedulerMetrics,
) (*taskScheduler, error) {
	firstDue, err := meta.NextDueRun()
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())
	ts := &taskScheduler{
		now:           &s.now,
		task:          task,
		cancel:        cancel,
		runners:       make([]*runner, meta.MaxConcurrency),
		logger:        s.logger.With(zap.String("task_id", task.ID.String())),
		metrics:       s.metrics,
		nextDue:       firstDue,
		nextDueSource: math.MinInt64,
		hasQueue:      len(meta.ManualRuns) > 0,
	}

	for i := range ts.runners {
		logger := ts.logger.With(zap.Int("run_slot", i))
		ts.runners[i] = newRunner(ctx, logger, task, s.desiredState, s.executor, s.logWriter, ts)
	}

	return ts, nil
}

// Work begins a work cycle on the taskScheduler.
// As many runners are started as possible.
func (ts *taskScheduler) Work() {
	for _, r := range ts.runners {
		r.Start()
		if r.IsIdle() {
			// Ran out of jobs to start.
			break
		}
	}
}

// Cancel interrupts this taskScheduler and its runners.
func (ts *taskScheduler) Cancel() {
	ts.cancel()
}

// NextDue returns the next due timestamp, and whether there is a queue.
func (ts *taskScheduler) NextDue() (int64, bool) {
	ts.nextDueMu.RLock()
	defer ts.nextDueMu.RUnlock()
	return ts.nextDue, ts.hasQueue
}

// SetNextDue sets the next due timestamp and whether the task has a queue,
// and records the source (the now value of the run who reported nextDue).
func (ts *taskScheduler) SetNextDue(nextDue int64, hasQueue bool, source int64) {
	// TODO(mr): we may need some logic around source to handle if SetNextDue is called out of order.
	ts.nextDueMu.Lock()
	defer ts.nextDueMu.Unlock()
	ts.nextDue = nextDue
	ts.nextDueSource = source
	ts.hasQueue = hasQueue
}

// A runner is one eligible "concurrency slot" for a given task.
type runner struct {
	state *uint32

	// Cancelable context from parent taskScheduler.
	ctx context.Context

	task *StoreTask

	desiredState DesiredState
	executor     Executor
	logWriter    LogWriter

	// Parent taskScheduler.
	ts *taskScheduler

	logger *zap.Logger
}

func newRunner(
	ctx context.Context,
	logger *zap.Logger,
	task *StoreTask,
	desiredState DesiredState,
	executor Executor,
	logWriter LogWriter,
	ts *taskScheduler,
) *runner {
	return &runner{
		ctx:          ctx,
		state:        new(uint32),
		task:         task,
		desiredState: desiredState,
		executor:     executor,
		logWriter:    logWriter,
		ts:           ts,
		logger:       logger,
	}
}

// Valid runner states.
const (
	// Available to pick up a new run.
	runnerIdle uint32 = iota

	// Busy, cannot pick up a new run.
	runnerWorking

	// TODO(mr): use more granular runner states, so we can inspect the overall state of a taskScheduler.
)

// IsIdle returns true if the runner is idle.
// This uses an atomic load, so it is possible that the runner is no longer idle immediately after this returns true.
func (r *runner) IsIdle() bool {
	return atomic.LoadUint32(r.state) == runnerIdle
}

// Start checks if a new run is ready to be scheduled, and if so,
// creates a run on this goroutine and begins executing it on a separate goroutine.
func (r *runner) Start() {
	if !atomic.CompareAndSwapUint32(r.state, runnerIdle, runnerWorking) {
		// Already working. Cannot start.
		return
	}

	r.startFromWorking(atomic.LoadInt64(r.ts.now))
}

// startFromWorking attempts to create a run if one is due, and then begins execution on a separate goroutine.
// r.state must be runnerWorking when this is called.
func (r *runner) startFromWorking(now int64) {
	if nextDue, hasQueue := r.ts.NextDue(); now < nextDue && !hasQueue {
		// Not ready for a new run. Go idle again.
		atomic.StoreUint32(r.state, runnerIdle)
		return
	}

	rc, err := r.desiredState.CreateNextRun(r.ctx, r.task.ID, now)
	if err != nil {
		r.logger.Info("Failed to create run", zap.Error(err))
		atomic.StoreUint32(r.state, runnerIdle)
		return
	}
	qr := rc.Created
	r.ts.SetNextDue(rc.NextDue, rc.HasQueue, qr.Now)

	// Create a new child logger for the individual run.
	// We can't do r.logger = r.logger.With(zap.String("run_id", qr.RunID.String()) because zap doesn't deduplicate fields,
	// and we'll quickly end up with many run_ids associated with the log.
	runLogger := r.logger.With(zap.String("run_id", qr.RunID.String()), zap.Int64("now", qr.Now))

	runLogger.Info("Created run; beginning execution")
	go r.executeAndWait(now, qr, runLogger)

	r.updateRunState(qr, RunStarted, runLogger)
}

func (r *runner) executeAndWait(now int64, qr QueuedRun, runLogger *zap.Logger) {
	rp, err := r.executor.Execute(r.ctx, qr)
	if err != nil {
		// TODO(mr): retry? and log error.
		atomic.StoreUint32(r.state, runnerIdle)
		r.updateRunState(qr, RunFail, runLogger)
		return
	}

	ready := make(chan struct{})
	go func() {
		// If the runner's context is canceled, cancel the RunPromise.
		select {
		// Canceled context.
		case <-r.ctx.Done():
			rp.Cancel()
		// Wait finished.
		case <-ready:
		}
	}()

	// TODO(mr): handle res.IsRetryable().
	_, err = rp.Wait()
	close(ready)
	if err != nil {
		if err == ErrRunCanceled {
			_ = r.desiredState.FinishRun(r.ctx, qr.TaskID, qr.RunID)
			r.updateRunState(qr, RunCanceled, runLogger)

			// Move on to the next execution, for a canceled run.
			r.startFromWorking(atomic.LoadInt64(r.ts.now))
			return
		}

		runLogger.Info("Failed to wait for execution result", zap.Error(err))
		// TODO(mr): retry?
		r.updateRunState(qr, RunFail, runLogger)
		atomic.StoreUint32(r.state, runnerIdle)
		return
	}

	if err := r.desiredState.FinishRun(r.ctx, qr.TaskID, qr.RunID); err != nil {
		runLogger.Info("Failed to finish run", zap.Error(err))
		// TODO(mr): retry?
		// Need to think about what it means if there was an error finishing a run.
		atomic.StoreUint32(r.state, runnerIdle)
		r.updateRunState(qr, RunFail, runLogger)
		return
	}
	r.updateRunState(qr, RunSuccess, runLogger)
	runLogger.Info("Execution succeeded")

	// Check again if there is a new run available, without returning to idle state.
	r.startFromWorking(atomic.LoadInt64(r.ts.now))
}

func (r *runner) updateRunState(qr QueuedRun, s RunStatus, runLogger *zap.Logger) {
	switch s {
	case RunStarted:
		r.ts.metrics.StartRun(r.task.ID.String())
	case RunSuccess:
		r.ts.metrics.FinishRun(r.task.ID.String(), true)
	case RunFail, RunCanceled:
		r.ts.metrics.FinishRun(r.task.ID.String(), false)
	default:
		// We are deliberately not handling RunQueued yet.
		// There is not really a notion of being queued in this runner architecture.
		runLogger.Warn("Unhandled run state", zap.Stringer("state", s))
	}

	// Arbitrarily chosen short time limit for how fast the log write must complete.
	// If we start seeing errors from this, we know the time limit is too short or the system is overloaded.
	ctx, cancel := context.WithTimeout(r.ctx, 10*time.Millisecond)
	defer cancel()
	if err := r.logWriter.UpdateRunState(ctx, r.task, qr.RunID, time.Now(), s); err != nil {
		runLogger.Info("Error updating run state", zap.Stringer("state", s), zap.Error(err))
	}
}

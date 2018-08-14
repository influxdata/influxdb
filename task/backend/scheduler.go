package backend

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/influxdata/platform"
	"github.com/influxdata/platform/task/options"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
	"gopkg.in/robfig/cron.v2"
)

var ErrRunCanceled = errors.New("run canceled")
var ErrTaskNotClaimed = errors.New("task not claimed")

// DesiredState persists the desired state of a run.
type DesiredState interface {
	// CreateRun returns a run ID for a task and a now timestamp.
	// If a run already exists for taskID and now, CreateRun must return an error without queuing a new run.
	CreateRun(ctx context.Context, taskID platform.ID, now int64) (QueuedRun, error)

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
	// The timing schedule for the task is parsed from the script.
	// startExecutionFrom is an exclusive timestamp, after which execution should start;
	// you can set startExecutionFrom in the past to backfill a task.
	// concurrencyLimit is how many runs may be concurrently queued or executing.
	// concurrencyLimit must be positive.
	ClaimTask(task *StoreTask, startExecutionFrom int64, opt *options.Options) error
	// ReleaseTask immediately cancels any in-progress runs for the given task ID,
	// and releases any resources related to management of that task.
	ReleaseTask(taskID platform.ID) error
}

type SchedulerOption func(Scheduler)

func WithTicker(ctx context.Context, d time.Duration) SchedulerOption {
	return func(s Scheduler) {
		ticker := time.NewTicker(d)

		go func() {
			<-ctx.Done()
			ticker.Stop()
		}()

		for time := range ticker.C {
			go s.Tick(time.Unix())
		}
	}
}

func WithCronTimer(ctx context.Context) SchedulerOption {
	return func(s Scheduler) {
		switch sched := s.(type) {
		case *outerScheduler:
			sched.cronTimer = cron.New()
			sched.cronTimer.Start()

			go func() {
				<-ctx.Done()
				sched.cronTimer.Stop()
			}()
		default:
			panic(fmt.Sprintf("cannot apply WithCronTimer to Scheduler of type %T", s))
		}
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
		desiredState: desiredState,
		executor:     executor,
		logWriter:    lw,
		now:          now,
		tasks:        make(map[string]*taskScheduler),
		logger:       zap.NewNop(),
		metrics:      newSchedulerMetrics(),
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

	now       int64
	logger    *zap.Logger
	cronTimer *cron.Cron

	metrics *schedulerMetrics

	mu sync.Mutex

	tasks map[string]*taskScheduler
}

func (s *outerScheduler) Tick(now int64) {
	atomic.StoreInt64(&s.now, now)

	s.mu.Lock()
	defer s.mu.Unlock()

	for _, ts := range s.tasks {
		ts.Start(now)
	}
}

func (s *outerScheduler) ClaimTask(task *StoreTask, startExecutionFrom int64, opts *options.Options) (err error) {
	defer s.metrics.ClaimTask(err == nil)

	timer := opts.Cron

	if timer == "" {
		if opts.Every < time.Second {
			return errors.New("timing options not set or set too quick")
		}

		if opts.Every.Truncate(time.Second) != opts.Every {
			return errors.New("invalid every granularity")
		}
		timer = "@every " + opts.Every.String()
	}

	sch, err := cron.Parse(timer)
	if err != nil {
		return fmt.Errorf("error parsing cron expression: %v", err)
	}

	ts := newTaskScheduler(
		s,
		task,
		sch,
		startExecutionFrom,
		uint8(opts.Concurrency),
	)

	if s.cronTimer != nil {
		cronID, err := s.cronTimer.AddFunc(timer, func() {
			ts.Start(time.Now().Unix())
		})
		if err != nil {
			return fmt.Errorf("error starting cron timer: %v", err)
		}
		ts.cronID = cronID
	}

	s.mu.Lock()
	_, ok := s.tasks[task.ID.String()]
	if ok {
		s.mu.Unlock()
		return errors.New("task has already been claimed")
	}

	s.tasks[task.ID.String()] = ts

	s.mu.Unlock()

	ts.Start(s.now)
	return nil
}

func (s *outerScheduler) ReleaseTask(taskID platform.ID) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	tid := taskID.String()
	t, ok := s.tasks[tid]
	if !ok {
		return ErrTaskNotClaimed
	}

	if s.cronTimer != nil {
		s.cronTimer.Remove(t.cronID)
	}

	t.Cancel()
	delete(s.tasks, tid)

	s.metrics.ReleaseTask(tid)

	return nil
}

func (s *outerScheduler) PrometheusCollectors() []prometheus.Collector {
	return s.metrics.PrometheusCollectors()
}

// taskScheduler is a lightweight wrapper around a collection of runners.
type taskScheduler struct {
	// Task we are scheduling for.
	task *StoreTask

	// Seconds since UTC epoch.
	now int64

	// CancelFunc for context passed to runners, to enable Cancel method.
	cancel context.CancelFunc

	// cronID is used when we need to remove a taskSchedule from the cron scheduler.
	cronID cron.EntryID

	// Fixed-length slice of runners.
	runners []*runner

	// Record updates to run state.
	logWriter LogWriter

	logger *zap.Logger
}

func newTaskScheduler(
	s *outerScheduler,
	task *StoreTask,
	cron cron.Schedule,
	startExecutionFrom int64,
	concurrencyLimit uint8,
) *taskScheduler {
	firstScheduled := cron.Next(time.Unix(startExecutionFrom, 0).UTC()).Unix()
	ctx, cancel := context.WithCancel(context.Background())
	ts := &taskScheduler{
		task:    task,
		now:     startExecutionFrom,
		cancel:  cancel,
		runners: make([]*runner, concurrencyLimit),
		logger:  s.logger.With(zap.String("task_id", task.ID.String())),
	}

	tt := &taskTimer{
		taskNow: &ts.now,
		cron:    cron,

		nextScheduledRun: firstScheduled,
		latestInProgress: startExecutionFrom,

		metrics: s.metrics,
	}

	for i := range ts.runners {
		logger := ts.logger.With(zap.Int("run_slot", i))
		ts.runners[i] = newRunner(ctx, logger, task, s.desiredState, s.executor, s.logWriter, tt)
	}

	return ts
}

// Start enqueues as many immediate jobs as possible,
// without exceeding the now timestamp from the outer scheduler.
func (ts *taskScheduler) Start(now int64) {
	atomic.StoreInt64(&ts.now, now)
	for _, r := range ts.runners {
		r.Start()
		if r.IsIdle() {
			// Ran out of jobs to start.
			break
		}
	}
}

func (ts *taskScheduler) Cancel() {
	ts.cancel()
}

// taskTimer holds information about global timing, and scheduled and in-progress runs of a task.
// A single taskTimer is shared among many runners in a taskScheduler.
type taskTimer struct {
	// Reference to task scheduler's now field that gets updated by Start().
	// By using a pointer here we are allowing any Start() calls to the task scheduler to
	// update the taskTimer with a single atomic update.
	// This value must be accessed with sync.Atomic.
	taskNow *int64

	// Schedule of task.
	cron cron.Schedule

	metrics *schedulerMetrics

	mu sync.RWMutex

	// Timestamp of the next scheduled run.
	nextScheduledRun int64

	// Timestamp of the latest run in progress, i.e. a run that has been created.
	// This value is not affected by a single runner going idle.
	latestInProgress int64
}

// NextScheduledRun returns the timestamp of the next run that should be scheduled,
// and whether it is okay to schedule that run now.
func (tt *taskTimer) NextScheduledRun() (int64, bool) {
	tt.mu.RLock()
	next := tt.nextScheduledRun
	tt.mu.RUnlock()

	return next, next <= atomic.LoadInt64(tt.taskNow)
}

// StartRun updates tt's internal state to indicate that a run is starting with the given timestamp.
func (tt *taskTimer) StartRun(now int64) {
	tt.mu.Lock()

	if now > tt.latestInProgress {
		tt.latestInProgress = now
	}
	if tt.latestInProgress == tt.nextScheduledRun {
		tt.nextScheduledRun = tt.cron.Next(time.Unix(now, 0).UTC()).Unix()
	} else if tt.latestInProgress > tt.nextScheduledRun {
		panic(fmt.Sprintf("skipped a scheduled run: %d", tt.nextScheduledRun))
	}

	tt.mu.Unlock()
}

// A runner is one eligible "concurrency slot" for a given task.
// Each runner in a taskScheduler shares a taskTimer, and by locking that taskTimer they decide whether
// there is a new run that needs to be created and executed.
type runner struct {
	state *uint32

	// Cancelable context from parent taskScheduler.
	ctx context.Context

	task *StoreTask

	desiredState DesiredState
	executor     Executor
	logWriter    LogWriter

	tt *taskTimer

	logger *zap.Logger
}

func newRunner(
	ctx context.Context,
	logger *zap.Logger,
	task *StoreTask,
	desiredState DesiredState,
	executor Executor,
	logWriter LogWriter,
	tt *taskTimer,
) *runner {
	return &runner{
		ctx:          ctx,
		state:        new(uint32),
		task:         task,
		desiredState: desiredState,
		executor:     executor,
		logWriter:    logWriter,
		tt:           tt,
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

	r.startFromWorking()
}

// startFromWorking attempts to create a run if one is due, and then begins execution on a separate goroutine.
// r.state must be runnerWorking when this is called.
func (r *runner) startFromWorking() {
	if next, ready := r.tt.NextScheduledRun(); ready {
		// It's possible that two runners may attempt to create the same run for this "next" timestamp,
		// but the contract of DesiredState requires that only one succeeds.
		qr, err := r.desiredState.CreateRun(r.ctx, r.task.ID, next)
		if err != nil {
			r.logger.Info("Failed to create run", zap.Error(err))
			atomic.StoreUint32(r.state, runnerIdle)
			return
		}

		// Create a new child logger for the individual run.
		// We can't do r.logger = r.logger.With(zap.String("run_id", qr.RunID.String()) because zap doesn't deduplicate fields,
		// and we'll quickly end up with many run_ids associated with the log.
		runLogger := r.logger.With(zap.String("run_id", qr.RunID.String()))

		r.tt.StartRun(next)

		go r.executeAndWait(qr, runLogger)

		r.updateRunState(qr, RunStarted, runLogger)
		return
	}

	// Wasn't ready for a new run, so we're idle again.
	atomic.StoreUint32(r.state, runnerIdle)
}

func (r *runner) executeAndWait(qr QueuedRun, runLogger *zap.Logger) {
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
		} else {
			runLogger.Info("Failed to wait for execution result", zap.Error(err))
			// TODO(mr): retry?
			r.updateRunState(qr, RunFail, runLogger)
		}
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

	// Check again if there is a new run available, without returning to idle state.
	r.startFromWorking()
}

func (r *runner) updateRunState(qr QueuedRun, s RunStatus, runLogger *zap.Logger) {
	switch s {
	case RunStarted:
		r.tt.metrics.StartRun(r.task.ID.String())
	case RunSuccess:
		r.tt.metrics.FinishRun(r.task.ID.String(), true)
	case RunFail, RunCanceled:
		r.tt.metrics.FinishRun(r.task.ID.String(), false)
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

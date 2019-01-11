package backend

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"

	platform "github.com/influxdata/influxdb"
	"github.com/opentracing/opentracing-go"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

var (
	// ErrRunCanceled is returned from the RunResult when a Run is Canceled.  It is used mostly internally.
	ErrRunCanceled = errors.New("run canceled")

	// ErrTaskNotClaimed is returned when attempting to operate against a task that must be claimed but is not.
	ErrTaskNotClaimed = errors.New("task not claimed")

	// ErrTaskAlreadyClaimed is returned when attempting to operate against a task that must not be claimed but is.
	ErrTaskAlreadyClaimed = errors.New("task already claimed")
)

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

	// Wait blocks until all RunPromises created through Execute have finished.
	// Once Wait has been called, it is an error to call Execute before Wait has returned.
	// After Wait returns, it is safe to call Execute again.
	Wait()
}

// QueuedRun is a task run that has been assigned an ID,
// but whose execution has not necessarily started.
type QueuedRun struct {
	TaskID, RunID platform.ID

	// The Unix timestamp (seconds since January 1, 1970 UTC) that will be set when a run a manually requested
	RequestedAt int64

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
	// Start allows the scheduler to Tick. A scheduler without start will do nothing
	Start(ctx context.Context)

	// Stop a scheduler from ticking.
	Stop()

	// ClaimTask begins control of task execution in this scheduler.
	ClaimTask(task *StoreTask, meta *StoreTaskMeta) error

	// UpdateTask will update the concurrency and the runners for a task
	UpdateTask(task *StoreTask, meta *StoreTaskMeta) error

	// ReleaseTask immediately cancels any in-progress runs for the given task ID,
	// and releases any resources related to management of that task.
	ReleaseTask(taskID platform.ID) error

	// Cancel stops an executing run.
	CancelRun(ctx context.Context, taskID, runID platform.ID) error
}

// TickSchedulerOption is a option you can use to modify the schedulers behavior.
type TickSchedulerOption func(*TickScheduler)

// WithTicker sets a time.Ticker with period d,
// and calls TickScheduler.Tick when the ticker rolls over to a new second.
// With a sub-second d, TickScheduler.Tick should be called roughly no later than d after a second:
// this can help ensure tasks happen early with a second window.
func WithTicker(ctx context.Context, d time.Duration) TickSchedulerOption {
	return func(s *TickScheduler) {
		ticker := time.NewTicker(d)

		go func() {
			prev := time.Now().Unix() - 1
			for {
				select {
				case t := <-ticker.C:
					u := t.Unix()
					if u > prev {
						prev = u
						go s.Tick(u)
					}
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
func WithLogger(logger *zap.Logger) TickSchedulerOption {
	return func(s *TickScheduler) {
		s.logger = logger.With(zap.String("svc", "taskd/scheduler"))
	}
}

// NewScheduler returns a new scheduler with the given desired state and the given now UTC timestamp.
func NewScheduler(desiredState DesiredState, executor Executor, lw LogWriter, now int64, opts ...TickSchedulerOption) *TickScheduler {
	o := &TickScheduler{
		desiredState:   desiredState,
		executor:       executor,
		logWriter:      lw,
		now:            now,
		taskSchedulers: make(map[platform.ID]*taskScheduler),
		logger:         zap.NewNop(),
		wg:             &sync.WaitGroup{},
		metrics:        newSchedulerMetrics(),
	}

	for _, opt := range opts {
		opt(o)
	}

	return o
}

type TickScheduler struct {
	desiredState DesiredState
	executor     Executor
	logWriter    LogWriter

	now    int64
	logger *zap.Logger

	metrics *schedulerMetrics

	ctx    context.Context
	cancel context.CancelFunc
	wg     *sync.WaitGroup

	schedulerMu    sync.Mutex                     // Protects access and modification of taskSchedulers map.
	taskSchedulers map[platform.ID]*taskScheduler // task ID -> task scheduler.
}

// CancelRun cancels a run, it has the unused Context argument so that it can implement a task.RunController
func (s *TickScheduler) CancelRun(_ context.Context, taskID, runID platform.ID) error {
	s.schedulerMu.Lock()
	defer s.schedulerMu.Unlock()
	ts, ok := s.taskSchedulers[taskID]
	if !ok {
		return ErrTaskNotFound
	}
	ts.runningMu.Lock()
	c, ok := ts.running[runID]
	if !ok {
		ts.runningMu.Unlock()
		return ErrRunNotFound
	}
	ts.runningMu.Unlock()
	if c.CancelFunc != nil {
		c.CancelFunc()
	}
	return nil
}

// Tick updates the time of the scheduler.
// Any owned tasks who are due to execute and who have a free concurrency slot,
// will begin a new execution.
func (s *TickScheduler) Tick(now int64) {
	s.schedulerMu.Lock()
	defer s.schedulerMu.Unlock()

	if s.ctx == nil {
		return
	}

	select {
	case <-s.ctx.Done():
		return
	default:
		// do nothing and allow ticks
	}

	atomic.StoreInt64(&s.now, now)

	affected := 0
	for _, ts := range s.taskSchedulers {
		if nextDue, hasQueue := ts.NextDue(); now >= nextDue || hasQueue {
			ts.Work()
			affected++
		}
	}
	// TODO(mr): find a way to emit a more useful / less annoying tick message, maybe aggregated over the past 10s or 30s?
	s.logger.Debug("Ticked", zap.Int64("now", now), zap.Int("tasks_affected", affected))
}

func (s *TickScheduler) Start(ctx context.Context) {
	s.schedulerMu.Lock()
	defer s.schedulerMu.Unlock()

	s.ctx, s.cancel = context.WithCancel(ctx)
}

func (s *TickScheduler) Stop() {

	s.schedulerMu.Lock()
	defer s.schedulerMu.Unlock()

	// if I was never started I cant stop
	if s.cancel == nil {
		return
	}

	s.cancel()

	// release tasks
	for id := range s.taskSchedulers {
		delete(s.taskSchedulers, id)
		s.metrics.ReleaseTask(id.String())
	}

	// Wait for schedulers to clean up.
	s.wg.Wait()

	// Wait for outstanding executions to finish.
	s.executor.Wait()
}

func (s *TickScheduler) ClaimTask(task *StoreTask, meta *StoreTaskMeta) (err error) {
	s.schedulerMu.Lock()
	defer s.schedulerMu.Unlock()
	if s.ctx == nil {
		return errors.New("can not claim tasks when i've not been started")
	}

	select {
	case <-s.ctx.Done():
		return errors.New("can not claim a task if not started")
	default:
		// do nothing and allow ticks
	}

	defer s.metrics.ClaimTask(err == nil)

	ts, err := newTaskScheduler(s.ctx, s.wg, s, task, meta, s.metrics)
	if err != nil {
		return err
	}

	_, ok := s.taskSchedulers[task.ID]
	if ok {
		return ErrTaskAlreadyClaimed
	}

	s.taskSchedulers[task.ID] = ts

	if len(meta.CurrentlyRunning) > 0 {
		if err := ts.WorkCurrentlyRunning(meta); err != nil {
			return err
		}
	}

	next, hasQueue := ts.NextDue()
	if now := atomic.LoadInt64(&s.now); now >= next || hasQueue {
		ts.Work()
	}
	return nil
}

func (s *TickScheduler) UpdateTask(task *StoreTask, meta *StoreTaskMeta) error {
	s.schedulerMu.Lock()
	defer s.schedulerMu.Unlock()

	ts, ok := s.taskSchedulers[task.ID]
	if !ok {
		return ErrTaskNotClaimed
	}
	ts.Cancel()

	nts, err := newTaskScheduler(s.ctx, s.wg, s, task, meta, s.metrics)
	if err != nil {
		return err
	}

	s.taskSchedulers[task.ID] = nts

	next, hasQueue := ts.NextDue()
	if now := atomic.LoadInt64(&s.now); now >= next || hasQueue {
		ts.Work()
	}

	return nil
}

func (s *TickScheduler) ReleaseTask(taskID platform.ID) error {
	s.schedulerMu.Lock()
	defer s.schedulerMu.Unlock()

	t, ok := s.taskSchedulers[taskID]
	if !ok {
		return ErrTaskNotClaimed
	}

	t.Cancel()
	delete(s.taskSchedulers, taskID)

	s.metrics.ReleaseTask(taskID.String())

	return nil
}

func (s *TickScheduler) PrometheusCollectors() []prometheus.Collector {
	return s.metrics.PrometheusCollectors()
}

type runCtx struct {
	Context    context.Context
	CancelFunc context.CancelFunc
}

// taskScheduler is a lightweight wrapper around a collection of runners.
type taskScheduler struct {
	// Reference to outerScheduler.now. Must be accessed atomically.
	now *int64

	// Task we are scheduling for.
	task *StoreTask

	// CancelFunc for context passed to runners, to enable Cancel method.
	cancel context.CancelFunc
	wg     *sync.WaitGroup

	// Fixed-length slice of runners.
	runners   []*runner
	running   map[platform.ID]runCtx
	runningMu sync.Mutex

	logger *zap.Logger

	metrics *schedulerMetrics

	nextDueMu     sync.RWMutex // Protects following fields.
	nextDue       int64        // Unix timestamp of next due.
	nextDueSource int64        // Run time that produced nextDue.
	hasQueue      bool         // Whether there is a queue of manual runs.
}

func newTaskScheduler(
	ctx context.Context,
	wg *sync.WaitGroup,
	s *TickScheduler,
	task *StoreTask,
	meta *StoreTaskMeta,
	metrics *schedulerMetrics,
) (*taskScheduler, error) {
	firstDue, err := meta.NextDueRun()
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(ctx)
	ts := &taskScheduler{
		now:           &s.now,
		task:          task,
		cancel:        cancel,
		wg:            wg,
		runners:       make([]*runner, meta.MaxConcurrency),
		running:       make(map[platform.ID]runCtx, meta.MaxConcurrency),
		logger:        s.logger.With(zap.String("task_id", task.ID.String())),
		metrics:       s.metrics,
		nextDue:       firstDue,
		nextDueSource: math.MinInt64,
		hasQueue:      len(meta.ManualRuns) > 0,
	}

	for i := range ts.runners {
		logger := ts.logger.With(zap.Int("run_slot", i))
		ts.runners[i] = newRunner(ctx, wg, logger, task, s.desiredState, s.executor, s.logWriter, ts)
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

func (ts *taskScheduler) WorkCurrentlyRunning(meta *StoreTaskMeta) error {
	for _, cr := range meta.CurrentlyRunning {
		foundWorker := false
		for _, r := range ts.runners {
			qr := QueuedRun{TaskID: ts.task.ID, RunID: platform.ID(cr.RunID), Now: cr.Now}
			if r.RestartRun(qr) {
				foundWorker = true
				break
			}
		}

		if !foundWorker {
			return errors.New("worker not found to resume work")
		}
	}

	return nil
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
	wg  *sync.WaitGroup

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
	wg *sync.WaitGroup,
	logger *zap.Logger,
	task *StoreTask,
	desiredState DesiredState,
	executor Executor,
	logWriter LogWriter,
	ts *taskScheduler,
) *runner {
	return &runner{
		ctx:          ctx,
		wg:           wg,
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

// RestartRun attempts to restart a queued run if the runner is available to do the work.
// If the runner was already busy we return false.
func (r *runner) RestartRun(qr QueuedRun) bool {
	if !atomic.CompareAndSwapUint32(r.state, runnerIdle, runnerWorking) {
		// already working
		return false
	}
	// create a QueuedRun because we cant stm.CreateNextRun
	runLogger := r.logger.With(zap.String("run_id", qr.RunID.String()), zap.Int64("now", qr.Now))
	r.wg.Add(1)
	r.ts.runningMu.Lock()
	rCtx, ok := r.ts.running[qr.RunID]
	if !ok {
		ctx, cancel := context.WithCancel(context.TODO())
		rCtx = runCtx{Context: ctx, CancelFunc: cancel}
		r.ts.running[qr.RunID] = rCtx
	}
	r.ts.runningMu.Unlock()
	go r.executeAndWait(rCtx.Context, qr, runLogger)

	r.updateRunState(qr, RunStarted, runLogger)
	return true
}

// startFromWorking attempts to create a run if one is due, and then begins execution on a separate goroutine.
// r.state must be runnerWorking when this is called.
func (r *runner) startFromWorking(now int64) {
	if nextDue, hasQueue := r.ts.NextDue(); now < nextDue && !hasQueue {
		// Not ready for a new run. Go idle again.
		atomic.StoreUint32(r.state, runnerIdle)
		return
	}
	ctx, cancel := context.WithCancel(r.ctx)
	rc, err := r.desiredState.CreateNextRun(ctx, r.task.ID, now)
	if err != nil {
		r.logger.Info("Failed to create run", zap.Error(err))
		atomic.StoreUint32(r.state, runnerIdle)
		cancel() // cancel to prevent context leak
		return
	}
	qr := rc.Created
	r.ts.runningMu.Lock()
	r.ts.running[qr.RunID] = runCtx{Context: ctx, CancelFunc: cancel}
	r.ts.runningMu.Unlock()
	r.ts.SetNextDue(rc.NextDue, rc.HasQueue, qr.Now)

	// Create a new child logger for the individual run.
	// We can't do r.logger = r.logger.With(zap.String("run_id", qr.RunID.String()) because zap doesn't deduplicate fields,
	// and we'll quickly end up with many run_ids associated with the log.
	runLogger := r.logger.With(zap.String("run_id", qr.RunID.String()), zap.Int64("now", qr.Now))

	runLogger.Info("Created run; beginning execution")
	r.wg.Add(1)
	go r.executeAndWait(ctx, qr, runLogger)

	r.updateRunState(qr, RunStarted, runLogger)
}

func (r *runner) clearRunning(id platform.ID) {
	r.ts.runningMu.Lock()
	r.ts.running[id].CancelFunc() // cleanup
	delete(r.ts.running, id)
	r.ts.runningMu.Unlock()
}

func (r *runner) executeAndWait(ctx context.Context, qr QueuedRun, runLogger *zap.Logger) {
	defer r.wg.Done()

	sp, spCtx := opentracing.StartSpanFromContext(ctx, "task.run.execution")
	defer sp.Finish()

	rp, err := r.executor.Execute(spCtx, qr)

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
		case <-ctx.Done():
			r.clearRunning(qr.RunID)
			rp.Cancel()
		// Canceled context.
		case <-r.ctx.Done():
			r.clearRunning(qr.RunID)
			rp.Cancel()
		// Wait finished.
		case <-ready:
			r.clearRunning(qr.RunID)
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
	rlb := RunLogBase{
		Task:            r.task,
		RunID:           qr.RunID,
		RunScheduledFor: qr.Now,
		RequestedAt:     qr.RequestedAt,
	}

	switch s {
	case RunStarted:
		r.ts.metrics.StartRun(r.task.ID.String())
		r.logWriter.AddRunLog(r.ctx, rlb, time.Now(), fmt.Sprintf("Started task from script: %q", r.task.Script))
	case RunSuccess:
		r.ts.metrics.FinishRun(r.task.ID.String(), true)
		r.logWriter.AddRunLog(r.ctx, rlb, time.Now(), "Completed successfully")
	case RunFail:
		r.ts.metrics.FinishRun(r.task.ID.String(), false)
		r.logWriter.AddRunLog(r.ctx, rlb, time.Now(), "Failed")
	case RunCanceled:
		r.ts.metrics.FinishRun(r.task.ID.String(), false)
		r.logWriter.AddRunLog(r.ctx, rlb, time.Now(), "Canceled")
	default: // We are deliberately not handling RunQueued yet.
		// There is not really a notion of being queued in this runner architecture.
		runLogger.Warn("Unhandled run state", zap.Stringer("state", s))
	}

	// Arbitrarily chosen short time limit for how fast the log write must complete.
	// If we start seeing errors from this, we know the time limit is too short or the system is overloaded.
	ctx, cancel := context.WithTimeout(r.ctx, 10*time.Millisecond)
	defer cancel()
	if err := r.logWriter.UpdateRunState(ctx, rlb, time.Now(), s); err != nil {
		runLogger.Info("Error updating run state", zap.Stringer("state", s), zap.Error(err))
	}
}

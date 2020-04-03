package executor

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/influxdata/flux"
	"github.com/influxdata/flux/lang"
	"github.com/influxdata/flux/runtime"
	"github.com/influxdata/influxdb"
	icontext "github.com/influxdata/influxdb/context"
	"github.com/influxdata/influxdb/kit/tracing"
	"github.com/influxdata/influxdb/query"
	"github.com/influxdata/influxdb/task/backend"
	"github.com/influxdata/influxdb/task/backend/scheduler"
	"go.uber.org/zap"
)

var _ scheduler.Executor = (*Executor)(nil)

type Promise interface {
	ID() influxdb.ID
	Cancel(ctx context.Context)
	Done() <-chan struct{}
	Error() error
}

// MultiLimit allows us to create a single limit func that applies more then one limit.
func MultiLimit(limits ...LimitFunc) LimitFunc {
	return func(task *influxdb.Task, run *influxdb.Run) error {
		for _, lf := range limits {
			if err := lf(task, run); err != nil {
				return err
			}
		}
		return nil
	}
}

// LimitFunc is a function the executor will use to
type LimitFunc func(*influxdb.Task, *influxdb.Run) error

// NewExecutor creates a new task executor
func NewExecutor(log *zap.Logger, qs query.QueryService, as influxdb.AuthorizationService, ts influxdb.TaskService, tcs backend.TaskControlService) (*Executor, *ExecutorMetrics) {
	e := &Executor{
		log: log,
		ts:  ts,
		tcs: tcs,
		qs:  qs,
		as:  as,

		currentPromises: sync.Map{},
		promiseQueue:    make(chan *promise, 1000),                                //TODO(lh): make this configurable
		workerLimit:     make(chan struct{}, 100),                                 //TODO(lh): make this configurable
		limitFunc:       func(*influxdb.Task, *influxdb.Run) error { return nil }, // noop
	}

	e.metrics = NewExecutorMetrics(e)

	wm := &workerMaker{
		e: e,
	}

	e.workerPool = sync.Pool{New: wm.new}
	return e, e.metrics
}

// Executor it a task specific executor that works with the new scheduler system.
type Executor struct {
	log *zap.Logger
	ts  influxdb.TaskService
	tcs backend.TaskControlService

	qs query.QueryService
	as influxdb.AuthorizationService

	metrics *ExecutorMetrics

	// currentPromises are all the promises we are made that have not been fulfilled
	currentPromises sync.Map

	// keep a pool of promise's we have in queue
	promiseQueue chan *promise

	limitFunc LimitFunc

	// keep a pool of execution workers.
	workerPool  sync.Pool
	workerLimit chan struct{}
}

// SetLimitFunc sets the limit func for this task executor
func (e *Executor) SetLimitFunc(l LimitFunc) {
	e.limitFunc = l
}

// Execute is a executor to satisfy the needs of tasks
func (e *Executor) Execute(ctx context.Context, id scheduler.ID, scheduledFor time.Time, runAt time.Time) error {
	_, err := e.PromisedExecute(ctx, id, scheduledFor, runAt)
	return err
}

// PromisedExecute begins execution for the tasks id with a specific scheduledFor time.
// When we execute we will first build a run for the scheduledFor time,
// We then want to add to the queue anything that was manually queued to run.
// If the queue is full the call to execute should hang and apply back pressure to the caller
// We then start a worker to work the newly queued jobs.
func (e *Executor) PromisedExecute(ctx context.Context, id scheduler.ID, scheduledFor time.Time, runAt time.Time) (Promise, error) {
	iid := influxdb.ID(id)
	// create a run
	p, err := e.createRun(ctx, iid, scheduledFor, runAt)
	if err != nil {
		return nil, err
	}

	e.startWorker()
	return p, nil
}

func (e *Executor) ManualRun(ctx context.Context, id influxdb.ID, runID influxdb.ID) (Promise, error) {
	// create promises for any manual runs
	r, err := e.tcs.StartManualRun(ctx, id, runID)
	if err != nil {
		return nil, err
	}
	p, err := e.createPromise(ctx, r)

	e.startWorker()
	e.metrics.manualRunsCounter.WithLabelValues(id.String()).Inc()
	return p, err
}

func (e *Executor) ResumeCurrentRun(ctx context.Context, id influxdb.ID, runID influxdb.ID) (Promise, error) {
	cr, err := e.tcs.CurrentlyRunning(ctx, id)
	if err != nil {
		return nil, err
	}

	for _, run := range cr {
		if run.ID == runID {
			if _, ok := e.currentPromises.Load(run.ID); ok {
				continue
			}

			p, err := e.createPromise(ctx, run)

			e.startWorker()
			e.metrics.resumeRunsCounter.WithLabelValues(id.String()).Inc()
			return p, err
		}
	}
	return nil, influxdb.ErrRunNotFound
}

func (e *Executor) createRun(ctx context.Context, id influxdb.ID, scheduledFor time.Time, runAt time.Time) (*promise, error) {
	r, err := e.tcs.CreateRun(ctx, id, scheduledFor.UTC(), runAt.UTC())
	if err != nil {
		return nil, err
	}

	return e.createPromise(ctx, r)
}

func (e *Executor) startWorker() {
	// see if have available workers
	select {
	case e.workerLimit <- struct{}{}:
	default:
		// we have reached our worker limit and we cannot start any more.
		return
	}
	// fire up some workers
	worker := e.workerPool.Get().(*worker)
	if worker != nil {
		// if the worker is nil all the workers are busy and one of them will pick up the work we enqueued.
		go func() {
			// don't forget to put the worker back when we are done
			defer e.workerPool.Put(worker)
			worker.work()

			// remove a struct from the worker limit to another worker to work
			<-e.workerLimit
		}()
	}
}

// Cancel a run of a specific task.
func (e *Executor) Cancel(ctx context.Context, runID influxdb.ID) error {
	// find the promise
	val, ok := e.currentPromises.Load(runID)
	if !ok {
		return nil
	}
	promise := val.(*promise)

	// call cancel on it.
	promise.Cancel(ctx)

	return nil
}

func (e *Executor) createPromise(ctx context.Context, run *influxdb.Run) (*promise, error) {
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	t, err := e.ts.FindTaskByID(ctx, run.TaskID)
	if err != nil {
		return nil, err
	}
	if !t.Authorization.GetUserID().Valid() {
		t.Authorization.UserID = t.OwnerID
	}

	ctx, cancel := context.WithCancel(ctx)
	// create promise
	p := &promise{
		run:        run,
		task:       t,
		auth:       t.Authorization,
		createdAt:  time.Now().UTC(),
		done:       make(chan struct{}),
		ctx:        ctx,
		cancelFunc: cancel,
	}

	// insert promise into queue to be worked
	// when the queue gets full we will hand and apply back pressure to the scheduler
	e.promiseQueue <- p

	// insert the promise into the registry
	e.currentPromises.Store(run.ID, p)
	return p, nil
}

type workerMaker struct {
	e *Executor
}

func (wm *workerMaker) new() interface{} {
	return &worker{wm.e, exhaustResultIterators}
}

type worker struct {
	e *Executor

	// exhaustResultIterators is used to exhaust the result
	// of a flux query
	exhaustResultIterators func(res flux.Result) error
}

func (w *worker) work() {
	// loop until we have no more work to do in the promise queue
	for {
		var prom *promise
		// check to see if we can execute
		select {
		case p, ok := <-w.e.promiseQueue:

			if !ok {
				// the promiseQueue has been closed
				return
			}
			prom = p
		default:
			// if nothing is left in the queue we are done
			return
		}

		// check to make sure we are below the limits.
		for {
			err := w.e.limitFunc(prom.task, prom.run)
			if err == nil {
				break
			}

			// add to the run log
			w.e.tcs.AddRunLog(prom.ctx, prom.task.ID, prom.run.ID, time.Now().UTC(), fmt.Sprintf("Task limit reached: %s", err.Error()))

			// sleep
			select {
			// If done the promise was canceled
			case <-prom.ctx.Done():
				w.e.tcs.AddRunLog(prom.ctx, prom.task.ID, prom.run.ID, time.Now().UTC(), "Run canceled")
				w.e.tcs.UpdateRunState(prom.ctx, prom.task.ID, prom.run.ID, time.Now().UTC(), influxdb.RunCanceled)
				prom.err = influxdb.ErrRunCanceled
				close(prom.done)
				return
			case <-time.After(time.Second):
			}
		}

		// execute the promise
		w.executeQuery(prom)

		// close promise done channel and set appropriate error
		close(prom.done)

		// remove promise from registry
		w.e.currentPromises.Delete(prom.run.ID)
	}
}

func (w *worker) start(p *promise) {
	// trace
	span, ctx := tracing.StartSpanFromContext(p.ctx)
	defer span.Finish()

	// add to run log
	w.e.tcs.AddRunLog(p.ctx, p.task.ID, p.run.ID, time.Now().UTC(), fmt.Sprintf("Started task from script: %q", p.task.Flux))
	// update run status
	w.e.tcs.UpdateRunState(ctx, p.task.ID, p.run.ID, time.Now().UTC(), influxdb.RunStarted)

	// add to metrics
	w.e.metrics.StartRun(p.task, time.Since(p.createdAt), time.Since(p.run.RunAt))
	p.startedAt = time.Now()
}

func (w *worker) finish(p *promise, rs influxdb.RunStatus, err error) {

	// trace
	span, ctx := tracing.StartSpanFromContext(p.ctx)
	defer span.Finish()

	// add to run log
	w.e.tcs.AddRunLog(p.ctx, p.task.ID, p.run.ID, time.Now().UTC(), fmt.Sprintf("Completed(%s)", rs.String()))
	// update run status
	w.e.tcs.UpdateRunState(ctx, p.task.ID, p.run.ID, time.Now().UTC(), rs)

	// add to metrics
	rd := time.Since(p.startedAt)
	w.e.metrics.FinishRun(p.task, rs, rd)

	// log error
	if err != nil {
		w.e.tcs.AddRunLog(p.ctx, p.task.ID, p.run.ID, time.Now().UTC(), err.Error())
		w.e.log.Debug("Execution failed", zap.Error(err), zap.String("taskID", p.task.ID.String()))
		w.e.metrics.LogError(p.task.Type, err)

		if backend.IsUnrecoverable(err) {
			// TODO (al): once user notification system is put in place, this code should be uncommented
			// if we get an error that requires user intervention to fix, deactivate the task and alert the user
			// inactive := string(backend.TaskInactive)
			// w.te.ts.UpdateTask(p.ctx, p.task.ID, influxdb.TaskUpdate{Status: &inactive})

			// and add to run logs
			w.e.tcs.AddRunLog(p.ctx, p.task.ID, p.run.ID, time.Now().UTC(), fmt.Sprintf("Task encountered unrecoverable error, requires admin action: %v", err.Error()))
			// add to metrics
			w.e.metrics.LogUnrecoverableError(p.task.ID, err)
		}

		p.err = err
	} else {
		w.e.log.Debug("Completed successfully", zap.String("taskID", p.task.ID.String()))
	}

	if _, err := w.e.tcs.FinishRun(p.ctx, p.task.ID, p.run.ID); err != nil {
		w.e.log.Error("Failed to finish run", zap.String("taskID", p.task.ID.String()), zap.String("runID", p.run.ID.String()), zap.Error(err))
	}
}

func (w *worker) executeQuery(p *promise) {
	span, ctx := tracing.StartSpanFromContext(p.ctx)
	defer span.Finish()

	// start
	w.start(p)

	pkg, err := runtime.ParseToJSON(p.task.Flux)
	if err != nil {
		w.finish(p, influxdb.RunFail, influxdb.ErrFluxParseError(err))
		return
	}

	sf := p.run.ScheduledFor

	req := &query.Request{
		Authorization:  p.auth,
		OrganizationID: p.task.OrganizationID,
		Compiler: lang.ASTCompiler{
			AST: pkg,
			Now: sf,
		},
	}
	req.WithReturnNoContent(true)
	ctx = icontext.SetAuthorizer(ctx, p.task.Authorization)
	it, err := w.e.qs.Query(ctx, req)
	if err != nil {
		// Assume the error should not be part of the runResult.
		w.finish(p, influxdb.RunFail, influxdb.ErrQueryError(err))
		return
	}

	var runErr error
	// Drain the result iterator.
	for it.More() {
		// Consume the full iterator so that we don't leak outstanding iterators.
		res := it.Next()
		if runErr = w.exhaustResultIterators(res); runErr != nil {
			w.e.log.Info("Error exhausting result iterator", zap.Error(runErr), zap.String("name", res.Name()))
		}
	}

	it.Release()

	// log the trace id and whether or not it was sampled into the run log
	if traceID, isSampled, ok := tracing.InfoFromSpan(span); ok {
		msg := fmt.Sprintf("trace_id=%s is_sampled=%t", traceID, isSampled)
		w.e.tcs.AddRunLog(p.ctx, p.task.ID, p.run.ID, time.Now().UTC(), msg)
	}

	if runErr != nil {
		w.finish(p, influxdb.RunFail, influxdb.ErrRunExecutionError(runErr))
		return
	}

	if it.Err() != nil {
		w.finish(p, influxdb.RunFail, influxdb.ErrResultIteratorError(it.Err()))
		return
	}

	w.finish(p, influxdb.RunSuccess, nil)
}

// RunsActive returns the current number of workers, which is equivalent to
// the number of runs actively running
func (e *Executor) RunsActive() int {
	return len(e.workerLimit)
}

// WorkersBusy returns the percent of total workers that are busy
func (e *Executor) WorkersBusy() float64 {
	return float64(len(e.workerLimit)) / float64(cap(e.workerLimit))
}

// PromiseQueueUsage returns the percent of the Promise Queue that is currently filled
func (e *Executor) PromiseQueueUsage() float64 {
	return float64(len(e.promiseQueue)) / float64(cap(e.promiseQueue))
}

// promise represents a promise the executor makes to finish a run's execution asynchronously.
type promise struct {
	run  *influxdb.Run
	task *influxdb.Task
	auth *influxdb.Authorization

	done chan struct{}
	err  error

	createdAt time.Time
	startedAt time.Time

	ctx        context.Context
	cancelFunc context.CancelFunc
}

// ID is the id of the run that was created
func (p *promise) ID() influxdb.ID {
	return p.run.ID
}

// Cancel is used to cancel a executing query
func (p *promise) Cancel(ctx context.Context) {
	// call cancelfunc
	p.cancelFunc()

	// wait for ctx.Done or p.Done
	select {
	case <-p.Done():
	case <-ctx.Done():
	}
}

// Done provides a channel that closes on completion of a rpomise
func (p *promise) Done() <-chan struct{} {
	return p.done
}

// Error returns the error resulting from a run execution.
// If the execution is not complete error waits on Done().
func (p *promise) Error() error {
	<-p.done
	return p.err
}

// exhaustResultIterators drains all the iterators from a flux query Result.
func exhaustResultIterators(res flux.Result) error {
	return res.Tables().Do(func(tbl flux.Table) error {
		return tbl.Do(func(flux.ColReader) error {
			return nil
		})
	})
}

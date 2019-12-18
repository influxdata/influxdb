// Package executor contains implementations of backend.Executor
// that depend on the query service.
package executor

import (
	"context"
	"sync"
	"time"

	"github.com/influxdata/flux"
	"github.com/influxdata/flux/lang"
	"github.com/influxdata/influxdb"
	icontext "github.com/influxdata/influxdb/context"
	"github.com/influxdata/influxdb/kit/tracing"
	"github.com/influxdata/influxdb/logger"
	"github.com/influxdata/influxdb/query"
	"github.com/influxdata/influxdb/task/backend"
	"go.uber.org/zap"
)

// queryServiceExecutor is an implementation of backend.Executor that depends on a QueryService.
type queryServiceExecutor struct {
	qs  query.QueryService
	as  influxdb.AuthorizationService
	ts  influxdb.TaskService
	log *zap.Logger
	wg  sync.WaitGroup
}

var _ backend.Executor = (*queryServiceExecutor)(nil)

// NewQueryServiceExecutor returns a new executor based on the given QueryService.
// In general, you should prefer NewAsyncQueryServiceExecutor, as that code is smaller and simpler,
// because asynchronous queries are more in line with the Executor interface.
func NewQueryServiceExecutor(log *zap.Logger, qs query.QueryService, as influxdb.AuthorizationService, ts influxdb.TaskService) *queryServiceExecutor {
	return &queryServiceExecutor{log: log, qs: qs, as: as, ts: ts}
}

// AddTaskService is a temporary solution to a chicken and egg problem. It takes a executor and sets the task service.
// This is required because the platform adaptor requires a executor but the executor requires a task service.
// TODO(lh): Remove this function once we are no longer using the PlatformAdaptor
func AddTaskService(e backend.Executor, ts influxdb.TaskService) {
	qe, ok := e.(*queryServiceExecutor)
	if ok {
		qe.ts = ts
	}
	ae, ok := e.(*asyncQueryServiceExecutor)
	if ok {
		ae.ts = ts
	}
}

func (e *queryServiceExecutor) Execute(ctx context.Context, run backend.QueuedRun) (backend.RunPromise, error) {
	t, err := e.ts.FindTaskByID(ctx, run.TaskID)
	if err != nil {
		return nil, err
	}

	// TODO(goller): remove need for context authorization.
	return newSyncRunPromise(icontext.SetAuthorizer(ctx, t.Authorization), t.Authorization, run, e, t), nil
}

func (e *queryServiceExecutor) Wait() {
	e.wg.Wait()
}

// syncRunPromise implements backend.RunPromise for a synchronous QueryService.
type syncRunPromise struct {
	qr     backend.QueuedRun
	auth   *influxdb.Authorization
	qs     query.QueryService
	t      *influxdb.Task
	ctx    context.Context
	cancel context.CancelFunc
	log    *zap.Logger
	logEnd func() // Called to log the end of the run operation.

	finishOnce sync.Once     // Ensure we set the values only once.
	ready      chan struct{} // Closed inside finish. Indicates Wait will no longer block.
	res        *runResult
	err        error
}

var _ backend.RunPromise = (*syncRunPromise)(nil)

func newSyncRunPromise(ctx context.Context, auth *influxdb.Authorization, qr backend.QueuedRun, e *queryServiceExecutor, t *influxdb.Task) *syncRunPromise {
	ctx, cancel := context.WithCancel(ctx)
	opLogger := e.log.With(zap.Stringer("task_id", qr.TaskID), zap.Stringer("run_id", qr.RunID))
	log, logEnd := logger.NewOperation(ctx, opLogger, "Executing task", "execute")
	rp := &syncRunPromise{
		qr:     qr,
		auth:   auth,
		qs:     e.qs,
		t:      t,
		log:    log,
		logEnd: logEnd,
		ctx:    ctx,
		cancel: cancel,
		ready:  make(chan struct{}),
	}

	e.wg.Add(2)
	go rp.doQuery(&e.wg)
	go rp.cancelOnContextDone(&e.wg)

	return rp
}

func (p *syncRunPromise) Run() backend.QueuedRun {
	return p.qr
}

func (p *syncRunPromise) Wait() (backend.RunResult, error) {
	<-p.ready

	// Need an explicit return nil to avoid the non-nil interface value issue.
	if p.err != nil {
		return nil, p.err
	}
	return p.res, nil
}

func (p *syncRunPromise) Cancel() {
	p.finish(nil, influxdb.ErrRunCanceled)
}

func (p *syncRunPromise) finish(res *runResult, err error) {
	p.finishOnce.Do(func() {
		defer func() {
			// Always cancel p's context.
			// If finish is called before p.qs.Query completes, the query will be interrupted.
			// If afterwards, then p.cancel is just a resource cleanup.
			p.cancel()

			p.logEnd()

			close(p.ready)
		}()

		p.res, p.err = res, err

		if err != nil {
			p.log.Debug("Execution failed to get result", zap.Error(err))
		} else if res.err != nil {
			p.log.Debug("Got result with error", zap.Error(res.err))
		} else {
			p.log.Debug("Completed successfully")
		}
	})
}

func (p *syncRunPromise) doQuery(wg *sync.WaitGroup) {
	defer wg.Done()

	pkg, err := flux.Parse(p.t.Flux)
	if err != nil {
		p.finish(nil, err)
		return
	}

	req := &query.Request{
		Authorization:  p.auth,
		OrganizationID: p.t.OrganizationID,
		Compiler: lang.ASTCompiler{
			AST: pkg,
			Now: time.Unix(p.qr.Now, 0),
		},
	}
	it, err := p.qs.Query(p.ctx, req)
	if err != nil {
		// Assume the error should not be part of the runResult.
		p.finish(nil, err)
		return
	}
	defer it.Release()

	// Drain the result iterator.
	for it.More() {
		// Consume the full iterator so that we don't leak outstanding iterators.
		res := it.Next()
		if err = exhaustResultIterators(res); err != nil {
			p.log.Info("Error exhausting result iterator", zap.Error(err), zap.String("name", res.Name()))
		}
	}

	// Must call Release to ensure Statistics are ready.
	// It's safe for Release to be called multiple times.
	it.Release()

	if err == nil {
		err = it.Err()
	}

	// Is it okay to assume it.Err will be set if the query context is canceled?
	p.finish(&runResult{err: err, statistics: it.Statistics()}, nil)
}

func (p *syncRunPromise) cancelOnContextDone(wg *sync.WaitGroup) {
	defer wg.Done()

	select {
	case <-p.ready:
		// Nothing to do.
	case <-p.ctx.Done():
		// Maybe the parent context was canceled,
		// or maybe finish was called already.
		// If it's the latter, this call to finish will be a no-op.
		p.finish(nil, p.ctx.Err())
	}
}

// asyncQueryServiceExecutor is an implementation of backend.Executor that depends on an AsyncQueryService.
type asyncQueryServiceExecutor struct {
	qs  query.AsyncQueryService
	as  influxdb.AuthorizationService
	ts  influxdb.TaskService
	log *zap.Logger
	wg  sync.WaitGroup
}

var _ backend.Executor = (*asyncQueryServiceExecutor)(nil)

// NewAsyncQueryServiceExecutor returns a new executor based on the given AsyncQueryService.
func NewAsyncQueryServiceExecutor(log *zap.Logger, qs query.AsyncQueryService, as influxdb.AuthorizationService, ts influxdb.TaskService) backend.Executor {
	return &asyncQueryServiceExecutor{log: log, qs: qs, as: as, ts: ts}
}

func (e *asyncQueryServiceExecutor) Execute(ctx context.Context, run backend.QueuedRun) (backend.RunPromise, error) {
	t, err := e.ts.FindTaskByID(ctx, run.TaskID)
	if err != nil {
		return nil, err
	}

	return newAsyncRunPromise(icontext.SetAuthorizer(ctx, t.Authorization), t.Authorization, run, e, t), nil
}

func (e *asyncQueryServiceExecutor) Wait() {
	e.wg.Wait()
}

// asyncRunPromise implements backend.RunPromise for an AsyncQueryService.
type asyncRunPromise struct {
	qr   backend.QueuedRun
	auth *influxdb.Authorization
	qs   query.AsyncQueryService
	t    *influxdb.Task
	ctx  context.Context

	log    *zap.Logger
	logEnd func() // Called to log the end of the run operation.

	finishOnce sync.Once     // Ensure we set the values only once.
	ready      chan struct{} // Closed inside finish. Indicates Wait will no longer block.
	res        *runResult
	err        error
}

var _ backend.RunPromise = (*asyncRunPromise)(nil)

func newAsyncRunPromise(ctx context.Context, auth *influxdb.Authorization, qr backend.QueuedRun, e *asyncQueryServiceExecutor, t *influxdb.Task) *asyncRunPromise {
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	opLogger := e.log.With(zap.Stringer("task_id", qr.TaskID), zap.Stringer("run_id", qr.RunID))
	log, logEnd := logger.NewOperation(ctx, opLogger, "Executing task", "execute")

	p := &asyncRunPromise{
		qr:     qr,
		auth:   auth,
		qs:     e.qs,
		t:      t,
		log:    log,
		logEnd: logEnd,
		ctx:    ctx,
		ready:  make(chan struct{}),
	}

	e.wg.Add(1)
	go p.doQuery(&e.wg)
	return p
}

func (p *asyncRunPromise) Run() backend.QueuedRun {
	return p.qr
}

func (p *asyncRunPromise) Wait() (backend.RunResult, error) {
	<-p.ready

	// Need an explicit return nil to avoid the non-nil interface value issue.
	if p.err != nil {
		return nil, p.err
	}
	return p.res, nil
}

func (p *asyncRunPromise) Cancel() {
	p.finish(nil, influxdb.ErrRunCanceled)
}

// doQuery waits for the query to become ready and sets p's results.
// If the promise is finished somewhere else first, such as if it is canceled,
// doQuery will return.
func (p *asyncRunPromise) doQuery(wg *sync.WaitGroup) {
	defer wg.Done()

	pkg, err := flux.Parse(p.t.Flux)
	if err != nil {
		p.finish(nil, err)
		return
	}

	req := &query.Request{
		Authorization:  p.t.Authorization,
		OrganizationID: p.t.OrganizationID,
		Compiler: lang.ASTCompiler{
			AST: pkg,
			Now: time.Unix(p.qr.Now, 0),
		},
	}
	q, err := p.qs.Query(p.ctx, req)
	if err != nil {
		// Assume the error should not be part of the runResult.
		p.finish(nil, err)
		return
	}
	// Always need to call Done after query is finished.
	defer q.Done()

	var rwg sync.WaitGroup
SelectLoop:
	for {
		select {
		case <-p.ready:
			// The promise was finished somewhere else, so we don't need to call p.finish.
			// But we do need to cancel the flux. This could be a no-op.
			q.Cancel()
			return
		case r, ok := <-q.Results():
			if !ok {
				break SelectLoop
			}

			rwg.Add(1)
			go func() {
				defer rwg.Done()
				if err := exhaustResultIterators(r); err != nil {
					p.log.Info("Error exhausting result iterator", zap.Error(err), zap.String("name", r.Name()))
				}
			}()
		}
	}

	rwg.Wait()

	if q.Err() != nil {
		// Something went wrong with the flux. Set the error in the run result.
		rr := &runResult{err: q.Err()}
		p.finish(rr, nil)
		return
	}

	// Otherwise, query was successful.
	// Must call query.Done before collecting statistics. It's safe to call multiple times.
	q.Done()
	p.finish(&runResult{statistics: q.Statistics()}, nil)
}

func (p *asyncRunPromise) finish(res *runResult, err error) {
	p.finishOnce.Do(func() {
		defer p.logEnd()

		p.res, p.err = res, err
		close(p.ready)

		if err != nil {
			p.log.Info("Execution failed to get result", zap.Error(err))
		} else if res.err != nil {
			p.log.Info("Got result with error", zap.Error(res.err))
		} else {
			p.log.Debug("Completed successfully")
		}
	})
}

type runResult struct {
	err        error
	retryable  bool
	statistics flux.Statistics
}

var _ backend.RunResult = (*runResult)(nil)

func (rr *runResult) Err() error                  { return rr.err }
func (rr *runResult) IsRetryable() bool           { return rr.retryable }
func (rr *runResult) Statistics() flux.Statistics { return rr.statistics }

// exhaustResultIterators drains all the iterators from a flux query Result.
func exhaustResultIterators(res flux.Result) error {
	return res.Tables().Do(func(tbl flux.Table) error {
		return tbl.Do(func(flux.ColReader) error {
			return nil
		})
	})
}

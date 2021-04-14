// Package control keeps track of resources and manages queries.
//
// The Controller manages the resources available to each query by
// managing the memory allocation and concurrency usage of each query.
// The Controller will compile a program by using the passed in language
// and it will start the program using the ResourceManager.
//
// It will guarantee that each program that is started has at least
// one goroutine that it can use with the dispatcher and it will
// ensure a minimum amount of memory is available before the program
// runs.
//
// Other goroutines and memory usage is at the will of the specific
// resource strategy that the Controller is using.
//
// The Controller also provides visibility into the lifetime of the query
// and its current resource usage.
package control

import (
	"context"
	"fmt"
	"runtime/debug"
	"sync"
	"sync/atomic"

	errors2 "github.com/influxdata/influxdb/v2/kit/platform/errors"

	"github.com/influxdata/flux"
	"github.com/influxdata/flux/codes"
	"github.com/influxdata/flux/execute/table"
	"github.com/influxdata/flux/lang"
	"github.com/influxdata/flux/memory"
	"github.com/influxdata/flux/runtime"
	"github.com/influxdata/influxdb/v2/kit/errors"
	"github.com/influxdata/influxdb/v2/kit/feature"
	"github.com/influxdata/influxdb/v2/kit/prom"
	"github.com/influxdata/influxdb/v2/kit/tracing"
	influxlogger "github.com/influxdata/influxdb/v2/logger"
	"github.com/influxdata/influxdb/v2/query"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// orgLabel is the metric label to use in the controller
const orgLabel = "org"

// Controller provides a central location to manage all incoming queries.
// The controller is responsible for compiling, queueing, and executing queries.
type Controller struct {
	config     Config
	lastID     uint64
	queriesMu  sync.RWMutex
	queries    map[QueryID]*Query
	queryQueue chan *Query
	wg         sync.WaitGroup
	shutdown   bool
	done       chan struct{}
	abortOnce  sync.Once
	abort      chan struct{}
	memory     *memoryManager

	metrics   *controllerMetrics
	labelKeys []string

	log *zap.Logger

	dependencies []flux.Dependency
}

type Config struct {
	// ConcurrencyQuota is the number of queries that are allowed to execute concurrently.
	//
	// This value is limited to an int32 because it's used to set the initial delta on the
	// controller's WaitGroup, and WG deltas have an effective limit of math.MaxInt32.
	// See: https://github.com/golang/go/issues/20687
	ConcurrencyQuota int32

	// InitialMemoryBytesQuotaPerQuery is the initial number of bytes allocated for a query
	// when it is started. If this is unset, then the MemoryBytesQuotaPerQuery will be used.
	InitialMemoryBytesQuotaPerQuery int64

	// MemoryBytesQuotaPerQuery is the maximum number of bytes (in table memory) a query is allowed to use at
	// any given time.
	//
	// A query may not be able to use its entire quota of memory if requesting more memory would conflict
	// with the maximum amount of memory that the controller can request.
	MemoryBytesQuotaPerQuery int64

	// MaxMemoryBytes is the maximum amount of memory the controller is allowed to
	// allocated to queries.
	//
	// If this is unset, then this number is ConcurrencyQuota * MemoryBytesQuotaPerQuery.
	// This number must be greater than or equal to the ConcurrencyQuota * InitialMemoryBytesQuotaPerQuery.
	// This number may be less than the ConcurrencyQuota * MemoryBytesQuotaPerQuery.
	MaxMemoryBytes int64

	// QueueSize is the number of queries that are allowed to be awaiting execution before new queries are rejected.
	//
	// This value is limited to an int32 because it's used to make(chan *Query, QueueSize) on controller startup.
	// Through trial-and-error I found that make(chan *Query, N) starts to panic for N > 1<<45 - 12, so not all
	// ints or int64s are safe to pass here. Using that max value still immediately crashes the program with an OOM,
	// because it tries to allocate TBs of memory for the channel.
	// I was able to boot influxd locally using math.MaxInt32 for this parameter.
	//
	// Less-scientifically, this was the only Config parameter other than ConcurrencyQuota to be typed as an int
	// instead of an explicit int64. When ConcurrencyQuota changed to an int32, it felt like a decent idea for
	// this to follow suit.
	QueueSize int32

	// MetricLabelKeys is a list of labels to add to the metrics produced by the controller.
	// The value for a given key will be read off the context.
	// The context value must be a string or an implementation of the Stringer interface.
	MetricLabelKeys []string

	ExecutorDependencies []flux.Dependency
}

// complete will fill in the defaults, validate the configuration, and
// return the new Config.
func (c *Config) complete() (Config, error) {
	config := *c
	if config.InitialMemoryBytesQuotaPerQuery == 0 {
		config.InitialMemoryBytesQuotaPerQuery = config.MemoryBytesQuotaPerQuery
	}

	if err := config.validate(true); err != nil {
		return Config{}, err
	}
	return config, nil
}

func (c *Config) validate(isComplete bool) error {
	if c.ConcurrencyQuota < 0 {
		return errors.New("ConcurrencyQuota must not be negative")
	} else if c.ConcurrencyQuota == 0 {
		if c.QueueSize != 0 {
			return errors.New("QueueSize must be unlimited when ConcurrencyQuota is unlimited")
		}
		if c.MaxMemoryBytes != 0 {
			// This is because we have to account for the per-query reserved memory and remove it from
			// the max total memory. If there is not a maximum number of queries this is not possible.
			return errors.New("Cannot limit max memory when ConcurrencyQuota is unlimited")
		}
	} else {
		if c.QueueSize <= 0 {
			return errors.New("QueueSize must be positive when ConcurrencyQuota is limited")
		}
	}
	if c.MemoryBytesQuotaPerQuery < 0 || (isComplete && c.MemoryBytesQuotaPerQuery == 0) {
		return errors.New("MemoryBytesQuotaPerQuery must be positive")
	}
	if c.InitialMemoryBytesQuotaPerQuery < 0 || (isComplete && c.InitialMemoryBytesQuotaPerQuery == 0) {
		return errors.New("InitialMemoryBytesQuotaPerQuery must be positive")
	}
	if c.MaxMemoryBytes < 0 {
		return errors.New("MaxMemoryBytes must be positive")
	}
	if c.MaxMemoryBytes != 0 {
		if minMemory := int64(c.ConcurrencyQuota) * c.InitialMemoryBytesQuotaPerQuery; c.MaxMemoryBytes < minMemory {
			return fmt.Errorf("MaxMemoryBytes must be greater than or equal to the ConcurrencyQuota * InitialMemoryBytesQuotaPerQuery: %d < %d (%d * %d)", c.MaxMemoryBytes, minMemory, c.ConcurrencyQuota, c.InitialMemoryBytesQuotaPerQuery)
		}
	}
	return nil
}

// Validate will validate that the controller configuration is valid.
func (c *Config) Validate() error {
	return c.validate(false)
}

type QueryID uint64

func New(config Config, logger *zap.Logger) (*Controller, error) {
	c, err := config.complete()
	if err != nil {
		return nil, errors.Wrap(err, "invalid controller config")
	}
	metricLabelKeys := append(c.MetricLabelKeys, orgLabel)
	if logger == nil {
		logger = zap.NewNop()
	}
	logger.Info("Starting query controller",
		zap.Int32("concurrency_quota", c.ConcurrencyQuota),
		zap.Int64("initial_memory_bytes_quota_per_query", c.InitialMemoryBytesQuotaPerQuery),
		zap.Int64("memory_bytes_quota_per_query", c.MemoryBytesQuotaPerQuery),
		zap.Int64("max_memory_bytes", c.MaxMemoryBytes),
		zap.Int32("queue_size", c.QueueSize))

	mm := &memoryManager{
		initialBytesQuotaPerQuery: c.InitialMemoryBytesQuotaPerQuery,
		memoryBytesQuotaPerQuery:  c.MemoryBytesQuotaPerQuery,
	}
	if c.MaxMemoryBytes > 0 {
		mm.unusedMemoryBytes = c.MaxMemoryBytes - (int64(c.ConcurrencyQuota) * c.InitialMemoryBytesQuotaPerQuery)
	} else {
		mm.unlimited = true
	}
	queryQueue := make(chan *Query, c.QueueSize)
	if c.ConcurrencyQuota == 0 {
		queryQueue = nil
	}
	ctrl := &Controller{
		config:       c,
		queries:      make(map[QueryID]*Query),
		queryQueue:   queryQueue,
		done:         make(chan struct{}),
		abort:        make(chan struct{}),
		memory:       mm,
		log:          logger,
		metrics:      newControllerMetrics(metricLabelKeys),
		labelKeys:    metricLabelKeys,
		dependencies: c.ExecutorDependencies,
	}
	if c.ConcurrencyQuota != 0 {
		quota := int(c.ConcurrencyQuota)
		ctrl.wg.Add(quota)
		for i := 0; i < quota; i++ {
			go func() {
				defer ctrl.wg.Done()
				ctrl.processQueryQueue()
			}()
		}
	}
	return ctrl, nil
}

// Query satisfies the AsyncQueryService while ensuring the request is propagated on the context.
func (c *Controller) Query(ctx context.Context, req *query.Request) (flux.Query, error) {
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	// Set the request on the context so platform specific Flux operations can retrieve it later.
	ctx = query.ContextWithRequest(ctx, req)
	// Set the org label value for controller metrics
	ctx = context.WithValue(ctx, orgLabel, req.OrganizationID.String()) //lint:ignore SA1029 this is a temporary ignore until we have time to create an appropriate type
	// The controller injects the dependencies for each incoming request.
	for _, dep := range c.dependencies {
		ctx = dep.Inject(ctx)
	}
	// Add per-transformation spans if the feature flag is set.
	if feature.QueryTracing().Enabled(ctx) {
		ctx = flux.WithQueryTracingEnabled(ctx)
	}
	q, err := c.query(ctx, req.Compiler)
	if err != nil {
		return q, err
	}

	return q, nil
}

// query submits a query for execution returning immediately.
// Done must be called on any returned Query objects.
func (c *Controller) query(ctx context.Context, compiler flux.Compiler) (flux.Query, error) {
	q, err := c.createQuery(ctx, compiler.CompilerType())
	if err != nil {
		return nil, handleFluxError(err)
	}

	if err := c.compileQuery(q, compiler); err != nil {
		q.setErr(err)
		c.finish(q)
		c.countQueryRequest(q, labelCompileError)
		return nil, q.Err()
	}
	if err := c.enqueueQuery(q); err != nil {
		q.setErr(err)
		c.finish(q)
		c.countQueryRequest(q, labelQueueError)
		return nil, q.Err()
	}
	return q, nil
}

func (c *Controller) createQuery(ctx context.Context, ct flux.CompilerType) (*Query, error) {
	c.queriesMu.RLock()
	if c.shutdown {
		c.queriesMu.RUnlock()
		return nil, errors.New("query controller shutdown")
	}
	c.queriesMu.RUnlock()

	id := c.nextID()
	labelValues := make([]string, len(c.labelKeys))
	compileLabelValues := make([]string, len(c.labelKeys)+1)
	for i, k := range c.labelKeys {
		value := ctx.Value(k)
		var str string
		switch v := value.(type) {
		case string:
			str = v
		case fmt.Stringer:
			str = v.String()
		}
		labelValues[i] = str
		compileLabelValues[i] = str
	}
	compileLabelValues[len(compileLabelValues)-1] = string(ct)

	cctx, cancel := context.WithCancel(ctx)
	parentSpan, parentCtx := tracing.StartSpanFromContextWithPromMetrics(
		cctx,
		"all",
		c.metrics.allDur.WithLabelValues(labelValues...),
		c.metrics.all.WithLabelValues(labelValues...),
	)
	q := &Query{
		id:                 id,
		labelValues:        labelValues,
		compileLabelValues: compileLabelValues,
		state:              Created,
		c:                  c,
		results:            make(chan flux.Result),
		parentCtx:          parentCtx,
		parentSpan:         parentSpan,
		cancel:             cancel,
		doneCh:             make(chan struct{}),
	}

	// Lock the queries mutex for the rest of this method.
	c.queriesMu.Lock()
	defer c.queriesMu.Unlock()

	if c.shutdown {
		// Query controller was shutdown between when we started
		// creating the query and ending it.
		err := &flux.Error{
			Code: codes.Unavailable,
			Msg:  "query controller shutdown",
		}
		q.setErr(err)
		return nil, err
	}
	c.queries[id] = q
	return q, nil
}

func (c *Controller) nextID() QueryID {
	nextID := atomic.AddUint64(&c.lastID, 1)
	return QueryID(nextID)
}

func (c *Controller) countQueryRequest(q *Query, result requestsLabel) {
	l := len(q.labelValues)
	lvs := make([]string, l+1)
	copy(lvs, q.labelValues)
	lvs[l] = string(result)
	c.metrics.requests.WithLabelValues(lvs...).Inc()
}

func (c *Controller) compileQuery(q *Query, compiler flux.Compiler) (err error) {
	log := c.log.With(influxlogger.TraceFields(q.parentCtx)...)

	defer func() {
		if e := recover(); e != nil {
			var ok bool
			err, ok = e.(error)
			if !ok {
				err = fmt.Errorf("panic: %v", e)
			}
			if entry := log.Check(zapcore.InfoLevel, "panic during compile"); entry != nil {
				entry.Stack = string(debug.Stack())
				entry.Write(zap.Error(err))
			}
		}
	}()

	ctx, ok := q.tryCompile()
	if !ok {
		return &flux.Error{
			Code: codes.Internal,
			Msg:  "failed to transition query to compiling state",
		}
	}

	prog, err := compiler.Compile(ctx, runtime.Default)
	if err != nil {
		return &flux.Error{
			Msg: "compilation failed",
			Err: err,
		}
	}

	if p, ok := prog.(lang.LoggingProgram); ok {
		p.SetLogger(log)
	}

	q.program = prog
	return nil
}

func (c *Controller) enqueueQuery(q *Query) error {
	if _, ok := q.tryQueue(); !ok {
		return &flux.Error{
			Code: codes.Internal,
			Msg:  "failed to transition query to queueing state",
		}
	}

	if c.queryQueue == nil {
		// unlimited queries case
		c.queriesMu.RLock()
		defer c.queriesMu.RUnlock()
		if c.shutdown {
			return &flux.Error{
				Code: codes.Internal,
				Msg:  "controller is shutting down, query not runnable",
			}
		}
		// we can't start shutting down until unlock, so it is safe to add to the waitgroup
		c.wg.Add(1)

		// unlimited queries, so start a goroutine for every query
		go func() {
			defer c.wg.Done()
			c.executeQuery(q)
		}()
	} else {
		select {
		case c.queryQueue <- q:
		default:
			return &flux.Error{
				Code: codes.ResourceExhausted,
				Msg:  "queue length exceeded",
			}
		}
	}

	return nil
}

func (c *Controller) processQueryQueue() {
	for {
		select {
		case <-c.done:
			return
		case q := <-c.queryQueue:
			c.executeQuery(q)
		}
	}
}

// executeQuery will execute a compiled program and wait for its completion.
func (c *Controller) executeQuery(q *Query) {

	defer c.waitForQuery(q)
	defer func() {
		if e := recover(); e != nil {
			var ok bool
			err, ok := e.(error)
			if !ok {
				err = fmt.Errorf("panic: %v", e)
			}
			q.setErr(err)
			if entry := c.log.With(influxlogger.TraceFields(q.parentCtx)...).
				Check(zapcore.InfoLevel, "panic during program start"); entry != nil {
				entry.Stack = string(debug.Stack())
				entry.Write(zap.Error(err))
			}
		}
	}()

	ctx, ok := q.tryExec()
	if !ok {
		// This may happen if the query was cancelled (either because the
		// client cancelled it, or because the controller is shutting down)
		// In the case of cancellation, SetErr() should reset the error to an
		// appropriate message.
		q.setErr(&flux.Error{
			Code: codes.Internal,
			Msg:  "impossible state transition",
		})

		return
	}

	q.c.createAllocator(q)
	// Record unused memory before start.
	q.recordUnusedMemory()
	exec, err := q.program.Start(ctx, q.alloc)
	if err != nil {
		q.setErr(err)
		return
	}
	q.exec = exec
	q.pump(exec, ctx.Done())
}

// waitForQuery will wait until the query is done.
func (c *Controller) waitForQuery(q *Query) {
	select {
	case <-q.doneCh:
	case <-c.done:
	}
}

func (c *Controller) finish(q *Query) {
	c.queriesMu.Lock()
	delete(c.queries, q.id)
	if len(c.queries) == 0 && c.shutdown {
		close(c.done)
	}
	c.queriesMu.Unlock()
}

// Queries reports the active queries.
func (c *Controller) Queries() []*Query {
	c.queriesMu.RLock()
	defer c.queriesMu.RUnlock()
	queries := make([]*Query, 0, len(c.queries))
	for _, q := range c.queries {
		queries = append(queries, q)
	}
	return queries
}

// Shutdown will signal to the Controller that it should not accept any
// new queries and that it should finish executing any existing queries.
// This will return once the Controller's run loop has been exited and all
// queries have been finished or until the Context has been canceled.
func (c *Controller) Shutdown(ctx context.Context) error {
	// Wait for query processing goroutines to finish.
	defer c.wg.Wait()

	// Mark that the controller is shutdown so it does not
	// accept new queries.
	func() {
		c.queriesMu.Lock()
		defer c.queriesMu.Unlock()
		if !c.shutdown {
			c.shutdown = true
			if len(c.queries) == 0 {
				// We hold the lock. No other queries can be spawned.
				// No other queries are waiting to be finished, so we have to
				// close the done channel here instead of in finish(*Query)
				close(c.done)
			}
		}
	}()

	// Cancel all of the currently active queries.
	c.queriesMu.RLock()
	for _, q := range c.queries {
		q.Cancel()
	}
	c.queriesMu.RUnlock()

	// Wait for query processing goroutines to finish.
	defer c.wg.Wait()

	// Wait for all of the queries to be cleaned up or until the
	// context is done.
	select {
	case <-c.done:
		return nil
	case <-ctx.Done():
		c.abortOnce.Do(func() {
			close(c.abort)
		})
		return ctx.Err()
	}
}

// PrometheusCollectors satisfies the prom.PrometheusCollector interface.
func (c *Controller) PrometheusCollectors() []prometheus.Collector {
	collectors := c.metrics.PrometheusCollectors()
	for _, dep := range c.dependencies {
		if pc, ok := dep.(prom.PrometheusCollector); ok {
			collectors = append(collectors, pc.PrometheusCollectors()...)
		}
	}
	return collectors
}

func (c *Controller) GetUnusedMemoryBytes() int64 {
	return c.memory.getUnusedMemoryBytes()
}

func (c *Controller) GetUsedMemoryBytes() int64 {
	return c.config.MaxMemoryBytes - c.GetUnusedMemoryBytes()
}

// Query represents a single request.
type Query struct {
	id QueryID

	labelValues        []string
	compileLabelValues []string

	c *Controller

	// query state. The stateMu protects access for the group below.
	stateMu     sync.RWMutex
	state       State
	err         error
	runtimeErrs []error
	cancel      func()

	parentCtx               context.Context
	parentSpan, currentSpan *tracing.Span
	stats                   flux.Statistics

	done   sync.Once
	doneCh chan struct{}

	program flux.Program
	exec    flux.Query
	results chan flux.Result

	memoryManager *queryMemoryManager
	alloc         *memory.Allocator
}

func (q *Query) ProfilerResults() (flux.ResultIterator, error) {
	p := q.program.(*lang.AstProgram)
	if len(p.Profilers) == 0 {
		return nil, nil
	}
	tables := make([]flux.Table, 0)
	for _, profiler := range p.Profilers {
		if result, err := profiler.GetResult(q, q.alloc); err != nil {
			return nil, err
		} else {
			tables = append(tables, result)
		}
	}
	res := table.NewProfilerResult(tables...)
	return flux.NewSliceResultIterator([]flux.Result{&res}), nil
}

// ID reports an ephemeral unique ID for the query.
func (q *Query) ID() QueryID {
	return q.id
}

// Cancel will stop the query execution.
func (q *Query) Cancel() {
	// Call the cancel function to signal that execution should
	// be interrupted.
	q.cancel()
}

// Results returns a channel that will deliver the query results.
//
// It's possible that the channel is closed before any results arrive.
// In particular, if a query's context or the query itself is canceled,
// the query may close the results channel before any results are computed.
//
// The query may also have an error during execution so the Err()
// function should be used to check if an error happened.
func (q *Query) Results() <-chan flux.Result {
	return q.results
}

func (q *Query) recordUnusedMemory() {
	unused := q.c.GetUnusedMemoryBytes()
	q.c.metrics.memoryUnused.WithLabelValues(q.labelValues...).Set(float64(unused))
}

// Done signals to the Controller that this query is no longer
// being used and resources related to the query may be freed.
func (q *Query) Done() {
	// This must only be invoked once.
	q.done.Do(func() {
		// All done calls should block until the first done call succeeds.
		defer close(q.doneCh)

		// Lock the state mutex and transition to the finished state.
		// Then force the query to cancel to tell it to stop executing.
		// We transition to the new state first so that we do not enter
		// the canceled state at any point (as we have not been canceled).
		q.stateMu.Lock()
		q.transitionTo(Finished)
		q.cancel()
		q.stateMu.Unlock()

		// Ensure that all of the results have been drained.
		// It is ok to read this as the user has already indicated they don't
		// care about the results. When this is closed, it tells us an error has
		// been set or the results have finished being pumped.
		for range q.results {
			// Do nothing with the results.
		}

		// No other goroutines should be modifying state at this point so we
		// can do things that would be unsafe in another context.
		if q.exec != nil {
			// Mark the program as being done and copy out the error if it exists.
			q.exec.Done()
			if q.err == nil {
				// TODO(jsternberg): The underlying program never returns
				// this so maybe their interface should change?
				q.err = q.exec.Err()
			}
			// Merge the metadata from the program into the controller stats.
			stats := q.exec.Statistics()
			q.stats.Metadata = stats.Metadata
		}

		// Retrieve the runtime errors that have been accumulated.
		errMsgs := make([]string, 0, len(q.runtimeErrs))
		for _, e := range q.runtimeErrs {
			errMsgs = append(errMsgs, e.Error())
		}
		q.stats.RuntimeErrors = errMsgs

		// Mark the query as finished so it is removed from the query map.
		q.c.finish(q)

		// Release the additional memory associated with this query.
		if q.memoryManager != nil {
			q.memoryManager.Release()
			// Record unused memory after finish.
			q.recordUnusedMemory()
		}

		// Count query request.
		if q.err != nil || len(q.runtimeErrs) > 0 {
			q.c.countQueryRequest(q, labelRuntimeError)
		} else {
			q.c.countQueryRequest(q, labelSuccess)
		}

	})
	<-q.doneCh
}

// Statistics reports the statistics for the query.
//
// This method must be called after Done. It will block until
// the query has been finalized unless a context is given.
func (q *Query) Statistics() flux.Statistics {
	stats := q.stats
	if q.alloc != nil {
		stats.MaxAllocated = q.alloc.MaxAllocated()
	}
	return stats
}

// State reports the current state of the query.
func (q *Query) State() State {
	q.stateMu.RLock()
	state := q.state
	if !isFinishedState(state) {
		// If the query is a non-finished state, check the
		// context to see if we have been interrupted.
		select {
		case <-q.parentCtx.Done():
			// The query has been canceled so report to the
			// outside world that we have been canceled.
			// Do NOT attempt to change the internal state
			// variable here. It is a minefield. Leave the
			// normal query execution to figure that out.
			state = Canceled
		default:
			// The context has not been canceled.
		}
	}
	q.stateMu.RUnlock()
	return state
}

// transitionTo will transition from one state to another. If a list of current states
// is given, then the query must be in one of those states for the transition to succeed.
// This method must be called with a lock and it must be called from within the run loop.
func (q *Query) transitionTo(newState State, currentState ...State) (context.Context, bool) {
	// If we are transitioning to a non-finished state, the query
	// may have been canceled. If the query was canceled, then
	// we need to transition to the canceled state
	if !isFinishedState(newState) {
		select {
		case <-q.parentCtx.Done():
			// Transition to the canceled state and report that
			// we failed to transition to the desired state.
			_, _ = q.transitionTo(Canceled)
			return nil, false
		default:
		}
	}

	if len(currentState) > 0 {
		// Find the current state in the list of current states.
		for _, st := range currentState {
			if q.state == st {
				goto TRANSITION
			}
		}
		return nil, false
	}

TRANSITION:
	// We are transitioning to a new state. Close the current span (if it exists).
	if q.currentSpan != nil {
		q.currentSpan.Finish()
		switch q.state {
		case Compiling:
			q.stats.CompileDuration += q.currentSpan.Duration
		case Queueing:
			q.stats.QueueDuration += q.currentSpan.Duration
		case Executing:
			q.stats.ExecuteDuration += q.currentSpan.Duration
		}
	}
	q.currentSpan = nil

	if isFinishedState(newState) {
		// Invoke the cancel function to ensure that we have signaled that the query should be done.
		// The user is supposed to read the entirety of the tables returned before we end up in a finished
		// state, but user error may have caused this not to happen so there's no harm to canceling multiple
		// times.
		q.cancel()

		// If we are transitioning to a finished state from a non-finished state, finish the parent span.
		if q.parentSpan != nil {
			q.parentSpan.Finish()
			q.stats.TotalDuration = q.parentSpan.Duration
			q.parentSpan = nil
		}
	}

	// Transition to the new state.
	q.state = newState

	// Start a new span and set a new context.
	var (
		dur         *prometheus.HistogramVec
		gauge       *prometheus.GaugeVec
		labelValues = q.labelValues
	)
	switch newState {
	case Compiling:
		dur, gauge = q.c.metrics.compilingDur, q.c.metrics.compiling
		labelValues = q.compileLabelValues
	case Queueing:
		dur, gauge = q.c.metrics.queueingDur, q.c.metrics.queueing
	case Executing:
		dur, gauge = q.c.metrics.executingDur, q.c.metrics.executing
	default:
		// This state is not tracked so do not create a new span or context for it.
		// Use the parent context if one is needed.
		return q.parentCtx, true
	}
	var currentCtx context.Context
	q.currentSpan, currentCtx = tracing.StartSpanFromContextWithPromMetrics(
		q.parentCtx,
		newState.String(),
		dur.WithLabelValues(labelValues...),
		gauge.WithLabelValues(labelValues...),
	)
	return currentCtx, true
}

// Err reports any error the query may have encountered.
func (q *Query) Err() error {
	q.stateMu.Lock()
	err := q.err
	q.stateMu.Unlock()
	return handleFluxError(err)
}

// setErr marks this query with an error. If the query was
// canceled, then the error is ignored.
//
// This will mark the query as ready so setResults must not
// be called if this method is invoked.
func (q *Query) setErr(err error) {
	q.stateMu.Lock()
	defer q.stateMu.Unlock()

	// We may have this get called when the query is canceled.
	// If that is the case, transition to the canceled state
	// instead and record the error from that since the error
	// we received is probably wrong.
	select {
	case <-q.parentCtx.Done():
		q.transitionTo(Canceled)
		err = q.parentCtx.Err()
	default:
		q.transitionTo(Errored)
	}
	q.err = err

	// Close the ready channel to report that no results
	// will be sent.
	close(q.results)
}

func (q *Query) addRuntimeError(e error) {
	q.stateMu.Lock()
	defer q.stateMu.Unlock()

	q.runtimeErrs = append(q.runtimeErrs, e)
}

// pump will read from the executing query results and pump the
// results to our destination.
// When there are no more results, then this will close our own
// results channel.
func (q *Query) pump(exec flux.Query, done <-chan struct{}) {
	defer close(q.results)

	// When our context is canceled, we need to propagate that cancel
	// signal down to the executing program just in case it is waiting
	// for a cancel signal and is ignoring the passed in context.
	// We want this signal to only be sent once and we want to continue
	// draining the results until the underlying program has actually
	// been finished so we copy this to a new channel and set it to
	// nil when it has been closed.
	signalCh := done
	for {
		select {
		case res, ok := <-exec.Results():
			if !ok {
				return
			}

			// It is possible for the underlying query to misbehave.
			// We have to continue pumping results even if this is the
			// case, but if the query has been canceled or finished with
			// done, nobody is going to read these values so we need
			// to avoid blocking.
			ecr := &errorCollectingResult{
				Result: res,
				q:      q,
			}
			select {
			case <-done:
			case q.results <- ecr:
			}
		case <-signalCh:
			// Signal to the underlying executor that the query
			// has been canceled. Usually, the signal on the context
			// is likely enough, but this explicitly signals just in case.
			exec.Cancel()

			// Set the done channel to nil so we don't do this again
			// and we continue to drain the results.
			signalCh = nil
		case <-q.c.abort:
			// If we get here, then any running queries should have been cancelled
			// in controller.Shutdown().
			return
		}
	}
}

// tryCompile attempts to transition the query into the Compiling state.
func (q *Query) tryCompile() (context.Context, bool) {
	q.stateMu.Lock()
	defer q.stateMu.Unlock()

	return q.transitionTo(Compiling, Created)
}

// tryQueue attempts to transition the query into the Queueing state.
func (q *Query) tryQueue() (context.Context, bool) {
	q.stateMu.Lock()
	defer q.stateMu.Unlock()

	return q.transitionTo(Queueing, Compiling)
}

// tryExec attempts to transition the query into the Executing state.
func (q *Query) tryExec() (context.Context, bool) {
	q.stateMu.Lock()
	defer q.stateMu.Unlock()

	return q.transitionTo(Executing, Queueing)
}

type errorCollectingResult struct {
	flux.Result
	q *Query
}

func (r *errorCollectingResult) Tables() flux.TableIterator {
	return &errorCollectingTableIterator{
		TableIterator: r.Result.Tables(),
		q:             r.q,
	}
}

type errorCollectingTableIterator struct {
	flux.TableIterator
	q *Query
}

func (ti *errorCollectingTableIterator) Do(f func(t flux.Table) error) error {
	err := ti.TableIterator.Do(f)
	if err != nil {
		err = handleFluxError(err)
		ti.q.addRuntimeError(err)
	}
	return err
}

// State is the query state.
type State int

const (
	// Created indicates the query has been created.
	Created State = iota

	// Compiling indicates that the query is in the process
	// of executing the compiler associated with the query.
	Compiling

	// Queueing indicates the query is waiting inside of the
	// scheduler to be executed.
	Queueing

	// Executing indicates that the query is currently executing.
	Executing

	// Errored indicates that there was an error when attempting
	// to execute a query within any state inside of the controller.
	Errored

	// Finished indicates that the query has been marked as Done
	// and it is awaiting removal from the Controller or has already
	// been removed.
	Finished

	// Canceled indicates that the query was signaled to be
	// canceled. A canceled query must still be released with Done.
	Canceled
)

func (s State) String() string {
	switch s {
	case Created:
		return "created"
	case Compiling:
		return "compiling"
	case Queueing:
		return "queueing"
	case Executing:
		return "executing"
	case Errored:
		return "errored"
	case Finished:
		return "finished"
	case Canceled:
		return "canceled"
	default:
		return "unknown"
	}
}

func isFinishedState(state State) bool {
	switch state {
	case Canceled, Errored, Finished:
		return true
	default:
		return false
	}
}

// handleFluxError will take a flux.Error and convert it into an influxdb.Error.
// It will match certain codes to the equivalent in influxdb.
//
// If the error is any other type of error, it will return the error untouched.
//
// TODO(jsternberg): This likely becomes a public function, but this is just an initial
// implementation so playing it safe by making it package local for now.
func handleFluxError(err error) error {
	ferr, ok := err.(*flux.Error)
	if !ok {
		return err
	}
	werr := handleFluxError(ferr.Err)

	code := errors2.EInternal
	switch ferr.Code {
	case codes.Inherit:
		// If we are inheriting the error code, influxdb doesn't
		// have an equivalent of this so we need to retrieve
		// the error code from the wrapped error which has already
		// been translated to an influxdb error (if possible).
		if werr != nil {
			code = errors2.ErrorCode(werr)
		}
	case codes.NotFound:
		code = errors2.ENotFound
	case codes.Invalid:
		code = errors2.EInvalid
	// These don't really map correctly, but we want
	// them to show up as 4XX so until influxdb error
	// codes are updated for more types of failures,
	// mapping these to invalid.
	case codes.Canceled,
		codes.ResourceExhausted,
		codes.FailedPrecondition,
		codes.Aborted,
		codes.OutOfRange,
		codes.Unimplemented:
		code = errors2.EInvalid
	case codes.PermissionDenied:
		code = errors2.EForbidden
	case codes.Unauthenticated:
		code = errors2.EUnauthorized
	default:
		// Everything else is treated as an internal error
		// which is set above.
	}
	return &errors2.Error{
		Code: code,
		Msg:  ferr.Msg,
		Err:  werr,
	}
}

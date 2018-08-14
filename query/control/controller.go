// Package control controls which resources a query may consume.
//
// The Controller manages the resources available to each query and ensures
// an optimal use of those resources to execute queries in a timely manner.
// The controller also maintains the state of a query as it goes through the
// various stages of execution and is responsible for killing currently
// executing queries when requested by the user.
//
// The Controller manages when a query is executed. This can be based on
// anything within the query's requested resources. For example, a basic
// implementation of the Controller may decide to execute anything with a high
// priority before anything with a low priority.  The implementation of the
// Controller will vary and change over time and this package may provide
// multiple implementations for different controller algorithms.
//
// During execution, the Controller manages the resources used by the query and
// provides observabiility into what resources are being used and by which
// queries. The Controller also imposes limitations so a query that uses more
// than its allocated resources or more resources than available on the system
// will be aborted.
package control

import (
	"context"
	"fmt"
	"log"
	"math"
	"sync"
	"time"

	"github.com/influxdata/platform"
	"github.com/influxdata/platform/query"
	"github.com/influxdata/platform/query/execute"
	"github.com/influxdata/platform/query/plan"
	opentracing "github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
)

// Controller provides a central location to manage all incoming queries.
// The controller is responsible for queueing, planning, and executing queries.
type Controller struct {
	newQueries    chan *Query
	lastID        QueryID
	queriesMu     sync.RWMutex
	queries       map[QueryID]*Query
	queryDone     chan *Query
	cancelRequest chan QueryID

	metrics *controllerMetrics

	verbose bool

	lplanner plan.LogicalPlanner
	pplanner plan.Planner
	executor execute.Executor

	maxConcurrency       int
	availableConcurrency int
	availableMemory      int64
}

type Config struct {
	ConcurrencyQuota     int
	MemoryBytesQuota     int64
	ExecutorDependencies execute.Dependencies
	Verbose              bool
}

type QueryID uint64

func New(c Config) *Controller {
	ctrl := &Controller{
		newQueries:           make(chan *Query),
		queries:              make(map[QueryID]*Query),
		queryDone:            make(chan *Query),
		cancelRequest:        make(chan QueryID),
		maxConcurrency:       c.ConcurrencyQuota,
		availableConcurrency: c.ConcurrencyQuota,
		availableMemory:      c.MemoryBytesQuota,
		lplanner:             plan.NewLogicalPlanner(),
		pplanner:             plan.NewPlanner(),
		executor:             execute.NewExecutor(c.ExecutorDependencies),
		metrics:              newControllerMetrics(),
		verbose:              c.Verbose,
	}
	go ctrl.run()
	return ctrl
}

// Query submits a query for execution returning immediately.
// Done must be called on any returned Query objects.
func (c *Controller) Query(ctx context.Context, req *query.Request) (query.Query, error) {
	q := c.createQuery(ctx, req.OrganizationID)
	if err := c.compileQuery(q, req.Compiler); err != nil {
		q.parentSpan.Finish()
		return nil, err
	}
	if err := c.enqueueQuery(q); err != nil {
		q.parentSpan.Finish()
		return nil, err
	}
	return q, nil
}

func (c *Controller) createQuery(ctx context.Context, orgID platform.ID) *Query {
	id := c.nextID()
	labelValues := []string{
		orgID.String(),
	}
	cctx, cancel := context.WithCancel(ctx)
	parentSpan, parentCtx := StartSpanFromContext(
		cctx,
		"all",
		c.metrics.allDur.WithLabelValues(labelValues...),
		c.metrics.all.WithLabelValues(labelValues...),
	)
	ready := make(chan map[string]query.Result, 1)
	return &Query{
		id:          id,
		orgID:       orgID,
		labelValues: labelValues,
		state:       Created,
		c:           c,
		now:         time.Now().UTC(),
		ready:       ready,
		parentCtx:   parentCtx,
		parentSpan:  parentSpan,
		cancel:      cancel,
	}
}

func (c *Controller) compileQuery(q *Query, compiler query.Compiler) error {
	if !q.tryCompile() {
		return errors.New("failed to transition query to compiling state")
	}
	spec, err := compiler.Compile(q.compilingCtx)
	if err != nil {
		return errors.Wrap(err, "failed to compile query")
	}

	// Incoming query spec may have been produced by an entity other than the
	// Flux interpreter, so we must set the default Now time if not already set.
	if spec.Now.IsZero() {
		spec.Now = q.now
	}

	q.spec = *spec
	return nil
}

func (c *Controller) enqueueQuery(q *Query) error {
	if c.verbose {
		log.Println("query", query.Formatted(&q.spec, query.FmtJSON))
	}
	if !q.tryQueue() {
		return errors.New("failed to transition query to queueing state")
	}
	if err := q.spec.Validate(); err != nil {
		return errors.Wrap(err, "invalid query")
	}
	// Add query to the queue
	select {
	case c.newQueries <- q:
		return nil
	case <-q.parentCtx.Done():
		return q.parentCtx.Err()
	}
}

func (c *Controller) nextID() QueryID {
	c.queriesMu.Lock()
	defer c.queriesMu.Unlock()
	ok := true
	for ok {
		c.lastID++
		_, ok = c.queries[c.lastID]
	}
	return c.lastID
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

func (c *Controller) run() {
	pq := newPriorityQueue()
	for {
		select {
		// Wait for resources to free
		case q := <-c.queryDone:
			c.free(q)
			c.queriesMu.Lock()
			delete(c.queries, q.id)
			c.queriesMu.Unlock()
		// Wait for new queries
		case q := <-c.newQueries:
			pq.Push(q)
			c.queriesMu.Lock()
			c.queries[q.id] = q
			c.queriesMu.Unlock()
		// Wait for cancel query requests
		case id := <-c.cancelRequest:
			c.queriesMu.RLock()
			q := c.queries[id]
			c.queriesMu.RUnlock()
			q.Cancel()
		}

		// Peek at head of priority queue
		q := pq.Peek()
		if q != nil {
			pop, err := c.processQuery(q)
			if pop {
				pq.Pop()
			}
			if err != nil {
				go q.setErr(err)
			}
		}
	}
}

// processQuery move the query through the state machine and returns and errors and if the query should be popped.
func (c *Controller) processQuery(q *Query) (pop bool, err error) {
	defer func() {
		if e := recover(); e != nil {
			// If a query panicked, always pop it from the queue so we don't
			// try to reprocess it.
			pop = true

			// Update the error with information about the query if this is an
			// error type and create an error if it isn't.
			switch e := e.(type) {
			case error:
				err = errors.Wrap(e, "panic")
			default:
				err = fmt.Errorf("panic: %s", e)
			}
		}
	}()

	if q.tryPlan() {
		// Plan query to determine needed resources
		lp, err := c.lplanner.Plan(&q.spec)
		if err != nil {
			return true, errors.Wrap(err, "failed to create logical plan")
		}
		if c.verbose {
			log.Println("logical plan", plan.Formatted(lp))
		}

		p, err := c.pplanner.Plan(lp, nil)
		if err != nil {
			return true, errors.Wrap(err, "failed to create physical plan")
		}
		q.plan = p
		q.concurrency = p.Resources.ConcurrencyQuota
		if q.concurrency > c.maxConcurrency {
			q.concurrency = c.maxConcurrency
		}
		q.memory = p.Resources.MemoryBytesQuota
		if c.verbose {
			log.Println("physical plan", plan.Formatted(q.plan))
		}
	}

	// Check if we have enough resources
	if c.check(q) {
		// Update resource gauges
		c.consume(q)

		// Remove the query from the queue
		pop = true

		// Execute query
		if !q.tryExec() {
			return true, errors.New("failed to transition query into executing state")
		}
		q.alloc = new(execute.Allocator)
		r, err := c.executor.Execute(q.executeCtx, q.orgID, q.plan, q.alloc)
		if err != nil {
			return true, errors.Wrap(err, "failed to execute query")
		}
		q.setResults(r)
	} else {
		// update state to queueing
		if !q.tryRequeue() {
			return true, errors.New("failed to transition query into requeueing state")
		}
	}
	return pop, nil
}

func (c *Controller) check(q *Query) bool {
	return c.availableConcurrency >= q.concurrency && (q.memory == math.MaxInt64 || c.availableMemory >= q.memory)
}
func (c *Controller) consume(q *Query) {
	c.availableConcurrency -= q.concurrency

	if q.memory != math.MaxInt64 {
		c.availableMemory -= q.memory
	}
}

func (c *Controller) free(q *Query) {
	c.availableConcurrency += q.concurrency

	if q.memory != math.MaxInt64 {
		c.availableMemory += q.memory
	}
}

// PrometheusCollectors satisifies the prom.PrometheusCollector interface.
func (c *Controller) PrometheusCollectors() []prometheus.Collector {
	return c.metrics.PrometheusCollectors()
}

// Query represents a single request.
type Query struct {
	id QueryID

	orgID platform.ID

	labelValues []string

	c *Controller

	spec query.Spec
	now  time.Time

	err error

	ready chan map[string]query.Result

	mu       sync.Mutex
	state    State
	cancel   func()
	canceled bool

	parentCtx,
	compilingCtx,
	queueCtx,
	planCtx,
	requeueCtx,
	executeCtx context.Context

	parentSpan,
	compileSpan,
	queueSpan,
	planSpan,
	requeueSpan,
	executeSpan *span

	plan *plan.PlanSpec

	concurrency int
	memory      int64

	alloc *execute.Allocator
}

// ID reports an ephemeral unique ID for the query.
func (q *Query) ID() QueryID {
	return q.id
}

func (q *Query) OrganizationID() platform.ID {
	return q.orgID
}

func (q *Query) Spec() *query.Spec {
	return &q.spec
}

// Concurrency reports the number of goroutines allowed to process the request.
func (q *Query) Concurrency() int {
	return q.concurrency
}

// Cancel will stop the query execution.
func (q *Query) Cancel() {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.canceled {
		// We have already been canceled
		return
	}
	q.canceled = true

	// call cancel func
	q.cancel()

	// Finish the query immediately.
	// This allows for receiving from the Ready channel in the same goroutine
	// that has called defer q.Done()
	q.finish()

	if q.state != Errored {
		q.state = Canceled
	}
}

// Ready returns a channel that will deliver the query results.
// Its possible that the channel is closed before any results arrive, in which case the query should be
// inspected for an error using Err().
func (q *Query) Ready() <-chan map[string]query.Result {
	return q.ready
}

// finish informs the controller and the Ready channel that the query is finished.
func (q *Query) finish() {
	switch q.state {
	case Compiling:
		q.compileSpan.Finish()
	case Queueing:
		q.queueSpan.Finish()
	case Planning:
		q.planSpan.Finish()
	case Requeueing:
		q.requeueSpan.Finish()
	case Executing:
		q.executeSpan.Finish()
	case Errored:
		// The query has already been finished in the call to setErr.
		return
	case Canceled:
		// The query has already been finished in the call to Cancel.
		return
	case Finished:
		// The query has already finished
		return
	default:
		panic("unreachable, all states have been accounted for")
	}

	q.parentSpan.Finish()
	q.c.queryDone <- q
	close(q.ready)
}

// Done must always be called to free resources.
func (q *Query) Done() {
	q.mu.Lock()
	defer q.mu.Unlock()

	q.finish()

	q.state = Finished
}

// Statistics reports the statisitcs for the query.
// The statisitcs are not complete until the query is finished.
func (q *Query) Statistics() query.Statistics {
	q.mu.Lock()
	defer q.mu.Unlock()

	stats := query.Statistics{}
	stats.TotalDuration = q.parentSpan.Duration
	if q.compileSpan != nil {
		stats.CompileDuration = q.compileSpan.Duration
	}
	if q.queueSpan != nil {
		stats.QueueDuration = q.queueSpan.Duration
	}
	if q.planSpan != nil {
		stats.PlanDuration = q.planSpan.Duration
	}
	if q.requeueSpan != nil {
		stats.RequeueDuration = q.requeueSpan.Duration
	}
	if q.executeSpan != nil {
		stats.ExecuteDuration = q.executeSpan.Duration
	}
	stats.Concurrency = q.concurrency
	if q.alloc != nil {
		stats.MaxAllocated = q.alloc.Max()
	}
	return stats
}

// State reports the current state of the query.
func (q *Query) State() State {
	q.mu.Lock()
	s := q.state
	q.mu.Unlock()
	return s
}

func (q *Query) isOK() bool {
	q.mu.Lock()
	ok := q.state != Canceled && q.state != Errored && q.state != Finished
	q.mu.Unlock()
	return ok
}

// Err reports any error the query may have encountered.
func (q *Query) Err() error {
	q.mu.Lock()
	err := q.err
	q.mu.Unlock()
	return err
}
func (q *Query) setErr(err error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.err = err

	// Finish the query immediately.
	// This allows for receiving from the Ready channel in the same goroutine
	// that has called defer q.Done()
	q.finish()

	q.state = Errored
}

func (q *Query) setResults(r map[string]query.Result) {
	q.mu.Lock()
	if q.state == Executing {
		q.ready <- r
	}
	q.mu.Unlock()
}

// tryCompile attempts to transition the query into the Compiling state.
func (q *Query) tryCompile() bool {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.state == Created {
		q.compileSpan, q.compilingCtx = StartSpanFromContext(
			q.parentCtx,
			"compiling",
			q.c.metrics.compilingDur.WithLabelValues(q.labelValues...),
			q.c.metrics.compiling.WithLabelValues(q.labelValues...),
		)

		q.state = Compiling
		return true
	}
	return false
}

// tryQueue attempts to transition the query into the Queueing state.
func (q *Query) tryQueue() bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.state == Compiling || q.state == Created {
		if q.state == Compiling {
			q.compileSpan.Finish()
		}
		q.queueSpan, q.queueCtx = StartSpanFromContext(
			q.parentCtx,
			"queueing",
			q.c.metrics.queueingDur.WithLabelValues(q.labelValues...),
			q.c.metrics.queueing.WithLabelValues(q.labelValues...),
		)

		q.state = Queueing
		return true
	}
	return false
}

// tryRequeue attempts to transition the query into the Requeueing state.
func (q *Query) tryRequeue() bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.state == Planning {
		q.planSpan.Finish()

		q.requeueSpan, q.requeueCtx = StartSpanFromContext(
			q.parentCtx,
			"requeueing",
			q.c.metrics.requeueingDur.WithLabelValues(q.labelValues...),
			q.c.metrics.requeueing.WithLabelValues(q.labelValues...),
		)

		q.state = Requeueing
		return true
	}
	return false
}

// tryPlan attempts to transition the query into the Planning state.
func (q *Query) tryPlan() bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.state == Queueing {
		q.queueSpan.Finish()

		q.planSpan, q.planCtx = StartSpanFromContext(
			q.parentCtx,
			"planning",
			q.c.metrics.planningDur.WithLabelValues(q.labelValues...),
			q.c.metrics.planning.WithLabelValues(q.labelValues...),
		)

		q.state = Planning
		return true
	}
	return false
}

// tryExec attempts to transition the query into the Executing state.
func (q *Query) tryExec() bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.state == Requeueing || q.state == Planning {
		switch q.state {
		case Requeueing:
			q.requeueSpan.Finish()
		case Planning:
			q.planSpan.Finish()
		}

		q.executeSpan, q.executeCtx = StartSpanFromContext(
			q.parentCtx,
			"executing",
			q.c.metrics.executingDur.WithLabelValues(q.labelValues...),
			q.c.metrics.executing.WithLabelValues(q.labelValues...),
		)

		q.state = Executing
		return true
	}
	return false
}

// State is the query state.
type State int

const (
	Created State = iota
	Compiling
	Queueing
	Planning
	Requeueing
	Executing
	Errored
	Finished
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
	case Planning:
		return "planning"
	case Requeueing:
		return "requeing"
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

// span is a simple wrapper around opentracing.Span in order to
// get access to the duration of the span for metrics reporting.
type span struct {
	s        opentracing.Span
	start    time.Time
	Duration time.Duration
	hist     prometheus.Observer
	gauge    prometheus.Gauge
}

func StartSpanFromContext(ctx context.Context, operationName string, hist prometheus.Observer, gauge prometheus.Gauge) (*span, context.Context) {
	start := time.Now()
	s, sctx := opentracing.StartSpanFromContext(ctx, operationName, opentracing.StartTime(start))
	gauge.Inc()
	return &span{
		s:     s,
		start: start,
		hist:  hist,
		gauge: gauge,
	}, sctx
}

func (s *span) Finish() {
	finish := time.Now()
	s.Duration = finish.Sub(s.start)
	s.s.FinishWithOptions(opentracing.FinishOptions{
		FinishTime: finish,
	})
	s.hist.Observe(s.Duration.Seconds())
	s.gauge.Dec()
}

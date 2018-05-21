package control

import (
	"context"
	"log"
	"math"
	"sync"
	"time"

	"github.com/influxdata/platform/query/id"
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
		verbose:              c.Verbose,
	}
	go ctrl.run()
	return ctrl
}

// QueryWithCompile submits a query for execution returning immediately.
// The query will first be compiled before submitting for execution.
// Done must be called on any returned Query objects.
func (c *Controller) QueryWithCompile(ctx context.Context, orgID id.ID, queryStr string) (*Query, error) {
	q := c.createQuery(ctx, orgID)
	err := c.compileQuery(q, queryStr)
	if err != nil {
		return nil, err
	}
	err = c.enqueueQuery(q)
	return q, err
}

// Query submits a query for execution returning immediately.
// The spec must not be modified while the query is still active.
// Done must be called on any returned Query objects.
func (c *Controller) Query(ctx context.Context, orgID id.ID, qSpec *query.Spec) (*Query, error) {
	q := c.createQuery(ctx, orgID)
	q.spec = *qSpec
	err := c.enqueueQuery(q)
	return q, err
}

func (c *Controller) createQuery(ctx context.Context, orgID id.ID) *Query {
	id := c.nextID()
	cctx, cancel := context.WithCancel(ctx)
	ready := make(chan map[string]execute.Result, 1)
	return &Query{
		id:    id,
		orgID: orgID,
		labelValues: []string{
			orgID.String(),
		},
		state:     Created,
		c:         c,
		now:       time.Now().UTC(),
		ready:     ready,
		parentCtx: cctx,
		cancel:    cancel,
	}
}

func (c *Controller) compileQuery(q *Query, queryStr string) error {
	if !q.tryCompile() {
		return errors.New("failed to transition query to compiling state")
	}
	spec, err := query.Compile(q.compilingCtx, queryStr, query.Verbose(c.verbose))
	if err != nil {
		return errors.Wrap(err, "failed to compile query")
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
	c.newQueries <- q
	return nil
}

func (c *Controller) nextID() QueryID {
	c.queriesMu.RLock()
	defer c.queriesMu.RUnlock()
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
			err := c.processQuery(pq, q)
			if err != nil {
				go q.setErr(err)
			}
		}
	}
}

func (c *Controller) processQuery(pq *PriorityQueue, q *Query) error {
	if q.tryPlan() {
		// Plan query to determine needed resources
		lp, err := c.lplanner.Plan(&q.spec)
		if err != nil {
			return errors.Wrap(err, "failed to create logical plan")
		}
		if c.verbose {
			log.Println("logical plan", plan.Formatted(lp))
		}

		p, err := c.pplanner.Plan(lp, nil, q.now)
		if err != nil {
			return errors.Wrap(err, "failed to create physical plan")
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
		pq.Pop()

		// Execute query
		if !q.tryExec() {
			return errors.New("failed to transition query into executing state")
		}
		r, err := c.executor.Execute(q.executeCtx, q.orgID, q.plan)
		if err != nil {
			return errors.Wrap(err, "failed to execute query")
		}
		q.setResults(r)
	} else {
		// update state to queueing
		if !q.tryRequeue() {
			return errors.New("failed to transition query into requeueing state")
		}
	}
	return nil
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

// Query represents a single request.
type Query struct {
	id QueryID

	orgID id.ID

	labelValues []string

	c *Controller

	spec query.Spec
	now  time.Time

	err error

	ready chan map[string]execute.Result

	mu     sync.Mutex
	state  State
	cancel func()

	parentCtx,
	compilingCtx,
	queueCtx,
	planCtx,
	requeueCtx,
	executeCtx context.Context

	compileSpan,
	queueSpan,
	planSpan,
	requeueSpan,
	executeSpan *span

	plan *plan.PlanSpec

	concurrency int
	memory      int64
}

// ID reports an ephemeral unique ID for the query.
func (q *Query) ID() QueryID {
	return q.id
}

func (q *Query) OrganizationID() id.ID {
	return q.orgID
}

func (q *Query) Spec() *query.Spec {
	return &q.spec
}

// Cancel will stop the query execution.
func (q *Query) Cancel() {
	q.mu.Lock()
	defer q.mu.Unlock()

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
func (q *Query) Ready() <-chan map[string]execute.Result {
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

// State reports the current state of the query.
func (q *Query) State() State {
	q.mu.Lock()
	s := q.state
	q.mu.Unlock()
	return s
}

func (q *Query) isOK() bool {
	q.mu.Lock()
	ok := q.state != Canceled && q.state != Errored
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

func (q *Query) setResults(r map[string]execute.Result) {
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
			compilingHist.WithLabelValues(q.labelValues...),
			compilingGauge.WithLabelValues(q.labelValues...),
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
			queueingHist.WithLabelValues(q.labelValues...),
			queueingGauge.WithLabelValues(q.labelValues...),
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
			requeueingHist.WithLabelValues(q.labelValues...),
			requeueingGauge.WithLabelValues(q.labelValues...),
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
			planningHist.WithLabelValues(q.labelValues...),
			planningGauge.WithLabelValues(q.labelValues...),
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
			executingHist.WithLabelValues(q.labelValues...),
			executingGauge.WithLabelValues(q.labelValues...),
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

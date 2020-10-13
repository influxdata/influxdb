package control

import (
	"context"
	"time"

	"github.com/influxdata/flux"
	"github.com/influxdata/flux/lang"
	"github.com/influxdata/flux/memory"
	"github.com/influxdata/influxdb/coordinator"
	"github.com/influxdata/influxdb/flux/builtin"
	"github.com/influxdata/influxdb/flux/stdlib/influxdata/influxdb"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

type MetaClient = coordinator.MetaClient
type Authorizer = influxdb.Authorizer

// query is a wrapper that lets us accumulate statistics about
// how long it took to compile and execute a query.
type query struct {
	flux.Query

	// The time that this query was requested by invoking Controller.Query
	requestStart time.Time

	// The duration of compiling this query
	compileDuration time.Duration

	// The time this query began execution
	execStart time.Time

	stats flux.Statistics
}

func (q *query) Done() {
	q.Query.Done()
	q.stats = q.Query.Statistics()
	q.stats.CompileDuration = q.compileDuration
	q.stats.ExecuteDuration = time.Since(q.execStart)
	q.stats.TotalDuration = time.Since(q.requestStart)
}

func (q *query) Statistics() flux.Statistics {
	return q.stats
}

// program is a wrapper that lets us return a wrapped flux.Query
// that let's us accumulate statistics about how long it took to
// compile and execute a query.
type program struct {
	flux.Program
	requestStart    time.Time
	compileDuration time.Duration
}

func (p *program) Start(ctx context.Context, allocator *memory.Allocator) (flux.Query, error) {
	start := time.Now()
	q, err := p.Program.Start(ctx, allocator)
	if err != nil {
		return nil, err
	}

	return &query{
		Query:           q,
		requestStart:    p.requestStart,
		compileDuration: p.compileDuration,
		execStart:       start,
	}, nil
}

func NewController(mc MetaClient, reader influxdb.Reader, auth Authorizer, authEnabled bool, logger *zap.Logger) *Controller {
	builtin.Initialize()

	storageDeps, err := influxdb.NewDependencies(mc, reader, auth, authEnabled)
	if err != nil {
		panic(err)
	}

	return &Controller{
		deps:   []flux.Dependency{storageDeps},
		logger: logger,
	}
}

type Controller struct {
	deps   []flux.Dependency
	logger *zap.Logger
}

func (c *Controller) Query(ctx context.Context, compiler flux.Compiler) (flux.Query, error) {
	requestStart := time.Now()
	for _, dep := range c.deps {
		ctx = dep.Inject(ctx)
	}

	p, err := c.compile(ctx, compiler, requestStart)
	if err != nil {
		return nil, err
	}

	alloc := &memory.Allocator{}
	return p.Start(ctx, alloc)
}

func (c *Controller) compile(ctx context.Context, compiler flux.Compiler, requestStart time.Time) (flux.Program, error) {
	start := time.Now()
	p, err := compiler.Compile(ctx)
	if err != nil {
		return nil, err
	}
	p = &program{
		Program:         p,
		requestStart:    requestStart,
		compileDuration: time.Since(start),
	}

	if p, ok := p.(lang.LoggingProgram); ok {
		p.SetLogger(c.logger)
	}
	return p, nil
}

func (c *Controller) PrometheusCollectors() []prometheus.Collector {
	return nil
}

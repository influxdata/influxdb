package query

import (
	"context"
	"errors"
	"fmt"
	"os"
	"runtime/debug"
	"strconv"
	"time"

	iql "github.com/influxdata/influxdb/v2/influxql"
	"github.com/influxdata/influxdb/v2/influxql/control"
	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/kit/tracing"
	"github.com/influxdata/influxdb/v2/models"
	"github.com/influxdata/influxql"
	"github.com/opentracing/opentracing-go/log"
	"go.uber.org/zap"
)

var (
	// ErrInvalidQuery is returned when executing an unknown query type.
	ErrInvalidQuery = errors.New("invalid query")

	// ErrNotExecuted is returned when a statement is not executed in a query.
	// This can occur when a previous statement in the same query has errored.
	ErrNotExecuted = errors.New("not executed")

	// ErrQueryInterrupted is an error returned when the query is interrupted.
	ErrQueryInterrupted = errors.New("query interrupted")
)

const (
	// PanicCrashEnv is the environment variable that, when set, will prevent
	// the handler from recovering any panics.
	PanicCrashEnv = "INFLUXDB_PANIC_CRASH"
)

// ErrDatabaseNotFound returns a database not found error for the given database name.
func ErrDatabaseNotFound(name string) error { return fmt.Errorf("database not found: %s", name) }

// ErrMaxSelectPointsLimitExceeded is an error when a query hits the maximum number of points.
func ErrMaxSelectPointsLimitExceeded(n, limit int) error {
	return fmt.Errorf("max-select-point limit exceeed: (%d/%d)", n, limit)
}

// ErrMaxConcurrentQueriesLimitExceeded is an error when a query cannot be run
// because the maximum number of queries has been reached.
func ErrMaxConcurrentQueriesLimitExceeded(n, limit int) error {
	return fmt.Errorf("max-concurrent-queries limit exceeded(%d, %d)", n, limit)
}

// Authorizer determines if certain operations are authorized.
type Authorizer interface {
	// AuthorizeDatabase indicates whether the given Privilege is authorized on the database with the given name.
	AuthorizeDatabase(p influxql.Privilege, name string) bool

	// AuthorizeQuery returns an error if the query cannot be executed
	AuthorizeQuery(database string, query *influxql.Query) error

	// AuthorizeSeriesRead determines if a series is authorized for reading
	AuthorizeSeriesRead(database string, measurement []byte, tags models.Tags) bool

	// AuthorizeSeriesWrite determines if a series is authorized for writing
	AuthorizeSeriesWrite(database string, measurement []byte, tags models.Tags) bool
}

// OpenAuthorizer is the Authorizer used when authorization is disabled.
// It allows all operations.
type openAuthorizer struct{}

// OpenAuthorizer can be shared by all goroutines.
var OpenAuthorizer = openAuthorizer{}

// AuthorizeDatabase returns true to allow any operation on a database.
func (a openAuthorizer) AuthorizeDatabase(influxql.Privilege, string) bool { return true }

// AuthorizeSeriesRead allows access to any series.
func (a openAuthorizer) AuthorizeSeriesRead(database string, measurement []byte, tags models.Tags) bool {
	return true
}

// AuthorizeSeriesWrite allows access to any series.
func (a openAuthorizer) AuthorizeSeriesWrite(database string, measurement []byte, tags models.Tags) bool {
	return true
}

// AuthorizeSeriesRead allows any query to execute.
func (a openAuthorizer) AuthorizeQuery(_ string, _ *influxql.Query) error { return nil }

// AuthorizerIsOpen returns true if the provided Authorizer is guaranteed to
// authorize anything. A nil Authorizer returns true for this function, and this
// function should be preferred over directly checking if an Authorizer is nil
// or not.
func AuthorizerIsOpen(a Authorizer) bool {
	if u, ok := a.(interface{ AuthorizeUnrestricted() bool }); ok {
		return u.AuthorizeUnrestricted()
	}
	return a == nil || a == OpenAuthorizer
}

// ExecutionOptions contains the options for executing a query.
type ExecutionOptions struct {
	// OrgID is the organization for which this query is being executed.
	OrgID platform.ID

	// The database the query is running against.
	Database string

	// The retention policy the query is running against.
	RetentionPolicy string

	// How to determine whether the query is allowed to execute,
	// what resources can be returned in SHOW queries, etc.
	Authorizer Authorizer

	// The requested maximum number of points to return in each result.
	ChunkSize int

	// If this query is being executed in a read-only context.
	ReadOnly bool

	// Node to execute on.
	NodeID uint64

	// Quiet suppresses non-essential output from the query executor.
	Quiet bool
}

type (
	iteratorsContextKey struct{}
)

// NewContextWithIterators returns a new context.Context with the *Iterators slice added.
// The query planner will add instances of AuxIterator to the Iterators slice.
func NewContextWithIterators(ctx context.Context, itr *Iterators) context.Context {
	return context.WithValue(ctx, iteratorsContextKey{}, itr)
}

// StatementExecutor executes a statement within the Executor.
type StatementExecutor interface {
	// ExecuteStatement executes a statement. Results should be sent to the
	// results channel in the ExecutionContext.
	ExecuteStatement(ctx context.Context, stmt influxql.Statement, ectx *ExecutionContext) error
}

// StatementNormalizer normalizes a statement before it is executed.
type StatementNormalizer interface {
	// NormalizeStatement adds a default database and policy to the
	// measurements in the statement.
	NormalizeStatement(ctx context.Context, stmt influxql.Statement, database, retentionPolicy string, ectx *ExecutionContext) error
}

var (
	nullNormalizer StatementNormalizer = &nullNormalizerImpl{}
)

type nullNormalizerImpl struct{}

func (n *nullNormalizerImpl) NormalizeStatement(ctx context.Context, stmt influxql.Statement, database, retentionPolicy string, ectx *ExecutionContext) error {
	return nil
}

// Executor executes every statement in an Query.
type Executor struct {
	// Used for executing a statement in the query.
	StatementExecutor StatementExecutor

	// StatementNormalizer normalizes a statement before it is executed.
	StatementNormalizer StatementNormalizer

	Metrics *control.ControllerMetrics

	log *zap.Logger
}

// NewExecutor returns a new instance of Executor.
func NewExecutor(logger *zap.Logger, cm *control.ControllerMetrics) *Executor {
	return &Executor{
		StatementNormalizer: nullNormalizer,
		Metrics:             cm,
		log:                 logger.With(zap.String("service", "query")),
	}
}

// Close kills all running queries and prevents new queries from being attached.
func (e *Executor) Close() error {
	return nil
}

// ExecuteQuery executes each statement within a query.
func (e *Executor) ExecuteQuery(ctx context.Context, query *influxql.Query, opt ExecutionOptions) (<-chan *Result, *iql.Statistics) {
	results := make(chan *Result)
	statistics := new(iql.Statistics)
	go e.executeQuery(ctx, query, opt, results, statistics)
	return results, statistics
}

func (e *Executor) executeQuery(ctx context.Context, query *influxql.Query, opt ExecutionOptions, results chan *Result, statistics *iql.Statistics) {
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer func() {
		close(results)
		span.Finish()
	}()

	defer e.recover(query, results)

	gatherer := new(iql.StatisticsGatherer)

	statusLabel := control.LabelSuccess
	defer func(start time.Time) {
		dur := time.Since(start)
		e.Metrics.ExecutingDuration.WithLabelValues(statusLabel).Observe(dur.Seconds())
	}(time.Now())

	ectx := &ExecutionContext{StatisticsGatherer: gatherer, ExecutionOptions: opt}

	// Setup the execution context that will be used when executing statements.
	ectx.Results = results

	var i int
LOOP:
	for ; i < len(query.Statements); i++ {
		ectx.statementID = i
		stmt := query.Statements[i]

		// If a default database wasn't passed in by the caller, check the statement.
		defaultDB := opt.Database
		if defaultDB == "" {
			if s, ok := stmt.(influxql.HasDefaultDatabase); ok {
				defaultDB = s.DefaultDatabase()
			}
		}

		// Do not let queries manually use the system measurements. If we find
		// one, return an error. This prevents a person from using the
		// measurement incorrectly and causing a panic.
		if stmt, ok := stmt.(*influxql.SelectStatement); ok {
			for _, s := range stmt.Sources {
				switch s := s.(type) {
				case *influxql.Measurement:
					if influxql.IsSystemName(s.Name) {
						command := "the appropriate meta command"
						switch s.Name {
						case "_fieldKeys":
							command = "SHOW FIELD KEYS"
						case "_measurements":
							command = "SHOW MEASUREMENTS"
						case "_series":
							command = "SHOW SERIES"
						case "_tagKeys":
							command = "SHOW TAG KEYS"
						case "_tags":
							command = "SHOW TAG VALUES"
						}
						_ = ectx.Send(ctx, &Result{
							Err: fmt.Errorf("unable to use system source '%s': use %s instead", s.Name, command),
						})
						break LOOP
					}
				}
			}
		}

		// Rewrite statements, if necessary.
		// This can occur on meta read statements which convert to SELECT statements.
		newStmt, err := RewriteStatement(stmt)
		if err != nil {
			_ = ectx.Send(ctx, &Result{Err: err})
			break
		}
		stmt = newStmt

		if err := e.StatementNormalizer.NormalizeStatement(ctx, stmt, defaultDB, opt.RetentionPolicy, ectx); err != nil {
			if err := ectx.Send(ctx, &Result{Err: err}); err != nil {
				return
			}
			break
		}

		statistics.StatementCount += 1

		// Log each normalized statement.
		if !ectx.Quiet {
			e.log.Info("Executing query", zap.Stringer("query", stmt))
			span.LogFields(log.String("normalized_query", stmt.String()))
		}

		gatherer.Reset()
		stmtStart := time.Now()
		// Send any other statements to the underlying statement executor.
		err = tracing.LogError(span, e.StatementExecutor.ExecuteStatement(ctx, stmt, ectx))
		stmtDur := time.Since(stmtStart)
		stmtStats := gatherer.Statistics()
		stmtStats.ExecuteDuration = stmtDur - stmtStats.PlanDuration
		statistics.Add(stmtStats)

		// Send an error for this result if it failed for some reason.
		if err != nil {
			statusLabel = control.LabelNotExecuted
			e.Metrics.Requests.WithLabelValues(statusLabel).Inc()
			_ = ectx.Send(ctx, &Result{
				StatementID: i,
				Err:         err,
			})
			// Stop after the first error.
			break
		}

		e.Metrics.Requests.WithLabelValues(statusLabel).Inc()

		// Check if the query was interrupted during an uninterruptible statement.
		if err := ctx.Err(); err != nil {
			statusLabel = control.LabelInterruptedErr
			e.Metrics.Requests.WithLabelValues(statusLabel).Inc()
			break
		}
	}

	// Send error results for any statements which were not executed.
	for ; i < len(query.Statements)-1; i++ {
		if err := ectx.Send(ctx, &Result{
			StatementID: i,
			Err:         ErrNotExecuted,
		}); err != nil {
			break
		}
	}
}

// Determines if the Executor will recover any panics or let them crash
// the server.
var willCrash bool

func init() {
	var err error
	if willCrash, err = strconv.ParseBool(os.Getenv(PanicCrashEnv)); err != nil {
		willCrash = false
	}
}

func (e *Executor) recover(query *influxql.Query, results chan *Result) {
	if err := recover(); err != nil {
		e.log.Error(fmt.Sprintf("%s [panic:%s] %s", query.String(), err, debug.Stack()))
		results <- &Result{
			StatementID: -1,
			Err:         fmt.Errorf("%s [panic:%s]", query.String(), err),
		}

		if willCrash {
			e.log.Error("\n\n=====\nAll goroutines now follow:")
			e.log.Error(string(debug.Stack()))
			os.Exit(1)
		}
	}
}

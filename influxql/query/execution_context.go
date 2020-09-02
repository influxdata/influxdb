package query

import (
	"context"

	iql "github.com/influxdata/influxdb/v2/influxql"
)

// ExecutionContext contains state that the query is currently executing with.
type ExecutionContext struct {
	// The statement ID of the executing query.
	statementID int

	// Output channel where results and errors should be sent.
	Results chan *Result

	// StatisticsGatherer gathers metrics about the execution of a query.
	StatisticsGatherer *iql.StatisticsGatherer

	// Options used to start this query.
	ExecutionOptions
}

// Send sends a Result to the Results channel and will exit if the query has
// been interrupted or aborted.
func (ectx *ExecutionContext) Send(ctx context.Context, result *Result) error {
	result.StatementID = ectx.statementID
	select {
	case <-ctx.Done():
		return ctx.Err()
	case ectx.Results <- result:
	}
	return nil
}

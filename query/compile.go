package query

import (
	"fmt"
	"time"

	"github.com/influxdata/influxdb/influxql"
)

// CompileOptions are the customization options for the compiler.
type CompileOptions struct {
	Now time.Time
}

// Statement is a compiled query statement.
type Statement interface {
	// Prepare prepares the statement by mapping shards and finishing the creation
	// of the query plan.
	Prepare(shardMapper ShardMapper, opt SelectOptions) (PreparedStatement, error)
}

func Compile(stmt *influxql.SelectStatement, opt CompileOptions) (Statement, error) {
	// It is important to "stamp" this time so that everywhere we evaluate `now()` in the statement is EXACTLY the same `now`
	now := opt.Now
	if now.IsZero() {
		now = time.Now().UTC()
	}

	// Evaluate the now() condition immediately so we do not have to deal with this.
	nowValuer := influxql.NowValuer{Now: now, Location: stmt.Location}
	stmt = stmt.Reduce(&nowValuer)

	// Convert DISTINCT into a call.
	stmt.RewriteDistinct()

	// Remove "time" from fields list.
	stmt.RewriteTimeFields()

	// Rewrite time condition.
	if err := stmt.RewriteTimeCondition(now); err != nil {
		return nil, err
	}

	// Rewrite any regex conditions that could make use of the index.
	stmt.RewriteRegexConditions()
	return &compiledStatement{stmt: stmt}, nil
}

// compiledStatement represents a select statement that has undergone some initial processing to
// determine if it is valid and to have some initial modifications done on the AST.
type compiledStatement struct {
	stmt *influxql.SelectStatement
}

func (c *compiledStatement) Prepare(shardMapper ShardMapper, sopt SelectOptions) (PreparedStatement, error) {
	// Determine the time range spanned by the condition so we can map shards.
	nowValuer := influxql.NowValuer{Location: c.stmt.Location}
	_, timeRange, err := influxql.ConditionExpr(c.stmt.Condition, &nowValuer)
	if err != nil {
		return nil, err
	}

	// Create an iterator creator based on the shards in the cluster.
	shards, err := shardMapper.MapShards(c.stmt.Sources, timeRange, sopt)
	if err != nil {
		return nil, err
	}

	// Rewrite wildcards, if any exist.
	stmt, err := c.stmt.RewriteFields(shards)
	if err != nil {
		shards.Close()
		return nil, err
	}

	// Determine base options for iterators.
	opt, err := newIteratorOptionsStmt(stmt, sopt)
	if err != nil {
		shards.Close()
		return nil, err
	}

	if sopt.MaxBucketsN > 0 && !stmt.IsRawQuery {
		interval, err := stmt.GroupByInterval()
		if err != nil {
			shards.Close()
			return nil, err
		}

		if interval > 0 {
			// Determine the start and end time matched to the interval (may not match the actual times).
			first, _ := opt.Window(opt.StartTime)
			last, _ := opt.Window(opt.EndTime - 1)

			// Determine the number of buckets by finding the time span and dividing by the interval.
			buckets := (last - first + int64(interval)) / int64(interval)
			if int(buckets) > sopt.MaxBucketsN {
				shards.Close()
				return nil, fmt.Errorf("max-select-buckets limit exceeded: (%d/%d)", buckets, sopt.MaxBucketsN)
			}
		}
	}

	columns := stmt.ColumnNames()
	return &preparedStatement{
		stmt:    stmt,
		opt:     opt,
		ic:      shards,
		columns: columns,
	}, nil
}

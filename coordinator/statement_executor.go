package coordinator

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/monitor"
	"github.com/influxdata/influxdb/pkg/tracing"
	"github.com/influxdata/influxdb/pkg/tracing/fields"
	"github.com/influxdata/influxdb/query"
	"github.com/influxdata/influxdb/services/meta"
	"github.com/influxdata/influxdb/tsdb"
	"github.com/influxdata/influxql"
)

// ErrDatabaseNameRequired is returned when executing statements that require a database,
// when a database has not been provided.
var ErrDatabaseNameRequired = errors.New("database name required")

type pointsWriter interface {
	WritePointsInto(*IntoWriteRequest) error
}

// StatementExecutor executes a statement in the query.
type StatementExecutor struct {
	MetaClient MetaClient

	// TaskManager holds the StatementExecutor that handles task-related commands.
	TaskManager query.StatementExecutor

	// TSDB storage for local node.
	TSDBStore TSDBStore

	// ShardMapper for mapping shards when executing a SELECT statement.
	ShardMapper query.ShardMapper

	// Holds monitoring data for SHOW STATS and SHOW DIAGNOSTICS.
	Monitor *monitor.Monitor

	// Used for rewriting points back into system for SELECT INTO statements.
	PointsWriter pointsWriter

	// Select statement limits
	MaxSelectPointN   int
	MaxSelectSeriesN  int
	MaxSelectBucketsN int
}

// ExecuteStatement executes the given statement with the given execution context.
func (e *StatementExecutor) ExecuteStatement(stmt influxql.Statement, ctx query.ExecutionContext) error {
	// Select statements are handled separately so that they can be streamed.
	if stmt, ok := stmt.(*influxql.SelectStatement); ok {
		return e.executeSelectStatement(context.Background(), stmt, &ctx)
	}

	var rows models.Rows
	var messages []*query.Message
	var err error
	switch stmt := stmt.(type) {
	case *influxql.AlterRetentionPolicyStatement:
		if ctx.ReadOnly {
			messages = append(messages, query.ReadOnlyWarning(stmt.String()))
		}
		err = e.executeAlterRetentionPolicyStatement(stmt)
	case *influxql.CreateContinuousQueryStatement:
		if ctx.ReadOnly {
			messages = append(messages, query.ReadOnlyWarning(stmt.String()))
		}
		err = e.executeCreateContinuousQueryStatement(stmt)
	case *influxql.CreateDatabaseStatement:
		if ctx.ReadOnly {
			messages = append(messages, query.ReadOnlyWarning(stmt.String()))
		}
		err = e.executeCreateDatabaseStatement(stmt)
	case *influxql.CreateRetentionPolicyStatement:
		if ctx.ReadOnly {
			messages = append(messages, query.ReadOnlyWarning(stmt.String()))
		}
		err = e.executeCreateRetentionPolicyStatement(stmt)
	case *influxql.CreateSubscriptionStatement:
		if ctx.ReadOnly {
			messages = append(messages, query.ReadOnlyWarning(stmt.String()))
		}
		err = e.executeCreateSubscriptionStatement(stmt)
	case *influxql.CreateUserStatement:
		if ctx.ReadOnly {
			messages = append(messages, query.ReadOnlyWarning(stmt.String()))
		}
		err = e.executeCreateUserStatement(stmt)
	case *influxql.DeleteSeriesStatement:
		err = e.executeDeleteSeriesStatement(stmt, ctx.Database)
	case *influxql.DropContinuousQueryStatement:
		if ctx.ReadOnly {
			messages = append(messages, query.ReadOnlyWarning(stmt.String()))
		}
		err = e.executeDropContinuousQueryStatement(stmt)
	case *influxql.DropDatabaseStatement:
		if ctx.ReadOnly {
			messages = append(messages, query.ReadOnlyWarning(stmt.String()))
		}
		err = e.executeDropDatabaseStatement(stmt)
	case *influxql.DropMeasurementStatement:
		if ctx.ReadOnly {
			messages = append(messages, query.ReadOnlyWarning(stmt.String()))
		}
		err = e.executeDropMeasurementStatement(stmt, ctx.Database)
	case *influxql.DropSeriesStatement:
		if ctx.ReadOnly {
			messages = append(messages, query.ReadOnlyWarning(stmt.String()))
		}
		err = e.executeDropSeriesStatement(stmt, ctx.Database)
	case *influxql.DropRetentionPolicyStatement:
		if ctx.ReadOnly {
			messages = append(messages, query.ReadOnlyWarning(stmt.String()))
		}
		err = e.executeDropRetentionPolicyStatement(stmt)
	case *influxql.DropShardStatement:
		if ctx.ReadOnly {
			messages = append(messages, query.ReadOnlyWarning(stmt.String()))
		}
		err = e.executeDropShardStatement(stmt)
	case *influxql.DropSubscriptionStatement:
		if ctx.ReadOnly {
			messages = append(messages, query.ReadOnlyWarning(stmt.String()))
		}
		err = e.executeDropSubscriptionStatement(stmt)
	case *influxql.DropUserStatement:
		if ctx.ReadOnly {
			messages = append(messages, query.ReadOnlyWarning(stmt.String()))
		}
		err = e.executeDropUserStatement(stmt)
	case *influxql.ExplainStatement:
		if stmt.Analyze {
			rows, err = e.executeExplainAnalyzeStatement(stmt, &ctx)
		} else {
			rows, err = e.executeExplainStatement(stmt, &ctx)
		}
	case *influxql.GrantStatement:
		if ctx.ReadOnly {
			messages = append(messages, query.ReadOnlyWarning(stmt.String()))
		}
		err = e.executeGrantStatement(stmt)
	case *influxql.GrantAdminStatement:
		if ctx.ReadOnly {
			messages = append(messages, query.ReadOnlyWarning(stmt.String()))
		}
		err = e.executeGrantAdminStatement(stmt)
	case *influxql.RevokeStatement:
		if ctx.ReadOnly {
			messages = append(messages, query.ReadOnlyWarning(stmt.String()))
		}
		err = e.executeRevokeStatement(stmt)
	case *influxql.RevokeAdminStatement:
		if ctx.ReadOnly {
			messages = append(messages, query.ReadOnlyWarning(stmt.String()))
		}
		err = e.executeRevokeAdminStatement(stmt)
	case *influxql.ShowContinuousQueriesStatement:
		rows, err = e.executeShowContinuousQueriesStatement(stmt)
	case *influxql.ShowDatabasesStatement:
		rows, err = e.executeShowDatabasesStatement(stmt, &ctx)
	case *influxql.ShowDiagnosticsStatement:
		rows, err = e.executeShowDiagnosticsStatement(stmt)
	case *influxql.ShowGrantsForUserStatement:
		rows, err = e.executeShowGrantsForUserStatement(stmt)
	case *influxql.ShowMeasurementsStatement:
		return e.executeShowMeasurementsStatement(stmt, &ctx)
	case *influxql.ShowMeasurementCardinalityStatement:
		rows, err = e.executeShowMeasurementCardinalityStatement(stmt)
	case *influxql.ShowRetentionPoliciesStatement:
		rows, err = e.executeShowRetentionPoliciesStatement(stmt)
	case *influxql.ShowSeriesCardinalityStatement:
		rows, err = e.executeShowSeriesCardinalityStatement(stmt)
	case *influxql.ShowShardsStatement:
		rows, err = e.executeShowShardsStatement(stmt)
	case *influxql.ShowShardGroupsStatement:
		rows, err = e.executeShowShardGroupsStatement(stmt)
	case *influxql.ShowStatsStatement:
		rows, err = e.executeShowStatsStatement(stmt)
	case *influxql.ShowSubscriptionsStatement:
		rows, err = e.executeShowSubscriptionsStatement(stmt)
	case *influxql.ShowTagKeysStatement:
		return e.executeShowTagKeys(stmt, &ctx)
	case *influxql.ShowTagValuesStatement:
		return e.executeShowTagValues(stmt, &ctx)
	case *influxql.ShowUsersStatement:
		rows, err = e.executeShowUsersStatement(stmt)
	case *influxql.SetPasswordUserStatement:
		if ctx.ReadOnly {
			messages = append(messages, query.ReadOnlyWarning(stmt.String()))
		}
		err = e.executeSetPasswordUserStatement(stmt)
	case *influxql.ShowQueriesStatement, *influxql.KillQueryStatement:
		// Send query related statements to the task manager.
		return e.TaskManager.ExecuteStatement(stmt, ctx)
	default:
		return query.ErrInvalidQuery
	}

	if err != nil {
		return err
	}

	return ctx.Send(&query.Result{
		StatementID: ctx.StatementID,
		Series:      rows,
		Messages:    messages,
	})
}

func (e *StatementExecutor) executeAlterRetentionPolicyStatement(stmt *influxql.AlterRetentionPolicyStatement) error {
	rpu := &meta.RetentionPolicyUpdate{
		Duration:           stmt.Duration,
		ReplicaN:           stmt.Replication,
		ShardGroupDuration: stmt.ShardGroupDuration,
	}

	// Update the retention policy.
	return e.MetaClient.UpdateRetentionPolicy(stmt.Database, stmt.Name, rpu, stmt.Default)
}

func (e *StatementExecutor) executeCreateContinuousQueryStatement(q *influxql.CreateContinuousQueryStatement) error {
	// Verify that retention policies exist.
	var err error
	verifyRPFn := func(n influxql.Node) {
		if err != nil {
			return
		}
		switch m := n.(type) {
		case *influxql.Measurement:
			var rp *meta.RetentionPolicyInfo
			if rp, err = e.MetaClient.RetentionPolicy(m.Database, m.RetentionPolicy); err != nil {
				return
			} else if rp == nil {
				err = fmt.Errorf("%s: %s.%s", meta.ErrRetentionPolicyNotFound, m.Database, m.RetentionPolicy)
			}
		default:
			return
		}
	}

	influxql.WalkFunc(q, verifyRPFn)

	if err != nil {
		return err
	}

	return e.MetaClient.CreateContinuousQuery(q.Database, q.Name, q.String())
}

func (e *StatementExecutor) executeCreateDatabaseStatement(stmt *influxql.CreateDatabaseStatement) error {
	if !meta.ValidName(stmt.Name) {
		// TODO This should probably be in `(*meta.Data).CreateDatabase`
		// but can't go there until 1.1 is used everywhere
		return meta.ErrInvalidName
	}

	if !stmt.RetentionPolicyCreate {
		_, err := e.MetaClient.CreateDatabase(stmt.Name)
		return err
	}

	// If we're doing, for example, CREATE DATABASE "db" WITH DURATION 1d then
	// the name will not yet be set. We only need to validate non-empty
	// retention policy names, such as in the statement:
	// 	CREATE DATABASE "db" WITH DURATION 1d NAME "xyz"
	if stmt.RetentionPolicyName != "" && !meta.ValidName(stmt.RetentionPolicyName) {
		return meta.ErrInvalidName
	}

	spec := meta.RetentionPolicySpec{
		Name:               stmt.RetentionPolicyName,
		Duration:           stmt.RetentionPolicyDuration,
		ReplicaN:           stmt.RetentionPolicyReplication,
		ShardGroupDuration: stmt.RetentionPolicyShardGroupDuration,
	}
	_, err := e.MetaClient.CreateDatabaseWithRetentionPolicy(stmt.Name, &spec)
	return err
}

func (e *StatementExecutor) executeCreateRetentionPolicyStatement(stmt *influxql.CreateRetentionPolicyStatement) error {
	if !meta.ValidName(stmt.Name) {
		// TODO This should probably be in `(*meta.Data).CreateRetentionPolicy`
		// but can't go there until 1.1 is used everywhere
		return meta.ErrInvalidName
	}

	spec := meta.RetentionPolicySpec{
		Name:               stmt.Name,
		Duration:           &stmt.Duration,
		ReplicaN:           &stmt.Replication,
		ShardGroupDuration: stmt.ShardGroupDuration,
	}

	// Create new retention policy.
	_, err := e.MetaClient.CreateRetentionPolicy(stmt.Database, &spec, stmt.Default)
	return err
}

func (e *StatementExecutor) executeCreateSubscriptionStatement(q *influxql.CreateSubscriptionStatement) error {
	return e.MetaClient.CreateSubscription(q.Database, q.RetentionPolicy, q.Name, q.Mode, q.Destinations)
}

func (e *StatementExecutor) executeCreateUserStatement(q *influxql.CreateUserStatement) error {
	_, err := e.MetaClient.CreateUser(q.Name, q.Password, q.Admin)
	return err
}

func (e *StatementExecutor) executeDeleteSeriesStatement(stmt *influxql.DeleteSeriesStatement, database string) error {
	if dbi := e.MetaClient.Database(database); dbi == nil {
		return query.ErrDatabaseNotFound(database)
	}

	// Convert "now()" to current time.
	stmt.Condition = influxql.Reduce(stmt.Condition, &influxql.NowValuer{Now: time.Now().UTC()})

	// Locally delete the series.
	return e.TSDBStore.DeleteSeries(database, stmt.Sources, stmt.Condition)
}

func (e *StatementExecutor) executeDropContinuousQueryStatement(q *influxql.DropContinuousQueryStatement) error {
	return e.MetaClient.DropContinuousQuery(q.Database, q.Name)
}

// executeDropDatabaseStatement drops a database from the cluster.
// It does not return an error if the database was not found on any of
// the nodes, or in the Meta store.
func (e *StatementExecutor) executeDropDatabaseStatement(stmt *influxql.DropDatabaseStatement) error {
	if e.MetaClient.Database(stmt.Name) == nil {
		return nil
	}

	// Locally delete the datababse.
	if err := e.TSDBStore.DeleteDatabase(stmt.Name); err != nil {
		return err
	}

	// Remove the database from the Meta Store.
	return e.MetaClient.DropDatabase(stmt.Name)
}

func (e *StatementExecutor) executeDropMeasurementStatement(stmt *influxql.DropMeasurementStatement, database string) error {
	if dbi := e.MetaClient.Database(database); dbi == nil {
		return query.ErrDatabaseNotFound(database)
	}

	// Locally drop the measurement
	return e.TSDBStore.DeleteMeasurement(database, stmt.Name)
}

func (e *StatementExecutor) executeDropSeriesStatement(stmt *influxql.DropSeriesStatement, database string) error {
	if dbi := e.MetaClient.Database(database); dbi == nil {
		return query.ErrDatabaseNotFound(database)
	}

	// Check for time in WHERE clause (not supported).
	if influxql.HasTimeExpr(stmt.Condition) {
		return errors.New("DROP SERIES doesn't support time in WHERE clause")
	}

	// Locally drop the series.
	return e.TSDBStore.DeleteSeries(database, stmt.Sources, stmt.Condition)
}

func (e *StatementExecutor) executeDropShardStatement(stmt *influxql.DropShardStatement) error {
	// Locally delete the shard.
	if err := e.TSDBStore.DeleteShard(stmt.ID); err != nil {
		return err
	}

	// Remove the shard reference from the Meta Store.
	return e.MetaClient.DropShard(stmt.ID)
}

func (e *StatementExecutor) executeDropRetentionPolicyStatement(stmt *influxql.DropRetentionPolicyStatement) error {
	dbi := e.MetaClient.Database(stmt.Database)
	if dbi == nil {
		return nil
	}

	if dbi.RetentionPolicy(stmt.Name) == nil {
		return nil
	}

	// Locally drop the retention policy.
	if err := e.TSDBStore.DeleteRetentionPolicy(stmt.Database, stmt.Name); err != nil {
		return err
	}

	return e.MetaClient.DropRetentionPolicy(stmt.Database, stmt.Name)
}

func (e *StatementExecutor) executeDropSubscriptionStatement(q *influxql.DropSubscriptionStatement) error {
	return e.MetaClient.DropSubscription(q.Database, q.RetentionPolicy, q.Name)
}

func (e *StatementExecutor) executeDropUserStatement(q *influxql.DropUserStatement) error {
	return e.MetaClient.DropUser(q.Name)
}

func (e *StatementExecutor) executeExplainStatement(q *influxql.ExplainStatement, ectx *query.ExecutionContext) (models.Rows, error) {
	opt := query.SelectOptions{
		InterruptCh: ectx.InterruptCh,
		NodeID:      ectx.ExecutionOptions.NodeID,
		MaxSeriesN:  e.MaxSelectSeriesN,
		MaxBucketsN: e.MaxSelectBucketsN,
		Authorizer:  ectx.Authorizer,
	}

	// Prepare the query for execution, but do not actually execute it.
	// This should perform any needed substitutions.
	p, err := query.Prepare(q.Statement, e.ShardMapper, opt)
	if err != nil {
		return nil, err
	}
	defer p.Close()

	plan, err := p.Explain()
	if err != nil {
		return nil, err
	}
	plan = strings.TrimSpace(plan)

	row := &models.Row{
		Columns: []string{"QUERY PLAN"},
	}
	for _, s := range strings.Split(plan, "\n") {
		row.Values = append(row.Values, []interface{}{s})
	}
	return models.Rows{row}, nil
}

func (e *StatementExecutor) executeExplainAnalyzeStatement(q *influxql.ExplainStatement, ectx *query.ExecutionContext) (models.Rows, error) {
	stmt := q.Statement
	t, span := tracing.NewTrace("select")
	ctx := tracing.NewContextWithTrace(context.Background(), t)
	ctx = tracing.NewContextWithSpan(ctx, span)
	var aux query.Iterators
	ctx = query.NewContextWithIterators(ctx, &aux)
	start := time.Now()

	itrs, columns, err := e.createIterators(ctx, stmt, ectx)
	if err != nil {
		return nil, err
	}

	iterTime := time.Since(start)

	// Generate a row emitter from the iterator set.
	em := query.NewEmitter(itrs, stmt.TimeAscending(), ectx.ChunkSize)
	em.Columns = columns
	if stmt.Location != nil {
		em.Location = stmt.Location
	}
	em.OmitTime = stmt.OmitTime
	em.EmitName = stmt.EmitName

	// Emit rows to the results channel.
	var writeN int64
	for {
		var row *models.Row
		row, _, err = em.Emit()
		if err != nil {
			goto CLEANUP
		} else if row == nil {
			// Check if the query was interrupted while emitting.
			select {
			case <-ectx.InterruptCh:
				err = query.ErrQueryInterrupted
				goto CLEANUP
			default:
			}
			break
		}

		writeN += int64(len(row.Values))
	}

CLEANUP:
	em.Close()
	if err != nil {
		return nil, err
	}

	// close auxiliary iterators deterministically to finalize any captured measurements
	aux.Close()

	totalTime := time.Since(start)
	span.MergeFields(
		fields.Duration("total_time", totalTime),
		fields.Duration("planning_time", iterTime),
		fields.Duration("execution_time", totalTime-iterTime),
	)
	span.Finish()

	row := &models.Row{
		Columns: []string{"EXPLAIN ANALYZE"},
	}
	for _, s := range strings.Split(t.Tree().String(), "\n") {
		row.Values = append(row.Values, []interface{}{s})
	}

	return models.Rows{row}, nil
}

func (e *StatementExecutor) executeGrantStatement(stmt *influxql.GrantStatement) error {
	return e.MetaClient.SetPrivilege(stmt.User, stmt.On, stmt.Privilege)
}

func (e *StatementExecutor) executeGrantAdminStatement(stmt *influxql.GrantAdminStatement) error {
	return e.MetaClient.SetAdminPrivilege(stmt.User, true)
}

func (e *StatementExecutor) executeRevokeStatement(stmt *influxql.RevokeStatement) error {
	priv := influxql.NoPrivileges

	// Revoking all privileges means there's no need to look at existing user privileges.
	if stmt.Privilege != influxql.AllPrivileges {
		p, err := e.MetaClient.UserPrivilege(stmt.User, stmt.On)
		if err != nil {
			return err
		}
		// Bit clear (AND NOT) the user's privilege with the revoked privilege.
		priv = *p &^ stmt.Privilege
	}

	return e.MetaClient.SetPrivilege(stmt.User, stmt.On, priv)
}

func (e *StatementExecutor) executeRevokeAdminStatement(stmt *influxql.RevokeAdminStatement) error {
	return e.MetaClient.SetAdminPrivilege(stmt.User, false)
}

func (e *StatementExecutor) executeSetPasswordUserStatement(q *influxql.SetPasswordUserStatement) error {
	return e.MetaClient.UpdateUser(q.Name, q.Password)
}

func (e *StatementExecutor) executeSelectStatement(ctx context.Context, stmt *influxql.SelectStatement, ectx *query.ExecutionContext) error {
	itrs, columns, err := e.createIterators(ctx, stmt, ectx)
	if err != nil {
		return err
	}

	// Generate a row emitter from the iterator set.
	em := query.NewEmitter(itrs, stmt.TimeAscending(), ectx.ChunkSize)
	em.Columns = columns
	if stmt.Location != nil {
		em.Location = stmt.Location
	}
	em.OmitTime = stmt.OmitTime
	em.EmitName = stmt.EmitName
	defer em.Close()

	// Emit rows to the results channel.
	var writeN int64
	var emitted bool

	var pointsWriter *BufferedPointsWriter
	if stmt.Target != nil {
		pointsWriter = NewBufferedPointsWriter(e.PointsWriter, stmt.Target.Measurement.Database, stmt.Target.Measurement.RetentionPolicy, 10000)
	}

	for {
		row, partial, err := em.Emit()
		if err != nil {
			return err
		} else if row == nil {
			// Check if the query was interrupted while emitting.
			select {
			case <-ectx.InterruptCh:
				return query.ErrQueryInterrupted
			default:
			}
			break
		}

		// Write points back into system for INTO statements.
		if stmt.Target != nil {
			if err := e.writeInto(pointsWriter, stmt, row); err != nil {
				return err
			}
			writeN += int64(len(row.Values))
			continue
		}

		result := &query.Result{
			StatementID: ectx.StatementID,
			Series:      []*models.Row{row},
			Partial:     partial,
		}

		// Send results or exit if closing.
		if err := ectx.Send(result); err != nil {
			return err
		}

		emitted = true
	}

	// Flush remaining points and emit write count if an INTO statement.
	if stmt.Target != nil {
		if err := pointsWriter.Flush(); err != nil {
			return err
		}

		var messages []*query.Message
		if ectx.ReadOnly {
			messages = append(messages, query.ReadOnlyWarning(stmt.String()))
		}

		return ectx.Send(&query.Result{
			StatementID: ectx.StatementID,
			Messages:    messages,
			Series: []*models.Row{{
				Name:    "result",
				Columns: []string{"time", "written"},
				Values:  [][]interface{}{{time.Unix(0, 0).UTC(), writeN}},
			}},
		})
	}

	// Always emit at least one result.
	if !emitted {
		return ectx.Send(&query.Result{
			StatementID: ectx.StatementID,
			Series:      make([]*models.Row, 0),
		})
	}

	return nil
}

func (e *StatementExecutor) createIterators(ctx context.Context, stmt *influxql.SelectStatement, ectx *query.ExecutionContext) ([]query.Iterator, []string, error) {
	opt := query.SelectOptions{
		InterruptCh: ectx.InterruptCh,
		NodeID:      ectx.ExecutionOptions.NodeID,
		MaxSeriesN:  e.MaxSelectSeriesN,
		MaxBucketsN: e.MaxSelectBucketsN,
		Authorizer:  ectx.Authorizer,
	}

	// Create a set of iterators from a selection.
	itrs, columns, err := query.Select(ctx, stmt, e.ShardMapper, opt)
	if err != nil {
		return nil, nil, err
	}

	if e.MaxSelectPointN > 0 {
		monitor := query.PointLimitMonitor(itrs, query.DefaultStatsInterval, e.MaxSelectPointN)
		ectx.Query.Monitor(monitor)
	}
	return itrs, columns, nil
}

func (e *StatementExecutor) executeShowContinuousQueriesStatement(stmt *influxql.ShowContinuousQueriesStatement) (models.Rows, error) {
	dis := e.MetaClient.Databases()

	rows := []*models.Row{}
	for _, di := range dis {
		row := &models.Row{Columns: []string{"name", "query"}, Name: di.Name}
		for _, cqi := range di.ContinuousQueries {
			row.Values = append(row.Values, []interface{}{cqi.Name, cqi.Query})
		}
		rows = append(rows, row)
	}
	return rows, nil
}

func (e *StatementExecutor) executeShowDatabasesStatement(q *influxql.ShowDatabasesStatement, ctx *query.ExecutionContext) (models.Rows, error) {
	dis := e.MetaClient.Databases()
	a := ctx.ExecutionOptions.Authorizer

	row := &models.Row{Name: "databases", Columns: []string{"name"}}
	for _, di := range dis {
		// Only include databases that the user is authorized to read or write.
		if a.AuthorizeDatabase(influxql.ReadPrivilege, di.Name) || a.AuthorizeDatabase(influxql.WritePrivilege, di.Name) {
			row.Values = append(row.Values, []interface{}{di.Name})
		}
	}
	return []*models.Row{row}, nil
}

func (e *StatementExecutor) executeShowDiagnosticsStatement(stmt *influxql.ShowDiagnosticsStatement) (models.Rows, error) {
	diags, err := e.Monitor.Diagnostics()
	if err != nil {
		return nil, err
	}

	// Get a sorted list of diagnostics keys.
	sortedKeys := make([]string, 0, len(diags))
	for k := range diags {
		sortedKeys = append(sortedKeys, k)
	}
	sort.Strings(sortedKeys)

	rows := make([]*models.Row, 0, len(diags))
	for _, k := range sortedKeys {
		if stmt.Module != "" && k != stmt.Module {
			continue
		}

		row := &models.Row{Name: k}

		row.Columns = diags[k].Columns
		row.Values = diags[k].Rows
		rows = append(rows, row)
	}
	return rows, nil
}

func (e *StatementExecutor) executeShowGrantsForUserStatement(q *influxql.ShowGrantsForUserStatement) (models.Rows, error) {
	priv, err := e.MetaClient.UserPrivileges(q.Name)
	if err != nil {
		return nil, err
	}

	row := &models.Row{Columns: []string{"database", "privilege"}}
	for d, p := range priv {
		row.Values = append(row.Values, []interface{}{d, p.String()})
	}
	return []*models.Row{row}, nil
}

func (e *StatementExecutor) executeShowMeasurementsStatement(q *influxql.ShowMeasurementsStatement, ctx *query.ExecutionContext) error {
	if q.Database == "" {
		return ErrDatabaseNameRequired
	}

	names, err := e.TSDBStore.MeasurementNames(ctx.Authorizer, q.Database, q.Condition)
	if err != nil || len(names) == 0 {
		return ctx.Send(&query.Result{
			StatementID: ctx.StatementID,
			Err:         err,
		})
	}

	if q.Offset > 0 {
		if q.Offset >= len(names) {
			names = nil
		} else {
			names = names[q.Offset:]
		}
	}

	if q.Limit > 0 {
		if q.Limit < len(names) {
			names = names[:q.Limit]
		}
	}

	values := make([][]interface{}, len(names))
	for i, name := range names {
		values[i] = []interface{}{string(name)}
	}

	if len(values) == 0 {
		return ctx.Send(&query.Result{
			StatementID: ctx.StatementID,
		})
	}

	return ctx.Send(&query.Result{
		StatementID: ctx.StatementID,
		Series: []*models.Row{{
			Name:    "measurements",
			Columns: []string{"name"},
			Values:  values,
		}},
	})
}

func (e *StatementExecutor) executeShowMeasurementCardinalityStatement(stmt *influxql.ShowMeasurementCardinalityStatement) (models.Rows, error) {
	if stmt.Database == "" {
		return nil, ErrDatabaseNameRequired
	}

	n, err := e.TSDBStore.MeasurementsCardinality(stmt.Database)
	if err != nil {
		return nil, err
	}

	return []*models.Row{&models.Row{
		Columns: []string{"cardinality estimation"},
		Values:  [][]interface{}{{n}},
	}}, nil
}

func (e *StatementExecutor) executeShowRetentionPoliciesStatement(q *influxql.ShowRetentionPoliciesStatement) (models.Rows, error) {
	if q.Database == "" {
		return nil, ErrDatabaseNameRequired
	}

	di := e.MetaClient.Database(q.Database)
	if di == nil {
		return nil, influxdb.ErrDatabaseNotFound(q.Database)
	}

	row := &models.Row{Columns: []string{"name", "duration", "shardGroupDuration", "replicaN", "default"}}
	for _, rpi := range di.RetentionPolicies {
		row.Values = append(row.Values, []interface{}{rpi.Name, rpi.Duration.String(), rpi.ShardGroupDuration.String(), rpi.ReplicaN, di.DefaultRetentionPolicy == rpi.Name})
	}
	return []*models.Row{row}, nil
}

func (e *StatementExecutor) executeShowShardsStatement(stmt *influxql.ShowShardsStatement) (models.Rows, error) {
	dis := e.MetaClient.Databases()

	rows := []*models.Row{}
	for _, di := range dis {
		row := &models.Row{Columns: []string{"id", "database", "retention_policy", "shard_group", "start_time", "end_time", "expiry_time", "owners"}, Name: di.Name}
		for _, rpi := range di.RetentionPolicies {
			for _, sgi := range rpi.ShardGroups {
				// Shards associated with deleted shard groups are effectively deleted.
				// Don't list them.
				if sgi.Deleted() {
					continue
				}

				for _, si := range sgi.Shards {
					ownerIDs := make([]uint64, len(si.Owners))
					for i, owner := range si.Owners {
						ownerIDs[i] = owner.NodeID
					}

					row.Values = append(row.Values, []interface{}{
						si.ID,
						di.Name,
						rpi.Name,
						sgi.ID,
						sgi.StartTime.UTC().Format(time.RFC3339),
						sgi.EndTime.UTC().Format(time.RFC3339),
						sgi.EndTime.Add(rpi.Duration).UTC().Format(time.RFC3339),
						joinUint64(ownerIDs),
					})
				}
			}
		}
		rows = append(rows, row)
	}
	return rows, nil
}

func (e *StatementExecutor) executeShowSeriesCardinalityStatement(stmt *influxql.ShowSeriesCardinalityStatement) (models.Rows, error) {
	if stmt.Database == "" {
		return nil, ErrDatabaseNameRequired
	}

	n, err := e.TSDBStore.SeriesCardinality(stmt.Database)
	if err != nil {
		return nil, err
	}

	return []*models.Row{&models.Row{
		Columns: []string{"cardinality estimation"},
		Values:  [][]interface{}{{n}},
	}}, nil
}

func (e *StatementExecutor) executeShowShardGroupsStatement(stmt *influxql.ShowShardGroupsStatement) (models.Rows, error) {
	dis := e.MetaClient.Databases()

	row := &models.Row{Columns: []string{"id", "database", "retention_policy", "start_time", "end_time", "expiry_time"}, Name: "shard groups"}
	for _, di := range dis {
		for _, rpi := range di.RetentionPolicies {
			for _, sgi := range rpi.ShardGroups {
				// Shards associated with deleted shard groups are effectively deleted.
				// Don't list them.
				if sgi.Deleted() {
					continue
				}

				row.Values = append(row.Values, []interface{}{
					sgi.ID,
					di.Name,
					rpi.Name,
					sgi.StartTime.UTC().Format(time.RFC3339),
					sgi.EndTime.UTC().Format(time.RFC3339),
					sgi.EndTime.Add(rpi.Duration).UTC().Format(time.RFC3339),
				})
			}
		}
	}

	return []*models.Row{row}, nil
}

func (e *StatementExecutor) executeShowStatsStatement(stmt *influxql.ShowStatsStatement) (models.Rows, error) {
	stats, err := e.Monitor.Statistics(nil)
	if err != nil {
		return nil, err
	}

	var rows []*models.Row
	for _, stat := range stats {
		if stmt.Module != "" && stat.Name != stmt.Module {
			continue
		}
		row := &models.Row{Name: stat.Name, Tags: stat.Tags}

		values := make([]interface{}, 0, len(stat.Values))
		for _, k := range stat.ValueNames() {
			row.Columns = append(row.Columns, k)
			values = append(values, stat.Values[k])
		}
		row.Values = [][]interface{}{values}
		rows = append(rows, row)
	}
	return rows, nil
}

func (e *StatementExecutor) executeShowSubscriptionsStatement(stmt *influxql.ShowSubscriptionsStatement) (models.Rows, error) {
	dis := e.MetaClient.Databases()

	rows := []*models.Row{}
	for _, di := range dis {
		row := &models.Row{Columns: []string{"retention_policy", "name", "mode", "destinations"}, Name: di.Name}
		for _, rpi := range di.RetentionPolicies {
			for _, si := range rpi.Subscriptions {
				row.Values = append(row.Values, []interface{}{rpi.Name, si.Name, si.Mode, si.Destinations})
			}
		}
		if len(row.Values) > 0 {
			rows = append(rows, row)
		}
	}
	return rows, nil
}

func (e *StatementExecutor) executeShowTagKeys(q *influxql.ShowTagKeysStatement, ctx *query.ExecutionContext) error {
	if q.Database == "" {
		return ErrDatabaseNameRequired
	}

	// Determine shard set based on database and time range.
	// SHOW TAG KEYS returns all tag keys for the default retention policy.
	di := e.MetaClient.Database(q.Database)
	if di == nil {
		return fmt.Errorf("database not found: %s", q.Database)
	}

	// Determine appropriate time range. If one or fewer time boundaries provided
	// then min/max possible time should be used instead.
	valuer := &influxql.NowValuer{Now: time.Now()}
	cond, timeRange, err := influxql.ConditionExpr(q.Condition, valuer)
	if err != nil {
		return err
	}

	// Get all shards for all retention policies.
	var allGroups []meta.ShardGroupInfo
	for _, rpi := range di.RetentionPolicies {
		sgis, err := e.MetaClient.ShardGroupsByTimeRange(q.Database, rpi.Name, timeRange.MinTime(), timeRange.MaxTime())
		if err != nil {
			return err
		}
		allGroups = append(allGroups, sgis...)
	}

	var shardIDs []uint64
	for _, sgi := range allGroups {
		for _, si := range sgi.Shards {
			shardIDs = append(shardIDs, si.ID)
		}
	}

	tagKeys, err := e.TSDBStore.TagKeys(ctx.Authorizer, shardIDs, cond)
	if err != nil {
		return ctx.Send(&query.Result{
			StatementID: ctx.StatementID,
			Err:         err,
		})
	}

	emitted := false
	for _, m := range tagKeys {
		keys := m.Keys

		if q.Offset > 0 {
			if q.Offset >= len(keys) {
				keys = nil
			} else {
				keys = keys[q.Offset:]
			}
		}
		if q.Limit > 0 && q.Limit < len(keys) {
			keys = keys[:q.Limit]
		}

		if len(keys) == 0 {
			continue
		}

		row := &models.Row{
			Name:    m.Measurement,
			Columns: []string{"tagKey"},
			Values:  make([][]interface{}, len(keys)),
		}
		for i, key := range keys {
			row.Values[i] = []interface{}{key}
		}

		if err := ctx.Send(&query.Result{
			StatementID: ctx.StatementID,
			Series:      []*models.Row{row},
		}); err != nil {
			return err
		}
		emitted = true
	}

	// Ensure at least one result is emitted.
	if !emitted {
		return ctx.Send(&query.Result{
			StatementID: ctx.StatementID,
		})
	}
	return nil
}

func (e *StatementExecutor) executeShowTagValues(q *influxql.ShowTagValuesStatement, ctx *query.ExecutionContext) error {
	if q.Database == "" {
		return ErrDatabaseNameRequired
	}

	// Determine shard set based on database and time range.
	// SHOW TAG VALUES returns all tag values for the default retention policy.
	di := e.MetaClient.Database(q.Database)
	if di == nil {
		return fmt.Errorf("database not found: %s", q.Database)
	}

	// Determine appropriate time range. If one or fewer time boundaries provided
	// then min/max possible time should be used instead.
	valuer := &influxql.NowValuer{Now: time.Now()}
	cond, timeRange, err := influxql.ConditionExpr(q.Condition, valuer)
	if err != nil {
		return err
	}

	// Get all shards for all retention policies.
	var allGroups []meta.ShardGroupInfo
	for _, rpi := range di.RetentionPolicies {
		sgis, err := e.MetaClient.ShardGroupsByTimeRange(q.Database, rpi.Name, timeRange.MinTime(), timeRange.MaxTime())
		if err != nil {
			return err
		}
		allGroups = append(allGroups, sgis...)
	}

	var shardIDs []uint64
	for _, sgi := range allGroups {
		for _, si := range sgi.Shards {
			shardIDs = append(shardIDs, si.ID)
		}
	}

	tagValues, err := e.TSDBStore.TagValues(ctx.Authorizer, shardIDs, cond)
	if err != nil {
		return ctx.Send(&query.Result{
			StatementID: ctx.StatementID,
			Err:         err,
		})
	}

	emitted := false
	for _, m := range tagValues {
		values := m.Values

		if q.Offset > 0 {
			if q.Offset >= len(values) {
				values = nil
			} else {
				values = values[q.Offset:]
			}
		}

		if q.Limit > 0 {
			if q.Limit < len(values) {
				values = values[:q.Limit]
			}
		}

		if len(values) == 0 {
			continue
		}

		row := &models.Row{
			Name:    m.Measurement,
			Columns: []string{"key", "value"},
			Values:  make([][]interface{}, len(values)),
		}
		for i, v := range values {
			row.Values[i] = []interface{}{v.Key, v.Value}
		}

		if err := ctx.Send(&query.Result{
			StatementID: ctx.StatementID,
			Series:      []*models.Row{row},
		}); err != nil {
			return err
		}
		emitted = true
	}

	// Ensure at least one result is emitted.
	if !emitted {
		return ctx.Send(&query.Result{
			StatementID: ctx.StatementID,
		})
	}
	return nil
}

func (e *StatementExecutor) executeShowUsersStatement(q *influxql.ShowUsersStatement) (models.Rows, error) {
	row := &models.Row{Columns: []string{"user", "admin"}}
	for _, ui := range e.MetaClient.Users() {
		row.Values = append(row.Values, []interface{}{ui.Name, ui.Admin})
	}
	return []*models.Row{row}, nil
}

// BufferedPointsWriter adds buffering to a pointsWriter so that SELECT INTO queries
// write their points to the destination in batches.
type BufferedPointsWriter struct {
	w               pointsWriter
	buf             []models.Point
	database        string
	retentionPolicy string
}

// NewBufferedPointsWriter returns a new BufferedPointsWriter.
func NewBufferedPointsWriter(w pointsWriter, database, retentionPolicy string, capacity int) *BufferedPointsWriter {
	return &BufferedPointsWriter{
		w:               w,
		buf:             make([]models.Point, 0, capacity),
		database:        database,
		retentionPolicy: retentionPolicy,
	}
}

// WritePointsInto implements pointsWriter for BufferedPointsWriter.
func (w *BufferedPointsWriter) WritePointsInto(req *IntoWriteRequest) error {
	// Make sure we're buffering points only for the expected destination.
	if req.Database != w.database || req.RetentionPolicy != w.retentionPolicy {
		return fmt.Errorf("writer for %s.%s can't write into %s.%s", w.database, w.retentionPolicy, req.Database, req.RetentionPolicy)
	}

	for i := 0; i < len(req.Points); {
		// Get the available space in the buffer.
		avail := cap(w.buf) - len(w.buf)

		// Calculate number of points to copy into the buffer.
		n := len(req.Points[i:])
		if n > avail {
			n = avail
		}

		// Copy points into buffer.
		w.buf = append(w.buf, req.Points[i:n+i]...)

		// Advance the index by number of points copied.
		i += n

		// If buffer is full, flush points to underlying writer.
		if len(w.buf) == cap(w.buf) {
			if err := w.Flush(); err != nil {
				return err
			}
		}
	}

	return nil
}

// Flush writes all buffered points to the underlying writer.
func (w *BufferedPointsWriter) Flush() error {
	if len(w.buf) == 0 {
		return nil
	}

	if err := w.w.WritePointsInto(&IntoWriteRequest{
		Database:        w.database,
		RetentionPolicy: w.retentionPolicy,
		Points:          w.buf,
	}); err != nil {
		return err
	}

	// Clear the buffer.
	w.buf = w.buf[:0]

	return nil
}

// Len returns the number of points buffered.
func (w *BufferedPointsWriter) Len() int { return len(w.buf) }

// Cap returns the capacity (in points) of the buffer.
func (w *BufferedPointsWriter) Cap() int { return cap(w.buf) }

func (e *StatementExecutor) writeInto(w pointsWriter, stmt *influxql.SelectStatement, row *models.Row) error {
	if stmt.Target.Measurement.Database == "" {
		return errNoDatabaseInTarget
	}

	// It might seem a bit weird that this is where we do this, since we will have to
	// convert rows back to points. The Executors (both aggregate and raw) are complex
	// enough that changing them to write back to the DB is going to be clumsy
	//
	// it might seem weird to have the write be in the QueryExecutor, but the interweaving of
	// limitedRowWriter and ExecuteAggregate/Raw makes it ridiculously hard to make sure that the
	// results will be the same as when queried normally.
	name := stmt.Target.Measurement.Name
	if name == "" {
		name = row.Name
	}

	points, err := convertRowToPoints(name, row)
	if err != nil {
		return err
	}

	if err := w.WritePointsInto(&IntoWriteRequest{
		Database:        stmt.Target.Measurement.Database,
		RetentionPolicy: stmt.Target.Measurement.RetentionPolicy,
		Points:          points,
	}); err != nil {
		return err
	}

	return nil
}

var errNoDatabaseInTarget = errors.New("no database in target")

// convertRowToPoints will convert a query result Row into Points that can be written back in.
func convertRowToPoints(measurementName string, row *models.Row) ([]models.Point, error) {
	// figure out which parts of the result are the time and which are the fields
	timeIndex := -1
	fieldIndexes := make(map[string]int)
	for i, c := range row.Columns {
		if c == "time" {
			timeIndex = i
		} else {
			fieldIndexes[c] = i
		}
	}

	if timeIndex == -1 {
		return nil, errors.New("error finding time index in result")
	}

	points := make([]models.Point, 0, len(row.Values))
	for _, v := range row.Values {
		vals := make(map[string]interface{})
		for fieldName, fieldIndex := range fieldIndexes {
			val := v[fieldIndex]
			if val != nil {
				vals[fieldName] = v[fieldIndex]
			}
		}

		p, err := models.NewPoint(measurementName, models.NewTags(row.Tags), vals, v[timeIndex].(time.Time))
		if err != nil {
			// Drop points that can't be stored
			continue
		}

		points = append(points, p)
	}

	return points, nil
}

// NormalizeStatement adds a default database and policy to the measurements in statement.
func (e *StatementExecutor) NormalizeStatement(stmt influxql.Statement, defaultDatabase string) (err error) {
	influxql.WalkFunc(stmt, func(node influxql.Node) {
		if err != nil {
			return
		}
		switch node := node.(type) {
		case *influxql.ShowRetentionPoliciesStatement:
			if node.Database == "" {
				node.Database = defaultDatabase
			}
		case *influxql.ShowMeasurementsStatement:
			if node.Database == "" {
				node.Database = defaultDatabase
			}
		case *influxql.ShowTagKeysStatement:
			if node.Database == "" {
				node.Database = defaultDatabase
			}
		case *influxql.ShowTagValuesStatement:
			if node.Database == "" {
				node.Database = defaultDatabase
			}
		case *influxql.ShowMeasurementCardinalityStatement:
			if node.Database == "" {
				node.Database = defaultDatabase
			}
		case *influxql.ShowSeriesCardinalityStatement:
			if node.Database == "" {
				node.Database = defaultDatabase
			}
		case *influxql.Measurement:
			switch stmt.(type) {
			case *influxql.DropSeriesStatement, *influxql.DeleteSeriesStatement:
				// DB and RP not supported by these statements so don't rewrite into invalid
				// statements
			default:
				err = e.normalizeMeasurement(node, defaultDatabase)
			}
		}
	})
	return
}

func (e *StatementExecutor) normalizeMeasurement(m *influxql.Measurement, defaultDatabase string) error {
	// Targets (measurements in an INTO clause) can have blank names, which means it will be
	// the same as the measurement name it came from in the FROM clause.
	if !m.IsTarget && m.Name == "" && m.SystemIterator == "" && m.Regex == nil {
		return errors.New("invalid measurement")
	}

	// Measurement does not have an explicit database? Insert default.
	if m.Database == "" {
		m.Database = defaultDatabase
	}

	// The database must now be specified by this point.
	if m.Database == "" {
		return ErrDatabaseNameRequired
	}

	// Find database.
	di := e.MetaClient.Database(m.Database)
	if di == nil {
		return influxdb.ErrDatabaseNotFound(m.Database)
	}

	// If no retention policy was specified, use the default.
	if m.RetentionPolicy == "" {
		if di.DefaultRetentionPolicy == "" {
			return fmt.Errorf("default retention policy not set for: %s", di.Name)
		}
		m.RetentionPolicy = di.DefaultRetentionPolicy
	}
	return nil
}

// IntoWriteRequest is a partial copy of cluster.WriteRequest
type IntoWriteRequest struct {
	Database        string
	RetentionPolicy string
	Points          []models.Point
}

// TSDBStore is an interface for accessing the time series data store.
type TSDBStore interface {
	CreateShard(database, policy string, shardID uint64, enabled bool) error
	WriteToShard(shardID uint64, points []models.Point) error

	RestoreShard(id uint64, r io.Reader) error
	BackupShard(id uint64, since time.Time, w io.Writer) error

	DeleteDatabase(name string) error
	DeleteMeasurement(database, name string) error
	DeleteRetentionPolicy(database, name string) error
	DeleteSeries(database string, sources []influxql.Source, condition influxql.Expr) error
	DeleteShard(id uint64) error

	MeasurementNames(auth query.Authorizer, database string, cond influxql.Expr) ([][]byte, error)
	TagKeys(auth query.Authorizer, shardIDs []uint64, cond influxql.Expr) ([]tsdb.TagKeys, error)
	TagValues(auth query.Authorizer, shardIDs []uint64, cond influxql.Expr) ([]tsdb.TagValues, error)

	SeriesCardinality(database string) (int64, error)
	MeasurementsCardinality(database string) (int64, error)
}

var _ TSDBStore = LocalTSDBStore{}

// LocalTSDBStore embeds a tsdb.Store and implements IteratorCreator
// to satisfy the TSDBStore interface.
type LocalTSDBStore struct {
	*tsdb.Store
}

// ShardIteratorCreator is an interface for creating an IteratorCreator to access a specific shard.
type ShardIteratorCreator interface {
	ShardIteratorCreator(id uint64) query.IteratorCreator
}

// joinUint64 returns a comma-delimited string of uint64 numbers.
func joinUint64(a []uint64) string {
	var buf bytes.Buffer
	for i, x := range a {
		buf.WriteString(strconv.FormatUint(x, 10))
		if i < len(a)-1 {
			buf.WriteRune(',')
		}
	}
	return buf.String()
}

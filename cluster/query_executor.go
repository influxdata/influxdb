package cluster

import (
	"bytes"
	"errors"
	"expvar"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"sort"
	"strconv"
	"time"

	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/influxql"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/monitor"
	"github.com/influxdata/influxdb/services/meta"
)

// A QueryExecutor is responsible for processing a influxql.Query and
// executing all of the statements within, on nodes in a cluster.
type QueryExecutor struct {
	// Reference to local node.
	Node *influxdb.Node

	MetaClient MetaClient

	// TSDB storage for local node.
	TSDBStore TSDBStore

	// Holds monitoring data for SHOW STATS and SHOW DIAGNOSTICS.
	Monitor *monitor.Monitor

	// Used for rewriting points back into system for SELECT INTO statements.
	PointsWriter *PointsWriter

	// Used for managing and tracking running queries.
	QueryManager influxql.QueryManager

	// Query execution timeout.
	QueryTimeout time.Duration

	// Select statement limits
	MaxSelectPointN   int
	MaxSelectSeriesN  int
	MaxSelectBucketsN int

	// Remote execution timeout
	Timeout time.Duration

	// Output of all logging.
	// Defaults to discarding all log output.
	LogOutput io.Writer

	// expvar-based stats.
	statMap *expvar.Map
}

// Statistics for the QueryExecutor
const (
	statQueriesActive          = "queriesActive"   // Number of queries currently being executed
	statQueryExecutionDuration = "queryDurationNs" // Total (wall) time spent executing queries
)

// NewQueryExecutor returns a new instance of QueryExecutor.
func NewQueryExecutor() *QueryExecutor {
	return &QueryExecutor{
		Timeout:      DefaultShardMapperTimeout,
		QueryTimeout: DefaultQueryTimeout,
		LogOutput:    ioutil.Discard,
		statMap:      influxdb.NewStatistics("queryExecutor", "queryExecutor", nil),
	}
}

// ExecuteQuery executes each statement within a query.
func (e *QueryExecutor) ExecuteQuery(query *influxql.Query, database string, chunkSize int, closing chan struct{}) <-chan *influxql.Result {
	results := make(chan *influxql.Result)
	go e.executeQuery(query, database, chunkSize, closing, results)
	return results
}

func (e *QueryExecutor) executeQuery(query *influxql.Query, database string, chunkSize int, closing <-chan struct{}, results chan *influxql.Result) {
	defer close(results)
	defer e.recover(query, results)

	e.statMap.Add(statQueriesActive, 1)
	defer func(start time.Time) {
		e.statMap.Add(statQueriesActive, -1)
		e.statMap.Add(statQueryExecutionDuration, time.Since(start).Nanoseconds())
	}(time.Now())

	qerr := &influxql.QueryError{}
	var qid uint64
	if e.QueryManager != nil {
		var err error
		qid, closing, err = e.QueryManager.AttachQuery(&influxql.QueryParams{
			Query:       query,
			Database:    database,
			Timeout:     e.QueryTimeout,
			InterruptCh: closing,
			Error:       qerr,
		})
		if err != nil {
			results <- &influxql.Result{Err: err}
			return
		}

		defer e.QueryManager.KillQuery(qid)
	}

	logger := e.logger()

	var i int
	for ; i < len(query.Statements); i++ {
		stmt := query.Statements[i]

		// If a default database wasn't passed in by the caller, check the statement.
		defaultDB := database
		if defaultDB == "" {
			if s, ok := stmt.(influxql.HasDefaultDatabase); ok {
				defaultDB = s.DefaultDatabase()
			}
		}

		// Rewrite statements, if necessary.
		// This can occur on meta read statements which convert to SELECT statements.
		newStmt, err := influxql.RewriteStatement(stmt)
		if err != nil {
			results <- &influxql.Result{Err: err}
			break
		}
		stmt = newStmt

		// Normalize each statement.
		if err := e.normalizeStatement(stmt, defaultDB); err != nil {
			results <- &influxql.Result{Err: err}
			break
		}

		// Log each normalized statement.
		logger.Println(stmt.String())

		// Select statements are handled separately so that they can be streamed.
		if stmt, ok := stmt.(*influxql.SelectStatement); ok {
			if err := e.executeSelectStatement(stmt, chunkSize, i, qid, results, closing); err != nil {
				if err == influxql.ErrQueryInterrupted {
					err = qerr.Error()
				}
				results <- &influxql.Result{StatementID: i, Err: err}
				break
			}
			continue
		}

		var rows models.Rows
		switch stmt := stmt.(type) {
		case *influxql.AlterRetentionPolicyStatement:
			err = e.executeAlterRetentionPolicyStatement(stmt)
		case *influxql.CreateContinuousQueryStatement:
			err = e.executeCreateContinuousQueryStatement(stmt)
		case *influxql.CreateDatabaseStatement:
			err = e.executeCreateDatabaseStatement(stmt)
		case *influxql.CreateRetentionPolicyStatement:
			err = e.executeCreateRetentionPolicyStatement(stmt)
		case *influxql.CreateSubscriptionStatement:
			err = e.executeCreateSubscriptionStatement(stmt)
		case *influxql.CreateUserStatement:
			err = e.executeCreateUserStatement(stmt)
		case *influxql.DropContinuousQueryStatement:
			err = e.executeDropContinuousQueryStatement(stmt)
		case *influxql.DropDatabaseStatement:
			err = e.executeDropDatabaseStatement(stmt)
		case *influxql.DropMeasurementStatement:
			err = e.executeDropMeasurementStatement(stmt, database)
		case *influxql.DropSeriesStatement:
			err = e.executeDropSeriesStatement(stmt, database)
		case *influxql.DropRetentionPolicyStatement:
			err = e.executeDropRetentionPolicyStatement(stmt)
		case *influxql.DropServerStatement:
			err = influxql.ErrInvalidQuery
		case *influxql.DropShardStatement:
			err = e.executeDropShardStatement(stmt)
		case *influxql.DropSubscriptionStatement:
			err = e.executeDropSubscriptionStatement(stmt)
		case *influxql.DropUserStatement:
			err = e.executeDropUserStatement(stmt)
		case *influxql.GrantStatement:
			err = e.executeGrantStatement(stmt)
		case *influxql.GrantAdminStatement:
			err = e.executeGrantAdminStatement(stmt)
		case *influxql.KillQueryStatement:
			err = e.executeKillQueryStatement(stmt)
		case *influxql.RevokeStatement:
			err = e.executeRevokeStatement(stmt)
		case *influxql.RevokeAdminStatement:
			err = e.executeRevokeAdminStatement(stmt)
		case *influxql.ShowContinuousQueriesStatement:
			rows, err = e.executeShowContinuousQueriesStatement(stmt)
		case *influxql.ShowDatabasesStatement:
			rows, err = e.executeShowDatabasesStatement(stmt)
		case *influxql.ShowDiagnosticsStatement:
			rows, err = e.executeShowDiagnosticsStatement(stmt)
		case *influxql.ShowGrantsForUserStatement:
			rows, err = e.executeShowGrantsForUserStatement(stmt)
		case *influxql.ShowQueriesStatement:
			rows, err = e.executeShowQueriesStatement(stmt)
		case *influxql.ShowRetentionPoliciesStatement:
			rows, err = e.executeShowRetentionPoliciesStatement(stmt)
		case *influxql.ShowServersStatement:
			// TODO: corylanou add this back for single node
			err = influxql.ErrInvalidQuery
		case *influxql.ShowShardsStatement:
			rows, err = e.executeShowShardsStatement(stmt)
		case *influxql.ShowShardGroupsStatement:
			rows, err = e.executeShowShardGroupsStatement(stmt)
		case *influxql.ShowStatsStatement:
			rows, err = e.executeShowStatsStatement(stmt)
		case *influxql.ShowSubscriptionsStatement:
			rows, err = e.executeShowSubscriptionsStatement(stmt)
		case *influxql.ShowTagValuesStatement:
			rows, err = e.executeShowTagValuesStatement(stmt, database)
		case *influxql.ShowUsersStatement:
			rows, err = e.executeShowUsersStatement(stmt)
		case *influxql.SetPasswordUserStatement:
			err = e.executeSetPasswordUserStatement(stmt)
		default:
			err = influxql.ErrInvalidQuery
		}

		// Send results for each statement.
		results <- &influxql.Result{
			StatementID: i,
			Series:      rows,
			Err:         err,
		}

		// Stop after the first error.
		if err != nil {
			break
		}
	}

	// Send error results for any statements which were not executed.
	for ; i < len(query.Statements)-1; i++ {
		results <- &influxql.Result{
			StatementID: i,
			Err:         influxql.ErrNotExecuted,
		}
	}
}

func (e *QueryExecutor) recover(query *influxql.Query, results chan *influxql.Result) {
	if err := recover(); err != nil {
		results <- &influxql.Result{
			StatementID: -1,
			Err:         fmt.Errorf("%s [panic:%s]", query.String(), err),
		}
	}
}

func (e *QueryExecutor) executeAlterRetentionPolicyStatement(stmt *influxql.AlterRetentionPolicyStatement) error {
	rpu := &meta.RetentionPolicyUpdate{
		Duration:           stmt.Duration,
		ReplicaN:           stmt.Replication,
		ShardGroupDuration: stmt.ShardGroupDuration,
	}

	// Update the retention policy.
	if err := e.MetaClient.UpdateRetentionPolicy(stmt.Database, stmt.Name, rpu); err != nil {
		return err
	}

	// If requested, set as default retention policy.
	if stmt.Default {
		if err := e.MetaClient.SetDefaultRetentionPolicy(stmt.Database, stmt.Name); err != nil {
			return err
		}
	}

	return nil
}

func (e *QueryExecutor) executeCreateContinuousQueryStatement(q *influxql.CreateContinuousQueryStatement) error {
	return e.MetaClient.CreateContinuousQuery(q.Database, q.Name, q.String())
}

func (e *QueryExecutor) executeCreateDatabaseStatement(stmt *influxql.CreateDatabaseStatement) error {
	if !stmt.RetentionPolicyCreate {
		_, err := e.MetaClient.CreateDatabase(stmt.Name)
		return err
	}

	rpi := meta.NewRetentionPolicyInfo(stmt.RetentionPolicyName)
	rpi.Duration = stmt.RetentionPolicyDuration
	rpi.ReplicaN = stmt.RetentionPolicyReplication
	rpi.ShardGroupDuration = stmt.RetentionPolicyShardGroupDuration
	_, err := e.MetaClient.CreateDatabaseWithRetentionPolicy(stmt.Name, rpi)
	return err
}

func (e *QueryExecutor) executeCreateRetentionPolicyStatement(stmt *influxql.CreateRetentionPolicyStatement) error {
	rpi := meta.NewRetentionPolicyInfo(stmt.Name)
	rpi.Duration = stmt.Duration
	rpi.ReplicaN = stmt.Replication
	rpi.ShardGroupDuration = stmt.ShardGroupDuration

	// Create new retention policy.
	if _, err := e.MetaClient.CreateRetentionPolicy(stmt.Database, rpi); err != nil {
		return err
	}

	// If requested, set new policy as the default.
	if stmt.Default {
		if err := e.MetaClient.SetDefaultRetentionPolicy(stmt.Database, stmt.Name); err != nil {
			return err
		}
	}
	return nil
}

func (e *QueryExecutor) executeCreateSubscriptionStatement(q *influxql.CreateSubscriptionStatement) error {
	return e.MetaClient.CreateSubscription(q.Database, q.RetentionPolicy, q.Name, q.Mode, q.Destinations)
}

func (e *QueryExecutor) executeCreateUserStatement(q *influxql.CreateUserStatement) error {
	_, err := e.MetaClient.CreateUser(q.Name, q.Password, q.Admin)
	return err
}

func (e *QueryExecutor) executeDropContinuousQueryStatement(q *influxql.DropContinuousQueryStatement) error {
	return e.MetaClient.DropContinuousQuery(q.Database, q.Name)
}

// executeDropDatabaseStatement drops a database from the cluster.
// It does not return an error if the database was not found on any of
// the nodes, or in the Meta store.
func (e *QueryExecutor) executeDropDatabaseStatement(stmt *influxql.DropDatabaseStatement) error {
	// Remove the database from the Meta Store.
	if err := e.MetaClient.DropDatabase(stmt.Name); err != nil {
		return err
	}

	// Locally delete the datababse.
	return e.TSDBStore.DeleteDatabase(stmt.Name)
}

func (e *QueryExecutor) executeDropMeasurementStatement(stmt *influxql.DropMeasurementStatement, database string) error {
	if dbi, err := e.MetaClient.Database(database); err != nil {
		return err
	} else if dbi == nil {
		return influxql.ErrDatabaseNotFound(database)
	}

	// Locally drop the measurement
	return e.TSDBStore.DeleteMeasurement(database, stmt.Name)
}

func (e *QueryExecutor) executeDropSeriesStatement(stmt *influxql.DropSeriesStatement, database string) error {
	if dbi, err := e.MetaClient.Database(database); err != nil {
		return err
	} else if dbi == nil {
		return influxql.ErrDatabaseNotFound(database)
	}

	// Check for time in WHERE clause (not supported).
	if influxql.HasTimeExpr(stmt.Condition) {
		return errors.New("DROP SERIES doesn't support time in WHERE clause")
	}

	// Locally drop the series.
	return e.TSDBStore.DeleteSeries(database, stmt.Sources, stmt.Condition)
}

func (e *QueryExecutor) executeDropShardStatement(stmt *influxql.DropShardStatement) error {
	// Remove the shard reference from the Meta Store.
	if err := e.MetaClient.DropShard(stmt.ID); err != nil {
		return err
	}

	// Locally delete the shard.
	return e.TSDBStore.DeleteShard(stmt.ID)
}

func (e *QueryExecutor) executeDropRetentionPolicyStatement(stmt *influxql.DropRetentionPolicyStatement) error {
	if err := e.MetaClient.DropRetentionPolicy(stmt.Database, stmt.Name); err != nil {
		return err
	}

	// Locally drop the retention policy.
	return e.TSDBStore.DeleteRetentionPolicy(stmt.Database, stmt.Name)
}

func (e *QueryExecutor) executeDropSubscriptionStatement(q *influxql.DropSubscriptionStatement) error {
	return e.MetaClient.DropSubscription(q.Database, q.RetentionPolicy, q.Name)
}

func (e *QueryExecutor) executeDropUserStatement(q *influxql.DropUserStatement) error {
	return e.MetaClient.DropUser(q.Name)
}

func (e *QueryExecutor) executeGrantStatement(stmt *influxql.GrantStatement) error {
	return e.MetaClient.SetPrivilege(stmt.User, stmt.On, stmt.Privilege)
}

func (e *QueryExecutor) executeGrantAdminStatement(stmt *influxql.GrantAdminStatement) error {
	return e.MetaClient.SetAdminPrivilege(stmt.User, true)
}

func (e *QueryExecutor) executeKillQueryStatement(stmt *influxql.KillQueryStatement) error {
	if e.QueryManager == nil {
		return influxql.ErrNoQueryManager
	}
	return e.QueryManager.KillQuery(stmt.QueryID)
}

func (e *QueryExecutor) executeRevokeStatement(stmt *influxql.RevokeStatement) error {
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

func (e *QueryExecutor) executeRevokeAdminStatement(stmt *influxql.RevokeAdminStatement) error {
	return e.MetaClient.SetAdminPrivilege(stmt.User, false)
}

func (e *QueryExecutor) executeSetPasswordUserStatement(q *influxql.SetPasswordUserStatement) error {
	return e.MetaClient.UpdateUser(q.Name, q.Password)
}

func (e *QueryExecutor) executeSelectStatement(stmt *influxql.SelectStatement, chunkSize, statementID int, qid uint64, results chan *influxql.Result, closing <-chan struct{}) error {
	// It is important to "stamp" this time so that everywhere we evaluate `now()` in the statement is EXACTLY the same `now`
	now := time.Now().UTC()
	opt := influxql.SelectOptions{InterruptCh: closing}

	// Replace instances of "now()" with the current time, and check the resultant times.
	stmt.Condition = influxql.Reduce(stmt.Condition, &influxql.NowValuer{Now: now})
	var err error
	opt.MinTime, opt.MaxTime, err = influxql.TimeRange(stmt.Condition)
	if err != nil {
		return err
	}

	if opt.MaxTime.IsZero() {
		opt.MaxTime = now
	}
	if opt.MinTime.IsZero() {
		opt.MinTime = time.Unix(0, 0)
	}

	// Convert DISTINCT into a call.
	stmt.RewriteDistinct()

	// Remove "time" from fields list.
	stmt.RewriteTimeFields()

	// Create an iterator creator based on the shards in the cluster.
	ic, err := e.iteratorCreator(stmt, &opt)
	if err != nil {
		return err
	}

	// Expand regex sources to their actual source names.
	if stmt.Sources.HasRegex() {
		sources, err := ic.ExpandSources(stmt.Sources)
		if err != nil {
			return err
		}
		stmt.Sources = sources
	}

	// Rewrite wildcards, if any exist.
	tmp, err := stmt.RewriteWildcards(ic)
	if err != nil {
		return err
	}
	stmt = tmp

	if e.MaxSelectBucketsN > 0 && !stmt.IsRawQuery {
		interval, err := stmt.GroupByInterval()
		if err != nil {
			return err
		}

		if interval > 0 {
			// Determine the start and end time matched to the interval (may not match the actual times).
			min := opt.MinTime.Truncate(interval)
			max := opt.MaxTime.Truncate(interval).Add(interval)

			// Determine the number of buckets by finding the time span and dividing by the interval.
			buckets := int64(max.Sub(min)) / int64(interval)
			if int(buckets) > e.MaxSelectBucketsN {
				return fmt.Errorf("max select bucket count exceeded: %d buckets", buckets)
			}
		}
	}

	// Create a set of iterators from a selection.
	itrs, err := influxql.Select(stmt, ic, &opt)
	if err != nil {
		return err
	}

	if qid != 0 && e.MaxSelectPointN > 0 {
		monitor := influxql.PointLimitMonitor(itrs, influxql.DefaultStatsInterval, e.MaxSelectPointN)
		e.QueryManager.MonitorQuery(qid, monitor)
	}

	// Generate a row emitter from the iterator set.
	em := influxql.NewEmitter(itrs, stmt.TimeAscending(), chunkSize)
	em.Columns = stmt.ColumnNames()
	em.OmitTime = stmt.OmitTime
	defer em.Close()

	// Calculate initial stats across all iterators.
	stats := influxql.Iterators(itrs).Stats()
	if e.MaxSelectSeriesN > 0 && stats.SeriesN > e.MaxSelectSeriesN {
		return fmt.Errorf("max select series count exceeded: %d series", stats.SeriesN)
	}

	// Emit rows to the results channel.
	var writeN int64
	var emitted bool
	for {
		row := em.Emit()
		if row == nil {
			// Check if the query was interrupted while emitting.
			select {
			case <-closing:
				return influxql.ErrQueryInterrupted
			default:
			}
			break
		}

		result := &influxql.Result{
			StatementID: statementID,
			Series:      []*models.Row{row},
		}

		// Write points back into system for INTO statements.
		if stmt.Target != nil {
			if err := e.writeInto(stmt, row); err != nil {
				return err
			}
			writeN += int64(len(row.Values))
			continue
		}

		// Send results or exit if closing.
		select {
		case <-closing:
			return influxql.ErrQueryInterrupted
		case results <- result:
		}

		emitted = true
	}

	// Emit write count if an INTO statement.
	if stmt.Target != nil {
		results <- &influxql.Result{
			StatementID: statementID,
			Series: []*models.Row{{
				Name:    "result",
				Columns: []string{"time", "written"},
				Values:  [][]interface{}{{time.Unix(0, 0).UTC(), writeN}},
			}},
		}
		return nil
	}

	// Always emit at least one result.
	if !emitted {
		results <- &influxql.Result{
			StatementID: statementID,
			Series:      make([]*models.Row, 0),
		}
	}

	return nil
}

// iteratorCreator returns a new instance of IteratorCreator based on stmt.
func (e *QueryExecutor) iteratorCreator(stmt *influxql.SelectStatement, opt *influxql.SelectOptions) (influxql.IteratorCreator, error) {
	// Retrieve a list of shard IDs.
	shards, err := e.MetaClient.ShardsByTimeRange(stmt.Sources, opt.MinTime, opt.MaxTime)
	if err != nil {
		return nil, err
	}

	// Generate iterators for each node.
	ics := make([]influxql.IteratorCreator, 0)
	if err := func() error {
		for _, shard := range shards {
			ic := e.TSDBStore.ShardIteratorCreator(shard.ID)
			if ic == nil {
				continue
			}
			ics = append(ics, ic)
		}

		return nil
	}(); err != nil {
		influxql.IteratorCreators(ics).Close()
		return nil, err
	}

	return influxql.IteratorCreators(ics), nil
}

func (e *QueryExecutor) executeShowContinuousQueriesStatement(stmt *influxql.ShowContinuousQueriesStatement) (models.Rows, error) {
	dis, err := e.MetaClient.Databases()
	if err != nil {
		return nil, err
	}

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

func (e *QueryExecutor) executeShowDatabasesStatement(q *influxql.ShowDatabasesStatement) (models.Rows, error) {
	dis, err := e.MetaClient.Databases()
	if err != nil {
		return nil, err
	}

	row := &models.Row{Name: "databases", Columns: []string{"name"}}
	for _, di := range dis {
		row.Values = append(row.Values, []interface{}{di.Name})
	}
	return []*models.Row{row}, nil
}

func (e *QueryExecutor) executeShowDiagnosticsStatement(stmt *influxql.ShowDiagnosticsStatement) (models.Rows, error) {
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

func (e *QueryExecutor) executeShowFieldKeysStatement(stmt *influxql.ShowFieldKeysStatement, database string) (models.Rows, error) {
	// FIXME(benbjohnson): Rewrite to use new query engine.
	return e.TSDBStore.ExecuteShowFieldKeysStatement(stmt, database)
}

func (e *QueryExecutor) executeShowGrantsForUserStatement(q *influxql.ShowGrantsForUserStatement) (models.Rows, error) {
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

func (e *QueryExecutor) executeShowQueriesStatement(q *influxql.ShowQueriesStatement) (models.Rows, error) {
	return influxql.ExecuteShowQueriesStatement(e.QueryManager, q)
}

func (e *QueryExecutor) executeShowRetentionPoliciesStatement(q *influxql.ShowRetentionPoliciesStatement) (models.Rows, error) {
	di, err := e.MetaClient.Database(q.Database)
	if err != nil {
		return nil, err
	} else if di == nil {
		return nil, influxdb.ErrDatabaseNotFound(q.Database)
	}

	row := &models.Row{Columns: []string{"name", "duration", "shardGroupDuration", "replicaN", "default"}}
	for _, rpi := range di.RetentionPolicies {
		row.Values = append(row.Values, []interface{}{rpi.Name, rpi.Duration.String(), rpi.ShardGroupDuration.String(), rpi.ReplicaN, di.DefaultRetentionPolicy == rpi.Name})
	}
	return []*models.Row{row}, nil
}

func (e *QueryExecutor) executeShowShardsStatement(stmt *influxql.ShowShardsStatement) (models.Rows, error) {
	dis, err := e.MetaClient.Databases()
	if err != nil {
		return nil, err
	}

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

func (e *QueryExecutor) executeShowShardGroupsStatement(stmt *influxql.ShowShardGroupsStatement) (models.Rows, error) {
	dis, err := e.MetaClient.Databases()
	if err != nil {
		return nil, err
	}

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

func (e *QueryExecutor) executeShowStatsStatement(stmt *influxql.ShowStatsStatement) (models.Rows, error) {
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

func (e *QueryExecutor) executeShowSubscriptionsStatement(stmt *influxql.ShowSubscriptionsStatement) (models.Rows, error) {
	dis, err := e.MetaClient.Databases()
	if err != nil {
		return nil, err
	}

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

func (e *QueryExecutor) executeShowTagValuesStatement(stmt *influxql.ShowTagValuesStatement, database string) (models.Rows, error) {
	return e.TSDBStore.ExecuteShowTagValuesStatement(stmt, database)
}

func (e *QueryExecutor) executeShowUsersStatement(q *influxql.ShowUsersStatement) (models.Rows, error) {
	row := &models.Row{Columns: []string{"user", "admin"}}
	for _, ui := range e.MetaClient.Users() {
		row.Values = append(row.Values, []interface{}{ui.Name, ui.Admin})
	}
	return []*models.Row{row}, nil
}

func (e *QueryExecutor) logger() *log.Logger {
	return log.New(e.LogOutput, "[query] ", log.LstdFlags)
}

func (e *QueryExecutor) writeInto(stmt *influxql.SelectStatement, row *models.Row) error {
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

	if err := e.PointsWriter.WritePointsInto(&IntoWriteRequest{
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

		p, err := models.NewPoint(measurementName, row.Tags, vals, v[timeIndex].(time.Time))
		if err != nil {
			// Drop points that can't be stored
			continue
		}

		points = append(points, p)
	}

	return points, nil
}

// normalizeStatement adds a default database and policy to the measurements in statement.
func (e *QueryExecutor) normalizeStatement(stmt influxql.Statement, defaultDatabase string) (err error) {
	influxql.WalkFunc(stmt, func(node influxql.Node) {
		if err != nil {
			return
		}
		switch node := node.(type) {
		case *influxql.Measurement:
			e := e.normalizeMeasurement(node, defaultDatabase)
			if e != nil {
				err = e
				return
			}
		}
	})
	return
}

func (e *QueryExecutor) normalizeMeasurement(m *influxql.Measurement, defaultDatabase string) error {
	// Targets (measurements in an INTO clause) can have blank names, which means it will be
	// the same as the measurement name it came from in the FROM clause.
	if !m.IsTarget && m.Name == "" && m.Regex == nil {
		return errors.New("invalid measurement")
	}

	// Measurement does not have an explicit database? Insert default.
	if m.Database == "" {
		m.Database = defaultDatabase
	}

	// The database must now be specified by this point.
	if m.Database == "" {
		return errors.New("database name required")
	}

	// Find database.
	di, err := e.MetaClient.Database(m.Database)
	if err != nil {
		return err
	} else if di == nil {
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
	CreateShard(database, policy string, shardID uint64) error
	WriteToShard(shardID uint64, points []models.Point) error

	DeleteDatabase(name string) error
	DeleteMeasurement(database, name string) error
	DeleteRetentionPolicy(database, name string) error
	DeleteSeries(database string, sources []influxql.Source, condition influxql.Expr) error
	DeleteShard(id uint64) error
	ExecuteShowFieldKeysStatement(stmt *influxql.ShowFieldKeysStatement, database string) (models.Rows, error)
	ExecuteShowTagValuesStatement(stmt *influxql.ShowTagValuesStatement, database string) (models.Rows, error)
	ExpandSources(sources influxql.Sources) (influxql.Sources, error)
	ShardIteratorCreator(id uint64) influxql.IteratorCreator
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

// stringSet represents a set of strings.
type stringSet map[string]struct{}

// newStringSet returns an empty stringSet.
func newStringSet() stringSet {
	return make(map[string]struct{})
}

// add adds strings to the set.
func (s stringSet) add(ss ...string) {
	for _, n := range ss {
		s[n] = struct{}{}
	}
}

// contains returns whether the set contains the given string.
func (s stringSet) contains(ss string) bool {
	_, ok := s[ss]
	return ok
}

// list returns the current elements in the set, in sorted order.
func (s stringSet) list() []string {
	l := make([]string, 0, len(s))
	for k := range s {
		l = append(l, k)
	}
	sort.Strings(l)
	return l
}

// union returns the union of this set and another.
func (s stringSet) union(o stringSet) stringSet {
	ns := newStringSet()
	for k := range s {
		ns[k] = struct{}{}
	}
	for k := range o {
		ns[k] = struct{}{}
	}
	return ns
}

// intersect returns the intersection of this set and another.
func (s stringSet) intersect(o stringSet) stringSet {
	shorter, longer := s, o
	if len(longer) < len(shorter) {
		shorter, longer = longer, shorter
	}

	ns := newStringSet()
	for k := range shorter {
		if _, ok := longer[k]; ok {
			ns[k] = struct{}{}
		}
	}
	return ns
}

type uint64Slice []uint64

func (a uint64Slice) Len() int           { return len(a) }
func (a uint64Slice) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a uint64Slice) Less(i, j int) bool { return a[i] < a[j] }

package coordinator

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/authorizer"
	iql "github.com/influxdata/influxdb/v2/influxql"
	"github.com/influxdata/influxdb/v2/influxql/query"
	"github.com/influxdata/influxdb/v2/models"
	"github.com/influxdata/influxdb/v2/pkg/tracing"
	"github.com/influxdata/influxdb/v2/pkg/tracing/fields"
	"github.com/influxdata/influxdb/v2/tsdb"
	"github.com/influxdata/influxdb/v2/v1/services/meta"
	"github.com/influxdata/influxql"
)

// ErrDatabaseNameRequired is returned when executing statements that require a database,
// when a database has not been provided.
var ErrDatabaseNameRequired = errors.New("database name required")

// StatementExecutor executes a statement in the query.
type StatementExecutor struct {
	MetaClient MetaClient

	// TSDB storage for local node.
	TSDBStore TSDBStore

	// ShardMapper for mapping shards when executing a SELECT statement.
	ShardMapper query.ShardMapper

	DBRP influxdb.DBRPMappingServiceV2

	// Select statement limits
	MaxSelectPointN   int
	MaxSelectSeriesN  int
	MaxSelectBucketsN int
}

// ExecuteStatement executes the given statement with the given execution context.
func (e *StatementExecutor) ExecuteStatement(ctx context.Context, stmt influxql.Statement, ectx *query.ExecutionContext) error {
	// Select statements are handled separately so that they can be streamed.
	if stmt, ok := stmt.(*influxql.SelectStatement); ok {
		return e.executeSelectStatement(ctx, stmt, ectx)
	}

	var rows models.Rows
	var messages []*query.Message
	var err error
	switch stmt := stmt.(type) {
	case *influxql.AlterRetentionPolicyStatement:
		err = iql.ErrNotImplemented("ALTER RETENTION POLICY")
	case *influxql.CreateContinuousQueryStatement:
		err = iql.ErrNotImplemented("CREATE CONTINUOUS QUERY")
	case *influxql.CreateDatabaseStatement:
		err = iql.ErrNotImplemented("CREATE DATABASE")
	case *influxql.CreateRetentionPolicyStatement:
		err = iql.ErrNotImplemented("CREATE RETENTION POLICY")
	case *influxql.CreateSubscriptionStatement:
		err = iql.ErrNotImplemented("CREATE SUBSCRIPTION")
	case *influxql.CreateUserStatement:
		err = iql.ErrNotImplemented("CREATE USER")
	case *influxql.DeleteSeriesStatement:
		return e.executeDeleteSeriesStatement(ctx, stmt, ectx.Database, ectx)
	case *influxql.DropContinuousQueryStatement:
		err = iql.ErrNotImplemented("DROP CONTINUOUS QUERY")
	case *influxql.DropDatabaseStatement:
		err = iql.ErrNotImplemented("DROP DATABASE")
	case *influxql.DropMeasurementStatement:
		return e.executeDropMeasurementStatement(ctx, stmt, ectx.Database, ectx)
	case *influxql.DropSeriesStatement:
		err = iql.ErrNotImplemented("DROP SERIES")
	case *influxql.DropRetentionPolicyStatement:
		err = iql.ErrNotImplemented("DROP RETENTION POLICY")
	case *influxql.DropShardStatement:
		err = iql.ErrNotImplemented("DROP SHARD")
	case *influxql.DropSubscriptionStatement:
		err = iql.ErrNotImplemented("DROP SUBSCRIPTION")
	case *influxql.DropUserStatement:
		err = iql.ErrNotImplemented("DROP USER")
	case *influxql.ExplainStatement:
		if stmt.Analyze {
			rows, err = e.executeExplainAnalyzeStatement(ctx, stmt, ectx)
		} else {
			rows, err = e.executeExplainStatement(ctx, stmt, ectx)
		}
	case *influxql.GrantStatement:
		err = iql.ErrNotImplemented("GRANT")
	case *influxql.GrantAdminStatement:
		err = iql.ErrNotImplemented("GRANT ALL")
	case *influxql.RevokeStatement:
		err = iql.ErrNotImplemented("REVOKE")
	case *influxql.RevokeAdminStatement:
		err = iql.ErrNotImplemented("REVOKE ALL")
	case *influxql.ShowContinuousQueriesStatement:
		rows, err = nil, iql.ErrNotImplemented("SHOW CONTINUOUS QUERIES")
	case *influxql.ShowDatabasesStatement:
		rows, err = e.executeShowDatabasesStatement(ctx, stmt, ectx)
	case *influxql.ShowDiagnosticsStatement:
		rows, err = nil, iql.ErrNotImplemented("SHOW DIAGNOSTICS")
	case *influxql.ShowGrantsForUserStatement:
		rows, err = nil, iql.ErrNotImplemented("SHOW GRANTS")
	case *influxql.ShowMeasurementsStatement:
		return e.executeShowMeasurementsStatement(ctx, stmt, ectx)
	case *influxql.ShowMeasurementCardinalityStatement:
		rows, err = nil, iql.ErrNotImplemented("SHOW MEASUREMENT CARDINALITY")
	case *influxql.ShowRetentionPoliciesStatement:
		rows, err = e.executeShowRetentionPoliciesStatement(ctx, stmt, ectx)
	case *influxql.ShowSeriesCardinalityStatement:
		rows, err = nil, iql.ErrNotImplemented("SHOW SERIES CARDINALITY")
	case *influxql.ShowShardsStatement:
		rows, err = nil, iql.ErrNotImplemented("SHOW SHARDS")
	case *influxql.ShowShardGroupsStatement:
		rows, err = nil, iql.ErrNotImplemented("SHOW SHARD GROUPS")
	case *influxql.ShowStatsStatement:
		rows, err = nil, iql.ErrNotImplemented("SHOW STATS")
	case *influxql.ShowSubscriptionsStatement:
		rows, err = nil, iql.ErrNotImplemented("SHOW SUBSCRIPTIONS")
	case *influxql.ShowTagKeysStatement:
		return e.executeShowTagKeys(ctx, stmt, ectx)
	case *influxql.ShowTagValuesStatement:
		return e.executeShowTagValues(ctx, stmt, ectx)
	case *influxql.ShowUsersStatement:
		rows, err = nil, iql.ErrNotImplemented("SHOW USERS")
	case *influxql.SetPasswordUserStatement:
		err = iql.ErrNotImplemented("SET PASSWORD")
	case *influxql.ShowQueriesStatement, *influxql.KillQueryStatement:
		err = iql.ErrNotImplemented("SHOW QUERIES")
	default:
		return query.ErrInvalidQuery
	}

	if err != nil {
		return err
	}

	return ectx.Send(ctx, &query.Result{
		Series:   rows,
		Messages: messages,
	})
}

func (e *StatementExecutor) executeExplainStatement(ctx context.Context, q *influxql.ExplainStatement, ectx *query.ExecutionContext) (models.Rows, error) {
	opt := query.SelectOptions{
		OrgID:       ectx.OrgID,
		NodeID:      ectx.ExecutionOptions.NodeID,
		MaxSeriesN:  e.MaxSelectSeriesN,
		MaxBucketsN: e.MaxSelectBucketsN,
	}

	// Prepare the query for execution, but do not actually execute it.
	// This should perform any needed substitutions.
	p, err := query.Prepare(ctx, q.Statement, e.ShardMapper, opt)
	if err != nil {
		return nil, err
	}
	defer p.Close()

	plan, err := p.Explain(ctx)
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

func (e *StatementExecutor) executeExplainAnalyzeStatement(ctx context.Context, q *influxql.ExplainStatement, ectx *query.ExecutionContext) (models.Rows, error) {
	stmt := q.Statement
	t, span := tracing.NewTrace("select")
	ctx = tracing.NewContextWithTrace(ctx, t)
	ctx = tracing.NewContextWithSpan(ctx, span)
	var aux query.Iterators
	ctx = query.NewContextWithIterators(ctx, &aux)
	start := time.Now()

	cur, err := e.createIterators(ctx, stmt, ectx.ExecutionOptions, ectx.StatisticsGatherer)
	if err != nil {
		return nil, err
	}

	iterTime := time.Since(start)

	// Generate a row emitter from the iterator set.
	em := query.NewEmitter(cur, ectx.ChunkSize)

	// Emit rows to the results channel.
	var writeN int64
	for {
		var row *models.Row
		row, _, err = em.Emit()
		if err != nil {
			goto CLEANUP
		} else if row == nil {
			// Check if the query was interrupted while emitting.
			if err = ctx.Err(); err != nil {
				goto CLEANUP
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

func (e *StatementExecutor) executeSelectStatement(ctx context.Context, stmt *influxql.SelectStatement, ectx *query.ExecutionContext) error {
	cur, err := e.createIterators(ctx, stmt, ectx.ExecutionOptions, ectx.StatisticsGatherer)
	if err != nil {
		return err
	}

	// Generate a row emitter from the iterator set.
	em := query.NewEmitter(cur, ectx.ChunkSize)
	defer em.Close()

	// Emit rows to the results channel.
	var emitted bool

	if stmt.Target != nil {
		// SELECT INTO is unsupported
		return iql.ErrNotImplemented("SELECT INTO")
	}

	for {
		row, partial, err := em.Emit()
		if err != nil {
			return err
		} else if row == nil {
			// Check if the query was interrupted while emitting.
			if err := ctx.Err(); err != nil {
				return err
			}
			break
		}

		result := &query.Result{
			Series:  []*models.Row{row},
			Partial: partial,
		}

		// Send results or exit if closing.
		if err := ectx.Send(ctx, result); err != nil {
			return err
		}

		emitted = true
	}

	// Always emit at least one result.
	if !emitted {
		return ectx.Send(ctx, &query.Result{
			Series: make([]*models.Row, 0),
		})
	}

	return nil
}

func (e *StatementExecutor) createIterators(ctx context.Context, stmt *influxql.SelectStatement, opt query.ExecutionOptions, gatherer *iql.StatisticsGatherer) (query.Cursor, error) {
	defer func(start time.Time) {
		dur := time.Since(start)
		gatherer.Append(iql.NewImmutableCollector(iql.Statistics{PlanDuration: dur}))
	}(time.Now())

	sopt := query.SelectOptions{
		OrgID:              opt.OrgID,
		NodeID:             opt.NodeID,
		MaxSeriesN:         e.MaxSelectSeriesN,
		MaxPointN:          e.MaxSelectPointN,
		MaxBucketsN:        e.MaxSelectBucketsN,
		StatisticsGatherer: gatherer,
	}

	// Create a set of iterators from a selection.
	cur, err := query.Select(ctx, stmt, e.ShardMapper, sopt)
	if err != nil {
		return nil, err
	}
	return cur, nil
}

func (e *StatementExecutor) executeShowDatabasesStatement(ctx context.Context, q *influxql.ShowDatabasesStatement, ectx *query.ExecutionContext) (models.Rows, error) {
	row := &models.Row{Name: "databases", Columns: []string{"name"}}
	dbrps, _, err := e.DBRP.FindMany(ctx, influxdb.DBRPMappingFilterV2{
		OrgID: &ectx.OrgID,
	})
	if err != nil {
		return nil, err
	}

	seenDbs := make(map[string]struct{}, len(dbrps))
	for _, dbrp := range dbrps {
		if _, ok := seenDbs[dbrp.Database]; ok {
			continue
		}

		perm, err := influxdb.NewPermissionAtID(dbrp.BucketID, influxdb.ReadAction, influxdb.BucketsResourceType, dbrp.OrganizationID)
		if err != nil {
			return nil, err
		}
		err = authorizer.IsAllowed(ctx, *perm)
		if err != nil {
			if influxdb.ErrorCode(err) == influxdb.EUnauthorized {
				continue
			}
			return nil, err
		}
		seenDbs[dbrp.Database] = struct{}{}
		row.Values = append(row.Values, []interface{}{dbrp.Database})
	}
	return []*models.Row{row}, nil
}

func (e *StatementExecutor) getDefaultRP(ctx context.Context, database string, ectx *query.ExecutionContext) (*influxdb.DBRPMappingV2, error) {
	defaultRP := true
	mappings, n, err := e.DBRP.FindMany(ctx, influxdb.DBRPMappingFilterV2{
		OrgID:    &ectx.OrgID,
		Database: &database,
		Default:  &defaultRP,
	})
	if err != nil {
		return nil, fmt.Errorf("finding DBRP mappings: %v", err)
	} else if n == 0 {
		return nil, fmt.Errorf("default retention policy not set for: %s", database)
	} else if n != 1 {
		return nil, fmt.Errorf("finding DBRP mappings: expected 1, found %d", n)
	}
	return mappings[0], nil
}

func (e *StatementExecutor) executeDeleteSeriesStatement(ctx context.Context, q *influxql.DeleteSeriesStatement, database string, ectx *query.ExecutionContext) error {
	mapping, err := e.getDefaultRP(ctx, database, ectx)
	if err != nil {
		return err
	}

	// Convert "now()" to current time.
	q.Condition = influxql.Reduce(q.Condition, &influxql.NowValuer{Now: time.Now().UTC()})

	return e.TSDBStore.DeleteSeries(mapping.BucketID.String(), q.Sources, q.Condition)
}

func (e *StatementExecutor) executeDropMeasurementStatement(ctx context.Context, q *influxql.DropMeasurementStatement, database string, ectx *query.ExecutionContext) error {
	mapping, err := e.getDefaultRP(ctx, database, ectx)
	if err != nil {
		return err
	}
	return e.TSDBStore.DeleteMeasurement(mapping.BucketID.String(), q.Name)
}

func (e *StatementExecutor) executeShowMeasurementsStatement(ctx context.Context, q *influxql.ShowMeasurementsStatement, ectx *query.ExecutionContext) error {
	if q.Database == "" {
		return ErrDatabaseNameRequired
	}

	mapping, err := e.getDefaultRP(ctx, q.Database, ectx)
	if err != nil {
		return err
	}

	names, err := e.TSDBStore.MeasurementNames(ctx, ectx.Authorizer, mapping.BucketID.String(), q.Condition)
	if err != nil || len(names) == 0 {
		return ectx.Send(ctx, &query.Result{
			Err: err,
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
		return ectx.Send(ctx, &query.Result{})
	}

	return ectx.Send(ctx, &query.Result{
		Series: []*models.Row{{
			Name:    "measurements",
			Columns: []string{"name"},
			Values:  values,
		}},
	})
}

func (e *StatementExecutor) executeShowRetentionPoliciesStatement(ctx context.Context, q *influxql.ShowRetentionPoliciesStatement, ectx *query.ExecutionContext) (models.Rows, error) {
	if q.Database == "" {
		return nil, ErrDatabaseNameRequired
	}

	dbrps, _, err := e.DBRP.FindMany(ctx, influxdb.DBRPMappingFilterV2{
		OrgID:    &ectx.OrgID,
		Database: &q.Database,
	})

	if err != nil {
		return nil, err
	}

	row := &models.Row{Columns: []string{"name", "duration", "shardGroupDuration", "replicaN", "default"}}
	for _, dbrp := range dbrps {
		perm, err := influxdb.NewPermissionAtID(dbrp.BucketID, influxdb.ReadAction, influxdb.BucketsResourceType, dbrp.OrganizationID)
		if err != nil {
			return nil, err
		}
		err = authorizer.IsAllowed(ctx, *perm)
		if err != nil {
			if influxdb.ErrorCode(err) == influxdb.EUnauthorized {
				continue
			}
			return nil, err
		}
		row.Values = append(row.Values, []interface{}{dbrp.RetentionPolicy, "0s", "168h0m0s", 1, dbrp.Default})
	}

	return []*models.Row{row}, nil
}

func (e *StatementExecutor) executeShowTagKeys(ctx context.Context, q *influxql.ShowTagKeysStatement, ectx *query.ExecutionContext) error {
	if q.Database == "" {
		return ErrDatabaseNameRequired
	}

	mapping, err := e.getDefaultRP(ctx, q.Database, ectx)
	if err != nil {
		return err
	}

	// Determine shard set based on database and time range.
	// SHOW TAG KEYS returns all tag keys for the default retention policy.
	di := e.MetaClient.Database(mapping.BucketID.String())
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
		sgis, err := e.MetaClient.ShardGroupsByTimeRange(mapping.BucketID.String(), rpi.Name, timeRange.MinTime(), timeRange.MaxTime())
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

	tagKeys, err := e.TSDBStore.TagKeys(ctx, ectx.Authorizer, shardIDs, cond)
	if err != nil {
		return ectx.Send(ctx, &query.Result{
			Err: err,
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

		if err := ectx.Send(ctx, &query.Result{
			Series: []*models.Row{row},
		}); err != nil {
			return err
		}
		emitted = true
	}

	// Ensure at least one result is emitted.
	if !emitted {
		return ectx.Send(ctx, &query.Result{})
	}
	return nil
}

func (e *StatementExecutor) executeShowTagValues(ctx context.Context, q *influxql.ShowTagValuesStatement, ectx *query.ExecutionContext) error {
	if q.Database == "" {
		return ErrDatabaseNameRequired
	}

	mapping, err := e.getDefaultRP(ctx, q.Database, ectx)
	if err != nil {
		return err
	}

	// Determine shard set based on database and time range.
	// SHOW TAG VALUES returns all tag values for the default retention policy.
	di := e.MetaClient.Database(mapping.BucketID.String())
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
		sgis, err := e.MetaClient.ShardGroupsByTimeRange(mapping.BucketID.String(), rpi.Name, timeRange.MinTime(), timeRange.MaxTime())
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

	tagValues, err := e.TSDBStore.TagValues(ctx, ectx.Authorizer, shardIDs, cond)
	if err != nil {
		return ectx.Send(ctx, &query.Result{Err: err})
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

		if err := ectx.Send(ctx, &query.Result{
			Series: []*models.Row{row},
		}); err != nil {
			return err
		}
		emitted = true
	}

	// Ensure at least one result is emitted.
	if !emitted {
		return ectx.Send(ctx, &query.Result{})
	}
	return nil
}

// NormalizeStatement adds a default database and policy to the measurements in statement.
// Parameter defaultRetentionPolicy can be "".
func (e *StatementExecutor) NormalizeStatement(ctx context.Context, stmt influxql.Statement, defaultDatabase, defaultRetentionPolicy string, ectx *query.ExecutionContext) (err error) {
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
				err = e.normalizeMeasurement(ctx, node, defaultDatabase, defaultRetentionPolicy, ectx)
			}
		}
	})
	return
}

func (e *StatementExecutor) normalizeMeasurement(ctx context.Context, m *influxql.Measurement, defaultDatabase, defaultRetentionPolicy string, ectx *query.ExecutionContext) error {
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

	// TODO(sgc): Validate database; fetch default RP
	filter := influxdb.DBRPMappingFilterV2{
		OrgID:    &ectx.OrgID,
		Database: &m.Database,
	}

	res, _, err := e.DBRP.FindMany(ctx, filter)
	if err != nil {
		return err
	}

	if len(res) == 0 {
		return query.ErrDatabaseNotFound(m.Database)
	}

	// If no retention policy was specified, use the default.
	if m.RetentionPolicy == "" {
		if defaultRetentionPolicy != "" {
			m.RetentionPolicy = defaultRetentionPolicy
		} else if rp := mappings(res).DefaultRetentionPolicy(m.Database); rp != "" {
			m.RetentionPolicy = rp
		} else {
			return fmt.Errorf("default retention policy not set for: %s", m.Database)
		}
	}

	return nil
}

type mappings []*influxdb.DBRPMappingV2

func (m mappings) DefaultRetentionPolicy(db string) string {
	for _, v := range m {
		if v.Database == db && v.Default {
			return v.RetentionPolicy
		}
	}
	return ""
}

// TSDBStore is an interface for accessing the time series data store.
type TSDBStore interface {
	DeleteMeasurement(database, name string) error
	DeleteSeries(database string, sources []influxql.Source, condition influxql.Expr) error
	MeasurementNames(ctx context.Context, auth query.Authorizer, database string, cond influxql.Expr) ([][]byte, error)
	TagKeys(ctx context.Context, auth query.Authorizer, shardIDs []uint64, cond influxql.Expr) ([]tsdb.TagKeys, error)
	TagValues(ctx context.Context, auth query.Authorizer, shardIDs []uint64, cond influxql.Expr) ([]tsdb.TagValues, error)
}

var _ TSDBStore = LocalTSDBStore{}

// LocalTSDBStore embeds a tsdb.Store and implements IteratorCreator
// to satisfy the TSDBStore interface.
type LocalTSDBStore struct {
	*tsdb.Store
}

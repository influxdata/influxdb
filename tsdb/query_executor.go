package tsdb

import (
	"errors"
	"fmt"
	"log"
	"os"
	"runtime"
	"sort"
	"strings"

	"github.com/influxdb/influxdb/influxql"
	"github.com/influxdb/influxdb/meta"
)

type QueryExecutor struct {
	// The meta store for accessing and updating cluster and schema data.
	MetaStore interface {
		Database(name string) (*meta.DatabaseInfo, error)
		Databases() ([]meta.DatabaseInfo, error)
		User(name string) (*meta.UserInfo, error)
		AdminUserExists() (bool, error)
		Authenticate(username, password string) (*meta.UserInfo, error)
		RetentionPolicy(database, name string) (rpi *meta.RetentionPolicyInfo, err error)
	}

	// Executes statements relating to meta data.
	MetaStatementExecutor interface {
		ExecuteStatement(stmt influxql.Statement) *influxql.Result
	}

	// The stats service to report to
	Stats interface {
		Add(key string, delta int64)
		Inc(key string)
		Name() string
		Walk(f func(string, int64))
	}

	Logger *log.Logger

	// the local daata store
	store *Store
}

func NewQueryExecutor(store *Store) *QueryExecutor {
	return &QueryExecutor{
		store:  store,
		Logger: log.New(os.Stderr, "[query] ", log.LstdFlags),
	}
}

// Begin is for influxql/engine.go to use to get a transaction object to start the query
func (q *QueryExecutor) Begin() (influxql.Tx, error) {
	return newTx(q.MetaStore, q.store), nil
}

// Authorize user u to execute query q on database.
// database can be "" for queries that do not require a database.
// If u is nil, this means authorization is disabled.
func (q *QueryExecutor) Authorize(u *meta.UserInfo, query *influxql.Query, database string) error {
	const authErrLogFmt = "unauthorized request | user: %q | query: %q | database %q\n"

	if u == nil {
		q.Logger.Printf(authErrLogFmt, "", query.String(), database)
		return ErrAuthorize{text: "no user provided"}
	}

	// Cluster admins can do anything.
	if u.Admin {
		return nil
	}

	// Check each statement in the query.
	for _, stmt := range query.Statements {
		// Get the privileges required to execute the statement.
		privs := stmt.RequiredPrivileges()

		// Make sure the user has each privilege required to execute
		// the statement.
		for _, p := range privs {
			// Use the db name specified by the statement or the db
			// name passed by the caller if one wasn't specified by
			// the statement.
			dbname := p.Name
			if dbname == "" {
				dbname = database
			}

			// Check if user has required privilege.
			if !u.Authorize(p.Privilege, dbname) {
				var msg string
				if dbname == "" {
					msg = "requires cluster admin"
				} else {
					msg = fmt.Sprintf("requires %s privilege on %s", p.Privilege.String(), dbname)
				}
				q.Logger.Printf(authErrLogFmt, u.Name, query.String(), database)
				return ErrAuthorize{
					text: fmt.Sprintf("%s not authorized to execute '%s'.  %s", u.Name, stmt.String(), msg),
				}
			}
		}
	}
	return nil
}

// ExecuteQuery executes an InfluxQL query against the server.
// It sends results down the passed in chan and closes it when done. It will close the chan
// on the first statement that throws an error.
func (q *QueryExecutor) ExecuteQuery(query *influxql.Query, database string, chunkSize int) (chan *influxql.Result, error) {
	q.Stats.Add("queriesRx", int64(len(query.Statements)))

	// Execute each statement. Keep the iterator external so we can
	// track how many of the statements were executed
	results := make(chan *influxql.Result)
	go func() {
		var i int
		var stmt influxql.Statement
		for i, stmt = range query.Statements {
			// If a default database wasn't passed in by the caller,
			// try to get it from the statement.
			defaultDB := database
			if defaultDB == "" {
				if s, ok := stmt.(influxql.HasDefaultDatabase); ok {
					defaultDB = s.DefaultDatabase()
				}

			}

			// If we have a default database, normalize the statement with it.
			if defaultDB != "" {
				if err := q.normalizeStatement(stmt, defaultDB); err != nil {
					results <- &influxql.Result{Err: err}
					break
				}
			}

			var res *influxql.Result
			switch stmt := stmt.(type) {
			case *influxql.SelectStatement:
				if err := q.executeSelectStatement(i, stmt, database, results, chunkSize); err != nil {
					results <- &influxql.Result{Err: err}
					break
				}
			case *influxql.DropSeriesStatement:
				res = q.executeDropSeriesStatement(stmt, database)
			case *influxql.ShowSeriesStatement:
				res = q.executeShowSeriesStatement(stmt, database)
			case *influxql.DropMeasurementStatement:
				res = q.executeDropMeasurementStatement(stmt, database)
			case *influxql.ShowMeasurementsStatement:
				res = q.executeShowMeasurementsStatement(stmt, database)
			case *influxql.ShowTagKeysStatement:
				res = q.executeShowTagKeysStatement(stmt, database)
			case *influxql.ShowTagValuesStatement:
				res = q.executeShowTagValuesStatement(stmt, database)
			case *influxql.ShowFieldKeysStatement:
				res = q.executeShowFieldKeysStatement(stmt, database)
			case *influxql.ShowStatsStatement:
				res = q.executeShowStatsStatement(stmt)
			case *influxql.ShowDiagnosticsStatement:
				res = q.executeShowDiagnosticsStatement(stmt)
			case *influxql.DeleteStatement:
				res = &influxql.Result{Err: ErrInvalidQuery}
			default:
				// Delegate all meta statements to a separate executor.
				res = q.MetaStatementExecutor.ExecuteStatement(stmt)
			}

			if res != nil {
				// set the StatementID for the handler on the other side to combine results
				res.StatementID = i

				// If an error occurs then stop processing remaining statements.
				results <- res
				if res.Err != nil {
					break
				}
			}

			q.Stats.Inc("queriesExecuted")
		}

		// if there was an error send results that the remaining statements weren't executed
		for ; i < len(query.Statements)-1; i++ {
			results <- &influxql.Result{Err: ErrNotExecuted}
		}

		close(results)
	}()

	return results, nil
}

// executeSelectStatement plans and executes a select statement against a database.
func (q *QueryExecutor) executeSelectStatement(statementID int, stmt *influxql.SelectStatement, database string, results chan *influxql.Result, chunkSize int) error {
	// Perform any necessary query re-writing.
	stmt, err := q.rewriteSelectStatement(stmt)
	if err != nil {
		return err
	}

	// Plan statement execution.
	p := influxql.NewPlanner(q)
	e, err := p.Plan(stmt, chunkSize)
	if err != nil {
		return err
	}

	// Execute plan.
	ch := e.Execute()

	// Stream results from the channel. We should send an empty result if nothing comes through.
	resultSent := false
	for row := range ch {
		if row.Err != nil {
			return row.Err
		} else {
			resultSent = true
			results <- &influxql.Result{StatementID: statementID, Series: []*influxql.Row{row}}
		}
	}

	if !resultSent {
		results <- &influxql.Result{StatementID: statementID, Series: make([]*influxql.Row, 0)}
	}

	return nil
}

// rewriteSelectStatement performs any necessary query re-writing.
func (q *QueryExecutor) rewriteSelectStatement(stmt *influxql.SelectStatement) (*influxql.SelectStatement, error) {
	var err error

	// Expand regex expressions in the FROM clause.
	sources, err := q.expandSources(stmt.Sources)
	if err != nil {
		return nil, err
	}
	stmt.Sources = sources

	// Expand wildcards in the fields or GROUP BY.
	if stmt.HasWildcard() {
		stmt, err = q.expandWildcards(stmt)
		if err != nil {
			return nil, err
		}
	}

	stmt.RewriteDistinct()

	return stmt, nil
}

// expandWildcards returns a new SelectStatement with wildcards in the fields
// and/or GROUP BY exapnded with actual field names.
func (q *QueryExecutor) expandWildcards(stmt *influxql.SelectStatement) (*influxql.SelectStatement, error) {
	// If there are no wildcards in the statement, return it as-is.
	if !stmt.HasWildcard() {
		return stmt, nil
	}

	// Use sets to avoid duplicate field names.
	fieldSet := map[string]struct{}{}
	dimensionSet := map[string]struct{}{}

	var fields influxql.Fields
	var dimensions influxql.Dimensions

	// Iterate measurements in the FROM clause getting the fields & dimensions for each.
	for _, src := range stmt.Sources {
		if m, ok := src.(*influxql.Measurement); ok {
			// Lookup the database.
			db := q.store.DatabaseIndex(m.Database)
			if db == nil {
				return nil, ErrDatabaseNotFound(m.Database)
			}

			// Lookup the measurement in the database.
			mm := db.measurements[m.Name]
			if mm == nil {
				return nil, ErrMeasurementNotFound(m.String())
			}

			// Get the fields for this measurement.
			for name, _ := range mm.FieldNames {
				if _, ok := fieldSet[name]; ok {
					continue
				}
				fieldSet[name] = struct{}{}
				fields = append(fields, &influxql.Field{Expr: &influxql.VarRef{Val: name}})
			}

			// Get the dimensions for this measurement.
			for _, t := range mm.tagKeys() {
				if _, ok := dimensionSet[t]; ok {
					continue
				}
				dimensionSet[t] = struct{}{}
				dimensions = append(dimensions, &influxql.Dimension{Expr: &influxql.VarRef{Val: t}})
			}
		}
	}

	// Return a new SelectStatement with the wild cards rewritten.
	return stmt.RewriteWildcards(fields, dimensions), nil
}

// expandSources expands regex sources and removes duplicates.
// NOTE: sources must be normalized (db and rp set) before calling this function.
func (q *QueryExecutor) expandSources(sources influxql.Sources) (influxql.Sources, error) {
	// Use a map as a set to prevent duplicates. Two regexes might produce
	// duplicates when expanded.
	set := map[string]influxql.Source{}
	names := []string{}

	// Iterate all sources, expanding regexes when they're found.
	for _, source := range sources {
		switch src := source.(type) {
		case *influxql.Measurement:
			if src.Regex == nil {
				name := src.String()
				set[name] = src
				names = append(names, name)
				continue
			}

			// Lookup the database.
			db := q.store.DatabaseIndex(src.Database)
			if db == nil {
				return nil, ErrDatabaseNotFound(src.Database)
			}

			// Get measurements from the database that match the regex.
			measurements := db.measurementsByRegex(src.Regex.Val)

			// Add those measurments to the set.
			for _, m := range measurements {
				m2 := &influxql.Measurement{
					Database:        src.Database,
					RetentionPolicy: src.RetentionPolicy,
					Name:            m.Name,
				}

				name := m2.String()
				if _, ok := set[name]; !ok {
					set[name] = m2
					names = append(names, name)
				}
			}

		default:
			return nil, fmt.Errorf("expandSources: unsuported source type: %T", source)
		}
	}

	// Sort the list of source names.
	sort.Strings(names)

	// Convert set to a list of Sources.
	expanded := make(influxql.Sources, 0, len(set))
	for _, name := range names {
		expanded = append(expanded, set[name])
	}

	return expanded, nil
}

func (q *QueryExecutor) executeDropMeasurementStatement(stmt *influxql.DropMeasurementStatement, database string) *influxql.Result {
	panic("not yet implemented")
}

func (q *QueryExecutor) executeDropSeriesStatement(stmt *influxql.DropSeriesStatement, database string) *influxql.Result {
	panic("not yet implemented")
}

func (q *QueryExecutor) executeShowSeriesStatement(stmt *influxql.ShowSeriesStatement, database string) *influxql.Result {
	// Find the database.
	db := q.store.DatabaseIndex(database)
	if db == nil {
		return &influxql.Result{Err: ErrDatabaseNotFound(database)}
	}

	// Get the list of measurements we're interested in.
	measurements, err := measurementsFromSourceOrDB(stmt.Source, db)
	if err != nil {
		return &influxql.Result{Err: err}
	}

	// Create result struct that will be populated and returned.
	result := &influxql.Result{
		Series: make(influxql.Rows, 0, len(measurements)),
	}

	// Loop through measurements to build result. One result row / measurement.
	for _, m := range measurements {
		var ids seriesIDs

		if stmt.Condition != nil {
			// Get series IDs that match the WHERE clause.
			filters := map[uint64]influxql.Expr{}
			ids, _, _, err = m.walkWhereForSeriesIds(stmt.Condition, filters)
			if err != nil {
				return &influxql.Result{Err: err}
			}

			// If no series matched, then go to the next measurement.
			if len(ids) == 0 {
				continue
			}

			// TODO: check return of walkWhereForSeriesIds for fields
		} else {
			// No WHERE clause so get all series IDs for this measurement.
			ids = m.seriesIDs
		}

		// Make a new row for this measurement.
		r := &influxql.Row{
			Name:    m.Name,
			Columns: m.tagKeys(),
		}

		// Loop through series IDs getting matching tag sets.
		for _, id := range ids {
			if s, ok := m.seriesByID[id]; ok {
				values := make([]interface{}, 0, len(r.Columns)+1)
				values = append(values, id)
				for _, column := range r.Columns {
					values = append(values, s.Tags[column])
				}

				// Add the tag values to the row.
				r.Values = append(r.Values, values)
			}
		}
		// make the id the first column
		r.Columns = append([]string{"_id"}, r.Columns...)

		// Append the row to the result.
		result.Series = append(result.Series, r)
	}

	if stmt.Limit > 0 || stmt.Offset > 0 {
		result.Series = q.filterShowSeriesResult(stmt.Limit, stmt.Offset, result.Series)
	}

	return result
}

// filterShowSeriesResult will limit the number of series returned based on the limit and the offset.
// Unlike limit and offset on SELECT statements, the limit and offset don't apply to the number of Rows, but
// to the number of total Values returned, since each Value represents a unique series.
func (q *QueryExecutor) filterShowSeriesResult(limit, offset int, rows influxql.Rows) influxql.Rows {
	var filteredSeries influxql.Rows
	seriesCount := 0
	for _, r := range rows {
		var currentSeries [][]interface{}

		// filter the values
		for _, v := range r.Values {
			if seriesCount >= offset && seriesCount-offset < limit {
				currentSeries = append(currentSeries, v)
			}
			seriesCount++
		}

		// only add the row back in if there are some values in it
		if len(currentSeries) > 0 {
			r.Values = currentSeries
			filteredSeries = append(filteredSeries, r)
			if seriesCount > limit+offset {
				return filteredSeries
			}
		}
	}
	return filteredSeries
}

func (q *QueryExecutor) executeShowMeasurementsStatement(stmt *influxql.ShowMeasurementsStatement, database string) *influxql.Result {
	// Find the database.
	db := q.store.DatabaseIndex(database)
	if db == nil {
		return &influxql.Result{Err: ErrDatabaseNotFound(database)}
	}

	var measurements Measurements

	// If a WHERE clause was specified, filter the measurements.
	if stmt.Condition != nil {
		var err error
		measurements, err = db.measurementsByExpr(stmt.Condition)
		if err != nil {
			return &influxql.Result{Err: err}
		}
	} else {
		// Otherwise, get all measurements from the database.
		measurements = db.Measurements()
	}
	sort.Sort(measurements)

	offset := stmt.Offset
	limit := stmt.Limit

	// If OFFSET is past the end of the array, return empty results.
	if offset > len(measurements)-1 {
		return &influxql.Result{}
	}

	// Calculate last index based on LIMIT.
	end := len(measurements)
	if limit > 0 && offset+limit < end {
		limit = offset + limit
	} else {
		limit = end
	}

	// Make a result row to hold all measurement names.
	row := &influxql.Row{
		Name:    "measurements",
		Columns: []string{"name"},
	}

	// Add one value to the row for each measurement name.
	for i := offset; i < limit; i++ {
		m := measurements[i]
		v := interface{}(m.Name)
		row.Values = append(row.Values, []interface{}{v})
	}

	// Make a result.
	result := &influxql.Result{
		Series: influxql.Rows{row},
	}

	return result
}

func (q *QueryExecutor) executeShowTagKeysStatement(stmt *influxql.ShowTagKeysStatement, database string) *influxql.Result {
	// Find the database.
	db := q.store.DatabaseIndex(database)
	if db == nil {
		return &influxql.Result{Err: ErrDatabaseNotFound(database)}
	}

	// Get the list of measurements we're interested in.
	measurements, err := measurementsFromSourceOrDB(stmt.Source, db)
	if err != nil {
		return &influxql.Result{Err: err}
	}

	// Make result.
	result := &influxql.Result{
		Series: make(influxql.Rows, 0, len(measurements)),
	}

	// Add one row per measurement to the result.
	for _, m := range measurements {
		// TODO: filter tag keys by stmt.Condition

		// Get the tag keys in sorted order.
		keys := m.tagKeys()

		// Convert keys to an [][]interface{}.
		values := make([][]interface{}, 0, len(m.seriesByTagKeyValue))
		for _, k := range keys {
			v := interface{}(k)
			values = append(values, []interface{}{v})
		}

		// Make a result row for the measurement.
		r := &influxql.Row{
			Name:    m.Name,
			Columns: []string{"tagKey"},
			Values:  values,
		}

		result.Series = append(result.Series, r)
	}

	// TODO: LIMIT & OFFSET

	return result
}

func (q *QueryExecutor) executeShowTagValuesStatement(stmt *influxql.ShowTagValuesStatement, database string) *influxql.Result {
	// Find the database.
	db := q.store.DatabaseIndex(database)
	if db == nil {
		return &influxql.Result{Err: ErrDatabaseNotFound(database)}
	}

	// Get the list of measurements we're interested in.
	measurements, err := measurementsFromSourceOrDB(stmt.Source, db)
	if err != nil {
		return &influxql.Result{Err: err}
	}

	// Make result.
	result := &influxql.Result{
		Series: make(influxql.Rows, 0),
	}

	tagValues := make(map[string]stringSet)
	for _, m := range measurements {
		var ids seriesIDs

		if stmt.Condition != nil {
			// Get series IDs that match the WHERE clause.
			filters := map[uint64]influxql.Expr{}
			ids, _, _, err = m.walkWhereForSeriesIds(stmt.Condition, filters)
			if err != nil {
				return &influxql.Result{Err: err}
			}

			// If no series matched, then go to the next measurement.
			if len(ids) == 0 {
				continue
			}

			// TODO: check return of walkWhereForSeriesIds for fields
		} else {
			// No WHERE clause so get all series IDs for this measurement.
			ids = m.seriesIDs
		}

		for k, v := range m.tagValuesByKeyAndSeriesID(stmt.TagKeys, ids) {
			_, ok := tagValues[k]
			if !ok {
				tagValues[k] = v
			}
			tagValues[k] = tagValues[k].union(v)
		}
	}

	for k, v := range tagValues {
		r := &influxql.Row{
			Name:    k + "TagValues",
			Columns: []string{k},
		}

		vals := v.list()
		sort.Strings(vals)

		for _, val := range vals {
			v := interface{}(val)
			r.Values = append(r.Values, []interface{}{v})
		}

		result.Series = append(result.Series, r)
	}

	sort.Sort(result.Series)
	return result
}

func (q *QueryExecutor) executeShowFieldKeysStatement(stmt *influxql.ShowFieldKeysStatement, database string) *influxql.Result {
	var err error

	// Find the database.
	db := q.store.DatabaseIndex(database)
	if db == nil {
		return &influxql.Result{Err: ErrDatabaseNotFound(database)}
	}

	// Get the list of measurements we're interested in.
	measurements, err := measurementsFromSourceOrDB(stmt.Source, db)
	if err != nil {
		return &influxql.Result{Err: err}
	}

	// Make result.
	result := &influxql.Result{
		Series: make(influxql.Rows, 0, len(measurements)),
	}

	// Loop through measurements, adding a result row for each.
	for _, m := range measurements {
		// Create a new row.
		r := &influxql.Row{
			Name:    m.Name,
			Columns: []string{"fieldKey"},
		}

		// Get a list of field names from the measurement then sort them.
		names := make([]string, 0, len(m.FieldNames))
		for n, _ := range m.FieldNames {
			names = append(names, n)
		}
		sort.Strings(names)

		// Add the field names to the result row values.
		for _, n := range names {
			v := interface{}(n)
			r.Values = append(r.Values, []interface{}{v})
		}

		// Append the row to the result.
		result.Series = append(result.Series, r)
	}

	return result
}

// measurementsFromSourceOrDB returns a list of measurements from the
// statement passed in or, if the statement is nil, a list of all
// measurement names from the database passed in.
func measurementsFromSourceOrDB(stmt influxql.Source, db *DatabaseIndex) (Measurements, error) {
	var measurements Measurements
	if stmt != nil {
		// TODO: handle multiple measurement sources
		if m, ok := stmt.(*influxql.Measurement); ok {
			measurement := db.measurements[m.Name]
			if measurement == nil {
				return nil, ErrMeasurementNotFound(m.Name)
			}

			measurements = append(measurements, measurement)
		} else {
			return nil, errors.New("identifiers in FROM clause must be measurement names")
		}
	} else {
		// No measurements specified in FROM clause so get all measurements that have series.
		for _, m := range db.Measurements() {
			if len(m.seriesIDs) > 0 {
				measurements = append(measurements, m)
			}
		}
	}
	sort.Sort(measurements)

	return measurements, nil
}

// normalizeStatement adds a default database and policy to the measurements in statement.
func (q *QueryExecutor) normalizeStatement(stmt influxql.Statement, defaultDatabase string) (err error) {
	// Track prefixes for replacing field names.
	prefixes := make(map[string]string)

	// Qualify all measurements.
	influxql.WalkFunc(stmt, func(n influxql.Node) {
		if err != nil {
			return
		}
		switch n := n.(type) {
		case *influxql.Measurement:
			e := q.normalizeMeasurement(n, defaultDatabase)
			if e != nil {
				err = e
				return
			}
			prefixes[n.Name] = n.Name
		}
	})
	if err != nil {
		return err
	}

	// Replace all variable references that used measurement prefixes.
	influxql.WalkFunc(stmt, func(n influxql.Node) {
		switch n := n.(type) {
		case *influxql.VarRef:
			for k, v := range prefixes {
				if strings.HasPrefix(n.Val, k+".") {
					n.Val = v + "." + influxql.QuoteIdent(n.Val[len(k)+1:])
				}
			}
		}
	})

	return
}

// normalizeMeasurement inserts the default database or policy into all measurement names.
func (q *QueryExecutor) normalizeMeasurement(m *influxql.Measurement, defaultDatabase string) error {
	if defaultDatabase == "" {
		return errors.New("no default database specified")
	}
	if m.Name == "" && m.Regex == nil {
		return errors.New("invalid measurement")
	}

	if m.Database == "" {
		m.Database = defaultDatabase
	}

	// Find database.
	di, err := q.MetaStore.Database(m.Database)
	if err != nil {
		return err
	} else if di == nil {
		return ErrDatabaseNotFound(m.Database)
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

func (q *QueryExecutor) executeShowStatsStatement(stmt *influxql.ShowStatsStatement) *influxql.Result {
	var rows []*influxql.Row
	// Server stats.
	serverRow := &influxql.Row{Columns: []string{}}
	serverRow.Name = q.Stats.Name()
	var values []interface{}
	q.Stats.Walk(func(k string, v int64) {
		serverRow.Columns = append(serverRow.Columns, k)
		values = append(values, v)
	})
	serverRow.Values = append(serverRow.Values, values)
	rows = append(rows, serverRow)

	// Shard-level stats.
	// TODO: wire up shard level stats
	// for _, sh := range s.shards {
	// 	if sh.store == nil {
	// 		// No stats for non-local shards
	// 		continue
	// 	}

	// 	row := &influxql.Row{Columns: []string{}}
	// 	row.Name = sh.stats.Name()
	// 	var values []interface{}
	// 	sh.stats.Walk(func(k string, v int64) {
	// 		row.Columns = append(row.Columns, k)
	// 		values = append(values, v)
	// 	})
	// 	row.Values = append(row.Values, values)
	// 	rows = append(rows, row)
	// }

	return &influxql.Result{Series: rows}
}

func (q *QueryExecutor) executeShowDiagnosticsStatement(stmt *influxql.ShowDiagnosticsStatement) *influxql.Result {
	return &influxql.Result{Series: q.DiagnosticsAsRows()}
}

// DiagnosticsAsRows returns diagnostic information about the server, as a slice of
// InfluxQL rows.
func (q *QueryExecutor) DiagnosticsAsRows() []*influxql.Row {
	panic("not yet implemented")
	/*
	   s.mu.RLock()
	   defer s.mu.RUnlock()
	   now := time.Now().UTC()

	   // Common rows.
	   gd := NewGoDiagnostics()
	   sd := NewSystemDiagnostics()
	   md := NewMemoryDiagnostics()
	   bd := BuildDiagnostics{Version: s.Version, CommitHash: s.CommitHash}

	   // Common tagset.
	   tags := map[string]string{"serverID": strconv.FormatUint(s.id, 10)}

	   // Server row.
	   serverRow := &influxql.Row{
	       Name: "server_diag",
	       Columns: []string{"time", "startTime", "uptime", "id",
	           "path", "authEnabled", "index", "retentionAutoCreate", "numShards", "cqLastRun"},
	       Tags: tags,
	       Values: [][]interface{}{[]interface{}{now, startTime.String(), time.Since(startTime).String(), strconv.FormatUint(s.id, 10),
	           s.path, s.authenticationEnabled, int64(s.index), s.RetentionAutoCreate, len(s.shards), s.lastContinuousQueryRun.String()}},
	   }

	   // Shard groups.
	   shardGroupsRow := &influxql.Row{Columns: []string{}}
	   shardGroupsRow.Name = "shardGroups_diag"
	   shardGroupsRow.Columns = append(shardGroupsRow.Columns, "time", "database", "retentionPolicy", "id",
	       "startTime", "endTime", "duration", "numShards")
	   shardGroupsRow.Tags = tags
	   // Check all shard groups.
	   for _, db := range s.databases {
	       for _, rp := range db.policies {
	           for _, g := range rp.shardGroups {
	               shardGroupsRow.Values = append(shardGroupsRow.Values, []interface{}{now, db.name, rp.Name,
	                   strconv.FormatUint(g.ID, 10), g.StartTime.String(), g.EndTime.String(), g.Duration().String(), len(g.Shards)})
	           }
	       }
	   }

	   // Shards
	   shardsRow := &influxql.Row{Columns: []string{}}
	   shardsRow.Name = "shards_diag"
	   shardsRow.Columns = append(shardsRow.Columns, "time", "id", "dataNodes", "index", "path")
	   shardsRow.Tags = tags
	   for _, sh := range s.shards {
	       var nodes []string
	       for _, n := range sh.DataNodeIDs {
	           nodes = append(nodes, strconv.FormatUint(n, 10))
	       }
	       var path string
	       if sh.HasDataNodeID(s.id) {
	           path = sh.store.Path()
	       }
	       shardsRow.Values = append(shardsRow.Values, []interface{}{now, strconv.FormatUint(sh.ID, 10),
	           strings.Join(nodes, ","), strconv.FormatUint(sh.Index(), 10), path})
	   }

	   return []*influxql.Row{
	       gd.AsRow("server_go", tags),
	       sd.AsRow("server_system", tags),
	       md.AsRow("server_memory", tags),
	       bd.AsRow("server_build", tags),
	       serverRow,
	       shardGroupsRow,
	       shardsRow,
	   }
	*/
}

// ErrAuthorize represents an authorization error.
type ErrAuthorize struct {
	text string
}

// Error returns the text of the error.
func (e ErrAuthorize) Error() string {
	return e.text
}

// authorize satisfies isAuthorizationError
func (ErrAuthorize) authorize() {}

var (
	// ErrInvalidQuery is returned when executing an unknown query type.
	ErrInvalidQuery = errors.New("invalid query")

	// ErrNotExecuted is returned when a statement is not executed in a query.
	// This can occur when a previous statement in the same query has errored.
	ErrNotExecuted = errors.New("not executed")
)

func ErrDatabaseNotFound(name string) error { return Errorf("database not found: %s", name) }

func ErrMeasurementNotFound(name string) error { return Errorf("measurement not found: %s", name) }

func Errorf(format string, a ...interface{}) (err error) {
	if _, file, line, ok := runtime.Caller(2); ok {
		a = append(a, file, line)
		err = fmt.Errorf(format+" (%s:%d)", a...)
	} else {
		err = fmt.Errorf(format, a...)
	}
	return
}

package tsdb // import "github.com/influxdata/influxdb/tsdb"

import (
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/influxdata/influxdb/influxql"
	"github.com/influxdata/influxdb/models"
)

var (
	// ErrShardNotFound gets returned when trying to get a non existing shard.
	ErrShardNotFound = fmt.Errorf("shard not found")
	// ErrStoreClosed gets returned when trying to use a closed Store.
	ErrStoreClosed = fmt.Errorf("store is closed")
)

const (
	maintenanceCheckInterval = time.Minute
)

// Store manages shards and indexes for databases.
type Store struct {
	mu   sync.RWMutex
	path string

	databaseIndexes map[string]*DatabaseIndex

	// shardLocations is a map of shard IDs to both the associated
	// Shard, and meta information about where the shard is located on
	// disk.
	//
	// shardLocations stores mappings for all shards on all databases.
	shardLocations map[uint64]*shardLocation

	EngineOptions EngineOptions
	Logger        *log.Logger

	closing chan struct{}
	wg      sync.WaitGroup
	opened  bool
}

// NewStore returns a new store with the given path and a default configuration.
// The returned store must be initialized by calling Open before using it.
func NewStore(path string) *Store {
	opts := NewEngineOptions()
	opts.Config = NewConfig()

	return &Store{
		path:          path,
		EngineOptions: opts,
		Logger:        log.New(os.Stderr, "[store] ", log.LstdFlags),
	}
}

// Path returns the store's root path.
func (s *Store) Path() string { return s.path }

// Open initializes the store, creating all necessary directories, loading all
// shards and indexes and initializing periodic maintenance of all shards.
func (s *Store) Open() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.closing = make(chan struct{})

	s.shardLocations = map[uint64]*shardLocation{}
	s.databaseIndexes = map[string]*DatabaseIndex{}

	s.Logger.Printf("Using data dir: %v", s.Path())

	// Create directory.
	if err := os.MkdirAll(s.path, 0777); err != nil {
		return err
	}

	// TODO: Start AE for Node
	if err := s.loadIndexes(); err != nil {
		return err
	}

	if err := s.loadShards(); err != nil {
		return err
	}

	go s.periodicMaintenance()
	s.opened = true

	return nil
}

func (s *Store) loadIndexes() error {
	dbs, err := ioutil.ReadDir(s.path)
	if err != nil {
		return err
	}
	for _, db := range dbs {
		if !db.IsDir() {
			s.Logger.Printf("Skipping database dir: %s. Not a directory", db.Name())
			continue
		}
		s.databaseIndexes[db.Name()] = NewDatabaseIndex(db.Name())
	}
	return nil
}

func (s *Store) loadShards() error {
	// loop through the current database indexes
	for db := range s.databaseIndexes {
		rps, err := ioutil.ReadDir(filepath.Join(s.path, db))
		if err != nil {
			return err
		}

		for _, rp := range rps {
			// retention policies should be directories.  Skip anything that is not a dir.
			if !rp.IsDir() {
				s.Logger.Printf("Skipping retention policy dir: %s. Not a directory", rp.Name())
				continue
			}

			shards, err := ioutil.ReadDir(filepath.Join(s.path, db, rp.Name()))
			if err != nil {
				return err
			}
			for _, sh := range shards {
				path := filepath.Join(s.path, db, rp.Name(), sh.Name())
				walPath := filepath.Join(s.EngineOptions.Config.WALDir, db, rp.Name(), sh.Name())

				// Shard file names are numeric shardIDs
				shardID, err := strconv.ParseUint(sh.Name(), 10, 64)
				if err != nil {
					s.Logger.Printf("%s is not a valid ID. Skipping shard.", sh.Name())
					continue
				}

				shard := NewShard(shardID, s.databaseIndexes[db], path, walPath, s.EngineOptions)
				err = shard.Open()
				if err != nil {
					return fmt.Errorf("failed to open shard %d: %s", shardID, err)
				}

				s.shardLocations[shardID] = &shardLocation{Database: db, RetentionPolicy: rp.Name(), Shard: shard}
			}
		}
	}

	return nil
}

// Close closes the store and all associated shards. After calling Close accessing
// shards through the Store will result in ErrStoreClosed being returned.
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.opened {
		close(s.closing)
	}
	s.wg.Wait()

	for _, sl := range s.shardLocations {
		if err := sl.Shard.Close(); err != nil {
			return err
		}
	}
	s.opened = false
	s.shardLocations = nil
	s.databaseIndexes = nil

	return nil
}

// DatabaseIndexN returns the number of databases indicies in the store.
func (s *Store) DatabaseIndexN() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.databaseIndexes)
}

// Shard returns a shard by id.
func (s *Store) Shard(id uint64) *Shard {
	s.mu.RLock()
	defer s.mu.RUnlock()
	sl, ok := s.shardLocations[id]
	if !ok {
		return nil
	}
	return sl.Shard
}

// Shards returns a list of shards by id.
func (s *Store) Shards(ids []uint64) []*Shard {
	s.mu.RLock()
	defer s.mu.RUnlock()
	a := make([]*Shard, 0, len(ids))
	for _, id := range ids {
		sl, ok := s.shardLocations[id]
		if !ok {
			continue
		}
		a = append(a, sl.Shard)
	}
	return a
}

// ShardN returns the number of shards in the store.
func (s *Store) ShardN() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.shardLocations)
}

// CreateShard creates a shard with the given id and retention policy on a database.
func (s *Store) CreateShard(database, retentionPolicy string, shardID uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	select {
	case <-s.closing:
		return ErrStoreClosed
	default:
	}

	// shard already exists
	if _, ok := s.shardLocations[shardID]; ok {
		return nil
	}

	// created the db and retention policy dirs if they don't exist
	if err := os.MkdirAll(filepath.Join(s.path, database, retentionPolicy), 0700); err != nil {
		return err
	}

	// create the WAL directory
	walPath := filepath.Join(s.EngineOptions.Config.WALDir, database, retentionPolicy, fmt.Sprintf("%d", shardID))
	if err := os.MkdirAll(walPath, 0700); err != nil {
		return err
	}

	// create the database index if it does not exist
	db, ok := s.databaseIndexes[database]
	if !ok {
		db = NewDatabaseIndex(database)
		s.databaseIndexes[database] = db
	}

	shardPath := filepath.Join(s.path, database, retentionPolicy, strconv.FormatUint(shardID, 10))
	shard := NewShard(shardID, db, shardPath, walPath, s.EngineOptions)
	if err := shard.Open(); err != nil {
		return err
	}

	s.shardLocations[shardID] = &shardLocation{Database: database, RetentionPolicy: retentionPolicy, Shard: shard}

	return nil
}

// DeleteShard removes a shard from disk.
func (s *Store) DeleteShard(shardID uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.deleteShard(shardID)
}

// deleteShard removes a shard from disk. Callers of deleteShard need
// to handle locks appropriately.
func (s *Store) deleteShard(shardID uint64) error {
	// ensure shard exists
	sl, ok := s.shardLocations[shardID]
	if !ok {
		return nil
	}

	if err := sl.Shard.Close(); err != nil {
		return err
	}

	if err := os.RemoveAll(sl.Shard.path); err != nil {
		return err
	}

	if err := os.RemoveAll(sl.Shard.walPath); err != nil {
		return err
	}

	delete(s.shardLocations, shardID)
	return nil
}

// ShardIteratorCreator returns an iterator creator for a shard.
func (s *Store) ShardIteratorCreator(id uint64) influxql.IteratorCreator {
	sh := s.Shard(id)
	if sh == nil {
		return nil
	}
	return &shardIteratorCreator{sh: sh}
}

// DeleteDatabase will close all shards associated with a database and remove the directory and files from disk.
func (s *Store) DeleteDatabase(name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Close and delete all shards on the database.
	for shardID, location := range s.shardLocations {
		if location.IsDatabase(name) {
			// Delete the shard from disk.
			if err := s.deleteShard(shardID); err != nil {
				return err
			}
		}
	}

	if err := os.RemoveAll(filepath.Join(s.path, name)); err != nil {
		return err
	}
	if err := os.RemoveAll(filepath.Join(s.EngineOptions.Config.WALDir, name)); err != nil {
		return err
	}

	delete(s.databaseIndexes, name)
	return nil
}

// DeleteRetentionPolicy will close all shards associated with the
// provided retention policy, remove the retention policy directories on
// both the DB and WAL, and remove all shard files from disk.
func (s *Store) DeleteRetentionPolicy(database, name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Close and delete all shards under the retention policy on the
	// database.
	for shardID, location := range s.shardLocations {
		if location.IsDatabase(database) && location.IsRetentionPolicy(name) {
			// Delete the shard from disk.
			if err := s.deleteShard(shardID); err != nil {
				return err
			}
		}
	}

	// Remove the rentention policy folder.
	if err := os.RemoveAll(filepath.Join(s.path, database, name)); err != nil {
		return err
	}

	// Remove the retention policy folder from the the WAL.
	return os.RemoveAll(filepath.Join(s.EngineOptions.Config.WALDir, database, name))
}

// DeleteMeasurement removes a measurement and all associated series from a database.
func (s *Store) DeleteMeasurement(database, name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Find the database.
	db := s.databaseIndexes[database]
	if db == nil {
		return nil
	}

	// Find the measurement.
	m := db.Measurement(name)
	if m == nil {
		return influxql.ErrMeasurementNotFound(name)
	}

	// Remove measurement from index.
	db.DropMeasurement(m.Name)

	// Remove underlying data.
	for _, sl := range s.shardLocations {
		if !sl.IsDatabase(database) {
			continue
		}

		if err := sl.Shard.DeleteMeasurement(m.Name, m.SeriesKeys()); err != nil {
			return err
		}
	}

	return nil
}

// ShardIDs returns a slice of all ShardIDs under management.
func (s *Store) ShardIDs() []uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.shardIDs()
}

func (s *Store) shardIDs() []uint64 {
	a := make([]uint64, 0, len(s.shardLocations))
	for shardID := range s.shardLocations {
		a = append(a, shardID)
	}
	return a
}

// shardsSlice returns an ordered list of shards.
func (s *Store) shardsSlice() []*Shard {
	a := make([]*Shard, 0, len(s.shardLocations))
	for _, sl := range s.shardLocations {
		a = append(a, sl.Shard)
	}
	sort.Sort(Shards(a))
	return a
}

// DatabaseIndex returns the index for a database by its name.
func (s *Store) DatabaseIndex(name string) *DatabaseIndex {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.databaseIndexes[name]
}

// Databases returns all the databases in the indexes
func (s *Store) Databases() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	databases := make([]string, 0, len(s.databaseIndexes))
	for db := range s.databaseIndexes {
		databases = append(databases, db)
	}
	return databases
}

// Measurement returns a measurement by name from the given database.
func (s *Store) Measurement(database, name string) *Measurement {
	s.mu.RLock()
	db := s.databaseIndexes[database]
	s.mu.RUnlock()
	if db == nil {
		return nil
	}
	return db.Measurement(name)
}

// DiskSize returns the size of all the shard files in bytes.  This size does not include the WAL size.
func (s *Store) DiskSize() (int64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var size int64
	for _, shardID := range s.ShardIDs() {
		shard := s.Shard(shardID)
		sz, err := shard.DiskSize()
		if err != nil {
			return 0, err
		}
		size += sz
	}
	return size, nil
}

// BackupShard will get the shard and have the engine backup since the passed in time to the writer
func (s *Store) BackupShard(id uint64, since time.Time, w io.Writer) error {
	shard := s.Shard(id)
	if shard == nil {
		return fmt.Errorf("shard %d doesn't exist on this server", id)
	}

	path, err := relativePath(s.path, shard.path)
	if err != nil {
		return err
	}

	return shard.engine.Backup(w, path, since)
}

// ShardRelativePath will return the relative path to the shard. i.e. <database>/<retention>/<id>
func (s *Store) ShardRelativePath(id uint64) (string, error) {
	shard := s.Shard(id)
	if shard == nil {
		return "", fmt.Errorf("shard %d doesn't exist on this server", id)
	}
	return relativePath(s.path, shard.path)
}

// DeleteSeries loops through the local shards and deletes the series data and metadata for the passed in series keys
func (s *Store) DeleteSeries(database string, sources []influxql.Source, condition influxql.Expr) error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Find the database.
	db := s.DatabaseIndex(database)
	if db == nil {
		return nil
	}

	// Expand regex expressions in the FROM clause.
	a, err := s.expandSources(sources)
	if err != nil {
		return err
	} else if sources != nil && len(sources) != 0 && len(a) == 0 {
		return nil
	}
	sources = a

	measurements, err := measurementsFromSourcesOrDB(db, sources...)
	if err != nil {
		return err
	}

	var seriesKeys []string
	for _, m := range measurements {
		var ids SeriesIDs
		var filters FilterExprs
		if condition != nil {
			// Get series IDs that match the WHERE clause.
			ids, filters, err = m.walkWhereForSeriesIds(condition)
			if err != nil {
				return err
			}

			// Delete boolean literal true filter expressions.
			// These are returned for `WHERE tagKey = 'tagVal'` type expressions and are okay.
			filters.DeleteBoolLiteralTrues()

			// Check for unsupported field filters.
			// Any remaining filters means there were fields (e.g., `WHERE value = 1.2`).
			if filters.Len() > 0 {
				return errors.New("DROP SERIES doesn't support fields in WHERE clause")
			}
		} else {
			// No WHERE clause so get all series IDs for this measurement.
			ids = m.seriesIDs
		}

		for _, id := range ids {
			seriesKeys = append(seriesKeys, m.seriesByID[id].Key)
		}
	}

	// delete the raw series data
	if err := s.deleteSeries(database, seriesKeys); err != nil {
		return err
	}

	// remove them from the index
	db.DropSeries(seriesKeys)

	return nil
}

func (s *Store) deleteSeries(database string, seriesKeys []string) error {
	if _, ok := s.databaseIndexes[database]; !ok {
		return influxql.ErrDatabaseNotFound(database)
	}

	for _, sl := range s.shardLocations {
		if !sl.IsDatabase(database) {
			continue
		}
		if err := sl.Shard.DeleteSeries(seriesKeys); err != nil {
			return err
		}
	}
	return nil
}

// periodicMaintenance is the method called in a goroutine on the opening of the store
// to perform periodic maintenance of the shards.
func (s *Store) periodicMaintenance() {
	t := time.NewTicker(maintenanceCheckInterval)
	for {
		select {
		case <-t.C:
			s.performMaintenance()
		case <-s.closing:
			t.Stop()
			return
		}
	}
}

// performMaintenance loops through shards and executes any maintenance
// tasks. Those tasks should run in their own goroutines if they will
// take significant time.
func (s *Store) performMaintenance() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, sl := range s.shardLocations {
		s.performMaintenanceOnShard(sl.Shard)
	}
}

func (s *Store) performMaintenanceOnShard(shard *Shard) {
	defer func() {
		if r := recover(); r != nil {
			s.Logger.Printf("recovered error in maintenance on shard %d", shard.id)
		}
	}()
	shard.PerformMaintenance()
}

// ExpandSources expands regex sources and removes duplicates.
// NOTE: sources must be normalized (db and rp set) before calling this function.
func (s *Store) ExpandSources(sources influxql.Sources) (influxql.Sources, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.expandSources(sources)
}

func (s *Store) expandSources(sources influxql.Sources) (influxql.Sources, error) {
	// Use a map as a set to prevent duplicates.
	set := map[string]influxql.Source{}

	// Iterate all sources, expanding regexes when they're found.
	for _, source := range sources {
		switch src := source.(type) {
		case *influxql.Measurement:
			// Add non-regex measurements directly to the set.
			if src.Regex == nil {
				set[src.String()] = src
				continue
			}

			// Lookup the database.
			db := s.databaseIndexes[src.Database]
			if db == nil {
				return nil, nil
			}

			// Loop over matching measurements.
			for _, m := range db.measurementsByRegex(src.Regex.Val) {
				other := &influxql.Measurement{
					Database:        src.Database,
					RetentionPolicy: src.RetentionPolicy,
					Name:            m.Name,
				}
				set[other.String()] = other
			}

		default:
			return nil, fmt.Errorf("expandSources: unsupported source type: %T", source)
		}
	}

	// Convert set to sorted slice.
	names := make([]string, 0, len(set))
	for name := range set {
		names = append(names, name)
	}
	sort.Strings(names)

	// Convert set to a list of Sources.
	expanded := make(influxql.Sources, 0, len(set))
	for _, name := range names {
		expanded = append(expanded, set[name])
	}

	return expanded, nil
}

// WriteToShard writes a list of points to a shard identified by its ID.
func (s *Store) WriteToShard(shardID uint64, points []models.Point) error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	select {
	case <-s.closing:
		return ErrStoreClosed
	default:
	}

	sl, ok := s.shardLocations[shardID]
	if !ok {
		return ErrShardNotFound
	}

	return sl.Shard.WritePoints(points)
}

func (s *Store) ExecuteShowFieldKeysStatement(stmt *influxql.ShowFieldKeysStatement, database string) (models.Rows, error) {
	// NOTE(benbjohnson):
	// This function is temporarily moved here until reimplemented in the new query engine.

	// Find the database.
	db := s.DatabaseIndex(database)
	if db == nil {
		return nil, nil
	}

	// Expand regex expressions in the FROM clause.
	sources, err := s.ExpandSources(stmt.Sources)
	if err != nil {
		return nil, err
	}

	measurements, err := measurementsFromSourcesOrDB(db, sources...)
	if err != nil {
		return nil, err
	}

	// Make result.
	rows := make(models.Rows, 0, len(measurements))

	// Loop through measurements, adding a result row for each.
	for _, m := range measurements {
		// Create a new row.
		r := &models.Row{
			Name:    m.Name,
			Columns: []string{"fieldKey"},
		}

		// Get a list of field names from the measurement then sort them.
		names := m.FieldNames()
		sort.Strings(names)

		// Add the field names to the result row values.
		for _, n := range names {
			v := interface{}(n)
			r.Values = append(r.Values, []interface{}{v})
		}

		// Append the row to the result.
		rows = append(rows, r)
	}

	return rows, nil
}

func (s *Store) ExecuteShowSeriesStatement(stmt *influxql.ShowSeriesStatement, database string) (models.Rows, error) {
	// NOTE(benbjohnson):
	// This function is temporarily moved here until reimplemented in the new query engine.

	// Check for time in WHERE clause (not supported).
	if influxql.HasTimeExpr(stmt.Condition) {
		return nil, errors.New("SHOW SERIES doesn't support time in WHERE clause")
	}

	// Find the database.
	db := s.DatabaseIndex(database)
	if db == nil {
		return nil, nil
	}

	// Expand regex expressions in the FROM clause.
	sources, err := s.ExpandSources(stmt.Sources)
	if err != nil {
		return nil, err
	}

	// Get the list of measurements we're interested in.
	measurements, err := measurementsFromSourcesOrDB(db, sources...)
	if err != nil {
		return nil, err
	}

	// Create result struct that will be populated and returned.
	rows := make(models.Rows, 0, len(measurements))

	// Loop through measurements to build result. One result row / measurement.
	for _, m := range measurements {
		var ids SeriesIDs
		var filters FilterExprs

		if stmt.Condition != nil {
			// Get series IDs that match the WHERE clause.
			ids, filters, err = m.walkWhereForSeriesIds(stmt.Condition)
			if err != nil {
				return nil, err
			}

			// Delete boolean literal true filter expressions.
			filters.DeleteBoolLiteralTrues()

			// Check for unsupported field filters.
			if filters.Len() > 0 {
				return nil, errors.New("SHOW SERIES doesn't support fields in WHERE clause")
			}

			// If no series matched, then go to the next measurement.
			if len(ids) == 0 {
				continue
			}
		} else {
			// No WHERE clause so get all series IDs for this measurement.
			ids = m.seriesIDs
		}

		// Make a new row for this measurement.
		r := &models.Row{
			Name:    m.Name,
			Columns: m.TagKeys(),
		}

		// Loop through series IDs getting matching tag sets.
		for _, id := range ids {
			if s, ok := m.seriesByID[id]; ok {
				values := make([]interface{}, 0, len(r.Columns))

				// make the series key the first value
				values = append(values, s.Key)

				for _, column := range r.Columns {
					values = append(values, s.Tags[column])
				}

				// Add the tag values to the row.
				r.Values = append(r.Values, values)
			}
		}
		// make the id the first column
		r.Columns = append([]string{"_key"}, r.Columns...)

		// Append the row.
		rows = append(rows, r)
	}

	if stmt.Limit > 0 || stmt.Offset > 0 {
		rows = s.filterShowSeriesResult(stmt.Limit, stmt.Offset, rows)
	}

	return rows, nil
}

// filterShowSeriesResult will limit the number of series returned based on the limit and the offset.
// Unlike limit and offset on SELECT statements, the limit and offset don't apply to the number of Rows, but
// to the number of total Values returned, since each Value represents a unique series.
func (e *Store) filterShowSeriesResult(limit, offset int, rows models.Rows) models.Rows {
	var filteredSeries models.Rows
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

func (s *Store) ExecuteShowTagValuesStatement(stmt *influxql.ShowTagValuesStatement, database string) (models.Rows, error) {
	// NOTE(benbjohnson):
	// This function is temporarily moved here until reimplemented in the new query engine.

	// Check for time in WHERE clause (not supported).
	if influxql.HasTimeExpr(stmt.Condition) {
		return nil, errors.New("SHOW TAG VALUES doesn't support time in WHERE clause")
	}

	// Find the database.
	db := s.DatabaseIndex(database)
	if db == nil {
		return nil, nil
	}

	// Expand regex expressions in the FROM clause.
	sources, err := s.ExpandSources(stmt.Sources)
	if err != nil {
		return nil, err
	}

	// Get the list of measurements we're interested in.
	measurements, err := measurementsFromSourcesOrDB(db, sources...)
	if err != nil {
		return nil, err
	}

	// Make result.
	var rows models.Rows
	tagValues := make(map[string]stringSet)
	for _, m := range measurements {
		var ids SeriesIDs

		if stmt.Condition != nil {
			// Get series IDs that match the WHERE clause.
			ids, _, err = m.walkWhereForSeriesIds(stmt.Condition)
			if err != nil {
				return nil, err
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
		r := &models.Row{
			Name:    k + "TagValues",
			Columns: []string{k},
		}

		vals := v.list()
		sort.Strings(vals)

		for _, val := range vals {
			v := interface{}(val)
			r.Values = append(r.Values, []interface{}{v})
		}

		rows = append(rows, r)
	}

	sort.Sort(rows)
	return rows, nil
}

// shardLocation is a wrapper around a shard that provides extra
// information about which database and retention policy the shard
// belongs to.
//
// shardLocation is safe for use from multiple goroutines.
type shardLocation struct {
	mu              sync.RWMutex
	Database        string
	RetentionPolicy string
	Shard           *Shard
}

func (s *shardLocation) IsDatabase(db string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.Database == db
}

func (s *shardLocation) IsRetentionPolicy(rp string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.RetentionPolicy == rp
}

// IsRetryable returns true if this error is temporary and could be retried
func IsRetryable(err error) bool {
	if err == nil {
		return true
	}

	if strings.Contains(err.Error(), "field type conflict") {
		return false
	}
	return true
}

// relativePath will expand out the full paths passed in and return
// the relative shard path from the store
func relativePath(storePath, shardPath string) (string, error) {
	path, err := filepath.Abs(storePath)
	if err != nil {
		return "", fmt.Errorf("store abs path: %s", err)
	}

	fp, err := filepath.Abs(shardPath)
	if err != nil {
		return "", fmt.Errorf("file abs path: %s", err)
	}

	name, err := filepath.Rel(path, fp)
	if err != nil {
		return "", fmt.Errorf("file rel path: %s", err)
	}

	return name, nil
}

// measurementsFromSourcesOrDB returns a list of measurements from the
// sources passed in or, if sources is empty, a list of all
// measurement names from the database passed in.
func measurementsFromSourcesOrDB(db *DatabaseIndex, sources ...influxql.Source) (Measurements, error) {
	var measurements Measurements
	if len(sources) > 0 {
		for _, source := range sources {
			if m, ok := source.(*influxql.Measurement); ok {
				measurement := db.measurements[m.Name]
				if measurement == nil {
					continue
				}

				measurements = append(measurements, measurement)
			} else {
				return nil, errors.New("identifiers in FROM clause must be measurement names")
			}
		}
	} else {
		// No measurements specified in FROM clause so get all measurements that have series.
		for _, m := range db.Measurements() {
			if m.HasSeries() {
				measurements = append(measurements, m)
			}
		}
	}
	sort.Sort(measurements)

	return measurements, nil
}

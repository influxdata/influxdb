package tsdb // import "github.com/influxdata/influxdb/tsdb"

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/influxdata/influxdb/logger"
	"github.com/influxdata/influxdb/pkg/estimator/hll"

	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/pkg/estimator"
	"github.com/influxdata/influxdb/pkg/limiter"
	"github.com/influxdata/influxdb/query"
	"github.com/influxdata/influxql"
	"go.uber.org/zap"
)

var (
	// ErrShardNotFound is returned when trying to get a non existing shard.
	ErrShardNotFound = fmt.Errorf("shard not found")
	// ErrStoreClosed is returned when trying to use a closed Store.
	ErrStoreClosed = fmt.Errorf("store is closed")
)

// Statistics gathered by the store.
const (
	statDatabaseSeries       = "numSeries"       // number of series in a database
	statDatabaseMeasurements = "numMeasurements" // number of measurements in a database
)

// SeriesFileDirectory is the name of the directory containing series files for
// a database.
const SeriesFileDirectory = "_series"

// Store manages shards and indexes for databases.
type Store struct {
	mu                sync.RWMutex
	shards            map[uint64]*Shard
	databases         map[string]struct{}
	sfiles            map[string]*SeriesFile
	SeriesFileMaxSize int64 // Determines size of series file mmap. Can be altered in tests.
	path              string

	// shared per-database indexes, only if using "inmem".
	indexes map[string]interface{}

	// Maintains a set of shards that are in the process of deletion.
	// This prevents new shards from being created while old ones are being deleted.
	pendingShardDeletes map[uint64]struct{}

	EngineOptions EngineOptions

	baseLogger *zap.Logger
	Logger     *zap.Logger

	closing chan struct{}
	wg      sync.WaitGroup
	opened  bool
}

// NewStore returns a new store with the given path and a default configuration.
// The returned store must be initialized by calling Open before using it.
func NewStore(path string) *Store {
	logger := zap.NewNop()
	return &Store{
		databases:           make(map[string]struct{}),
		path:                path,
		sfiles:              make(map[string]*SeriesFile),
		indexes:             make(map[string]interface{}),
		pendingShardDeletes: make(map[uint64]struct{}),
		EngineOptions:       NewEngineOptions(),
		Logger:              logger,
		baseLogger:          logger,
	}
}

// WithLogger sets the logger for the store.
func (s *Store) WithLogger(log *zap.Logger) {
	s.baseLogger = log
	s.Logger = log.With(zap.String("service", "store"))
	for _, sh := range s.shards {
		sh.WithLogger(s.baseLogger)
	}
}

// Statistics returns statistics for period monitoring.
func (s *Store) Statistics(tags map[string]string) []models.Statistic {
	s.mu.RLock()
	shards := s.shardsSlice()
	s.mu.RUnlock()

	// Add all the series and measurements cardinality estimations.
	databases := s.Databases()
	statistics := make([]models.Statistic, 0, len(databases))
	for _, database := range databases {
		sc, err := s.SeriesCardinality(database)
		if err != nil {
			s.Logger.Info("Cannot retrieve series cardinality", zap.Error(err))
			continue
		}

		mc, err := s.MeasurementsCardinality(database)
		if err != nil {
			s.Logger.Info("Cannot retrieve measurement cardinality", zap.Error(err))
			continue
		}

		statistics = append(statistics, models.Statistic{
			Name: "database",
			Tags: models.StatisticTags{"database": database}.Merge(tags),
			Values: map[string]interface{}{
				statDatabaseSeries:       sc,
				statDatabaseMeasurements: mc,
			},
		})
	}

	// Gather all statistics for all shards.
	for _, shard := range shards {
		statistics = append(statistics, shard.Statistics(tags)...)
	}
	return statistics
}

// Path returns the store's root path.
func (s *Store) Path() string { return s.path }

// Open initializes the store, creating all necessary directories, loading all
// shards as well as initializing periodic maintenance of them.
func (s *Store) Open() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.opened {
		// Already open
		return nil
	}

	s.closing = make(chan struct{})
	s.shards = map[uint64]*Shard{}

	s.Logger.Info("Using data dir", zap.String("path", s.Path()))

	// Create directory.
	if err := os.MkdirAll(s.path, 0777); err != nil {
		return err
	}

	if err := s.loadShards(); err != nil {
		return err
	}

	s.opened = true
	s.wg.Add(1)
	go s.monitorShards()

	return nil
}

func (s *Store) loadShards() error {
	// res holds the result from opening each shard in a goroutine
	type res struct {
		s   *Shard
		err error
	}

	// Setup a shared limiter for compactions
	lim := s.EngineOptions.Config.MaxConcurrentCompactions
	if lim == 0 {
		lim = runtime.GOMAXPROCS(0) / 2 // Default to 50% of cores for compactions

		// On systems with more cores, cap at 4 to reduce disk utilization
		if lim > 4 {
			lim = 4
		}

		if lim < 1 {
			lim = 1
		}
	}

	// Don't allow more compactions to run than cores.
	if lim > runtime.GOMAXPROCS(0) {
		lim = runtime.GOMAXPROCS(0)
	}

	s.EngineOptions.CompactionLimiter = limiter.NewFixed(lim)

	// Env var to disable throughput limiter.  This will be moved to a config option in 1.5.
	if os.Getenv("INFLUXDB_DATA_COMPACTION_THROUGHPUT") == "" {
		s.EngineOptions.CompactionThroughputLimiter = limiter.NewRate(48*1024*1024, 48*1024*1024)
	} else {
		s.Logger.Info("Compaction throughput limit disabled")
	}

	log, logEnd := logger.NewOperation(s.Logger, "Open store", "tsdb.open")
	defer logEnd()

	t := limiter.NewFixed(runtime.GOMAXPROCS(0))
	resC := make(chan *res)
	var n int

	// Determine how many shards we need to open by checking the store path.
	dbDirs, err := ioutil.ReadDir(s.path)
	if err != nil {
		return err
	}

	for _, db := range dbDirs {
		dbPath := filepath.Join(s.path, db.Name())
		if !db.IsDir() {
			log.Info("Skipping database dir", zap.String("name", db.Name()), zap.String("reason", "not a directory"))
			continue
		}

		// Load series file.
		sfile, err := s.openSeriesFile(db.Name())
		if err != nil {
			return err
		}

		// Retrieve database index.
		idx, err := s.createIndexIfNotExists(db.Name())
		if err != nil {
			return err
		}

		// Load each retention policy within the database directory.
		rpDirs, err := ioutil.ReadDir(dbPath)
		if err != nil {
			return err
		}

		for _, rp := range rpDirs {
			rpPath := filepath.Join(s.path, db.Name(), rp.Name())
			if !rp.IsDir() {
				log.Info("Skipping retention policy dir", zap.String("name", rp.Name()), zap.String("reason", "not a directory"))
				continue
			}

			// The .series directory is not a retention policy.
			if rp.Name() == SeriesFileDirectory {
				continue
			}

			shardDirs, err := ioutil.ReadDir(rpPath)
			if err != nil {
				return err
			}

			for _, sh := range shardDirs {
				n++
				go func(db, rp, sh string) {
					t.Take()
					defer t.Release()

					start := time.Now()
					path := filepath.Join(s.path, db, rp, sh)
					walPath := filepath.Join(s.EngineOptions.Config.WALDir, db, rp, sh)

					// Shard file names are numeric shardIDs
					shardID, err := strconv.ParseUint(sh, 10, 64)
					if err != nil {
						log.Info("invalid shard ID found at path", zap.String("path", path))
						resC <- &res{err: fmt.Errorf("%s is not a valid ID. Skipping shard.", sh)}
						return
					}

					// Copy options and assign shared index.
					opt := s.EngineOptions
					opt.InmemIndex = idx

					// Provide an implementation of the ShardIDSets
					opt.SeriesIDSets = shardSet{store: s, db: db}

					// Existing shards should continue to use inmem index.
					if _, err := os.Stat(filepath.Join(path, "index")); os.IsNotExist(err) {
						opt.IndexVersion = "inmem"
					}

					// Open engine.
					shard := NewShard(shardID, path, walPath, sfile, opt)

					// Disable compactions, writes and queries until all shards are loaded
					shard.EnableOnOpen = false
					shard.WithLogger(s.baseLogger)

					err = shard.Open()
					if err != nil {
						log.Info("Failed to open shard", logger.Shard(shardID), zap.Error(err))
						resC <- &res{err: fmt.Errorf("Failed to open shard: %d: %s", shardID, err)}
						return
					}

					resC <- &res{s: shard}
					log.Info("Opened shard", zap.String("path", path), zap.Duration("duration", time.Since(start)))
				}(db.Name(), rp.Name(), sh.Name())
			}
		}
	}

	// Gather results of opening shards concurrently, keeping track of how
	// many databases we are managing.
	for i := 0; i < n; i++ {
		res := <-resC
		if res.err != nil {
			continue
		}
		s.shards[res.s.id] = res.s
		s.databases[res.s.database] = struct{}{}
	}
	close(resC)

	// Enable all shards
	for _, sh := range s.shards {
		sh.SetEnabled(true)
		if sh.IsIdle() {
			if err := sh.Free(); err != nil {
				return err
			}
		}
	}

	return nil
}

// Close closes the store and all associated shards. After calling Close accessing
// shards through the Store will result in ErrStoreClosed being returned.
func (s *Store) Close() error {
	s.mu.Lock()
	if s.opened {
		close(s.closing)
	}
	s.mu.Unlock()

	s.wg.Wait()
	// No other goroutines accessing the store, so no need for a Lock.

	// Close all the shards in parallel.
	if err := s.walkShards(s.shardsSlice(), func(sh *Shard) error {
		return sh.Close()
	}); err != nil {
		return err
	}

	s.mu.Lock()
	for _, sfile := range s.sfiles {
		// Close out the series files.
		if err := sfile.Close(); err != nil {
			return err
		}
	}

	s.shards = nil
	s.sfiles = map[string]*SeriesFile{}
	s.opened = false // Store may now be opened again.
	s.mu.Unlock()
	return nil
}

// openSeriesFile either returns or creates a series file for the provided
// database. It must be called under a full lock.
func (s *Store) openSeriesFile(database string) (*SeriesFile, error) {
	if sfile := s.sfiles[database]; sfile != nil {
		return sfile, nil
	}

	sfile := NewSeriesFile(filepath.Join(s.path, database, SeriesFileDirectory))
	sfile.Logger = s.baseLogger
	if err := sfile.Open(); err != nil {
		return nil, err
	}
	s.sfiles[database] = sfile
	return sfile, nil
}

func (s *Store) seriesFile(database string) *SeriesFile {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.sfiles[database]
}

// createIndexIfNotExists returns a shared index for a database, if the inmem
// index is being used. If the TSI index is being used, then this method is
// basically a no-op.
func (s *Store) createIndexIfNotExists(name string) (interface{}, error) {
	if idx := s.indexes[name]; idx != nil {
		return idx, nil
	}

	sfile, err := s.openSeriesFile(name)
	if err != nil {
		return nil, err
	}

	idx, err := NewInmemIndex(name, sfile)
	if err != nil {
		return nil, err
	}

	s.indexes[name] = idx
	return idx, nil
}

// Shard returns a shard by id.
func (s *Store) Shard(id uint64) *Shard {
	s.mu.RLock()
	defer s.mu.RUnlock()
	sh, ok := s.shards[id]
	if !ok {
		return nil
	}
	return sh
}

// Shards returns a list of shards by id.
func (s *Store) Shards(ids []uint64) []*Shard {
	s.mu.RLock()
	defer s.mu.RUnlock()
	a := make([]*Shard, 0, len(ids))
	for _, id := range ids {
		sh, ok := s.shards[id]
		if !ok {
			continue
		}
		a = append(a, sh)
	}
	return a
}

// ShardGroup returns a ShardGroup with a list of shards by id.
func (s *Store) ShardGroup(ids []uint64) ShardGroup {
	return Shards(s.Shards(ids))
}

// ShardN returns the number of shards in the store.
func (s *Store) ShardN() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.shards)
}

// ShardDigest returns a digest of the shard with the specified ID.
func (s *Store) ShardDigest(id uint64) (io.ReadCloser, int64, error) {
	sh := s.Shard(id)
	if sh == nil {
		return nil, 0, ErrShardNotFound
	}

	return sh.Digest()
}

// CreateShard creates a shard with the given id and retention policy on a database.
func (s *Store) CreateShard(database, retentionPolicy string, shardID uint64, enabled bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	select {
	case <-s.closing:
		return ErrStoreClosed
	default:
	}

	// Shard already exists.
	if _, ok := s.shards[shardID]; ok {
		return nil
	}

	// Shard may be undergoing a pending deletion. While the shard can be
	// recreated, it must wait for the pending delete to finish.
	if _, ok := s.pendingShardDeletes[shardID]; ok {
		return fmt.Errorf("shard %d is pending deletion and cannot be created again until finished", shardID)
	}

	// Create the db and retention policy directories if they don't exist.
	if err := os.MkdirAll(filepath.Join(s.path, database, retentionPolicy), 0700); err != nil {
		return err
	}

	// Create the WAL directory.
	walPath := filepath.Join(s.EngineOptions.Config.WALDir, database, retentionPolicy, fmt.Sprintf("%d", shardID))
	if err := os.MkdirAll(walPath, 0700); err != nil {
		return err
	}

	// Retrieve database series file.
	sfile, err := s.openSeriesFile(database)
	if err != nil {
		return err
	}

	// Retrieve shared index, if needed.
	idx, err := s.createIndexIfNotExists(database)
	if err != nil {
		return err
	}

	// Copy index options and pass in shared index.
	opt := s.EngineOptions
	opt.InmemIndex = idx
	opt.SeriesIDSets = shardSet{store: s, db: database}

	path := filepath.Join(s.path, database, retentionPolicy, strconv.FormatUint(shardID, 10))
	shard := NewShard(shardID, path, walPath, sfile, opt)
	shard.WithLogger(s.baseLogger)
	shard.EnableOnOpen = enabled

	if err := shard.Open(); err != nil {
		return err
	}

	s.shards[shardID] = shard
	s.databases[database] = struct{}{} // Ensure we are tracking any new db.

	return nil
}

// CreateShardSnapShot will create a hard link to the underlying shard and return a path.
// The caller is responsible for cleaning up (removing) the file path returned.
func (s *Store) CreateShardSnapshot(id uint64) (string, error) {
	sh := s.Shard(id)
	if sh == nil {
		return "", ErrShardNotFound
	}

	return sh.CreateSnapshot()
}

// SetShardEnabled enables or disables a shard for read and writes.
func (s *Store) SetShardEnabled(shardID uint64, enabled bool) error {
	sh := s.Shard(shardID)
	if sh == nil {
		return ErrShardNotFound
	}
	sh.SetEnabled(enabled)
	return nil
}

// DeleteShard removes a shard from disk.
func (s *Store) DeleteShard(shardID uint64) error {
	sh := s.Shard(shardID)
	if sh == nil {
		return nil
	}

	// Remove the shard from Store so it's not returned to callers requesting
	// shards. Also mark that this shard is currently being deleted in a separate
	// map so that we do not have to retain the global store lock while deleting
	// files.
	s.mu.Lock()
	if _, ok := s.pendingShardDeletes[shardID]; ok {
		// We are already being deleted? This is possible if delete shard
		// was called twice in sequence before the shard could be removed from
		// the mapping.
		// This is not an error because deleting a shard twice is not an error.
		s.mu.Unlock()
		return nil
	}
	delete(s.shards, shardID)
	s.pendingShardDeletes[shardID] = struct{}{}
	s.mu.Unlock()

	// Ensure the pending deletion flag is cleared on exit.
	defer func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		delete(s.pendingShardDeletes, shardID)
	}()

	// Get the shard's local bitset of series IDs.
	index, err := sh.Index()
	if err != nil {
		return err
	}

	var ss *SeriesIDSet
	if i, ok := index.(interface {
		SeriesIDSet() *SeriesIDSet
	}); ok {
		ss = i.SeriesIDSet()
	}

	db := sh.Database()
	if err := sh.Close(); err != nil {
		return err
	}

	// Determine if the shard contained any series that are not present in any
	// other shards in the database.
	shards := s.filterShards(byDatabase(db))

	s.walkShards(shards, func(sh *Shard) error {
		index, err := sh.Index()
		if err != nil {
			return err
		}

		if i, ok := index.(interface {
			SeriesIDSet() *SeriesIDSet
		}); ok {
			ss.Diff(i.SeriesIDSet())
		} else {
			return fmt.Errorf("unable to get series id set for index in shard at %s", sh.Path())
		}
		return nil
	})

	// Remove any remaining series in the set from the series file, as they don't
	// exist in any of the database's remaining shards.
	if ss.Cardinality() > 0 {
		sfile := s.seriesFile(db)
		if sfile != nil {
			ss.ForEach(func(id uint64) {
				sfile.DeleteSeriesID(id)
			})
		}
	}

	// Remove the on-disk shard data.
	if err := os.RemoveAll(sh.path); err != nil {
		return err
	}

	return os.RemoveAll(sh.walPath)
}

// DeleteDatabase will close all shards associated with a database and remove the directory and files from disk.
func (s *Store) DeleteDatabase(name string) error {
	s.mu.RLock()
	if _, ok := s.databases[name]; !ok {
		s.mu.RUnlock()
		// no files locally, so nothing to do
		return nil
	}
	shards := s.filterShards(func(sh *Shard) bool {
		return sh.database == name
	})
	s.mu.RUnlock()

	if err := s.walkShards(shards, func(sh *Shard) error {
		if sh.database != name {
			return nil
		}

		return sh.Close()
	}); err != nil {
		return err
	}

	dbPath := filepath.Clean(filepath.Join(s.path, name))

	s.mu.Lock()
	defer s.mu.Unlock()

	sfile := s.sfiles[name]
	delete(s.sfiles, name)

	// Close series file.
	if sfile != nil {
		if err := sfile.Close(); err != nil {
			return err
		}
	}

	// extra sanity check to make sure that even if someone named their database "../.."
	// that we don't delete everything because of it, they'll just have extra files forever
	if filepath.Clean(s.path) != filepath.Dir(dbPath) {
		return fmt.Errorf("invalid database directory location for database '%s': %s", name, dbPath)
	}

	if err := os.RemoveAll(dbPath); err != nil {
		return err
	}
	if err := os.RemoveAll(filepath.Join(s.EngineOptions.Config.WALDir, name)); err != nil {
		return err
	}

	for _, sh := range shards {
		delete(s.shards, sh.id)
	}

	// Remove database from store list of databases
	delete(s.databases, name)

	// Remove shared index for database if using inmem index.
	delete(s.indexes, name)

	return nil
}

// DeleteRetentionPolicy will close all shards associated with the
// provided retention policy, remove the retention policy directories on
// both the DB and WAL, and remove all shard files from disk.
func (s *Store) DeleteRetentionPolicy(database, name string) error {
	s.mu.RLock()
	if _, ok := s.databases[database]; !ok {
		s.mu.RUnlock()
		// unknown database, nothing to do
		return nil
	}
	shards := s.filterShards(func(sh *Shard) bool {
		return sh.database == database && sh.retentionPolicy == name
	})
	s.mu.RUnlock()

	// Close and delete all shards under the retention policy on the
	// database.
	if err := s.walkShards(shards, func(sh *Shard) error {
		if sh.database != database || sh.retentionPolicy != name {
			return nil
		}

		return sh.Close()
	}); err != nil {
		return err
	}

	// Remove the retention policy folder.
	rpPath := filepath.Clean(filepath.Join(s.path, database, name))

	// ensure Store's path is the grandparent of the retention policy
	if filepath.Clean(s.path) != filepath.Dir(filepath.Dir(rpPath)) {
		return fmt.Errorf("invalid path for database '%s', retention policy '%s': %s", database, name, rpPath)
	}

	// Remove the retention policy folder.
	if err := os.RemoveAll(filepath.Join(s.path, database, name)); err != nil {
		return err
	}

	// Remove the retention policy folder from the the WAL.
	if err := os.RemoveAll(filepath.Join(s.EngineOptions.Config.WALDir, database, name)); err != nil {
		return err
	}

	s.mu.Lock()
	for _, sh := range shards {
		delete(s.shards, sh.id)
	}
	s.mu.Unlock()
	return nil
}

// DeleteMeasurement removes a measurement and all associated series from a database.
func (s *Store) DeleteMeasurement(database, name string) error {
	s.mu.RLock()
	shards := s.filterShards(byDatabase(database))
	s.mu.RUnlock()

	// Limit to 1 delete for each shard since expanding the measurement into the list
	// of series keys can be very memory intensive if run concurrently.
	limit := limiter.NewFixed(1)
	return s.walkShards(shards, func(sh *Shard) error {
		limit.Take()
		defer limit.Release()

		return sh.DeleteMeasurement([]byte(name))
	})
}

// filterShards returns a slice of shards where fn returns true
// for the shard. If the provided predicate is nil then all shards are returned.
func (s *Store) filterShards(fn func(sh *Shard) bool) []*Shard {
	var shards []*Shard
	if fn == nil {
		shards = make([]*Shard, 0, len(s.shards))
		fn = func(*Shard) bool { return true }
	} else {
		shards = make([]*Shard, 0)
	}

	for _, sh := range s.shards {
		if fn(sh) {
			shards = append(shards, sh)
		}
	}
	return shards
}

// byDatabase provides a predicate for filterShards that matches on the name of
// the database passed in.
func byDatabase(name string) func(sh *Shard) bool {
	return func(sh *Shard) bool {
		return sh.database == name
	}
}

// walkShards apply a function to each shard in parallel. fn must be safe for
// concurrent use. If any of the functions return an error, the first error is
// returned.
func (s *Store) walkShards(shards []*Shard, fn func(sh *Shard) error) error {
	// struct to hold the result of opening each reader in a goroutine
	type res struct {
		err error
	}

	resC := make(chan res)
	var n int

	for _, sh := range shards {
		n++

		go func(sh *Shard) {
			if err := fn(sh); err != nil {
				resC <- res{err: fmt.Errorf("shard %d: %s", sh.id, err)}
				return
			}

			resC <- res{}
		}(sh)
	}

	var err error
	for i := 0; i < n; i++ {
		res := <-resC
		if res.err != nil {
			err = res.err
		}
	}
	close(resC)
	return err
}

// ShardIDs returns a slice of all ShardIDs under management.
func (s *Store) ShardIDs() []uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.shardIDs()
}

func (s *Store) shardIDs() []uint64 {
	a := make([]uint64, 0, len(s.shards))
	for shardID := range s.shards {
		a = append(a, shardID)
	}
	return a
}

// shardsSlice returns an ordered list of shards.
func (s *Store) shardsSlice() []*Shard {
	a := make([]*Shard, 0, len(s.shards))
	for _, sh := range s.shards {
		a = append(a, sh)
	}
	sort.Sort(Shards(a))
	return a
}

// Databases returns the names of all databases managed by the store.
func (s *Store) Databases() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	databases := make([]string, 0, len(s.databases))
	for k, _ := range s.databases {
		databases = append(databases, k)
	}
	return databases
}

// DiskSize returns the size of all the shard files in bytes.
// This size does not include the WAL size.
func (s *Store) DiskSize() (int64, error) {
	var size int64

	s.mu.RLock()
	allShards := s.filterShards(nil)
	s.mu.RUnlock()

	for _, sh := range allShards {
		sz, err := sh.DiskSize()
		if err != nil {
			return 0, err
		}
		size += sz
	}
	return size, nil
}

// sketchesForDatabase returns merged sketches for the provided database, by
// walking each shard in the database and merging the sketches found there.
func (s *Store) sketchesForDatabase(dbName string, getSketches func(*Shard) (estimator.Sketch, estimator.Sketch, error)) (estimator.Sketch, estimator.Sketch, error) {
	var (
		ss estimator.Sketch // Sketch estimating number of items.
		ts estimator.Sketch // Sketch estimating number of tombstoned items.
	)

	s.mu.RLock()
	shards := s.filterShards(byDatabase(dbName))
	s.mu.RUnlock()

	// Never return nil sketches. In the case that db exists but no data written
	// return empty sketches.
	if len(shards) == 0 {
		ss, ts = hll.NewDefaultPlus(), hll.NewDefaultPlus()
	}

	// Iterate over all shards for the database and combine all of the sketches.
	for _, shard := range shards {
		s, t, err := getSketches(shard)
		if err != nil {
			return nil, nil, err
		}

		if ss == nil {
			ss, ts = s, t
		} else if err = ss.Merge(s); err != nil {
			return nil, nil, err
		} else if err = ts.Merge(t); err != nil {
			return nil, nil, err
		}
	}
	return ss, ts, nil
}

// SeriesCardinality returns the exact series cardinality for the provided
// database.
//
// Cardinality is calculated exactly by unioning all shards' bitsets of series
// IDs. The result of this method cannot be combined with any other results.
//
func (s *Store) SeriesCardinality(database string) (int64, error) {
	s.mu.RLock()
	shards := s.filterShards(byDatabase(database))
	s.mu.RUnlock()

	var setMu sync.Mutex
	others := make([]*SeriesIDSet, 0, len(shards))

	s.walkShards(shards, func(sh *Shard) error {
		index, err := sh.Index()
		if err != nil {
			return err
		}

		if i, ok := index.(interface {
			SeriesIDSet() *SeriesIDSet
		}); ok {
			seriesIDs := i.SeriesIDSet()
			setMu.Lock()
			others = append(others, seriesIDs)
			setMu.Unlock()
		} else {
			return fmt.Errorf("unable to get series id set for index in shard at %s", sh.Path())
		}
		return nil
	})

	ss := NewSeriesIDSet()
	ss.Merge(others...)
	return int64(ss.Cardinality()), nil
}

// SeriesSketches returns the sketches associated with the series data in all
// the shards in the provided database.
//
// The returned sketches can be combined with other sketches to provide an
// estimation across distributed databases.
func (s *Store) SeriesSketches(database string) (estimator.Sketch, estimator.Sketch, error) {
	return s.sketchesForDatabase(database, func(sh *Shard) (estimator.Sketch, estimator.Sketch, error) {
		if sh == nil {
			return nil, nil, errors.New("shard nil, can't get cardinality")
		}
		return sh.SeriesSketches()
	})
}

// MeasurementsCardinality returns an estimation of the measurement cardinality
// for the provided database.
//
// Cardinality is calculated using a sketch-based estimation. The result of this
// method cannot be combined with any other results.
func (s *Store) MeasurementsCardinality(database string) (int64, error) {
	ss, ts, err := s.sketchesForDatabase(database, func(sh *Shard) (estimator.Sketch, estimator.Sketch, error) {
		if sh == nil {
			return nil, nil, errors.New("shard nil, can't get cardinality")
		}
		return sh.MeasurementsSketches()
	})

	if err != nil {
		return 0, err
	}
	return int64(ss.Count() - ts.Count()), nil
}

// MeasurementsSketches returns the sketches associated with the measurement
// data in all the shards in the provided database.
//
// The returned sketches can be combined with other sketches to provide an
// estimation across distributed databases.
func (s *Store) MeasurementsSketches(database string) (estimator.Sketch, estimator.Sketch, error) {
	return s.sketchesForDatabase(database, func(sh *Shard) (estimator.Sketch, estimator.Sketch, error) {
		if sh == nil {
			return nil, nil, errors.New("shard nil, can't get cardinality")
		}
		return sh.MeasurementsSketches()
	})
}

// BackupShard will get the shard and have the engine backup since the passed in
// time to the writer.
func (s *Store) BackupShard(id uint64, since time.Time, w io.Writer) error {
	shard := s.Shard(id)
	if shard == nil {
		return fmt.Errorf("shard %d doesn't exist on this server", id)
	}

	path, err := relativePath(s.path, shard.path)
	if err != nil {
		return err
	}

	return shard.Backup(w, path, since)
}

func (s *Store) ExportShard(id uint64, start time.Time, end time.Time, w io.Writer) error {
	shard := s.Shard(id)
	if shard == nil {
		return fmt.Errorf("shard %d doesn't exist on this server", id)
	}

	path, err := relativePath(s.path, shard.path)
	if err != nil {
		return err
	}

	return shard.Export(w, path, start, end)
}

// RestoreShard restores a backup from r to a given shard.
// This will only overwrite files included in the backup.
func (s *Store) RestoreShard(id uint64, r io.Reader) error {
	shard := s.Shard(id)
	if shard == nil {
		return fmt.Errorf("shard %d doesn't exist on this server", id)
	}

	path, err := relativePath(s.path, shard.path)
	if err != nil {
		return err
	}

	return shard.Restore(r, path)
}

// ImportShard imports the contents of r to a given shard.
// All files in the backup are added as new files which may
// cause duplicated data to occur requiring more expensive
// compactions.
func (s *Store) ImportShard(id uint64, r io.Reader) error {
	shard := s.Shard(id)
	if shard == nil {
		return fmt.Errorf("shard %d doesn't exist on this server", id)
	}

	path, err := relativePath(s.path, shard.path)
	if err != nil {
		return err
	}

	return shard.Import(r, path)
}

// ShardRelativePath will return the relative path to the shard, i.e.,
// <database>/<retention>/<id>.
func (s *Store) ShardRelativePath(id uint64) (string, error) {
	shard := s.Shard(id)
	if shard == nil {
		return "", fmt.Errorf("shard %d doesn't exist on this server", id)
	}
	return relativePath(s.path, shard.path)
}

// DeleteSeries loops through the local shards and deletes the series data for
// the passed in series keys.
func (s *Store) DeleteSeries(database string, sources []influxql.Source, condition influxql.Expr) error {
	// Expand regex expressions in the FROM clause.
	a, err := s.ExpandSources(sources)
	if err != nil {
		return err
	} else if len(sources) > 0 && len(a) == 0 {
		return nil
	}
	sources = a

	// Determine deletion time range.
	condition, timeRange, err := influxql.ConditionExpr(condition, nil)
	if err != nil {
		return err
	}

	var min, max int64
	if !timeRange.Min.IsZero() {
		min = timeRange.Min.UnixNano()
	} else {
		min = influxql.MinTime
	}
	if !timeRange.Max.IsZero() {
		max = timeRange.Max.UnixNano()
	} else {
		max = influxql.MaxTime
	}

	s.mu.RLock()
	sfile := s.sfiles[database]
	if sfile == nil {
		s.mu.RUnlock()
		// No series file means nothing has been written to this DB and thus nothing to delete.
		return nil
	}
	shards := s.filterShards(byDatabase(database))
	s.mu.RUnlock()

	// Limit to 1 delete for each shard since expanding the measurement into the list
	// of series keys can be very memory intensive if run concurrently.
	limit := limiter.NewFixed(1)

	return s.walkShards(shards, func(sh *Shard) error {
		// Determine list of measurements from sources.
		// Use all measurements if no FROM clause was provided.
		var names []string
		if len(sources) > 0 {
			for _, source := range sources {
				names = append(names, source.(*influxql.Measurement).Name)
			}
		} else {
			if err := sh.ForEachMeasurementName(func(name []byte) error {
				names = append(names, string(name))
				return nil
			}); err != nil {
				return err
			}
		}
		sort.Strings(names)

		limit.Take()
		defer limit.Release()

		index, err := sh.Index()
		if err != nil {
			return err
		}

		indexSet := IndexSet{Indexes: []Index{index}, SeriesFile: sfile}
		// Find matching series keys for each measurement.
		for _, name := range names {
			itr, err := indexSet.MeasurementSeriesByExprIterator([]byte(name), condition)
			if err != nil {
				return err
			} else if itr == nil {
				continue
			}
			defer itr.Close()
			if err := sh.DeleteSeriesRange(NewSeriesIteratorAdapter(sfile, itr), min, max); err != nil {
				return err
			}

		}

		return nil
	})
}

// ExpandSources expands sources against all local shards.
func (s *Store) ExpandSources(sources influxql.Sources) (influxql.Sources, error) {
	shards := func() Shards {
		s.mu.RLock()
		defer s.mu.RUnlock()
		return Shards(s.shardsSlice())
	}()
	return shards.ExpandSources(sources)
}

// WriteToShard writes a list of points to a shard identified by its ID.
func (s *Store) WriteToShard(shardID uint64, points []models.Point) error {
	s.mu.RLock()

	select {
	case <-s.closing:
		s.mu.RUnlock()
		return ErrStoreClosed
	default:
	}

	sh := s.shards[shardID]
	if sh == nil {
		s.mu.RUnlock()
		return ErrShardNotFound
	}
	s.mu.RUnlock()

	// Ensure snapshot compactions are enabled since the shard might have been cold
	// and disabled by the monitor.
	if sh.IsIdle() {
		sh.SetCompactionsEnabled(true)
	}

	return sh.WritePoints(points)
}

// MeasurementNames returns a slice of all measurements. Measurements accepts an
// optional condition expression. If cond is nil, then all measurements for the
// database will be returned.
func (s *Store) MeasurementNames(auth query.Authorizer, database string, cond influxql.Expr) ([][]byte, error) {
	s.mu.RLock()
	shards := s.filterShards(byDatabase(database))
	s.mu.RUnlock()

	sfile := s.seriesFile(database)
	if sfile == nil {
		return nil, nil
	}

	// Build indexset.
	is := IndexSet{Indexes: make([]Index, 0, len(shards)), SeriesFile: sfile}
	for _, sh := range shards {
		index, err := sh.Index()
		if err != nil {
			return nil, err
		}
		is.Indexes = append(is.Indexes, index)
	}
	is = is.DedupeInmemIndexes()
	return is.MeasurementNamesByExpr(auth, cond)
}

// MeasurementSeriesCounts returns the number of measurements and series in all
// the shards' indices.
func (s *Store) MeasurementSeriesCounts(database string) (measuments int, series int) {
	// TODO: implement me
	return 0, 0
}

type TagKeys struct {
	Measurement string
	Keys        []string
}

type TagKeysSlice []TagKeys

func (a TagKeysSlice) Len() int           { return len(a) }
func (a TagKeysSlice) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a TagKeysSlice) Less(i, j int) bool { return a[i].Measurement < a[j].Measurement }

// TagKeys returns the tag keys in the given database, matching the condition.
func (s *Store) TagKeys(auth query.Authorizer, shardIDs []uint64, cond influxql.Expr) ([]TagKeys, error) {
	if len(shardIDs) == 0 {
		return nil, nil
	}

	measurementExpr := influxql.CloneExpr(cond)
	measurementExpr = influxql.Reduce(influxql.RewriteExpr(measurementExpr, func(e influxql.Expr) influxql.Expr {
		switch e := e.(type) {
		case *influxql.BinaryExpr:
			switch e.Op {
			case influxql.EQ, influxql.NEQ, influxql.EQREGEX, influxql.NEQREGEX:
				tag, ok := e.LHS.(*influxql.VarRef)
				if !ok || tag.Val != "_name" {
					return nil
				}
			}
		}
		return e
	}), nil)

	filterExpr := influxql.CloneExpr(cond)
	filterExpr = influxql.Reduce(influxql.RewriteExpr(filterExpr, func(e influxql.Expr) influxql.Expr {
		switch e := e.(type) {
		case *influxql.BinaryExpr:
			switch e.Op {
			case influxql.EQ, influxql.NEQ, influxql.EQREGEX, influxql.NEQREGEX:
				tag, ok := e.LHS.(*influxql.VarRef)
				if !ok || strings.HasPrefix(tag.Val, "_") {
					return nil
				}
			}
		}
		return e
	}), nil)

	// Get all the shards we're interested in.
	is := IndexSet{Indexes: make([]Index, 0, len(shardIDs))}
	s.mu.RLock()
	for _, sid := range shardIDs {
		shard, ok := s.shards[sid]
		if !ok {
			continue
		}

		if is.SeriesFile == nil {
			is.SeriesFile = shard.sfile
		}

		is.Indexes = append(is.Indexes, shard.index)
	}
	s.mu.RUnlock()

	// Determine list of measurements.
	is = is.DedupeInmemIndexes()
	names, err := is.MeasurementNamesByExpr(nil, measurementExpr)
	if err != nil {
		return nil, err
	}

	// Iterate over each measurement.
	var results []TagKeys
	for _, name := range names {

		// Build keyset over all indexes for measurement.
		tagKeySet, err := is.MeasurementTagKeysByExpr(name, nil)
		if err != nil {
			return nil, err
		} else if len(tagKeySet) == 0 {
			continue
		}

		keys := make([]string, 0, len(tagKeySet))
		// If no tag value filter is present then all the tag keys can be returned
		// If they have authorized series associated with them.
		if filterExpr == nil {
			for tagKey := range tagKeySet {
				ok, err := is.TagKeyHasAuthorizedSeries(auth, []byte(name), []byte(tagKey))
				if err != nil {
					return nil, err
				} else if ok {
					keys = append(keys, tagKey)
				}
			}
			sort.Strings(keys)

			// Add to resultset.
			results = append(results, TagKeys{
				Measurement: string(name),
				Keys:        keys,
			})

			continue
		}

		// Tag filter provided so filter keys first.

		// Sort the tag keys.
		for k := range tagKeySet {
			keys = append(keys, k)
		}
		sort.Strings(keys)

		// Filter against tag values, skip if no values exist.
		values, err := is.MeasurementTagKeyValuesByExpr(auth, name, keys, filterExpr, true)
		if err != nil {
			return nil, err
		}

		// Filter final tag keys using the matching values. If a key has one or
		// more matching values then it will be included in the final set.
		finalKeys := keys[:0] // Use same backing array as keys to save allocation.
		for i, k := range keys {
			if len(values[i]) > 0 {
				// Tag key k has one or more matching tag values.
				finalKeys = append(finalKeys, k)
			}
		}

		// Add to resultset.
		results = append(results, TagKeys{
			Measurement: string(name),
			Keys:        finalKeys,
		})
	}
	return results, nil
}

type TagValues struct {
	Measurement string
	Values      []KeyValue
}

type TagValuesSlice []TagValues

func (a TagValuesSlice) Len() int           { return len(a) }
func (a TagValuesSlice) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a TagValuesSlice) Less(i, j int) bool { return a[i].Measurement < a[j].Measurement }

// tagValues is a temporary representation of a TagValues. Rather than allocating
// KeyValues as we build up a TagValues object, We hold off allocating KeyValues
// until we have merged multiple tagValues together.
type tagValues struct {
	name   []byte
	keys   []string
	values [][]string
}

// Is a slice of tagValues that can be sorted by measurement.
type tagValuesSlice []tagValues

func (a tagValuesSlice) Len() int           { return len(a) }
func (a tagValuesSlice) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a tagValuesSlice) Less(i, j int) bool { return bytes.Compare(a[i].name, a[j].name) == -1 }

// TagValues returns the tag keys and values for the provided shards, where the
// tag values satisfy the provided condition.
func (s *Store) TagValues(auth query.Authorizer, shardIDs []uint64, cond influxql.Expr) ([]TagValues, error) {
	if cond == nil {
		return nil, errors.New("a condition is required")
	}

	measurementExpr := influxql.CloneExpr(cond)
	measurementExpr = influxql.Reduce(influxql.RewriteExpr(measurementExpr, func(e influxql.Expr) influxql.Expr {
		switch e := e.(type) {
		case *influxql.BinaryExpr:
			switch e.Op {
			case influxql.EQ, influxql.NEQ, influxql.EQREGEX, influxql.NEQREGEX:
				tag, ok := e.LHS.(*influxql.VarRef)
				if !ok || tag.Val != "_name" {
					return nil
				}
			}
		}
		return e
	}), nil)

	filterExpr := influxql.CloneExpr(cond)
	filterExpr = influxql.Reduce(influxql.RewriteExpr(filterExpr, func(e influxql.Expr) influxql.Expr {
		switch e := e.(type) {
		case *influxql.BinaryExpr:
			switch e.Op {
			case influxql.EQ, influxql.NEQ, influxql.EQREGEX, influxql.NEQREGEX:
				tag, ok := e.LHS.(*influxql.VarRef)
				if !ok || strings.HasPrefix(tag.Val, "_") {
					return nil
				}
			}
		}
		return e
	}), nil)

	// Build index set to work on.
	is := IndexSet{Indexes: make([]Index, 0, len(shardIDs))}
	s.mu.RLock()
	for _, sid := range shardIDs {
		shard, ok := s.shards[sid]
		if !ok {
			continue
		}

		if is.SeriesFile == nil {
			is.SeriesFile = shard.sfile
		}
		is.Indexes = append(is.Indexes, shard.index)
	}
	s.mu.RUnlock()
	is = is.DedupeInmemIndexes()

	// Stores each list of TagValues for each measurement.
	var allResults []tagValues
	var maxMeasurements int // Hint as to lower bound on number of measurements.
	// names will be sorted by MeasurementNamesByExpr.
	// Authorisation can be done later on, when series may have been filtered
	// out by other conditions.
	names, err := is.MeasurementNamesByExpr(nil, measurementExpr)
	if err != nil {
		return nil, err
	}

	if len(names) > maxMeasurements {
		maxMeasurements = len(names)
	}

	if allResults == nil {
		allResults = make([]tagValues, 0, len(is.Indexes)*len(names)) // Assuming all series in all shards.
	}

	// Iterate over each matching measurement in the shard. For each
	// measurement we'll get the matching tag keys (e.g., when a WITH KEYS)
	// statement is used, and we'll then use those to fetch all the relevant
	// values from matching series. Series may be filtered using a WHERE
	// filter.
	for _, name := range names {
		// Determine a list of keys from condition.
		keySet, err := is.MeasurementTagKeysByExpr(name, cond)
		if err != nil {
			return nil, err
		}

		if len(keySet) == 0 {
			// No matching tag keys for this measurement
			continue
		}

		result := tagValues{
			name: name,
			keys: make([]string, 0, len(keySet)),
		}

		// Add the keys to the tagValues and sort them.
		for k := range keySet {
			result.keys = append(result.keys, k)
		}
		sort.Sort(sort.StringSlice(result.keys))

		// get all the tag values for each key in the keyset.
		// Each slice in the results contains the sorted values associated
		// associated with each tag key for the measurement from the key set.
		if result.values, err = is.MeasurementTagKeyValuesByExpr(auth, name, result.keys, filterExpr, true); err != nil {
			return nil, err
		}

		// remove any tag keys that didn't have any authorized values
		j := 0
		for i := range result.keys {
			if len(result.values[i]) == 0 {
				continue
			}

			result.keys[j] = result.keys[i]
			result.values[j] = result.values[i]
			j++
		}
		result.keys = result.keys[:j]
		result.values = result.values[:j]

		// only include result if there are keys with values
		if len(result.keys) > 0 {
			allResults = append(allResults, result)
		}
	}

	result := make([]TagValues, 0, maxMeasurements)

	// We need to sort all results by measurement name.
	if len(is.Indexes) > 1 {
		sort.Sort(tagValuesSlice(allResults))
	}

	// The next stage is to merge the tagValue results for each shard's measurements.
	var i, j int
	// Used as a temporary buffer in mergeTagValues. There can be at most len(shards)
	// instances of tagValues for a given measurement.
	idxBuf := make([][2]int, 0, len(is.Indexes))
	for i < len(allResults) {
		// Gather all occurrences of the same measurement for merging.
		for j+1 < len(allResults) && bytes.Equal(allResults[j+1].name, allResults[i].name) {
			j++
		}

		// An invariant is that there can't be more than n instances of tag
		// key value pairs for a given measurement, where n is the number of
		// shards.
		if got, exp := j-i+1, len(is.Indexes); got > exp {
			return nil, fmt.Errorf("unexpected results returned engine. Got %d measurement sets for %d shards", got, exp)
		}

		nextResult := mergeTagValues(idxBuf, allResults[i:j+1]...)
		i = j + 1
		if len(nextResult.Values) > 0 {
			result = append(result, nextResult)
		}
	}
	return result, nil
}

// mergeTagValues merges multiple sorted sets of temporary tagValues using a
// direct k-way merge whilst also removing duplicated entries. The result is a
// single TagValue type.
//
// TODO(edd): a Tournament based merge (see: Knuth's TAOCP 5.4.1) might be more
// appropriate at some point.
//
func mergeTagValues(valueIdxs [][2]int, tvs ...tagValues) TagValues {
	var result TagValues
	if len(tvs) == 0 {
		return TagValues{}
	} else if len(tvs) == 1 {
		result.Measurement = string(tvs[0].name)
		// TODO(edd): will be too small likely. Find a hint?
		result.Values = make([]KeyValue, 0, len(tvs[0].values))

		for ki, key := range tvs[0].keys {
			for _, value := range tvs[0].values[ki] {
				result.Values = append(result.Values, KeyValue{Key: key, Value: value})
			}
		}
		return result
	}

	result.Measurement = string(tvs[0].name)

	var maxSize int
	for _, tv := range tvs {
		if len(tv.values) > maxSize {
			maxSize = len(tv.values)
		}
	}
	result.Values = make([]KeyValue, 0, maxSize) // This will likely be too small but it's a start.

	// Resize and reset to the number of TagValues we're merging.
	valueIdxs = valueIdxs[:len(tvs)]
	for i := 0; i < len(valueIdxs); i++ {
		valueIdxs[i][0], valueIdxs[i][1] = 0, 0
	}

	var (
		j              int
		keyCmp, valCmp int
	)

	for {
		// Which of the provided TagValue sets currently holds the smallest element.
		// j is the candidate we're going to next pick for the result set.
		j = -1

		// Find the smallest element
		for i := 0; i < len(tvs); i++ {
			if valueIdxs[i][0] >= len(tvs[i].keys) {
				continue // We have completely drained all tag keys and values for this shard.
			} else if len(tvs[i].values[valueIdxs[i][0]]) == 0 {
				// There are no tag values for these keys.
				valueIdxs[i][0]++
				valueIdxs[i][1] = 0
				continue
			} else if j == -1 {
				// We haven't picked a best TagValues set yet. Pick this one.
				j = i
				continue
			}

			// It this tag key is lower than the candidate's tag key
			keyCmp = strings.Compare(tvs[i].keys[valueIdxs[i][0]], tvs[j].keys[valueIdxs[j][0]])
			if keyCmp == -1 {
				j = i
			} else if keyCmp == 0 {
				valCmp = strings.Compare(tvs[i].values[valueIdxs[i][0]][valueIdxs[i][1]], tvs[j].values[valueIdxs[j][0]][valueIdxs[j][1]])
				// Same tag key but this tag value is lower than the candidate.
				if valCmp == -1 {
					j = i
				} else if valCmp == 0 {
					// Duplicate tag key/value pair.... Remove and move onto
					// the next value for shard i.
					valueIdxs[i][1]++
					if valueIdxs[i][1] >= len(tvs[i].values[valueIdxs[i][0]]) {
						// Drained all these tag values, move onto next key.
						valueIdxs[i][0]++
						valueIdxs[i][1] = 0
					}
				}
			}
		}

		// We could have drained all of the TagValue sets and be done...
		if j == -1 {
			break
		}

		// Append the smallest KeyValue
		result.Values = append(result.Values, KeyValue{
			Key:   string(tvs[j].keys[valueIdxs[j][0]]),
			Value: tvs[j].values[valueIdxs[j][0]][valueIdxs[j][1]],
		})
		// Increment the indexes for the chosen TagValue.
		valueIdxs[j][1]++
		if valueIdxs[j][1] >= len(tvs[j].values[valueIdxs[j][0]]) {
			// Drained all these tag values, move onto next key.
			valueIdxs[j][0]++
			valueIdxs[j][1] = 0
		}
	}
	return result
}

func (s *Store) monitorShards() {
	defer s.wg.Done()
	t := time.NewTicker(10 * time.Second)
	defer t.Stop()
	t2 := time.NewTicker(time.Minute)
	defer t2.Stop()
	for {
		select {
		case <-s.closing:
			return
		case <-t.C:
			s.mu.RLock()
			for _, sh := range s.shards {
				if sh.IsIdle() {
					if err := sh.Free(); err != nil {
						s.Logger.Warn("Error while freeing cold shard resources", zap.Error(err))
					}
				} else {
					sh.SetCompactionsEnabled(true)
				}
			}
			s.mu.RUnlock()
		case <-t2.C:
			if s.EngineOptions.Config.MaxValuesPerTag == 0 {
				continue
			}

			s.mu.RLock()
			shards := s.filterShards(func(sh *Shard) bool {
				return sh.IndexType() == "inmem"
			})
			s.mu.RUnlock()

			// No inmem shards...
			if len(shards) == 0 {
				continue
			}

			var dbLock sync.Mutex
			databases := make(map[string]struct{}, len(shards))

			s.walkShards(shards, func(sh *Shard) error {
				db := sh.database

				// Only process 1 shard from each database
				dbLock.Lock()
				if _, ok := databases[db]; ok {
					dbLock.Unlock()
					return nil
				}
				databases[db] = struct{}{}
				dbLock.Unlock()

				sfile := s.seriesFile(sh.database)
				if sfile == nil {
					return nil
				}

				firstShardIndex, err := sh.Index()
				if err != nil {
					return err
				}

				index, err := sh.Index()
				if err != nil {
					return err
				}

				// inmem shards share the same index instance so just use the first one to avoid
				// allocating the same measurements repeatedly
				indexSet := IndexSet{Indexes: []Index{firstShardIndex}, SeriesFile: sfile}
				names, err := indexSet.MeasurementNamesByExpr(nil, nil)
				if err != nil {
					s.Logger.Warn("Cannot retrieve measurement names", zap.Error(err))
					return nil
				}

				indexSet.Indexes = []Index{index}
				for _, name := range names {
					indexSet.ForEachMeasurementTagKey(name, func(k []byte) error {
						n := sh.TagKeyCardinality(name, k)
						perc := int(float64(n) / float64(s.EngineOptions.Config.MaxValuesPerTag) * 100)
						if perc > 100 {
							perc = 100
						}

						// Log at 80, 85, 90-100% levels
						if perc == 80 || perc == 85 || perc >= 90 {
							s.Logger.Warn("max-values-per-tag limit may be exceeded soon",
								zap.String("perc", fmt.Sprintf("%d%%", perc)),
								zap.Int("n", n),
								zap.Int("max", s.EngineOptions.Config.MaxValuesPerTag),
								logger.Database(db),
								zap.ByteString("measurement", name),
								zap.ByteString("tag", k))
						}
						return nil
					})
				}
				return nil
			})
		}
	}
}

// KeyValue holds a string key and a string value.
type KeyValue struct {
	Key, Value string
}

// KeyValues is a sortable slice of KeyValue.
type KeyValues []KeyValue

// Len implements sort.Interface.
func (a KeyValues) Len() int { return len(a) }

// Swap implements sort.Interface.
func (a KeyValues) Swap(i, j int) { a[i], a[j] = a[j], a[i] }

// Less implements sort.Interface. Keys are compared before values.
func (a KeyValues) Less(i, j int) bool {
	ki, kj := a[i].Key, a[j].Key
	if ki == kj {
		return a[i].Value < a[j].Value
	}
	return ki < kj
}

// decodeStorePath extracts the database and retention policy names
// from a given shard or WAL path.
func decodeStorePath(shardOrWALPath string) (database, retentionPolicy string) {
	// shardOrWALPath format: /maybe/absolute/base/then/:database/:retentionPolicy/:nameOfShardOrWAL

	// Discard the last part of the path (the shard name or the wal name).
	path, _ := filepath.Split(filepath.Clean(shardOrWALPath))

	// Extract the database and retention policy.
	path, rp := filepath.Split(filepath.Clean(path))
	_, db := filepath.Split(filepath.Clean(path))
	return db, rp
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

type shardSet struct {
	store *Store
	db    string
}

func (s shardSet) ForEach(f func(ids *SeriesIDSet)) error {
	s.store.mu.RLock()
	shards := s.store.filterShards(byDatabase(s.db))
	s.store.mu.RUnlock()

	for _, sh := range shards {
		idx, err := sh.Index()
		if err != nil {
			return err
		}

		if t, ok := idx.(interface {
			SeriesIDSet() *SeriesIDSet
		}); ok {
			f(t.SeriesIDSet())
		}
	}
	return nil
}

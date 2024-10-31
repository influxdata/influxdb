//lint:file-ignore ST1005 this is old code. we're not going to conform error messages
package tsdb // import "github.com/influxdata/influxdb/v2/tsdb"

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"sync"
	"time"

	errors3 "github.com/influxdata/influxdb/v2/pkg/errors"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/influxql/query"
	errors2 "github.com/influxdata/influxdb/v2/kit/platform/errors"
	"github.com/influxdata/influxdb/v2/logger"
	"github.com/influxdata/influxdb/v2/models"
	"github.com/influxdata/influxdb/v2/pkg/estimator"
	"github.com/influxdata/influxdb/v2/pkg/estimator/hll"
	"github.com/influxdata/influxdb/v2/pkg/limiter"
	"github.com/influxdata/influxql"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var (
	// ErrShardNotFound is returned when trying to get a non existing shard.
	ErrShardNotFound = fmt.Errorf("shard not found")
	// ErrStoreClosed is returned when trying to use a closed Store.
	ErrStoreClosed = fmt.Errorf("store is closed")
	// ErrShardDeletion is returned when trying to create a shard that is being deleted
	ErrShardDeletion = errors.New("shard is being deleted")
	// ErrMultipleIndexTypes is returned when trying to do deletes on a database with
	// multiple index types.
	ErrMultipleIndexTypes = errors.New("cannot delete data. DB contains shards using multiple indexes. Please convert all shards to use the same index type to delete data")
)

// SeriesFileDirectory is the name of the directory containing series files for
// a database.
const SeriesFileDirectory = "_series"

// databaseState keeps track of the state of a database.
type databaseState struct{ indexTypes map[string]int }

// struct to hold the result of opening each reader in a goroutine
type shardResponse struct {
	s   *Shard
	err error
}

// addIndexType records that the database has a shard with the given index type.
func (d *databaseState) addIndexType(indexType string) {
	if d.indexTypes == nil {
		d.indexTypes = make(map[string]int)
	}
	d.indexTypes[indexType]++
}

// addIndexType records that the database no longer has a shard with the given index type.
func (d *databaseState) removeIndexType(indexType string) {
	if d.indexTypes != nil {
		d.indexTypes[indexType]--
		if d.indexTypes[indexType] <= 0 {
			delete(d.indexTypes, indexType)
		}
	}
}

// hasMultipleIndexTypes returns true if the database has multiple index types.
func (d *databaseState) hasMultipleIndexTypes() bool { return d != nil && len(d.indexTypes) > 1 }

type shardErrorMap struct {
	mu          sync.Mutex
	shardErrors map[uint64]error
}

func (se *shardErrorMap) setShardOpenError(shardID uint64, err error) {
	se.mu.Lock()
	defer se.mu.Unlock()
	if err == nil {
		delete(se.shardErrors, shardID)
	} else {
		se.shardErrors[shardID] = &ErrPreviousShardFail{error: fmt.Errorf("opening shard previously failed with: %w", err)}
	}
}

func (se *shardErrorMap) shardError(shardID uint64) (error, bool) {
	se.mu.Lock()
	defer se.mu.Unlock()
	oldErr, hasErr := se.shardErrors[shardID]
	return oldErr, hasErr
}

// Store manages shards and indexes for databases.
type Store struct {
	mu                sync.RWMutex
	shards            map[uint64]*Shard
	databases         map[string]*databaseState
	sfiles            map[string]*SeriesFile
	SeriesFileMaxSize int64 // Determines size of series file mmap. Can be altered in tests.
	path              string

	// Maintains a set of shards that are in the process of deletion.
	// This prevents new shards from being created while old ones are being deleted.
	pendingShardDeletes map[uint64]struct{}

	// Maintains a set of shards that failed to open
	badShards shardErrorMap

	// Epoch tracker helps serialize writes and deletes that may conflict. It
	// is stored by shard.
	epochs map[uint64]*epochTracker

	EngineOptions EngineOptions

	baseLogger *zap.Logger
	Logger     *zap.Logger

	startupProgressMetrics interface {
		AddShard()
		CompletedShard()
	}

	closing chan struct{}
	wg      sync.WaitGroup
	opened  bool
}

// NewStore returns a new store with the given path and a default configuration.
// The returned store must be initialized by calling Open before using it.
func NewStore(path string) *Store {
	return &Store{
		databases:           make(map[string]*databaseState),
		path:                path,
		sfiles:              make(map[string]*SeriesFile),
		pendingShardDeletes: make(map[uint64]struct{}),
		badShards:           shardErrorMap{shardErrors: make(map[uint64]error)},
		epochs:              make(map[uint64]*epochTracker),
		EngineOptions:       NewEngineOptions(),
		Logger:              zap.NewNop(),
		baseLogger:          zap.NewNop(),
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

func (s *Store) WithStartupMetrics(sp interface {
	AddShard()
	CompletedShard()
}) {
	s.startupProgressMetrics = sp
}

// CollectBucketMetrics sets prometheus metrics for each bucket
func (s *Store) CollectBucketMetrics() {
	// Collect all the bucket cardinality estimations
	databases := s.Databases()
	for _, database := range databases {

		log := s.Logger.With(logger.Database(database))
		sc, err := s.SeriesCardinality(context.Background(), database)
		if err != nil {
			log.Info("Cannot retrieve series cardinality", zap.Error(err))
			continue
		}

		mc, err := s.MeasurementsCardinality(context.Background(), database)
		if err != nil {
			log.Info("Cannot retrieve measurement cardinality", zap.Error(err))
			continue
		}

		labels := prometheus.Labels{bucketLabel: database}
		seriesCardinality := globalBucketMetrics.seriesCardinality.With(labels)
		measureCardinality := globalBucketMetrics.measureCardinality.With(labels)

		seriesCardinality.Set(float64(sc))
		measureCardinality.Set(float64(mc))
	}
}

var globalBucketMetrics = newAllBucketMetrics()

const bucketSubsystem = "bucket"
const bucketLabel = "bucket"

type allBucketMetrics struct {
	seriesCardinality  *prometheus.GaugeVec
	measureCardinality *prometheus.GaugeVec
}

func newAllBucketMetrics() *allBucketMetrics {
	labels := []string{bucketLabel}
	return &allBucketMetrics{
		seriesCardinality: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: storageNamespace,
			Subsystem: bucketSubsystem,
			Name:      "series_num",
			Help:      "Gauge of series cardinality per bucket",
		}, labels),
		measureCardinality: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: storageNamespace,
			Subsystem: bucketSubsystem,
			Name:      "measurement_num",
			Help:      "Gauge of measurement cardinality per bucket",
		}, labels),
	}
}

func BucketCollectors() []prometheus.Collector {
	return []prometheus.Collector{
		globalBucketMetrics.seriesCardinality,
		globalBucketMetrics.measureCardinality,
	}
}

func (s *Store) IndexBytes() int {
	// Build index set to work on.
	is := IndexSet{Indexes: make([]Index, 0, len(s.shardIDs()))}
	s.mu.RLock()
	for _, sid := range s.shardIDs() {
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

	var b int
	for _, idx := range is.Indexes {
		b += idx.Bytes()
	}

	return b
}

// Path returns the store's root path.
func (s *Store) Path() string { return s.path }

// Open initializes the store, creating all necessary directories, loading all
// shards as well as initializing periodic maintenance of them.
func (s *Store) Open(ctx context.Context) error {
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

	if err := s.loadShards(ctx); err != nil {
		return err
	}

	s.opened = true

	if !s.EngineOptions.MonitorDisabled {
		s.wg.Add(1)
		go func() {
			s.wg.Done()
			s.monitorShards()
		}()
	}

	if !s.EngineOptions.MetricsDisabled {
		s.wg.Add(1)
		go func() {
			s.wg.Done()
			s.collectMetrics()
		}()
	}

	return nil
}

func (s *Store) loadShards(ctx context.Context) error {
	// Limit the number of concurrent TSM files to be opened to the number of cores.
	s.EngineOptions.OpenLimiter = limiter.NewFixed(runtime.GOMAXPROCS(0))

	// Setup a shared limiter for compactions
	lim := s.EngineOptions.Config.MaxConcurrentCompactions
	if lim == 0 {
		lim = runtime.GOMAXPROCS(0) / 2 // Default to 50% of cores for compactions

		if lim < 1 {
			lim = 1
		}
	}

	// Don't allow more compactions to run than cores.
	if lim > runtime.GOMAXPROCS(0) {
		lim = runtime.GOMAXPROCS(0)
	}

	s.EngineOptions.CompactionLimiter = limiter.NewFixed(lim)

	compactionSettings := []zapcore.Field{zap.Int("max_concurrent_compactions", lim)}
	throughput := int(s.EngineOptions.Config.CompactThroughput)
	throughputBurst := int(s.EngineOptions.Config.CompactThroughputBurst)
	if throughput > 0 {
		if throughputBurst < throughput {
			throughputBurst = throughput
		}

		compactionSettings = append(
			compactionSettings,
			zap.Int("throughput_bytes_per_second", throughput),
			zap.Int("throughput_bytes_per_second_burst", throughputBurst),
		)
		s.EngineOptions.CompactionThroughputLimiter = limiter.NewRate(throughput, throughputBurst)
	} else {
		compactionSettings = append(
			compactionSettings,
			zap.String("throughput_bytes_per_second", "unlimited"),
			zap.String("throughput_bytes_per_second_burst", "unlimited"),
		)
	}

	s.Logger.Info("Compaction settings", compactionSettings...)

	log, logEnd := logger.NewOperation(context.TODO(), s.Logger, "Open store", "tsdb_open")
	defer logEnd()

	shardLoaderWg := new(sync.WaitGroup)
	t := limiter.NewFixed(runtime.GOMAXPROCS(0))

	// Determine how many shards we need to open by checking the store path.
	dbDirs, err := os.ReadDir(s.path)
	if err != nil {
		return err
	}

	walkShardsAndProcess := func(fn func(sfile *SeriesFile, sh os.DirEntry, db os.DirEntry, rp os.DirEntry) error) error {
		for _, db := range dbDirs {
			rpDirs, err := s.getRetentionPolicyDirs(db, log)
			if err != nil {
				return err
			} else if rpDirs == nil {
				continue
			}

			// Load series file.
			sfile, err := s.openSeriesFile(db.Name())
			if err != nil {
				return err
			}

			for _, rp := range rpDirs {
				shardDirs, err := s.getShards(rp, db, log)
				if err != nil {
					return err
				} else if shardDirs == nil {
					continue
				}

				for _, sh := range shardDirs {
					// Series file should not be in a retention policy but skip just in case.
					if sh.Name() == SeriesFileDirectory {
						log.Warn("Skipping series file in retention policy dir", zap.String("path", filepath.Join(s.path, db.Name(), rp.Name())))
						continue
					}

					if err := fn(sfile, sh, db, rp); err != nil {
						return err
					}
				}
			}
		}

		return nil
	}

	// We use `rawShardCount` as a buffer size for channel creation below.
	// If there is no startupProgressMetrics count then this will be 0 creating a
	// zero buffer channel.
	rawShardCount := 0
	if s.startupProgressMetrics != nil {
		err := walkShardsAndProcess(func(sfile *SeriesFile, sh os.DirEntry, db os.DirEntry, rp os.DirEntry) error {
			rawShardCount++
			s.startupProgressMetrics.AddShard()
			return nil
		})
		if err != nil {
			return err
		}
	}

	shardResC := make(chan *shardResponse, rawShardCount)
	err = walkShardsAndProcess(func(sfile *SeriesFile, sh os.DirEntry, db os.DirEntry, rp os.DirEntry) error {
		shardLoaderWg.Add(1)

		go func(db, rp, sh string) {
			defer shardLoaderWg.Done()
			path := filepath.Join(s.path, db, rp, sh)
			walPath := filepath.Join(s.EngineOptions.Config.WALDir, db, rp, sh)

			if err := t.Take(ctx); err != nil {
				log.Error("failed to open shard at path", zap.String("path", path), zap.Error(err))
				shardResC <- &shardResponse{err: fmt.Errorf("failed to open shard at path %q: %w", path, err)}
				return
			}
			defer t.Release()

			start := time.Now()

			// Shard file names are numeric shardIDs
			shardID, err := strconv.ParseUint(sh, 10, 64)
			if err != nil {
				log.Info("invalid shard ID found at path", zap.String("path", path))
				shardResC <- &shardResponse{err: fmt.Errorf("%s is not a valid ID. Skipping shard.", sh)}
				if s.startupProgressMetrics != nil {
					s.startupProgressMetrics.CompletedShard()
				}
				return
			}

			if s.EngineOptions.ShardFilter != nil && !s.EngineOptions.ShardFilter(db, rp, shardID) {
				log.Info("skipping shard", zap.String("path", path), logger.Shard(shardID))
				shardResC <- &shardResponse{}
				if s.startupProgressMetrics != nil {
					s.startupProgressMetrics.CompletedShard()
				}
				return
			}

			// Copy options and assign shared index.
			opt := s.EngineOptions

			// Provide an implementation of the ShardIDSets
			opt.SeriesIDSets = shardSet{store: s, db: db}

			// Open engine.
			shard := NewShard(shardID, path, walPath, sfile, opt)

			// Disable compactions, writes and queries until all shards are loaded
			shard.EnableOnOpen = false
			shard.CompactionDisabled = s.EngineOptions.CompactionDisabled
			shard.WithLogger(s.baseLogger)

			err = s.OpenShard(ctx, shard, false)
			if err != nil {
				log.Error("Failed to open shard", logger.Shard(shardID), zap.Error(err))
				shardResC <- &shardResponse{err: fmt.Errorf("failed to open shard: %d: %w", shardID, err)}
				if s.startupProgressMetrics != nil {
					s.startupProgressMetrics.CompletedShard()
				}
				return
			}

			shardResC <- &shardResponse{s: shard}
			log.Info("Opened shard", zap.String("index_version", shard.IndexType()), zap.String("path", path), zap.Duration("duration", time.Since(start)))
			if s.startupProgressMetrics != nil {
				s.startupProgressMetrics.CompletedShard()
			}
		}(db.Name(), rp.Name(), sh.Name())

		return nil
	})
	if err != nil {
		return err
	}

	if err := s.enableShards(shardLoaderWg, shardResC); err != nil {
		return err
	}

	return nil
}

func (s *Store) enableShards(wg *sync.WaitGroup, resC chan *shardResponse) error {
	go func() {
		wg.Wait()
		close(resC)
	}()

	for res := range resC {
		if res.s == nil || res.err != nil {
			continue
		}
		s.shards[res.s.id] = res.s
		s.epochs[res.s.id] = newEpochTracker()
		if _, ok := s.databases[res.s.database]; !ok {
			s.databases[res.s.database] = new(databaseState)
		}
		s.databases[res.s.database].addIndexType(res.s.IndexType())
	}

	// Check if any databases are running multiple index types.
	for db, state := range s.databases {
		if state.hasMultipleIndexTypes() {
			var fields []zapcore.Field
			for idx, cnt := range state.indexTypes {
				fields = append(fields, zap.Int(fmt.Sprintf("%s_count", idx), cnt))
			}
			s.Logger.Warn("Mixed shard index types", append(fields, logger.Database(db))...)
		}
	}

	// Enable all shards
	for _, sh := range s.shards {
		sh.SetEnabled(true)
		if isIdle, _ := sh.IsIdle(); isIdle {
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
		if s.EngineOptions.Config.WALFlushOnShutdown {
			return sh.FlushAndClose()
		} else {
			return sh.Close()
		}
	}); err != nil {
		return err
	}

	s.mu.Lock()
	for _, sfile := range s.sfiles {
		// Close out the series files.
		if err := sfile.Close(); err != nil {
			s.mu.Unlock()
			return err
		}
	}

	s.databases = make(map[string]*databaseState)
	s.sfiles = map[string]*SeriesFile{}
	s.pendingShardDeletes = make(map[uint64]struct{})
	s.shards = nil
	s.opened = false // Store may now be opened again.
	s.mu.Unlock()
	return nil
}

// epochsForShards returns a copy of the epoch trackers only including what is necessary
// for the provided shards. Must be called under the lock.
func (s *Store) epochsForShards(shards []*Shard) map[uint64]*epochTracker {
	out := make(map[uint64]*epochTracker)
	for _, sh := range shards {
		out[sh.id] = s.epochs[sh.id]
	}
	return out
}

// openSeriesFile either returns or creates a series file for the provided
// database. It must be called under a full lock.
func (s *Store) openSeriesFile(database string) (*SeriesFile, error) {
	if sfile := s.sfiles[database]; sfile != nil {
		return sfile, nil
	}

	sfile := NewSeriesFile(filepath.Join(s.path, database, SeriesFileDirectory))
	sfile.WithMaxCompactionConcurrency(s.EngineOptions.Config.SeriesFileMaxConcurrentSnapshotCompactions)
	sfile.Logger = s.baseLogger
	if err := sfile.Open(); err != nil {
		return nil, err
	}
	s.sfiles[database] = sfile
	return sfile, nil
}

func (s *Store) SeriesFile(database string) *SeriesFile {
	return s.seriesFile(database)
}

func (s *Store) seriesFile(database string) *SeriesFile {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.sfiles[database]
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

type ErrPreviousShardFail struct {
	error
}

func (e ErrPreviousShardFail) Unwrap() error {
	return e.error
}

func (e ErrPreviousShardFail) Is(err error) bool {
	_, sOk := err.(ErrPreviousShardFail)
	_, pOk := err.(*ErrPreviousShardFail)
	return sOk || pOk
}

func (e ErrPreviousShardFail) Error() string {
	return e.error.Error()
}

func (s *Store) OpenShard(ctx context.Context, sh *Shard, force bool) error {
	if sh == nil {
		return errors.New("cannot open nil shard")
	}
	oldErr, bad := s.badShards.shardError(sh.ID())
	if force || !bad {
		err := sh.Open(ctx)
		s.badShards.setShardOpenError(sh.ID(), err)
		return err
	} else {
		return fmt.Errorf("not attempting to open shard %d; %w", sh.ID(), oldErr)
	}
}

func (s *Store) SetShardOpenErrorForTest(shardID uint64, err error) {
	s.badShards.setShardOpenError(shardID, err)
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

	readCloser, size, _, err := sh.Digest()
	return readCloser, size, err
}

// CreateShard creates a shard with the given id and retention policy on a database.
func (s *Store) CreateShard(ctx context.Context, database, retentionPolicy string, shardID uint64, enabled bool) error {
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
		return ErrShardDeletion
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

	// Copy index options and pass in shared index.
	opt := s.EngineOptions
	opt.SeriesIDSets = shardSet{store: s, db: database}

	path := filepath.Join(s.path, database, retentionPolicy, strconv.FormatUint(shardID, 10))
	shard := NewShard(shardID, path, walPath, sfile, opt)
	shard.WithLogger(s.baseLogger)
	shard.EnableOnOpen = enabled

	if err := s.OpenShard(ctx, shard, false); err != nil {
		return err
	}

	s.shards[shardID] = shard
	s.epochs[shardID] = newEpochTracker()
	if _, ok := s.databases[database]; !ok {
		s.databases[database] = new(databaseState)
	}
	s.databases[database].addIndexType(shard.IndexType())
	if state := s.databases[database]; state.hasMultipleIndexTypes() {
		var fields []zapcore.Field
		for idx, cnt := range state.indexTypes {
			fields = append(fields, zap.Int(fmt.Sprintf("%s_count", idx), cnt))
		}
		s.Logger.Warn("Mixed shard index types", append(fields, logger.Database(database))...)
	}

	return nil
}

// CreateShardSnapShot will create a hard link to the underlying shard and return a path.
// The caller is responsible for cleaning up (removing) the file path returned.
func (s *Store) CreateShardSnapshot(id uint64, skipCacheOk bool) (string, error) {
	sh := s.Shard(id)
	if sh == nil {
		return "", ErrShardNotFound
	}

	return sh.CreateSnapshot(skipCacheOk)
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

// DeleteShards removes all shards from disk.
func (s *Store) DeleteShards() error {
	for _, id := range s.ShardIDs() {
		if err := s.DeleteShard(id); err != nil {
			return err
		}
	}
	return nil
}

// SetShardNewReadersBlocked sets if new readers can access the shard. If blocked
// is true, the number of reader blocks is incremented and new readers will
// receive an error instead of shard access. If blocked is false, the number
// of reader blocks is decremented. If the reader blocks drops to 0, then
// new readers will be granted access to the shard.
func (s *Store) SetShardNewReadersBlocked(shardID uint64, blocked bool) error {
	sh := s.Shard(shardID)
	if sh == nil {
		return fmt.Errorf("SetShardNewReadersBlocked: shardID=%d, blocked=%t: %w", shardID, blocked, ErrShardNotFound)
	}
	return sh.SetNewReadersBlocked(blocked)
}

// ShardInUse returns true if a shard is in-use (e.g. has active readers).
// SetShardNewReadersBlocked(id, true) should be called before checking
// ShardInUse to prevent race conditions where a reader could gain
// access to the shard immediately after ShardInUse is called.
func (s *Store) ShardInUse(shardID uint64) (bool, error) {
	sh := s.Shard(shardID)
	if sh == nil {
		return false, fmt.Errorf("ShardInUse: shardID=%d: %w", shardID, ErrShardNotFound)
	}
	return sh.InUse()
}

// DeleteShard removes a shard from disk.
func (s *Store) DeleteShard(shardID uint64) error {
	sh := s.Shard(shardID)
	if sh == nil {
		return nil
	}

	// Remove the shard from Store, so it's not returned to callers requesting
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
	delete(s.epochs, shardID)
	s.pendingShardDeletes[shardID] = struct{}{}

	db := sh.Database()
	// Determine if the shard contained any series that are not present in any
	// other shards in the database.
	shards := s.filterShards(byDatabase(db))
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

	ss := index.SeriesIDSet()

	err = s.walkShards(shards, func(sh *Shard) error {
		index, err := sh.Index()
		if err != nil {
			s.Logger.Error("cannot find shard index", zap.Uint64("shard_id", sh.ID()), zap.Error(err))
			return err
		}

		ss.Diff(index.SeriesIDSet())
		return nil
	})

	if err != nil {
		// We couldn't get the index for a shard. Rather than deleting series which may
		// exist in that shard as well as in the current shard, we stop the current deletion
		return err
	}

	// Remove any remaining series in the set from the series file, as they don't
	// exist in any of the database's remaining shards.
	if ss.Cardinality() > 0 {
		sfile := s.seriesFile(db)
		if sfile != nil {
			ss.ForEach(func(id uint64) {
				if err := sfile.DeleteSeriesID(id); err != nil {
					sfile.Logger.Error(
						"cannot delete series in shard",
						zap.Uint64("series_id", id),
						zap.Uint64("shard_id", shardID),
						zap.Error(err))
				}
			})
		}
	}

	// Close the shard.
	if err := sh.Close(); err != nil {
		return err
	}

	// Remove the on-disk shard data.
	if err := os.RemoveAll(sh.path); err != nil {
		return err
	} else if err = os.RemoveAll(sh.walPath); err != nil {
		return err
	} else {
		// Remove index type from the database on success
		s.databases[db].removeIndexType(sh.IndexType())
		return nil
	}
}

// DeleteDatabase will close all shards associated with a database and remove the directory and files from disk.
//
// Returns nil if no database exists
func (s *Store) DeleteDatabase(name string) error {
	s.mu.RLock()
	if _, ok := s.databases[name]; !ok {
		s.mu.RUnlock()
		// no files locally, so nothing to do
		return nil
	}
	shards := s.filterShards(byDatabase(name))
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
		delete(s.epochs, sh.id)
	}

	// Remove database from store list of databases
	delete(s.databases, name)

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
	shards := s.filterShards(ComposeShardFilter(byDatabase(database), byRetentionPolicy(name)))
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

	// Remove the retention policy folder from the WAL.
	if err := os.RemoveAll(filepath.Join(s.EngineOptions.Config.WALDir, database, name)); err != nil {
		return err
	}

	s.mu.Lock()
	state := s.databases[database]
	for _, sh := range shards {
		delete(s.shards, sh.id)
		state.removeIndexType(sh.IndexType())
	}
	s.mu.Unlock()
	return nil
}

// DeleteMeasurement removes a measurement and all associated series from a database.
func (s *Store) DeleteMeasurement(ctx context.Context, database, name string) error {
	s.mu.RLock()
	if s.databases[database].hasMultipleIndexTypes() {
		s.mu.RUnlock()
		return ErrMultipleIndexTypes
	}
	shards := s.filterShards(byDatabase(database))
	epochs := s.epochsForShards(shards)
	s.mu.RUnlock()

	// Limit to 1 delete for each shard since expanding the measurement into the list
	// of series keys can be very memory intensive if run concurrently.
	limit := limiter.NewFixed(1)
	return s.walkShards(shards, func(sh *Shard) error {
		if err := limit.Take(ctx); err != nil {
			return err
		}
		defer limit.Release()

		// install our guard and wait for any prior deletes to finish. the
		// guard ensures future deletes that could conflict wait for us.
		guard := newGuard(influxql.MinTime, influxql.MaxTime, []string{name}, nil)
		waiter := epochs[sh.id].WaitDelete(guard)
		waiter.Wait()
		defer waiter.Done()

		return sh.DeleteMeasurement(ctx, []byte(name))
	})
}

// filterShards returns a slice of shards where fn returns true
// for the shard. If the provided predicate is nil then all shards are returned.
// filterShards should be called under a lock.
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

type ShardPredicate = func(sh *Shard) bool

func ComposeShardFilter(fns ...ShardPredicate) ShardPredicate {
	return func(sh *Shard) bool {
		for _, fn := range fns {
			if !fn(sh) {
				return false
			}
		}

		return true
	}
}

// byDatabase provides a predicate for filterShards that matches on the name of
// the database passed in.
func byDatabase(name string) func(sh *Shard) bool {
	return func(sh *Shard) bool {
		return sh.database == name
	}
}

// byRetentionPolicy provides a predicate for filterShards that matches on the name of
// the retention policy passed in.
func byRetentionPolicy(name string) ShardPredicate {
	return func(sh *Shard) bool {
		return sh.retentionPolicy == name
	}
}

// walkShards apply a function to each shard in parallel. fn must be safe for
// concurrent use. If any of the functions return an error, the first error is
// returned.
func (s *Store) walkShards(shards []*Shard, fn func(sh *Shard) error) error {

	resC := make(chan shardResponse, len(shards))
	var n int

	for _, sh := range shards {
		n++

		go func(sh *Shard) {
			if err := fn(sh); err != nil {
				resC <- shardResponse{err: fmt.Errorf("shard %d: %s", sh.id, err)}
				return
			}

			resC <- shardResponse{}
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
	for k := range s.databases {
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
func (s *Store) SeriesCardinality(ctx context.Context, database string) (int64, error) {
	s.mu.RLock()
	shards := s.filterShards(byDatabase(database))
	s.mu.RUnlock()

	ss, err := s.SeriesCardinalityFromShards(ctx, shards)
	if err != nil {
		return 0, err
	}

	return int64(ss.Cardinality()), nil
}

func (s *Store) SeriesCardinalityFromShards(ctx context.Context, shards []*Shard) (*SeriesIDSet, error) {
	var setMu sync.Mutex
	others := make([]*SeriesIDSet, 0, len(shards))

	err := s.walkShards(shards, func(sh *Shard) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		index, err := sh.Index()
		if err != nil {
			return err
		}

		seriesIDs := index.SeriesIDSet()
		setMu.Lock()
		others = append(others, seriesIDs)
		setMu.Unlock()

		return nil
	})
	if err != nil {
		return nil, err
	}

	ss := NewSeriesIDSet()
	ss.Merge(others...)
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	return ss, nil
}

// SeriesSketches returns the sketches associated with the series data in all
// the shards in the provided database.
//
// The returned sketches can be combined with other sketches to provide an
// estimation across distributed databases.
func (s *Store) SeriesSketches(ctx context.Context, database string) (estimator.Sketch, estimator.Sketch, error) {
	return s.sketchesForDatabase(database, func(sh *Shard) (estimator.Sketch, estimator.Sketch, error) {
		select {
		case <-ctx.Done():
			return nil, nil, ctx.Err()
		default:
		}
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
func (s *Store) MeasurementsCardinality(ctx context.Context, database string) (int64, error) {
	ss, ts, err := s.MeasurementsSketches(ctx, database)

	if err != nil {
		return 0, err
	}
	mc := int64(ss.Count() - ts.Count())
	if mc < 0 {
		mc = 0
	}
	return mc, nil
}

// MeasurementsSketches returns the sketches associated with the measurement
// data in all the shards in the provided database.
//
// The returned sketches can be combined with other sketches to provide an
// estimation across distributed databases.
func (s *Store) MeasurementsSketches(ctx context.Context, database string) (estimator.Sketch, estimator.Sketch, error) {
	return s.sketchesForDatabase(database, func(sh *Shard) (estimator.Sketch, estimator.Sketch, error) {
		// every iteration, check for timeout.
		select {
		case <-ctx.Done():
			return nil, nil, ctx.Err()
		default:
		}
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
		return &errors2.Error{
			Code: errors2.ENotFound,
			Msg:  fmt.Sprintf("shard %d not found", id),
		}
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
		return &errors2.Error{
			Code: errors2.ENotFound,
			Msg:  fmt.Sprintf("shard %d not found", id),
		}
	}

	path, err := relativePath(s.path, shard.path)
	if err != nil {
		return err
	}

	return shard.Export(w, path, start, end)
}

// RestoreShard restores a backup from r to a given shard.
// This will only overwrite files included in the backup.
func (s *Store) RestoreShard(ctx context.Context, id uint64, r io.Reader) error {
	shard := s.Shard(id)
	if shard == nil {
		return fmt.Errorf("shard %d doesn't exist on this server", id)
	}

	path, err := relativePath(s.path, shard.path)
	if err != nil {
		return err
	}

	return shard.Restore(ctx, r, path)
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
func (s *Store) DeleteSeriesWithPredicate(ctx context.Context, database string, min, max int64, pred influxdb.Predicate, measurement influxql.Expr) error {
	s.mu.RLock()
	if s.databases[database].hasMultipleIndexTypes() {
		s.mu.RUnlock()
		return ErrMultipleIndexTypes
	}
	sfile := s.sfiles[database]
	if sfile == nil {
		s.mu.RUnlock()
		// No series file means nothing has been written to this DB and thus nothing to delete.
		return nil
	}
	shards := s.filterShards(byDatabase(database))
	epochs := s.epochsForShards(shards)
	s.mu.RUnlock()

	// Limit to 1 delete for each shard since expanding the measurement into the list
	// of series keys can be very memory intensive if run concurrently.
	limit := limiter.NewFixed(1)

	return s.walkShards(shards, func(sh *Shard) (err error) {
		if err := limit.Take(ctx); err != nil {
			return err
		}
		defer limit.Release()

		// install our guard and wait for any prior deletes to finish. the
		// guard ensures future deletes that could conflict wait for us.
		waiter := epochs[sh.id].WaitDelete(newGuard(min, max, nil, nil))
		waiter.Wait()
		defer waiter.Done()

		index, err := sh.Index()
		if err != nil {
			return err
		}

		measurementName := make([]byte, 0)

		if measurement != nil {
			if m, ok := measurement.(*influxql.BinaryExpr); ok {
				rhs, ok := m.RHS.(*influxql.VarRef)
				if ok {
					measurementName = []byte(rhs.Val)
					exists, err := sh.MeasurementExists(measurementName)
					if err != nil {
						return err
					}
					if !exists {
						return nil
					}
				}
			}
		}

		// Find matching series keys for each measurement.
		mitr, err := index.MeasurementIterator()
		if err != nil {
			return err
		}
		defer errors3.Capture(&err, mitr.Close)()

		deleteSeries := func(mm []byte) error {
			sitr, err := index.MeasurementSeriesIDIterator(mm)
			if err != nil {
				return err
			} else if sitr == nil {
				return nil
			}
			defer errors3.Capture(&err, sitr.Close)()

			itr := NewSeriesIteratorAdapter(sfile, NewPredicateSeriesIDIterator(sitr, sfile, pred))
			return sh.DeleteSeriesRange(ctx, itr, min, max)
		}

		for {
			mm, err := mitr.Next()
			if err != nil {
				return err
			} else if mm == nil {
				break
			}

			// If we are deleting within a measurement and have found a match, we can return after the delete.
			if measurementName != nil && bytes.Equal(mm, measurementName) {
				return deleteSeries(mm)
			} else {
				err := deleteSeries(mm)
				if err != nil {
					return err
				}
			}
		}

		return nil
	})
}

// DeleteSeries loops through the local shards and deletes the series data for
// the passed in series keys.
func (s *Store) DeleteSeries(ctx context.Context, database string, sources []influxql.Source, condition influxql.Expr) error {
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
	if s.databases[database].hasMultipleIndexTypes() {
		s.mu.RUnlock()
		return ErrMultipleIndexTypes
	}
	sfile := s.sfiles[database]
	if sfile == nil {
		s.mu.RUnlock()
		// No series file means nothing has been written to this DB and thus nothing to delete.
		return nil
	}
	shards := s.filterShards(byDatabase(database))
	epochs := s.epochsForShards(shards)
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

		if err := limit.Take(ctx); err != nil {
			return err
		}
		defer limit.Release()

		// install our guard and wait for any prior deletes to finish. the
		// guard ensures future deletes that could conflict wait for us.
		waiter := epochs[sh.id].WaitDelete(newGuard(min, max, names, condition))
		waiter.Wait()
		defer waiter.Done()

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
			if err := sh.DeleteSeriesRange(ctx, NewSeriesIteratorAdapter(sfile, itr), min, max); err != nil {
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
func (s *Store) WriteToShard(ctx context.Context, shardID uint64, points []models.Point) error {
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

	epoch := s.epochs[shardID]

	s.mu.RUnlock()

	// enter the epoch tracker
	guards, gen := epoch.StartWrite()
	defer epoch.EndWrite(gen)

	// wait for any guards before writing the points.
	for _, guard := range guards {
		if guard.Matches(points) {
			guard.Wait()
		}
	}

	// Ensure snapshot compactions are enabled since the shard might have been cold
	// and disabled by the monitor.
	if isIdle, _ := sh.IsIdle(); isIdle {
		sh.SetCompactionsEnabled(true)
	}

	return sh.WritePoints(ctx, points)
}

// MeasurementNames returns a slice of all measurements. Measurements accepts an
// optional condition expression. If cond is nil, then all measurements for the
// database will be returned.
func (s *Store) MeasurementNames(ctx context.Context, auth query.Authorizer, database string, cond influxql.Expr) ([][]byte, error) {
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
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	return is.MeasurementNamesByExpr(auth, cond)
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
func (s *Store) TagKeys(ctx context.Context, auth query.Authorizer, shardIDs []uint64, cond influxql.Expr) ([]TagKeys, error) {
	if len(shardIDs) == 0 {
		return nil, nil
	}

	// take out the _name = 'mymeasurement' clause from 'FROM' clause
	measurementExpr, remainingExpr, err := influxql.PartitionExpr(influxql.CloneExpr(cond), func(e influxql.Expr) (bool, error) {
		switch e := e.(type) {
		case *influxql.BinaryExpr:
			switch e.Op {
			case influxql.EQ, influxql.NEQ, influxql.EQREGEX, influxql.NEQREGEX:
				tag, ok := e.LHS.(*influxql.VarRef)
				if ok && tag.Val == "_name" {
					return true, nil
				}
			}
		}
		return false, nil
	})
	if err != nil {
		return nil, err
	}

	// take out the _tagKey = 'mykey' clause from 'WITH KEY' clause
	tagKeyExpr, filterExpr, err := influxql.PartitionExpr(remainingExpr, isTagKeyClause)
	if err != nil {
		return nil, err
	}
	if err = isBadQuoteTagValueClause(filterExpr); err != nil {
		return nil, err
	}

	// Get all the shards we're interested in.
	is := IndexSet{Indexes: make([]Index, 0, len(shardIDs))}
	s.mu.RLock()
	for _, sid := range shardIDs {
		shard, ok := s.shards[sid]
		if !ok {
			continue
		}

		if is.SeriesFile == nil {
			sfile, err := shard.SeriesFile()
			if err != nil {
				s.mu.RUnlock()
				return nil, err
			}
			is.SeriesFile = sfile
		}

		index, err := shard.Index()
		if err != nil {
			s.mu.RUnlock()
			return nil, err
		}
		is.Indexes = append(is.Indexes, index)
	}
	s.mu.RUnlock()

	// Determine list of measurements.
	names, err := is.MeasurementNamesByExpr(nil, measurementExpr)
	if err != nil {
		return nil, err
	}

	// Iterate over each measurement.
	var results []TagKeys
	for _, name := range names {

		// Check for timeouts.
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		// Build keyset over all indexes for measurement.
		tagKeySet, err := is.MeasurementTagKeysByExpr(name, tagKeyExpr)
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
				// check for timeouts
				select {
				case <-ctx.Done():
					return nil, ctx.Err()
				default:
				}
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

func isTagKeyClause(e influxql.Expr) (bool, error) {
	switch e := e.(type) {
	case *influxql.BinaryExpr:
		switch e.Op {
		case influxql.EQ, influxql.NEQ, influxql.EQREGEX, influxql.NEQREGEX:
			tag, ok := e.LHS.(*influxql.VarRef)
			if ok && tag.Val == "_tagKey" {
				return true, nil
			}
		case influxql.OR, influxql.AND:
			ok1, err := isTagKeyClause(e.LHS)
			if err != nil {
				return false, err
			}
			ok2, err := isTagKeyClause(e.RHS)
			if err != nil {
				return false, err
			}
			return ok1 && ok2, nil
		}
	case *influxql.ParenExpr:
		return isTagKeyClause(e.Expr)
	}
	return false, nil
}

func isBadQuoteTagValueClause(e influxql.Expr) error {
	switch e := e.(type) {
	case *influxql.BinaryExpr:
		switch e.Op {
		case influxql.EQ, influxql.NEQ:
			_, lOk := e.LHS.(*influxql.VarRef)
			_, rOk := e.RHS.(*influxql.VarRef)
			if lOk && rOk {
				return fmt.Errorf("bad WHERE clause for metaquery; one term must be a string literal tag value within single quotes: %s", e.String())
			}
		case influxql.OR, influxql.AND:
			if err := isBadQuoteTagValueClause(e.LHS); err != nil {
				return err
			} else if err = isBadQuoteTagValueClause(e.RHS); err != nil {
				return err
			} else {
				return nil
			}
		}
	case *influxql.ParenExpr:
		return isBadQuoteTagValueClause(e.Expr)
	}
	return nil
}

// TagValues returns the tag keys and values for the provided shards, where the
// tag values satisfy the provided condition.
func (s *Store) TagValues(ctx context.Context, auth query.Authorizer, shardIDs []uint64, cond influxql.Expr) ([]TagValues, error) {
	if len(shardIDs) == 0 {
		return nil, nil
	}

	if cond == nil {
		return nil, errors.New("a condition is required")
	}

	// take out the _name = 'mymeasurement' clause from 'FROM' clause
	measurementExpr, remainingExpr, err := influxql.PartitionExpr(influxql.CloneExpr(cond), func(e influxql.Expr) (bool, error) {
		switch e := e.(type) {
		case *influxql.BinaryExpr:
			switch e.Op {
			case influxql.EQ, influxql.NEQ, influxql.EQREGEX, influxql.NEQREGEX:
				tag, ok := e.LHS.(*influxql.VarRef)
				if ok && tag.Val == "_name" {
					return true, nil
				}
			}
		}
		return false, nil
	})
	if err != nil {
		return nil, err
	}

	// take out the _tagKey = 'mykey' clause from 'WITH KEY' / 'WITH KEY IN' clause
	tagKeyExpr, filterExpr, err := influxql.PartitionExpr(remainingExpr, isTagKeyClause)
	if err != nil {
		return nil, err
	}
	if err = isBadQuoteTagValueClause(filterExpr); err != nil {
		return nil, err
	}
	// Build index set to work on.
	is := IndexSet{Indexes: make([]Index, 0, len(shardIDs))}
	s.mu.RLock()
	for _, sid := range shardIDs {
		shard, ok := s.shards[sid]
		if !ok {
			continue
		}

		if is.SeriesFile == nil {
			sfile, err := shard.SeriesFile()
			if err != nil {
				s.mu.RUnlock()
				return nil, err
			}
			is.SeriesFile = sfile
		}

		index, err := shard.Index()
		if err != nil {
			s.mu.RUnlock()
			return nil, err
		}

		is.Indexes = append(is.Indexes, index)
	}
	s.mu.RUnlock()

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

	// Stores each list of TagValues for each measurement.
	allResults := make([]tagValues, 0, len(names))

	// Iterate over each matching measurement in the shard. For each
	// measurement we'll get the matching tag keys (e.g., when a WITH KEYS)
	// statement is used, and we'll then use those to fetch all the relevant
	// values from matching series. Series may be filtered using a WHERE
	// filter.
	for _, name := range names {
		// check for timeouts
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		// Determine a list of keys from condition.
		keySet, err := is.MeasurementTagKeysByExpr(name, tagKeyExpr)
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
		sort.Strings(result.keys)

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

	// Not sure this is necessary, should be pre-sorted
	sort.Sort(tagValuesSlice(allResults))

	result := make([]TagValues, 0, maxMeasurements)
	for _, r := range allResults {
		// check for timeouts
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		nextResult := makeTagValues(r)
		if len(nextResult.Values) > 0 {
			result = append(result, nextResult)
		}
	}
	return result, nil
}

func makeTagValues(tv tagValues) TagValues {
	var result TagValues
	result.Measurement = string(tv.name)
	// TODO(edd): will be too small likely. Find a hint?
	result.Values = make([]KeyValue, 0, len(tv.values))

	for ki, key := range tv.keys {
		for _, value := range tv.values[ki] {
			result.Values = append(result.Values, KeyValue{Key: key, Value: value})
		}
	}
	return result
}

func (s *Store) monitorShards() {
	t := time.NewTicker(10 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-s.closing:
			return
		case <-t.C:
			s.mu.RLock()
			for _, sh := range s.shards {
				if isIdle, _ := sh.IsIdle(); isIdle {
					if err := sh.Free(); err != nil {
						s.Logger.Warn("Error while freeing cold shard resources",
							zap.Error(err),
							logger.Shard(sh.ID()))
					}
				} else {
					sh.SetCompactionsEnabled(true)
				}
			}
			s.mu.RUnlock()
		}
	}
}

func (s *Store) collectMetrics() {
	t := time.NewTicker(10 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-s.closing:
			return
		case <-t.C:
			s.CollectBucketMetrics()
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

		f(idx.SeriesIDSet())
	}
	return nil
}

func (s *Store) getRetentionPolicyDirs(db os.DirEntry, log *zap.Logger) ([]os.DirEntry, error) {
	dbPath := filepath.Join(s.path, db.Name())
	if !db.IsDir() {
		log.Info("Skipping database dir", zap.String("name", db.Name()), zap.String("reason", "not a directory"))
		return nil, nil
	}

	if s.EngineOptions.DatabaseFilter != nil && !s.EngineOptions.DatabaseFilter(db.Name()) {
		log.Info("Skipping database dir", logger.Database(db.Name()), zap.String("reason", "failed database filter"))
		return nil, nil
	}

	// Load each retention policy within the database directory.
	rpDirs, err := os.ReadDir(dbPath)
	if err != nil {
		return nil, err
	}

	return rpDirs, nil
}

func (s *Store) getShards(rpDir os.DirEntry, dbDir os.DirEntry, log *zap.Logger) ([]os.DirEntry, error) {
	rpPath := filepath.Join(s.path, dbDir.Name(), rpDir.Name())
	if !rpDir.IsDir() {
		log.Info("Skipping retention policy dir", zap.String("name", rpDir.Name()), zap.String("reason", "not a directory"))
		return nil, nil
	}

	// The .series directory is not a retention policy.
	if rpDir.Name() == SeriesFileDirectory {
		return nil, nil
	}

	if s.EngineOptions.RetentionPolicyFilter != nil && !s.EngineOptions.RetentionPolicyFilter(dbDir.Name(), rpDir.Name()) {
		log.Info("Skipping retention policy dir", logger.RetentionPolicy(rpDir.Name()), zap.String("reason", "failed retention policy filter"))
		return nil, nil
	}

	shardDirs, err := os.ReadDir(rpPath)
	if err != nil {
		return nil, err
	}

	return shardDirs, nil
}

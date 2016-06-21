package tsm1 // import "github.com/influxdata/influxdb/tsdb/engine/tsm1"

import (
	"archive/tar"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/influxdata/influxdb/influxql"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/tsdb"
)

//go:generate tmpl -data=@iterator.gen.go.tmpldata iterator.gen.go.tmpl
//go:generate tmpl -data=@file_store.gen.go.tmpldata file_store.gen.go.tmpl
//go:generate tmpl -data=@encoding.gen.go.tmpldata encoding.gen.go.tmpl

func init() {
	tsdb.RegisterEngine("tsm1", NewEngine)
}

// Ensure Engine implements the interface.
var _ tsdb.Engine = &Engine{}

const (
	// keyFieldSeparator separates the series key from the field name in the composite key
	// that identifies a specific field in series
	keyFieldSeparator = "#!~#"
)

// Engine represents a storage engine with compressed blocks.
type Engine struct {
	mu                 sync.RWMutex
	done               chan struct{}
	wg                 sync.WaitGroup
	compactionsEnabled bool

	path      string
	logger    *log.Logger
	logOutput io.Writer

	// TODO(benbjohnson): Index needs to be moved entirely into engine.
	index             *tsdb.DatabaseIndex
	measurementFields map[string]*tsdb.MeasurementFields

	WAL            *WAL
	Cache          *Cache
	Compactor      *Compactor
	CompactionPlan CompactionPlanner
	FileStore      *FileStore

	MaxPointsPerBlock int

	// CacheFlushMemorySizeThreshold specifies the minimum size threshodl for
	// the cache when the engine should write a snapshot to a TSM file
	CacheFlushMemorySizeThreshold uint64

	// CacheFlushWriteColdDuration specifies the length of time after which if
	// no writes have been committed to the WAL, the engine will write
	// a snapshot of the cache to a TSM file
	CacheFlushWriteColdDuration time.Duration

	// Controls whether to enabled compactions when the engine is open
	enableCompactionsOnOpen bool
}

// NewEngine returns a new instance of Engine.
func NewEngine(path string, walPath string, opt tsdb.EngineOptions) tsdb.Engine {
	w := NewWAL(walPath)
	w.LoggingEnabled = opt.Config.WALLoggingEnabled

	fs := NewFileStore(path)
	fs.traceLogging = opt.Config.DataLoggingEnabled

	cache := NewCache(uint64(opt.Config.CacheMaxMemorySize), path)

	c := &Compactor{
		Dir:       path,
		FileStore: fs,
	}

	e := &Engine{
		path:              path,
		measurementFields: make(map[string]*tsdb.MeasurementFields),

		WAL:   w,
		Cache: cache,

		FileStore: fs,
		Compactor: c,
		CompactionPlan: &DefaultPlanner{
			FileStore:                    fs,
			CompactFullWriteColdDuration: time.Duration(opt.Config.CompactFullWriteColdDuration),
		},
		MaxPointsPerBlock: opt.Config.MaxPointsPerBlock,

		CacheFlushMemorySizeThreshold: opt.Config.CacheSnapshotMemorySize,
		CacheFlushWriteColdDuration:   time.Duration(opt.Config.CacheSnapshotWriteColdDuration),
		enableCompactionsOnOpen:       true,
	}
	e.SetLogOutput(os.Stderr)

	return e
}

func (e *Engine) SetEnabled(enabled bool) {
	e.enableCompactionsOnOpen = enabled
	e.SetCompactionsEnabled(enabled)
}

// SetCompactionsEnabled enables compactions on the engine.  When disabled
// all running compactions are aborted and new compactions stop running.
func (e *Engine) SetCompactionsEnabled(enabled bool) {
	if enabled {
		e.mu.Lock()
		if e.compactionsEnabled {
			e.mu.Unlock()
			return
		}
		e.compactionsEnabled = true

		e.done = make(chan struct{})
		e.Compactor.Open()

		e.mu.Unlock()

		e.wg.Add(5)
		go e.compactCache()
		go e.compactTSMFull()
		go e.compactTSMLevel(true, 1)
		go e.compactTSMLevel(true, 2)
		go e.compactTSMLevel(false, 3)

		e.logger.Printf("compactions enabled for: %v", e.path)
	} else {
		e.mu.Lock()
		if !e.compactionsEnabled {
			e.mu.Unlock()
			return
		}
		// Prevent new compactions from starting
		e.compactionsEnabled = false
		e.mu.Unlock()

		// Stop all background compaction goroutines
		close(e.done)

		// Abort any running goroutines (this could take a while)
		e.Compactor.Close()

		// Wait for compaction goroutines to exit
		e.wg.Wait()

		e.logger.Printf("compactions disabled for: %v", e.path)
	}
}

// Path returns the path the engine was opened with.
func (e *Engine) Path() string { return e.path }

// Index returns the database index.
func (e *Engine) Index() *tsdb.DatabaseIndex {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.index
}

// MeasurementFields returns the measurement fields for a measurement.
func (e *Engine) MeasurementFields(measurement string) *tsdb.MeasurementFields {
	e.mu.RLock()
	m := e.measurementFields[measurement]
	e.mu.RUnlock()

	if m != nil {
		return m
	}

	e.mu.Lock()
	m = e.measurementFields[measurement]
	if m == nil {
		m = tsdb.NewMeasurementFields()
		e.measurementFields[measurement] = m
	}
	e.mu.Unlock()
	return m
}

// Format returns the format type of this engine
func (e *Engine) Format() tsdb.EngineFormat {
	return tsdb.TSM1Format
}

// Open opens and initializes the engine.
func (e *Engine) Open() error {
	e.done = make(chan struct{})

	if err := os.MkdirAll(e.path, 0777); err != nil {
		return err
	}

	if err := e.cleanup(); err != nil {
		return err
	}

	if err := e.WAL.Open(); err != nil {
		return err
	}

	if err := e.FileStore.Open(); err != nil {
		return err
	}

	if err := e.reloadCache(); err != nil {
		return err
	}

	if e.enableCompactionsOnOpen {
		e.SetCompactionsEnabled(true)
	}

	return nil
}

// Close closes the engine. Subsequent calls to Close are a nop.
func (e *Engine) Close() error {
	e.SetCompactionsEnabled(false)

	// Lock now and close everything else down.
	e.mu.Lock()
	defer e.mu.Unlock()
	e.done = nil // Ensures that the channel will not be closed again.

	if err := e.FileStore.Close(); err != nil {
		return err
	}
	return e.WAL.Close()
}

// SetLogOutput sets the logger used for all messages. It must not be called
// after the Open method has been called.
func (e *Engine) SetLogOutput(w io.Writer) {
	e.logger = log.New(w, "[tsm1] ", log.LstdFlags)
	e.WAL.SetLogOutput(w)
	e.FileStore.SetLogOutput(w)
	e.logOutput = w
}

// LoadMetadataIndex loads the shard metadata into memory.
func (e *Engine) LoadMetadataIndex(shardID uint64, index *tsdb.DatabaseIndex) error {
	// Save reference to index for iterator creation.
	e.index = index

	if err := e.FileStore.WalkKeys(func(key string, typ byte) error {
		fieldType, err := tsmFieldTypeToInfluxQLDataType(typ)
		if err != nil {
			return err
		}

		if err := e.addToIndexFromKey(shardID, key, fieldType, index); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return err
	}

	// load metadata from the Cache
	e.Cache.RLock() // shouldn't need the lock, but just to be safe
	defer e.Cache.RUnlock()

	for key, entry := range e.Cache.Store() {

		fieldType, err := entry.values.InfluxQLType()
		if err != nil {
			e.logger.Printf("error getting the data type of values for key %s: %s", key, err.Error())
			continue
		}

		if err := e.addToIndexFromKey(shardID, key, fieldType, index); err != nil {
			return err
		}
	}

	return nil
}

// Backup will write a tar archive of any TSM files modified since the passed
// in time to the passed in writer. The basePath will be prepended to the names
// of the files in the archive. It will force a snapshot of the WAL first
// then perform the backup with a read lock against the file store. This means
// that new TSM files will not be able to be created in this shard while the
// backup is running. For shards that are still acively getting writes, this
// could cause the WAL to backup, increasing memory usage and evenutally rejecting writes.
func (e *Engine) Backup(w io.Writer, basePath string, since time.Time) error {
	path, err := e.CreateSnapshot()
	if err != nil {
		return err
	}

	// Remove the temporary snapshot dir
	defer os.RemoveAll(path)

	snapDir, err := os.Open(path)
	if err != nil {
		return err
	}
	defer snapDir.Close()

	snapshotFiles, err := snapDir.Readdir(0)
	if err != nil {
		return err
	}

	var files []os.FileInfo
	// grab all the files and tombstones that have a modified time after since
	for _, f := range snapshotFiles {
		if f.ModTime().UnixNano() > since.UnixNano() {
			files = append(files, f)
		}
	}

	if len(files) == 0 {
		return nil
	}

	tw := tar.NewWriter(w)
	defer tw.Close()

	for _, f := range files {
		if err := e.writeFileToBackup(f, basePath, filepath.Join(path, f.Name()), tw); err != nil {
			return err
		}
	}

	return nil
}

// writeFileToBackup will copy the file into the tar archive. Files will use the shardRelativePath
// in their names. This should be the <db>/<retention policy>/<id> part of the path
func (e *Engine) writeFileToBackup(f os.FileInfo, shardRelativePath, fullPath string, tw *tar.Writer) error {
	h := &tar.Header{
		Name:    filepath.Join(shardRelativePath, f.Name()),
		ModTime: f.ModTime(),
		Size:    f.Size(),
		Mode:    int64(f.Mode()),
	}
	if err := tw.WriteHeader(h); err != nil {
		return err
	}
	fr, err := os.Open(fullPath)
	if err != nil {
		return err
	}

	defer fr.Close()

	_, err = io.CopyN(tw, fr, h.Size)

	return err
}

// Restore will read a tar archive generated by Backup().
// Only files that match basePath will be copied into the directory. This obtains
// a write lock so no operations can be performed while restoring.
func (e *Engine) Restore(r io.Reader, basePath string) error {
	// Copy files from archive while under lock to prevent reopening.
	if err := func() error {
		e.mu.Lock()
		defer e.mu.Unlock()

		tr := tar.NewReader(r)
		for {
			if err := e.readFileFromBackup(tr, basePath); err == io.EOF {
				break
			} else if err != nil {
				return err
			}
		}

		return syncDir(e.path)
	}(); err != nil {
		return err
	}

	return nil
}

// readFileFromBackup copies the next file from the archive into the shard.
// The file is skipped if it does not have a matching shardRelativePath prefix.
func (e *Engine) readFileFromBackup(tr *tar.Reader, shardRelativePath string) error {
	// Read next archive file.
	hdr, err := tr.Next()
	if err != nil {
		return err
	}

	// Skip file if it does not have a matching prefix.
	if !filepath.HasPrefix(hdr.Name, shardRelativePath) {
		return nil
	}
	path, err := filepath.Rel(shardRelativePath, hdr.Name)
	if err != nil {
		return err
	}

	destPath := filepath.Join(e.path, path)
	tmp := destPath + ".tmp"

	// Create new file on disk.
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return err
	}
	defer f.Close()

	// Copy from archive to the file.
	if _, err := io.CopyN(f, tr, hdr.Size); err != nil {
		return err
	}

	// Sync to disk & close.
	if err := f.Sync(); err != nil {
		return err
	}

	if err := f.Close(); err != nil {
		return err
	}

	return renameFile(tmp, destPath)
}

// addToIndexFromKey will pull the measurement name, series key, and field name from a composite key and add it to the
// database index and measurement fields
func (e *Engine) addToIndexFromKey(shardID uint64, key string, fieldType influxql.DataType, index *tsdb.DatabaseIndex) error {
	seriesKey, field := seriesAndFieldFromCompositeKey(key)
	measurement := tsdb.MeasurementFromSeriesKey(seriesKey)

	m := index.CreateMeasurementIndexIfNotExists(measurement)
	m.SetFieldName(field)

	mf := e.measurementFields[measurement]
	if mf == nil {
		mf = tsdb.NewMeasurementFields()
		e.measurementFields[measurement] = mf
	}

	if err := mf.CreateFieldIfNotExists(field, fieldType, false); err != nil {
		return err
	}

	// Have we already indexed this series?
	ss := index.Series(seriesKey)
	if ss != nil {
		// Add this shard to the existing series
		ss.AssignShard(shardID)
		return nil
	}

	// ignore error because ParseKey returns "missing fields" and we don't have
	// fields (in line protocol format) in the series key
	_, tags, _ := models.ParseKey(seriesKey)

	s := tsdb.NewSeries(seriesKey, tags)
	index.CreateSeriesIndexIfNotExists(measurement, s)
	s.AssignShard(shardID)

	return nil
}

// WritePoints writes metadata and point data into the engine.
// Returns an error if new points are added to an existing key.
func (e *Engine) WritePoints(points []models.Point) error {
	values := map[string][]Value{}
	for _, p := range points {
		for k, v := range p.Fields() {
			key := string(p.Key()) + keyFieldSeparator + k
			values[key] = append(values[key], NewValue(p.Time().UnixNano(), v))
		}
	}

	e.mu.RLock()
	defer e.mu.RUnlock()

	// first try to write to the cache
	err := e.Cache.WriteMulti(values)
	if err != nil {
		return err
	}

	_, err = e.WAL.WritePoints(values)
	return err
}

// ContainsSeries returns a map of keys indicating whether the key exists and
// has values or not.
func (e *Engine) ContainsSeries(keys []string) (map[string]bool, error) {
	// keyMap is used to see if a given key exists.  keys
	// are the measurement + tagset (minus separate & field)
	keyMap := map[string]bool{}
	for _, k := range keys {
		keyMap[k] = false
	}

	for _, k := range e.Cache.Keys() {
		seriesKey, _ := seriesAndFieldFromCompositeKey(k)
		keyMap[seriesKey] = true
	}

	if err := e.FileStore.WalkKeys(func(k string, _ byte) error {
		seriesKey, _ := seriesAndFieldFromCompositeKey(k)
		if _, ok := keyMap[seriesKey]; ok {
			keyMap[seriesKey] = true
		}
		return nil
	}); err != nil {
		return nil, err
	}
	return keyMap, nil
}

// DeleteSeries removes all series keys from the engine.
func (e *Engine) DeleteSeries(seriesKeys []string) error {
	return e.DeleteSeriesRange(seriesKeys, math.MinInt64, math.MaxInt64)
}

// DeleteSeriesRange removes the values between min and max (inclusive) from all series.
func (e *Engine) DeleteSeriesRange(seriesKeys []string, min, max int64) error {
	if len(seriesKeys) == 0 {
		return nil
	}

	e.mu.RLock()
	defer e.mu.RUnlock()

	// keyMap is used to see if a given key should be deleted.  seriesKey
	// are the measurement + tagset (minus separate & field)
	keyMap := map[string]struct{}{}
	for _, k := range seriesKeys {
		keyMap[k] = struct{}{}
	}

	var deleteKeys []string
	// go through the keys in the file store
	if err := e.FileStore.WalkKeys(func(k string, _ byte) error {
		seriesKey, _ := seriesAndFieldFromCompositeKey(k)
		if _, ok := keyMap[seriesKey]; ok {
			deleteKeys = append(deleteKeys, k)
		}
		return nil
	}); err != nil {
		return err
	}

	if err := e.FileStore.DeleteRange(deleteKeys, min, max); err != nil {
		return err
	}

	// find the keys in the cache and remove them
	walKeys := make([]string, 0)
	e.Cache.RLock()
	s := e.Cache.Store()
	for k, _ := range s {
		seriesKey, _ := seriesAndFieldFromCompositeKey(k)
		if _, ok := keyMap[seriesKey]; ok {
			walKeys = append(walKeys, k)
		}
	}
	e.Cache.RUnlock()

	e.Cache.DeleteRange(walKeys, min, max)

	// delete from the WAL
	_, err := e.WAL.DeleteRange(walKeys, min, max)

	return err
}

// DeleteMeasurement deletes a measurement and all related series.
func (e *Engine) DeleteMeasurement(name string, seriesKeys []string) error {
	e.mu.Lock()
	delete(e.measurementFields, name)
	e.mu.Unlock()

	return e.DeleteSeries(seriesKeys)
}

// SeriesCount returns the number of series buckets on the shard.
func (e *Engine) SeriesCount() (n int, err error) {
	return 0, nil
}

func (e *Engine) WriteTo(w io.Writer) (n int64, err error) { panic("not implemented") }

// WriteSnapshot will snapshot the cache and write a new TSM file with its contents, releasing the snapshot when done.
func (e *Engine) WriteSnapshot() error {
	// Lock and grab the cache snapshot along with all the closed WAL
	// filenames associated with the snapshot

	var started *time.Time

	defer func() {
		if started != nil {
			e.Cache.UpdateCompactTime(time.Now().Sub(*started))
		}
	}()

	closedFiles, snapshot, err := func() ([]string, *Cache, error) {
		e.mu.Lock()
		defer e.mu.Unlock()

		now := time.Now()
		started = &now

		if err := e.WAL.CloseSegment(); err != nil {
			return nil, nil, err
		}

		segments, err := e.WAL.ClosedSegments()
		if err != nil {
			return nil, nil, err
		}

		snapshot, err := e.Cache.Snapshot()
		if err != nil {
			return nil, nil, err
		}

		return segments, snapshot, nil
	}()

	if err != nil {
		return err
	}

	// The snapshotted cache may have duplicate points and unsorted data.  We need to deduplicate
	// it before writing the snapshot.  This can be very expensive so it's done while we are not
	// holding the engine write lock.
	snapshot.Deduplicate()

	return e.writeSnapshotAndCommit(closedFiles, snapshot)
}

// CreateSnapshot will create a temp directory that holds
// temporary hardlinks to the underylyng shard files
func (e *Engine) CreateSnapshot() (string, error) {
	if err := e.WriteSnapshot(); err != nil {
		return "", err
	}

	e.mu.RLock()
	defer e.mu.RUnlock()

	return e.FileStore.CreateSnapshot()
}

// writeSnapshotAndCommit will write the passed cache to a new TSM file and remove the closed WAL segments
func (e *Engine) writeSnapshotAndCommit(closedFiles []string, snapshot *Cache) (err error) {

	defer func() {
		if err != nil {
			e.Cache.ClearSnapshot(false)
		}
	}()
	// write the new snapshot files
	newFiles, err := e.Compactor.WriteSnapshot(snapshot)
	if err != nil {
		e.logger.Printf("error writing snapshot from compactor: %v", err)
		return err
	}

	e.mu.RLock()
	defer e.mu.RUnlock()

	// update the file store with these new files
	if err := e.FileStore.Replace(nil, newFiles); err != nil {
		e.logger.Printf("error adding new TSM files from snapshot: %v", err)
		return err
	}

	// clear the snapshot from the in-memory cache, then the old WAL files
	e.Cache.ClearSnapshot(true)

	if err := e.WAL.Remove(closedFiles); err != nil {
		e.logger.Printf("error removing closed wal segments: %v", err)
	}

	return nil
}

// compactCache continually checks if the WAL cache should be written to disk
func (e *Engine) compactCache() {
	defer e.wg.Done()
	for {
		select {
		case <-e.done:
			return

		default:
			e.Cache.UpdateAge()
			if e.ShouldCompactCache(e.WAL.LastWriteTime()) {
				err := e.WriteSnapshot()
				if err != nil {
					e.logger.Printf("error writing snapshot: %v", err)
				}
			}
		}
		time.Sleep(time.Second)
	}
}

// ShouldCompactCache returns true if the Cache is over its flush threshold
// or if the passed in lastWriteTime is older than the write cold threshold
func (e *Engine) ShouldCompactCache(lastWriteTime time.Time) bool {
	sz := e.Cache.Size()

	if sz == 0 {
		return false
	}

	return sz > e.CacheFlushMemorySizeThreshold ||
		time.Now().Sub(lastWriteTime) > e.CacheFlushWriteColdDuration
}

func (e *Engine) compactTSMLevel(fast bool, level int) {
	defer e.wg.Done()

	for {
		select {
		case <-e.done:
			return

		default:
			tsmFiles := e.CompactionPlan.PlanLevel(level)

			if len(tsmFiles) == 0 {
				time.Sleep(time.Second)
				continue
			}

			var wg sync.WaitGroup
			for i, group := range tsmFiles {
				wg.Add(1)
				go func(groupNum int, group CompactionGroup) {
					defer wg.Done()
					start := time.Now()
					e.logger.Printf("beginning level %d compaction of group %d, %d TSM files", level, groupNum, len(group))
					for i, f := range group {
						e.logger.Printf("compacting level %d group (%d) %s (#%d)", level, groupNum, f, i)
					}

					var files []string
					var err error

					if fast {
						files, err = e.Compactor.CompactFast(group)
						if err != nil {
							e.logger.Printf("error compacting TSM files: %v", err)
							time.Sleep(time.Second)
							return
						}
					} else {
						files, err = e.Compactor.CompactFull(group)
						if err != nil {
							e.logger.Printf("error compacting TSM files: %v", err)
							time.Sleep(time.Second)
							return
						}
					}

					if err := e.FileStore.Replace(group, files); err != nil {
						e.logger.Printf("error replacing new TSM files: %v", err)
						time.Sleep(time.Second)
						return
					}

					for i, f := range files {
						e.logger.Printf("compacted level %d group (%d) into %s (#%d)", level, groupNum, f, i)
					}
					e.logger.Printf("compacted level %d group %d of %d files into %d files in %s",
						level, groupNum, len(group), len(files), time.Since(start))
				}(i, group)
			}
			wg.Wait()
		}
	}
}

func (e *Engine) compactTSMFull() {
	defer e.wg.Done()

	for {
		select {
		case <-e.done:
			return

		default:
			tsmFiles := e.CompactionPlan.Plan(e.WAL.LastWriteTime())

			if len(tsmFiles) == 0 {
				time.Sleep(time.Second)
				continue
			}

			var wg sync.WaitGroup
			for i, group := range tsmFiles {
				wg.Add(1)
				go func(groupNum int, group CompactionGroup) {
					defer wg.Done()
					start := time.Now()
					e.logger.Printf("beginning full compaction of group %d, %d TSM files", groupNum, len(group))
					for i, f := range group {
						e.logger.Printf("compacting full group (%d) %s (#%d)", groupNum, f, i)
					}

					files, err := e.Compactor.CompactFull(group)
					if err != nil {
						e.logger.Printf("error compacting TSM files: %v", err)
						time.Sleep(time.Second)
						return
					}

					if err := e.FileStore.Replace(group, files); err != nil {
						e.logger.Printf("error replacing new TSM files: %v", err)
						time.Sleep(time.Second)
						return
					}

					for i, f := range files {
						e.logger.Printf("compacted full group (%d) into %s (#%d)", groupNum, f, i)
					}
					e.logger.Printf("compacted full %d files into %d files in %s",
						len(group), len(files), time.Since(start))
				}(i, group)
			}
			wg.Wait()
		}
	}
}

// reloadCache reads the WAL segment files and loads them into the cache.
func (e *Engine) reloadCache() error {
	files, err := segmentFileNames(e.WAL.Path())
	if err != nil {
		return err
	}

	limit := e.Cache.MaxSize()
	defer func() {
		e.Cache.SetMaxSize(limit)
	}()

	// Disable the max size during loading
	e.Cache.SetMaxSize(0)

	loader := NewCacheLoader(files)
	loader.SetLogOutput(e.logOutput)
	if err := loader.Load(e.Cache); err != nil {
		return err
	}

	return nil
}

func (e *Engine) cleanup() error {
	allfiles, err := ioutil.ReadDir(e.path)
	if err != nil {
		return err
	}

	for _, f := range allfiles {
		// Check to see if there are any `.tmp` directories that were left over from failed shard snapshots
		if f.IsDir() && strings.HasSuffix(f.Name(), ".tmp") {
			if err := os.RemoveAll(filepath.Join(e.path, f.Name())); err != nil {
				return fmt.Errorf("error removing tmp snapshot directory %q: %s", f.Name(), err)
			}
		}
	}

	files, err := filepath.Glob(filepath.Join(e.path, fmt.Sprintf("*.%s", CompactionTempExtension)))
	if err != nil {
		return fmt.Errorf("error getting compaction temp files: %s", err.Error())
	}

	for _, f := range files {
		if err := os.Remove(f); err != nil {
			return fmt.Errorf("error removing temp compaction files: %v", err)
		}
	}

	return nil
}

func (e *Engine) KeyCursor(key string, t int64, ascending bool) *KeyCursor {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.FileStore.KeyCursor(key, t, ascending)
}

func (e *Engine) CreateIterator(opt influxql.IteratorOptions) (influxql.Iterator, error) {
	if call, ok := opt.Expr.(*influxql.Call); ok {
		refOpt := opt
		refOpt.Expr = call.Args[0].(*influxql.VarRef)
		inputs, err := e.createVarRefIterator(refOpt, true)
		if err != nil {
			return nil, err
		} else if len(inputs) == 0 {
			return nil, nil
		}

		// Wrap each series in a call iterator.
		for i, input := range inputs {
			if opt.InterruptCh != nil {
				input = influxql.NewInterruptIterator(input, opt.InterruptCh)
			}

			itr, err := influxql.NewCallIterator(input, opt)
			if err != nil {
				return nil, err
			}
			inputs[i] = itr
		}

		return influxql.NewParallelMergeIterator(inputs, opt, runtime.GOMAXPROCS(0)), nil
	}

	itrs, err := e.createVarRefIterator(opt, false)
	if err != nil {
		return nil, err
	}

	itr := influxql.NewSortedMergeIterator(itrs, opt)
	if itr != nil && opt.InterruptCh != nil {
		itr = influxql.NewInterruptIterator(itr, opt.InterruptCh)
	}
	return itr, nil
}

// createVarRefIterator creates an iterator for a variable reference.
// The aggregate argument determines this is being created for an aggregate.
// If this is an aggregate, the limit optimization is disabled temporarily. See #6661.
func (e *Engine) createVarRefIterator(opt influxql.IteratorOptions, aggregate bool) ([]influxql.Iterator, error) {
	ref, _ := opt.Expr.(*influxql.VarRef)

	var itrs []influxql.Iterator
	if err := func() error {
		mms := tsdb.Measurements(e.index.MeasurementsByName(influxql.Sources(opt.Sources).Names()))

		for _, mm := range mms {
			// Determine tagsets for this measurement based on dimensions and filters.
			tagSets, err := mm.TagSets(opt.Dimensions, opt.Condition)
			if err != nil {
				return err
			}

			// Calculate tag sets and apply SLIMIT/SOFFSET.
			tagSets = influxql.LimitTagSets(tagSets, opt.SLimit, opt.SOffset)

			for _, t := range tagSets {
				inputs, err := e.createTagSetIterators(ref, mm, t, opt)
				if err != nil {
					return err
				}

				if !aggregate && len(inputs) > 0 && (opt.Limit > 0 || opt.Offset > 0) {
					itrs = append(itrs, newLimitIterator(influxql.NewSortedMergeIterator(inputs, opt), opt))
				} else {
					itrs = append(itrs, inputs...)
				}
			}
		}
		return nil
	}(); err != nil {
		influxql.Iterators(itrs).Close()
		return nil, err
	}

	return itrs, nil
}

// createTagSetIterators creates a set of iterators for a tagset.
func (e *Engine) createTagSetIterators(ref *influxql.VarRef, mm *tsdb.Measurement, t *influxql.TagSet, opt influxql.IteratorOptions) ([]influxql.Iterator, error) {
	// Set parallelism by number of logical cpus.
	parallelism := runtime.GOMAXPROCS(0)
	if parallelism > len(t.SeriesKeys) {
		parallelism = len(t.SeriesKeys)
	}

	// Create series key groupings w/ return error.
	groups := make([]struct {
		keys    []string
		filters []influxql.Expr
		itrs    []influxql.Iterator
		err     error
	}, parallelism)

	// Group series keys.
	n := len(t.SeriesKeys) / parallelism
	for i := 0; i < parallelism; i++ {
		group := &groups[i]

		if i < parallelism-1 {
			group.keys = t.SeriesKeys[i*n : (i+1)*n]
			group.filters = t.Filters[i*n : (i+1)*n]
		} else {
			group.keys = t.SeriesKeys[i*n:]
			group.filters = t.Filters[i*n:]
		}

		group.itrs = make([]influxql.Iterator, 0, len(group.keys))
	}

	// Read series groups in parallel.
	var wg sync.WaitGroup
	for i := range groups {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			groups[i].itrs, groups[i].err = e.createTagSetGroupIterators(ref, mm, groups[i].keys, t, groups[i].filters, opt)
		}(i)
	}
	wg.Wait()

	// Determine total number of iterators so we can allocate only once.
	var itrN int
	for _, group := range groups {
		itrN += len(group.itrs)
	}

	// Combine all iterators together and check for errors.
	var err error
	itrs := make([]influxql.Iterator, 0, itrN)
	for _, group := range groups {
		if group.err != nil {
			err = group.err
		}
		itrs = append(itrs, group.itrs...)
	}

	// If an error occurred, make sure we close all created iterators.
	if err != nil {
		influxql.Iterators(itrs).Close()
		return nil, err
	}

	return itrs, nil
}

// createTagSetGroupIterators creates a set of iterators for a subset of a tagset's series.
func (e *Engine) createTagSetGroupIterators(ref *influxql.VarRef, mm *tsdb.Measurement, seriesKeys []string, t *influxql.TagSet, filters []influxql.Expr, opt influxql.IteratorOptions) ([]influxql.Iterator, error) {
	conditionFields := make([]influxql.VarRef, len(influxql.ExprNames(opt.Condition)))

	itrs := make([]influxql.Iterator, 0, len(seriesKeys))
	for i, seriesKey := range seriesKeys {
		fields := 0
		if filters[i] != nil {
			// Retrieve non-time fields from this series filter and filter out tags.
			for _, f := range influxql.ExprNames(filters[i]) {
				conditionFields[fields] = f
				fields++
			}
		}

		itr, err := e.createVarRefSeriesIterator(ref, mm, seriesKey, t, filters[i], conditionFields[:fields], opt)
		if err != nil {
			return itrs, err
		} else if itr == nil {
			continue
		}
		itrs = append(itrs, itr)
	}
	return itrs, nil
}

// createVarRefSeriesIterator creates an iterator for a variable reference for a series.
func (e *Engine) createVarRefSeriesIterator(ref *influxql.VarRef, mm *tsdb.Measurement, seriesKey string, t *influxql.TagSet, filter influxql.Expr, conditionFields []influxql.VarRef, opt influxql.IteratorOptions) (influxql.Iterator, error) {
	tags := influxql.NewTags(e.index.TagsForSeries(seriesKey))

	// Create options specific for this series.
	itrOpt := opt
	itrOpt.Condition = filter

	// Build auxilary cursors.
	// Tag values should be returned if the field doesn't exist.
	var aux []cursorAt
	if len(opt.Aux) > 0 {
		aux = make([]cursorAt, len(opt.Aux))
		for i, ref := range opt.Aux {
			// Create cursor from field if a tag wasn't requested.
			if ref.Type != influxql.Tag {
				cur := e.buildCursor(mm.Name, seriesKey, &ref, opt)
				if cur != nil {
					aux[i] = newBufCursor(cur, opt.Ascending)
					continue
				}

				// If a field was requested, use a nil cursor of the requested type.
				switch ref.Type {
				case influxql.Float, influxql.AnyField:
					aux[i] = &floatNilLiteralCursor{}
					continue
				case influxql.Integer:
					aux[i] = &integerNilLiteralCursor{}
					continue
				case influxql.String:
					aux[i] = &stringNilLiteralCursor{}
					continue
				case influxql.Boolean:
					aux[i] = &booleanNilLiteralCursor{}
					continue
				}
			}

			// If field doesn't exist, use the tag value.
			if v := tags.Value(ref.Val); v == "" {
				// However, if the tag value is blank then return a null.
				aux[i] = &stringNilLiteralCursor{}
			} else {
				aux[i] = &stringLiteralCursor{value: v}
			}
		}
	}

	// Build conditional field cursors.
	// If a conditional field doesn't exist then ignore the series.
	var conds []cursorAt
	if len(conditionFields) > 0 {
		conds = make([]cursorAt, len(conditionFields))
		for i, ref := range conditionFields {
			// Create cursor from field if a tag wasn't requested.
			if ref.Type != influxql.Tag {
				cur := e.buildCursor(mm.Name, seriesKey, &ref, opt)
				if cur != nil {
					conds[i] = newBufCursor(cur, opt.Ascending)
					continue
				}

				// If a field was requested, use a nil cursor of the requested type.
				switch ref.Type {
				case influxql.Float, influxql.AnyField:
					conds[i] = &floatNilLiteralCursor{}
					continue
				case influxql.Integer:
					conds[i] = &integerNilLiteralCursor{}
					continue
				case influxql.String:
					conds[i] = &stringNilLiteralCursor{}
					continue
				case influxql.Boolean:
					conds[i] = &booleanNilLiteralCursor{}
					continue
				}
			}

			// If field doesn't exist, use the tag value.
			if v := tags.Value(ref.Val); v == "" {
				// However, if the tag value is blank then return a null.
				conds[i] = &stringNilLiteralCursor{}
			} else {
				conds[i] = &stringLiteralCursor{value: v}
			}
		}
	}
	condNames := influxql.VarRefs(conditionFields).Strings()

	// Limit tags to only the dimensions selected.
	tags = tags.Subset(opt.Dimensions)

	// If it's only auxiliary fields then it doesn't matter what type of iterator we use.
	if ref == nil {
		return newFloatIterator(mm.Name, tags, itrOpt, nil, aux, conds, condNames), nil
	}

	// Build main cursor.
	cur := e.buildCursor(mm.Name, seriesKey, ref, opt)

	// If the field doesn't exist then don't build an iterator.
	if cur == nil {
		return nil, nil
	}

	switch cur := cur.(type) {
	case floatCursor:
		return newFloatIterator(mm.Name, tags, itrOpt, cur, aux, conds, condNames), nil
	case integerCursor:
		return newIntegerIterator(mm.Name, tags, itrOpt, cur, aux, conds, condNames), nil
	case stringCursor:
		return newStringIterator(mm.Name, tags, itrOpt, cur, aux, conds, condNames), nil
	case booleanCursor:
		return newBooleanIterator(mm.Name, tags, itrOpt, cur, aux, conds, condNames), nil
	default:
		panic("unreachable")
	}
}

// buildCursor creates an untyped cursor for a field.
func (e *Engine) buildCursor(measurement, seriesKey string, ref *influxql.VarRef, opt influxql.IteratorOptions) cursor {
	// Look up fields for measurement.
	e.mu.RLock()
	mf := e.measurementFields[measurement]
	e.mu.RUnlock()

	if mf == nil {
		return nil
	}

	// Find individual field.
	f := mf.Field(ref.Val)
	if f == nil {
		return nil
	}

	// Check if we need to perform a cast. Performing a cast in the
	// engine (if it is possible) is much more efficient than an automatic cast.
	if ref.Type != influxql.Unknown && ref.Type != influxql.AnyField && ref.Type != f.Type {
		switch ref.Type {
		case influxql.Float:
			switch f.Type {
			case influxql.Integer:
				cur := e.buildIntegerCursor(measurement, seriesKey, ref.Val, opt)
				return &floatCastIntegerCursor{cursor: cur}
			}
		case influxql.Integer:
			switch f.Type {
			case influxql.Float:
				cur := e.buildFloatCursor(measurement, seriesKey, ref.Val, opt)
				return &integerCastFloatCursor{cursor: cur}
			}
		}
		return nil
	}

	// Return appropriate cursor based on type.
	switch f.Type {
	case influxql.Float:
		return e.buildFloatCursor(measurement, seriesKey, ref.Val, opt)
	case influxql.Integer:
		return e.buildIntegerCursor(measurement, seriesKey, ref.Val, opt)
	case influxql.String:
		return e.buildStringCursor(measurement, seriesKey, ref.Val, opt)
	case influxql.Boolean:
		return e.buildBooleanCursor(measurement, seriesKey, ref.Val, opt)
	default:
		panic("unreachable")
	}
}

// buildFloatCursor creates a cursor for a float field.
func (e *Engine) buildFloatCursor(measurement, seriesKey, field string, opt influxql.IteratorOptions) floatCursor {
	cacheValues := e.Cache.Values(SeriesFieldKey(seriesKey, field))
	keyCursor := e.KeyCursor(SeriesFieldKey(seriesKey, field), opt.SeekTime(), opt.Ascending)
	return newFloatCursor(opt.SeekTime(), opt.Ascending, cacheValues, keyCursor)
}

// buildIntegerCursor creates a cursor for an integer field.
func (e *Engine) buildIntegerCursor(measurement, seriesKey, field string, opt influxql.IteratorOptions) integerCursor {
	cacheValues := e.Cache.Values(SeriesFieldKey(seriesKey, field))
	keyCursor := e.KeyCursor(SeriesFieldKey(seriesKey, field), opt.SeekTime(), opt.Ascending)
	return newIntegerCursor(opt.SeekTime(), opt.Ascending, cacheValues, keyCursor)
}

// buildStringCursor creates a cursor for a string field.
func (e *Engine) buildStringCursor(measurement, seriesKey, field string, opt influxql.IteratorOptions) stringCursor {
	cacheValues := e.Cache.Values(SeriesFieldKey(seriesKey, field))
	keyCursor := e.KeyCursor(SeriesFieldKey(seriesKey, field), opt.SeekTime(), opt.Ascending)
	return newStringCursor(opt.SeekTime(), opt.Ascending, cacheValues, keyCursor)
}

// buildBooleanCursor creates a cursor for a boolean field.
func (e *Engine) buildBooleanCursor(measurement, seriesKey, field string, opt influxql.IteratorOptions) booleanCursor {
	cacheValues := e.Cache.Values(SeriesFieldKey(seriesKey, field))
	keyCursor := e.KeyCursor(SeriesFieldKey(seriesKey, field), opt.SeekTime(), opt.Ascending)
	return newBooleanCursor(opt.SeekTime(), opt.Ascending, cacheValues, keyCursor)
}

// SeriesFieldKey combine a series key and field name for a unique string to be hashed to a numeric ID
func SeriesFieldKey(seriesKey, field string) string {
	return seriesKey + keyFieldSeparator + field
}

func tsmFieldTypeToInfluxQLDataType(typ byte) (influxql.DataType, error) {
	switch typ {
	case BlockFloat64:
		return influxql.Float, nil
	case BlockInteger:
		return influxql.Integer, nil
	case BlockBoolean:
		return influxql.Boolean, nil
	case BlockString:
		return influxql.String, nil
	default:
		return influxql.Unknown, fmt.Errorf("unknown block type: %v", typ)
	}
}

func seriesAndFieldFromCompositeKey(key string) (string, string) {
	sep := strings.Index(key, keyFieldSeparator)
	if sep == -1 {
		// No field???
		return key, ""
	}
	return key[:sep], key[sep+len(keyFieldSeparator):]
}

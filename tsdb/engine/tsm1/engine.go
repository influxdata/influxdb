package tsm1

import (
	"archive/tar"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/influxdb/influxdb/influxql"
	"github.com/influxdb/influxdb/models"
	"github.com/influxdb/influxdb/tsdb"
)

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
	mu   sync.RWMutex
	done chan struct{}
	wg   sync.WaitGroup

	path   string
	logger *log.Logger

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
}

// NewEngine returns a new instance of Engine.
func NewEngine(path string, walPath string, opt tsdb.EngineOptions) tsdb.Engine {
	w := NewWAL(walPath)
	w.LoggingEnabled = opt.Config.WALLoggingEnabled

	fs := NewFileStore(path)
	fs.traceLogging = opt.Config.DataLoggingEnabled

	cache := NewCache(uint64(opt.Config.CacheMaxMemorySize))

	c := &Compactor{
		Dir:       path,
		FileStore: fs,
	}

	e := &Engine{
		path:   path,
		logger: log.New(os.Stderr, "[tsm1] ", log.LstdFlags),

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
	}

	return e
}

// Path returns the path the engine was opened with.
func (e *Engine) Path() string { return e.path }

// PerformMaintenance is for periodic maintenance of the store. A no-op for b1
func (e *Engine) PerformMaintenance() {
}

// Format returns the format type of this engine
func (e *Engine) Format() tsdb.EngineFormat {
	return tsdb.TSM1Format
}

// Open opens and initializes the engine.
func (e *Engine) Open() error {
	e.done = make(chan struct{})
	e.Compactor.Cancel = e.done

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

	e.wg.Add(5)
	go e.compactCache()
	go e.compactTSMFull()
	go e.compactTSMLevel(true, 1)
	go e.compactTSMLevel(true, 2)
	go e.compactTSMLevel(false, 3)

	return nil
}

// Close closes the engine.
func (e *Engine) Close() error {
	// Shutdown goroutines and wait.
	close(e.done)
	e.wg.Wait()

	// Lock now and close everything else down.
	e.mu.Lock()
	defer e.mu.Unlock()

	if err := e.FileStore.Close(); err != nil {
		return err
	}
	return e.WAL.Close()
}

// SetLogOutput is a no-op.
func (e *Engine) SetLogOutput(w io.Writer) {}

// LoadMetadataIndex loads the shard metadata into memory.
func (e *Engine) LoadMetadataIndex(_ *tsdb.Shard, index *tsdb.DatabaseIndex, measurementFields map[string]*tsdb.MeasurementFields) error {
	keys := e.FileStore.Keys()

	keysLoaded := make(map[string]bool)

	for _, k := range keys {
		typ, err := e.FileStore.Type(k)
		if err != nil {
			return err
		}
		fieldType, err := tsmFieldTypeToInfluxQLDataType(typ)
		if err != nil {
			return err
		}

		if err := e.addToIndexFromKey(k, fieldType, index, measurementFields); err != nil {
			return err
		}

		keysLoaded[k] = true
	}

	// load metadata from the Cache
	e.Cache.Lock() // shouldn't need the lock, but just to be safe
	defer e.Cache.Unlock()

	for key, entry := range e.Cache.Store() {
		if keysLoaded[key] {
			continue
		}

		fieldType, err := entry.values.InfluxQLType()
		if err != nil {
			log.Printf("error getting the data type of values for key %s: %s", key, err.Error())
			continue
		}

		if err := e.addToIndexFromKey(key, fieldType, index, measurementFields); err != nil {
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
	if err := e.WriteSnapshot(); err != nil {
		return err
	}
	e.FileStore.mu.RLock()
	defer e.FileStore.mu.RUnlock()

	var files []FileStat

	// grab all the files and tombstones that have a modified time after since
	for _, f := range e.FileStore.files {
		if stat := f.Stats(); stat.LastModified.After(since) {
			files = append(files, f.Stats())
		}
		for _, t := range f.TombstoneFiles() {
			if t.LastModified.After(since) {
				files = append(files, f.Stats())
			}
		}
	}

	tw := tar.NewWriter(w)
	defer tw.Close()

	for _, f := range files {
		if err := e.writeFileToBackup(f, basePath, tw); err != nil {
			return err
		}
	}

	return nil
}

// writeFileToBackup will copy the file into the tar archive. Files will use the shardRelativePath
// in their names. This should be the <db>/<retention policy>/<id> part of the path
func (e *Engine) writeFileToBackup(f FileStat, shardRelativePath string, tw *tar.Writer) error {
	h := &tar.Header{
		Name:    filepath.Join(shardRelativePath, filepath.Base(f.Path)),
		ModTime: f.LastModified,
		Size:    int64(f.Size),
	}
	if err := tw.WriteHeader(h); err != nil {
		return err
	}
	fr, err := os.Open(f.Path)
	if err != nil {
		return err
	}

	defer fr.Close()

	_, err = io.CopyN(tw, fr, h.Size)

	return err
}

// addToIndexFromKey will pull the measurement name, series key, and field name from a composite key and add it to the
// database index and measurement fields
func (e *Engine) addToIndexFromKey(key string, fieldType influxql.DataType, index *tsdb.DatabaseIndex, measurementFields map[string]*tsdb.MeasurementFields) error {
	seriesKey, field := seriesAndFieldFromCompositeKey(key)
	measurement := tsdb.MeasurementFromSeriesKey(seriesKey)

	m := index.CreateMeasurementIndexIfNotExists(measurement)
	m.SetFieldName(field)

	mf := measurementFields[measurement]
	if mf == nil {
		mf = &tsdb.MeasurementFields{
			Fields: map[string]*tsdb.Field{},
		}
		measurementFields[measurement] = mf
	}

	if err := mf.CreateFieldIfNotExists(field, fieldType, false); err != nil {
		return err
	}

	_, tags, err := models.ParseKey(seriesKey)
	if err == nil {
		return err
	}

	s := tsdb.NewSeries(seriesKey, tags)
	s.InitializeShards()
	index.CreateSeriesIndexIfNotExists(measurement, s)

	return nil
}

// WritePoints writes metadata and point data into the engine.
// Returns an error if new points are added to an existing key.
func (e *Engine) WritePoints(points []models.Point, measurementFieldsToSave map[string]*tsdb.MeasurementFields, seriesToCreate []*tsdb.SeriesCreate) error {
	values := map[string][]Value{}
	for _, p := range points {
		for k, v := range p.Fields() {
			key := string(p.Key()) + keyFieldSeparator + k
			values[key] = append(values[key], NewValue(p.Time(), v))
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

// DeleteSeries deletes the series from the engine.
func (e *Engine) DeleteSeries(seriesKeys []string) error {
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
	for _, k := range e.FileStore.Keys() {
		seriesKey, _ := seriesAndFieldFromCompositeKey(k)
		if _, ok := keyMap[seriesKey]; ok {
			deleteKeys = append(deleteKeys, k)
		}
	}
	e.FileStore.Delete(deleteKeys)

	// find the keys in the cache and remove them
	walKeys := make([]string, 0)
	e.Cache.Lock()
	defer e.Cache.Unlock()

	s := e.Cache.Store()
	for k, _ := range s {
		seriesKey, _ := seriesAndFieldFromCompositeKey(k)
		if _, ok := keyMap[seriesKey]; ok {
			walKeys = append(walKeys, k)
			delete(s, k)
		}
	}

	// delete from the WAL
	_, err := e.WAL.Delete(walKeys)

	return err
}

// DeleteMeasurement deletes a measurement and all related series.
func (e *Engine) DeleteMeasurement(name string, seriesKeys []string) error {
	return e.DeleteSeries(seriesKeys)
}

// SeriesCount returns the number of series buckets on the shard.
func (e *Engine) SeriesCount() (n int, err error) {
	return 0, nil
}

// Begin starts a new transaction on the engine.
func (e *Engine) Begin(writable bool) (tsdb.Tx, error) {
	return &tx{engine: e}, nil
}

func (e *Engine) WriteTo(w io.Writer) (n int64, err error) { panic("not implemented") }

// WriteSnapshot will snapshot the cache and write a new TSM file with its contents, releasing the snapshot when done.
func (e *Engine) WriteSnapshot() error {
	// Lock and grab the cache snapshot along with all the closed WAL
	// filenames associated with the snapshot
	closedFiles, snapshot, compactor, err := func() ([]string, *Cache, *Compactor, error) {
		e.mu.Lock()
		defer e.mu.Unlock()

		if err := e.WAL.CloseSegment(); err != nil {
			return nil, nil, nil, err
		}

		segments, err := e.WAL.ClosedSegments()
		if err != nil {
			return nil, nil, nil, err
		}

		snapshot := e.Cache.Snapshot()

		return segments, snapshot, e.Compactor.Clone(), nil
	}()

	if err != nil {
		return err
	}

	// The snapshotted cache may have duplicate points and unsorted data.  We need to deduplicate
	// it before writing the snapshot.  This can be very expensive so it's done while we are not
	// holding the engine write lock.
	snapshot.Deduplicate()

	return e.writeSnapshotAndCommit(closedFiles, snapshot, compactor)
}

// writeSnapshotAndCommit will write the passed cache to a new TSM file and remove the closed WAL segments
func (e *Engine) writeSnapshotAndCommit(closedFiles []string, snapshot *Cache, compactor *Compactor) error {
	// write the new snapshot files
	newFiles, err := compactor.WriteSnapshot(snapshot)
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
	e.Cache.ClearSnapshot()

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

	loader := NewCacheLoader(files)
	if err := loader.Load(e.Cache); err != nil {
		return err
	}

	return nil
}

func (e *Engine) cleanup() error {
	files, err := filepath.Glob(filepath.Join(e.path, fmt.Sprintf("*.%s", CompactionTempExtension)))
	if err != nil {
		return fmt.Errorf("error getting compaction checkpoints: %s", err.Error())
	}

	for _, f := range files {
		if err := os.Remove(f); err != nil {
			return fmt.Errorf("error removing temp compaction files: %v", err)
		}
	}
	return nil
}

func (e *Engine) KeyCursor(key string) *KeyCursor {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.FileStore.KeyCursor(key)
}

type tx struct {
	engine *Engine
}

// Cursor returns a cursor for all cached and TSM-based data.
func (t *tx) Cursor(series string, fields []string, dec *tsdb.FieldCodec, ascending bool) tsdb.Cursor {
	if len(fields) == 1 {
		key := SeriesFieldKey(series, fields[0])
		return &devCursor{
			series:       series,
			fields:       fields,
			cache:        t.engine.Cache.Values(key),
			tsmKeyCursor: t.engine.KeyCursor(key),
			ascending:    ascending,
		}
	}

	// multiple fields. use just the MultiFieldCursor, which also handles time collisions
	// so we don't need to use the combined cursor
	var cursors []tsdb.Cursor
	var cursorFields []string
	for _, field := range fields {
		key := SeriesFieldKey(series, field)
		wc := &devCursor{
			series:       series,
			fields:       []string{field},
			cache:        t.engine.Cache.Values(key),
			tsmKeyCursor: t.engine.KeyCursor(key),
			ascending:    ascending,
		}

		// double up the fields since there's one for the wal and one for the index
		cursorFields = append(cursorFields, field)
		cursors = append(cursors, wc)
	}
	return NewMultiFieldCursor(cursorFields, cursors, ascending)
}

func (t *tx) Rollback() error                          { return nil }
func (t *tx) Size() int64                              { panic("not implemented") }
func (t *tx) Commit() error                            { panic("not implemented") }
func (t *tx) WriteTo(w io.Writer) (n int64, err error) { panic("not implemented") }

// devCursor is a cursor that combines both TSM and cached data.
type devCursor struct {
	series string
	fields []string

	cache         Values
	cachePos      int
	cacheKeyBuf   int64
	cacheValueBuf interface{}

	tsmValues   Values
	tsmPos      int
	tsmKeyBuf   int64
	tsmValueBuf interface{}

	tsmKeyCursor *KeyCursor
	ascending    bool
}

// SeekTo positions the cursor at the timestamp specified by seek and returns the
// timestamp and value.
func (c *devCursor) SeekTo(seek int64) (int64, interface{}) {
	// Seek to position in cache.
	c.cacheKeyBuf, c.cacheValueBuf = func() (int64, interface{}) {
		// Seek to position in cache index.
		c.cachePos = sort.Search(len(c.cache), func(i int) bool {
			return c.cache[i].Time().UnixNano() >= seek
		})

		if c.cachePos < len(c.cache) {
			v := c.cache[c.cachePos]
			if v.UnixNano() == seek || c.ascending {
				// Exact seek found or, if ascending, next one is good.
				return v.UnixNano(), v.Value()
			}
			// Nothing available if descending.
			return tsdb.EOF, nil
		}

		// Ascending cursor, no match in the cache.
		if c.ascending {
			return tsdb.EOF, nil
		}

		// Descending cursor, go to previous value in cache, and return if it exists.
		c.cachePos--
		if c.cachePos < 0 {
			return tsdb.EOF, nil
		}
		return c.cache[c.cachePos].UnixNano(), c.cache[c.cachePos].Value()
	}()

	// Seek to position to tsm block.
	if c.ascending {
		c.tsmValues, _ = c.tsmKeyCursor.SeekTo(time.Unix(0, seek-1), c.ascending)
	} else {
		c.tsmValues, _ = c.tsmKeyCursor.SeekTo(time.Unix(0, seek+1), c.ascending)
	}

	c.tsmPos = sort.Search(len(c.tsmValues), func(i int) bool {
		return c.tsmValues[i].Time().UnixNano() >= seek
	})

	if !c.ascending {
		c.tsmPos--
	}

	if c.tsmPos >= 0 && c.tsmPos < len(c.tsmValues) {
		c.tsmKeyBuf = c.tsmValues[c.tsmPos].Time().UnixNano()
		c.tsmValueBuf = c.tsmValues[c.tsmPos].Value()
	} else {
		c.tsmKeyBuf = tsdb.EOF
		c.tsmKeyCursor.Close()
	}

	return c.read()
}

// Next returns the next value from the cursor.
func (c *devCursor) Next() (int64, interface{}) {
	return c.read()
}

// Ascending returns whether the cursor returns data in time-ascending order.
func (c *devCursor) Ascending() bool { return c.ascending }

// read returns the next value for the cursor.
func (c *devCursor) read() (int64, interface{}) {
	var key int64
	var value interface{}

	// Determine where the next datum should come from -- the cache or the TSM files.

	switch {
	// No more data in cache or in TSM files.
	case c.cacheKeyBuf == tsdb.EOF && c.tsmKeyBuf == tsdb.EOF:
		key = tsdb.EOF

	// Both cache and tsm files have the same key, cache takes precedence.
	case c.cacheKeyBuf == c.tsmKeyBuf:
		key = c.cacheKeyBuf
		value = c.cacheValueBuf
		c.cacheKeyBuf, c.cacheValueBuf = c.nextCache()
		c.tsmKeyBuf, c.tsmValueBuf = c.nextTSM()

	// Buffered cache key precedes that in TSM file.
	case c.ascending && (c.cacheKeyBuf != tsdb.EOF && (c.cacheKeyBuf < c.tsmKeyBuf || c.tsmKeyBuf == tsdb.EOF)),
		!c.ascending && (c.cacheKeyBuf != tsdb.EOF && (c.cacheKeyBuf > c.tsmKeyBuf || c.tsmKeyBuf == tsdb.EOF)):
		key = c.cacheKeyBuf
		value = c.cacheValueBuf
		c.cacheKeyBuf, c.cacheValueBuf = c.nextCache()

	// Buffered TSM key precedes that in cache.
	default:
		key = c.tsmKeyBuf
		value = c.tsmValueBuf
		c.tsmKeyBuf, c.tsmValueBuf = c.nextTSM()
	}

	return key, value
}

// nextCache returns the next value from the cache.
func (c *devCursor) nextCache() (int64, interface{}) {
	if c.ascending {
		c.cachePos++
		if c.cachePos >= len(c.cache) {
			return tsdb.EOF, nil
		}
		return c.cache[c.cachePos].UnixNano(), c.cache[c.cachePos].Value()
	} else {
		c.cachePos--
		if c.cachePos < 0 {
			return tsdb.EOF, nil
		}
		return c.cache[c.cachePos].UnixNano(), c.cache[c.cachePos].Value()
	}
}

// nextTSM returns the next value from the TSM files.
func (c *devCursor) nextTSM() (int64, interface{}) {
	if c.ascending {
		c.tsmPos++
		if c.tsmPos >= len(c.tsmValues) {
			c.tsmValues, _ = c.tsmKeyCursor.Next(c.ascending)
			if len(c.tsmValues) == 0 {
				return tsdb.EOF, nil
			}
			c.tsmPos = 0
		}
		return c.tsmValues[c.tsmPos].UnixNano(), c.tsmValues[c.tsmPos].Value()
	} else {
		c.tsmPos--
		if c.tsmPos < 0 {
			c.tsmValues, _ = c.tsmKeyCursor.Next(c.ascending)
			if len(c.tsmValues) == 0 {
				return tsdb.EOF, nil
			}
			c.tsmPos = len(c.tsmValues) - 1
		}
		return c.tsmValues[c.tsmPos].UnixNano(), c.tsmValues[c.tsmPos].Value()
	}
}

// SeriesFieldKey combine a series key and field name for a unique string to be hashed to a numeric ID
func SeriesFieldKey(seriesKey, field string) string {
	return seriesKey + keyFieldSeparator + field
}

func tsmFieldTypeToInfluxQLDataType(typ byte) (influxql.DataType, error) {
	switch typ {
	case BlockFloat64:
		return influxql.Float, nil
	case BlockInt64:
		return influxql.Integer, nil
	case BlockBool:
		return influxql.Boolean, nil
	case BlockString:
		return influxql.String, nil
	default:
		return influxql.Unknown, fmt.Errorf("unknown block type: %v", typ)
	}
}

func seriesAndFieldFromCompositeKey(key string) (string, string) {
	parts := strings.Split(key, keyFieldSeparator)
	if len(parts) != 0 {
		return parts[0], strings.Join(parts[1:], keyFieldSeparator)
	}
	return parts[0], parts[1]
}

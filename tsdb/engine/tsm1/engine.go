package tsm1

import (
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/influxdb/influxdb/influxql"
	"github.com/influxdb/influxdb/models"
	"github.com/influxdb/influxdb/tsdb"
)

func init() {
	tsdb.RegisterEngine("tsm1dev", NewDevEngine)
}

// Ensure Engine implements the interface.
var _ tsdb.Engine = &DevEngine{}

// Engine represents a storage engine with compressed blocks.
type DevEngine struct {
	mu sync.RWMutex

	path   string
	logger *log.Logger

	WAL            *WAL
	Cache          *Cache
	Compactor      *Compactor
	CompactionPlan CompactionPlanner
	FileStore      *FileStore

	RotateFileSize    uint32
	MaxFileSize       uint32
	MaxPointsPerBlock int

	CacheFlushMemorySizeThreshold uint64
}

// NewDevEngine returns a new instance of Engine.
func NewDevEngine(path string, walPath string, opt tsdb.EngineOptions) tsdb.Engine {
	w := NewWAL(walPath)
	w.LoggingEnabled = opt.Config.WALLoggingEnabled

	fs := NewFileStore(path)

	cache := NewCache(uint64(opt.Config.CacheMaxMemorySize))

	c := &Compactor{
		Dir:         path,
		MaxFileSize: maxTSMFileSize,
		FileStore:   fs,
	}

	e := &DevEngine{
		path:   path,
		logger: log.New(os.Stderr, "[tsm1dev] ", log.LstdFlags),

		WAL:   w,
		Cache: cache,

		FileStore: fs,
		Compactor: c,
		CompactionPlan: &DefaultPlanner{
			FileStore: fs,
		},
		RotateFileSize:    DefaultRotateFileSize,
		MaxFileSize:       MaxDataFileSize,
		MaxPointsPerBlock: DefaultMaxPointsPerBlock,

		CacheFlushMemorySizeThreshold: uint64(opt.Config.WALFlushMemorySizeThreshold),
	}

	return e
}

// Path returns the path the engine was opened with.
func (e *DevEngine) Path() string { return e.path }

// PerformMaintenance is for periodic maintenance of the store. A no-op for b1
func (e *DevEngine) PerformMaintenance() {
}

// Format returns the format type of this engine
func (e *DevEngine) Format() tsdb.EngineFormat {
	return tsdb.TSM1DevFormat
}

// Open opens and initializes the engine.
func (e *DevEngine) Open() error {
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

	go e.compactCache()
	go e.compactTSM()

	return nil
}

// Close closes the engine.
func (e *DevEngine) Close() error {
	e.mu.Lock()
	defer e.mu.Unlock()

	e.WAL.Close()

	return nil
}

// SetLogOutput is a no-op.
func (e *DevEngine) SetLogOutput(w io.Writer) {}

// LoadMetadataIndex loads the shard metadata into memory.
func (e *DevEngine) LoadMetadataIndex(_ *tsdb.Shard, index *tsdb.DatabaseIndex, measurementFields map[string]*tsdb.MeasurementFields) error {
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

// addToIndexFromKey will pull the measurement name, series key, and field name from a composite key and add it to the
// database index and measurement fields
func (e *DevEngine) addToIndexFromKey(key string, fieldType influxql.DataType, index *tsdb.DatabaseIndex, measurementFields map[string]*tsdb.MeasurementFields) error {
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
func (e *DevEngine) WritePoints(points []models.Point, measurementFieldsToSave map[string]*tsdb.MeasurementFields, seriesToCreate []*tsdb.SeriesCreate) error {
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
func (e *DevEngine) DeleteSeries(seriesKeys []string) error {
	e.mu.RLock()
	defer e.mu.RUnlock()

	keyMap := map[string]struct{}{}
	for _, k := range seriesKeys {
		keyMap[k] = struct{}{}
	}

	for _, k := range e.FileStore.Keys() {
		seriesKey, _ := seriesAndFieldFromCompositeKey(k)
		if _, ok := keyMap[seriesKey]; ok {
			e.FileStore.Delete(k)
		}
	}

	return nil
}

// DeleteMeasurement deletes a measurement and all related series.
func (e *DevEngine) DeleteMeasurement(name string, seriesKeys []string) error {
	return e.DeleteSeries(seriesKeys)
}

// SeriesCount returns the number of series buckets on the shard.
func (e *DevEngine) SeriesCount() (n int, err error) {
	return 0, nil
}

// Begin starts a new transaction on the engine.
func (e *DevEngine) Begin(writable bool) (tsdb.Tx, error) {
	return &devTx{engine: e}, nil
}

func (e *DevEngine) WriteTo(w io.Writer) (n int64, err error) { panic("not implemented") }

// WriteSnapshot will snapshot the cache and write a new TSM file with its contents, releasing the snapshot when done.
func (e *DevEngine) WriteSnapshot() error {
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

	return e.writeSnapshotAndCommit(closedFiles, snapshot, compactor)
}

// writeSnapshotAndCommit will write the passed cache to a new TSM file and remove the closed WAL segments
func (e *DevEngine) writeSnapshotAndCommit(closedFiles []string, snapshot *Cache, compactor *Compactor) error {
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
	e.Cache.ClearSnapshot(snapshot)

	if err := e.WAL.Remove(closedFiles); err != nil {
		e.logger.Printf("error removing closed wal segments: %v", err)
	}

	return nil
}

func (e *DevEngine) compactCache() {
	for {
		if e.Cache.Size() > e.CacheFlushMemorySizeThreshold {
			err := e.WriteSnapshot()
			if err != nil {
				e.logger.Printf("error writing snapshot: %v", err)
			}
		}
		time.Sleep(time.Second)
	}
}

func (e *DevEngine) compactTSM() {
	for {
		tsmFiles := e.CompactionPlan.Plan()

		if len(tsmFiles) == 0 {
			time.Sleep(time.Second)
			continue
		}

		start := time.Now()
		e.logger.Printf("compacting %d TSM files", len(tsmFiles))

		files, err := e.Compactor.Compact(tsmFiles)
		if err != nil {
			e.logger.Printf("error compacting TSM files: %v", err)
			time.Sleep(time.Second)
			continue
		}

		if err := e.FileStore.Replace(tsmFiles, files); err != nil {
			e.logger.Printf("error replacing new TSM files: %v", err)
			time.Sleep(time.Second)
			continue
		}

		e.logger.Printf("compacted %d tsm into %d files in %s",
			len(tsmFiles), len(files), time.Since(start))
	}
}

// reloadCache reads the WAL segment files and loads them into the cache.
func (e *DevEngine) reloadCache() error {
	files, err := segmentFileNames(e.WAL.Path())
	if err != nil {
		return err
	}

	loader := NewCacheLoader(files, e.Cache)
	cache, err := loader.Load()
	if err != nil {
		return err
	}
	e.Cache = cache

	return nil
}

func (e *DevEngine) cleanup() error {
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

func (e *DevEngine) KeyCursor(key string) *KeyCursor {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.FileStore.KeyCursor(key)
}

type devTx struct {
	engine *DevEngine
}

// Cursor returns a cursor for all cached and TSM-based data.
func (t *devTx) Cursor(series string, fields []string, dec *tsdb.FieldCodec, ascending bool) tsdb.Cursor {
	if len(fields) == 1 {
		return &devCursor{
			series:       series,
			fields:       fields,
			cache:        t.engine.Cache.Values(SeriesFieldKey(series, fields[0])),
			tsmKeyCursor: t.engine.KeyCursor(SeriesFieldKey(series, fields[0])),
			ascending:    ascending,
		}
	}

	// multiple fields. use just the MultiFieldCursor, which also handles time collisions
	// so we don't need to use the combined cursor
	var cursors []tsdb.Cursor
	var cursorFields []string
	for _, field := range fields {
		wc := &devCursor{
			series:       series,
			fields:       []string{field},
			cache:        t.engine.Cache.Values(SeriesFieldKey(series, field)),
			tsmKeyCursor: t.engine.KeyCursor(SeriesFieldKey(series, field)),
			ascending:    ascending,
		}

		// double up the fields since there's one for the wal and one for the index
		cursorFields = append(cursorFields, field)
		cursors = append(cursors, wc)
	}
	return NewMultiFieldCursor(cursorFields, cursors, ascending)
}

func (t *devTx) Rollback() error                          { return nil }
func (t *devTx) Size() int64                              { panic("not implemented") }
func (t *devTx) Commit() error                            { panic("not implemented") }
func (t *devTx) WriteTo(w io.Writer) (n int64, err error) { panic("not implemented") }

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
		return influxql.Unknown, fmt.Errorf("unkown block type: %v", typ)
	}
}

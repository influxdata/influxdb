package tsm1 // import "github.com/influxdata/influxdb/tsdb/engine/tsm1"

import (
	"archive/tar"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/influxdata/influxdb/influxql"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/tsdb"
)

//go:generate tmpl -data=@iterator.gen.go.tmpldata iterator.gen.go.tmpl

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
		logger:            log.New(os.Stderr, "[tsm1] ", log.LstdFlags),
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
	}

	return e
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

// Close closes the engine. Subsequent calls to Close are a nop.
func (e *Engine) Close() error {
	e.mu.RLock()
	if e.done == nil {
		e.mu.RUnlock()
		return nil
	}
	e.mu.RUnlock()

	// Shutdown goroutines and wait.
	close(e.done)
	e.wg.Wait()

	// Lock now and close everything else down.
	e.mu.Lock()
	defer e.mu.Unlock()
	e.done = nil // Ensures that the channel will not be closed again.

	if err := e.FileStore.Close(); err != nil {
		return err
	}
	return e.WAL.Close()
}

// SetLogOutput is a no-op.
func (e *Engine) SetLogOutput(w io.Writer) {}

// LoadMetadataIndex loads the shard metadata into memory.
func (e *Engine) LoadMetadataIndex(sh *tsdb.Shard, index *tsdb.DatabaseIndex) error {
	// Save reference to index for iterator creation.
	e.index = index

	start := time.Now()

	if err := e.FileStore.WalkKeys(func(key string, typ byte) error {
		fieldType, err := tsmFieldTypeToInfluxQLDataType(typ)
		if err != nil {
			return err
		}

		if err := e.addToIndexFromKey(key, fieldType, index); err != nil {
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

		if err := e.addToIndexFromKey(key, fieldType, index); err != nil {
			return err
		}
	}

	// sh may be nil in tests
	if sh != nil {
		e.logger.Printf("%s database index loaded in %s", sh.Path(), time.Now().Sub(start))
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
		if stat := f.Stats(); stat.LastModified > since.UnixNano() {
			files = append(files, f.Stats())
		}
		for _, t := range f.TombstoneFiles() {
			if t.LastModified > since.UnixNano() {
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
		ModTime: time.Unix(0, f.LastModified),
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
func (e *Engine) addToIndexFromKey(key string, fieldType influxql.DataType, index *tsdb.DatabaseIndex) error {
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
	for k := range e.FileStore.Keys() {
		seriesKey, _ := seriesAndFieldFromCompositeKey(k)
		if _, ok := keyMap[seriesKey]; ok {
			deleteKeys = append(deleteKeys, k)
		}
	}
	if err := e.FileStore.Delete(deleteKeys); err != nil {
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

	e.Cache.Delete(walKeys)

	// delete from the WAL
	_, err := e.WAL.Delete(walKeys)
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

	closedFiles, snapshot, compactor, err := func() ([]string, *Cache, *Compactor, error) {
		e.mu.Lock()
		defer e.mu.Unlock()

		now := time.Now()
		started = &now

		if err := e.WAL.CloseSegment(); err != nil {
			return nil, nil, nil, err
		}

		segments, err := e.WAL.ClosedSegments()
		if err != nil {
			return nil, nil, nil, err
		}

		snapshot, err := e.Cache.Snapshot()
		if err != nil {
			return nil, nil, nil, err
		}

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
func (e *Engine) writeSnapshotAndCommit(closedFiles []string, snapshot *Cache, compactor *Compactor) (err error) {

	defer func() {
		if err != nil {
			e.Cache.ClearSnapshot(false)
		}
	}()
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

func (e *Engine) KeyCursor(key string, t int64, ascending bool) *KeyCursor {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.FileStore.KeyCursor(key, t, ascending)
}

func (e *Engine) CreateIterator(opt influxql.IteratorOptions) (influxql.Iterator, error) {
	if call, ok := opt.Expr.(*influxql.Call); ok {
		refOpt := opt
		refOpt.Expr = call.Args[0].(*influxql.VarRef)
		inputs, err := e.createVarRefIterator(refOpt)
		if err != nil {
			return nil, err
		}

		input := influxql.NewMergeIterator(inputs, opt)
		if opt.InterruptCh != nil {
			input = influxql.NewInterruptIterator(input, opt.InterruptCh)
		}
		return influxql.NewCallIterator(input, opt)
	}

	itrs, err := e.createVarRefIterator(opt)
	if err != nil {
		return nil, err
	}
	itr := influxql.NewSortedMergeIterator(itrs, opt)
	if opt.InterruptCh != nil {
		itr = influxql.NewInterruptIterator(itr, opt.InterruptCh)
	}
	return itr, nil
}

func (e *Engine) SeriesKeys(opt influxql.IteratorOptions) (influxql.SeriesList, error) {
	seriesList := influxql.SeriesList{}
	mms := tsdb.Measurements(e.index.MeasurementsByName(influxql.Sources(opt.Sources).Names()))
	for _, mm := range mms {
		// Determine tagsets for this measurement based on dimensions and filters.
		tagSets, err := mm.TagSets(opt.Dimensions, opt.Condition)
		if err != nil {
			return nil, err
		}

		// Calculate tag sets and apply SLIMIT/SOFFSET.
		tagSets = influxql.LimitTagSets(tagSets, opt.SLimit, opt.SOffset)
		for _, t := range tagSets {
			tagMap := make(map[string]string)
			for k, v := range t.Tags {
				if v == "" {
					continue
				}
				tagMap[k] = v
			}
			tags := influxql.NewTags(tagMap)

			series := influxql.Series{
				Name: mm.Name,
				Tags: tags,
				Aux:  make([]influxql.DataType, len(opt.Aux)),
			}

			// Determine the aux field types.
			for _, seriesKey := range t.SeriesKeys {
				tags := influxql.NewTags(e.index.TagsForSeries(seriesKey))
				for i, field := range opt.Aux {
					typ := func() influxql.DataType {
						mf := e.measurementFields[mm.Name]
						if mf == nil {
							return influxql.Unknown
						}

						f := mf.Field(field)
						if f == nil {
							return influxql.Unknown
						}
						return f.Type
					}()

					if typ == influxql.Unknown {
						if v := tags.Value(field); v != "" {
							// All tags are strings.
							typ = influxql.String
						}
					}

					if typ != influxql.Unknown {
						if series.Aux[i] == influxql.Unknown || typ < series.Aux[i] {
							series.Aux[i] = typ
						}
					}
				}
			}
			seriesList = append(seriesList, series)
		}
	}
	return seriesList, nil
}

// createVarRefIterator creates an iterator for a variable reference.
func (e *Engine) createVarRefIterator(opt influxql.IteratorOptions) ([]influxql.Iterator, error) {
	ref, _ := opt.Expr.(*influxql.VarRef)

	var itrs []influxql.Iterator
	if err := func() error {
		mms := tsdb.Measurements(e.index.MeasurementsByName(influxql.Sources(opt.Sources).Names()))

		// Retrieve the maximum number of fields (without time).
		conditionFields := make([]string, len(influxql.ExprNames(opt.Condition)))

		for _, mm := range mms {
			// Determine tagsets for this measurement based on dimensions and filters.
			tagSets, err := mm.TagSets(opt.Dimensions, opt.Condition)
			if err != nil {
				return err
			}

			// Calculate tag sets and apply SLIMIT/SOFFSET.
			tagSets = influxql.LimitTagSets(tagSets, opt.SLimit, opt.SOffset)

			for _, t := range tagSets {
				for i, seriesKey := range t.SeriesKeys {
					fields := 0
					if t.Filters[i] != nil {
						// Retrieve non-time fields from this series filter and filter out tags.
						for _, f := range influxql.ExprNames(t.Filters[i]) {
							if mm.HasField(f) {
								conditionFields[fields] = f
								fields++
							}
						}
					}

					itr, err := e.createVarRefSeriesIterator(ref, mm, seriesKey, t, t.Filters[i], conditionFields[:fields], opt)
					if err != nil {
						return err
					} else if itr == nil {
						continue
					}
					itrs = append(itrs, itr)
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

// createVarRefSeriesIterator creates an iterator for a variable reference for a series.
func (e *Engine) createVarRefSeriesIterator(ref *influxql.VarRef, mm *tsdb.Measurement, seriesKey string, t *influxql.TagSet, filter influxql.Expr, conditionFields []string, opt influxql.IteratorOptions) (influxql.Iterator, error) {
	tags := influxql.NewTags(e.index.TagsForSeries(seriesKey))

	// Create options specific for this series.
	itrOpt := opt
	itrOpt.Condition = filter

	// Build auxilary cursors.
	// Tag values should be returned if the field doesn't exist.
	var aux []cursorAt
	if len(opt.Aux) > 0 {
		aux = make([]cursorAt, len(opt.Aux))
		for i := range aux {
			// Create cursor from field.
			cur := e.buildCursor(mm.Name, seriesKey, opt.Aux[i], opt)
			if cur != nil {
				aux[i] = newBufCursor(cur, opt.Ascending)
				continue
			}

			// If field doesn't exist, use the tag value.
			// However, if the tag value is blank then return a null.
			if v := tags.Value(opt.Aux[i]); v == "" {
				aux[i] = &stringNilLiteralCursor{}
			} else {
				aux[i] = &stringLiteralCursor{value: v}
			}
		}
	}

	// Build conditional field cursors.
	// If a conditional field doesn't exist then ignore the series.
	var conds []*bufCursor
	if len(conditionFields) > 0 {
		conds = make([]*bufCursor, len(conditionFields))
		for i := range conds {
			cur := e.buildCursor(mm.Name, seriesKey, conditionFields[i], opt)
			if cur == nil {
				return nil, nil
			}
			conds[i] = newBufCursor(cur, opt.Ascending)
		}
	}

	// Limit tags to only the dimensions selected.
	tags = tags.Subset(opt.Dimensions)

	// If it's only auxiliary fields then it doesn't matter what type of iterator we use.
	if ref == nil {
		return newFloatIterator(mm.Name, tags, itrOpt, nil, aux, conds, conditionFields), nil
	}

	// Build main cursor.
	cur := e.buildCursor(mm.Name, seriesKey, ref.Val, opt)

	// If the field doesn't exist then don't build an iterator.
	if cur == nil {
		return nil, nil
	}

	switch cur := cur.(type) {
	case floatCursor:
		return newFloatIterator(mm.Name, tags, itrOpt, cur, aux, conds, conditionFields), nil
	case integerCursor:
		return newIntegerIterator(mm.Name, tags, itrOpt, cur, aux, conds, conditionFields), nil
	case stringCursor:
		return newStringIterator(mm.Name, tags, itrOpt, cur, aux, conds, conditionFields), nil
	case booleanCursor:
		return newBooleanIterator(mm.Name, tags, itrOpt, cur, aux, conds, conditionFields), nil
	default:
		panic("unreachable")
	}
}

// buildCursor creates an untyped cursor for a field.
func (e *Engine) buildCursor(measurement, seriesKey, field string, opt influxql.IteratorOptions) cursor {
	// Look up fields for measurement.
	mf := e.measurementFields[measurement]
	if mf == nil {
		return nil
	}

	// Find individual field.
	f := mf.Field(field)
	if f == nil {
		return nil
	}

	// Return appropriate cursor based on type.
	switch f.Type {
	case influxql.Float:
		return e.buildFloatCursor(measurement, seriesKey, field, opt)
	case influxql.Integer:
		return e.buildIntegerCursor(measurement, seriesKey, field, opt)
	case influxql.String:
		return e.buildStringCursor(measurement, seriesKey, field, opt)
	case influxql.Boolean:
		return e.buildBooleanCursor(measurement, seriesKey, field, opt)
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
	parts := strings.Split(key, keyFieldSeparator)
	if len(parts) != 0 {
		return parts[0], strings.Join(parts[1:], keyFieldSeparator)
	}
	return parts[0], parts[1]
}

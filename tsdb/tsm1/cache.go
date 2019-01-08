package tsm1

import (
	"fmt"
	"math"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/tsdb"
	"github.com/influxdata/influxql"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

// ringShards specifies the number of partitions that the hash ring used to
// store the entry mappings contains. It must be a power of 2. From empirical
// testing, a value above the number of cores on the machine does not provide
// any additional benefit. For now we'll set it to the number of cores on the
// largest box we could imagine running influx.
const ringShards = 16

var (
	// ErrSnapshotInProgress is returned if a snapshot is attempted while one is already running.
	ErrSnapshotInProgress = fmt.Errorf("snapshot in progress")
)

// ErrCacheMemorySizeLimitExceeded returns an error indicating an operation
// could not be completed due to exceeding the cache-max-memory-size setting.
func ErrCacheMemorySizeLimitExceeded(n, limit uint64) error {
	return fmt.Errorf("cache-max-memory-size exceeded: (%d/%d)", n, limit)
}

// entry is a set of values and some metadata.
type entry struct {
	mu     sync.RWMutex
	values Values // All stored values.

	// The type of values stored. Read only so doesn't need to be protected by
	// mu.
	vtype byte
}

// newEntryValues returns a new instance of entry with the given values.  If the
// values are not valid, an error is returned.
func newEntryValues(values []Value) (*entry, error) {
	e := &entry{}
	e.values = make(Values, 0, len(values))
	e.values = append(e.values, values...)

	// No values, don't check types and ordering
	if len(values) == 0 {
		return e, nil
	}

	et := valueType(values[0])
	for _, v := range values {
		// Make sure all the values are the same type
		if et != valueType(v) {
			return nil, tsdb.ErrFieldTypeConflict
		}
	}

	// Set the type of values stored.
	e.vtype = et

	return e, nil
}

// add adds the given values to the entry.
func (e *entry) add(values []Value) error {
	if len(values) == 0 {
		return nil // Nothing to do.
	}

	// Are any of the new values the wrong type?
	if e.vtype != 0 {
		for _, v := range values {
			if e.vtype != valueType(v) {
				return tsdb.ErrFieldTypeConflict
			}
		}
	}

	// entry currently has no values, so add the new ones and we're done.
	e.mu.Lock()
	if len(e.values) == 0 {
		e.values = values
		e.vtype = valueType(values[0])
		e.mu.Unlock()
		return nil
	}

	// Append the new values to the existing ones...
	e.values = append(e.values, values...)
	e.mu.Unlock()
	return nil
}

// deduplicate sorts and orders the entry's values. If values are already deduped and sorted,
// the function does no work and simply returns.
func (e *entry) deduplicate() {
	e.mu.Lock()
	defer e.mu.Unlock()

	if len(e.values) <= 1 {
		return
	}
	e.values = e.values.Deduplicate()
}

// count returns the number of values in this entry.
func (e *entry) count() int {
	e.mu.RLock()
	n := len(e.values)
	e.mu.RUnlock()
	return n
}

// filter removes all values with timestamps between min and max inclusive.
func (e *entry) filter(min, max int64) {
	e.mu.Lock()
	if len(e.values) > 1 {
		e.values = e.values.Deduplicate()
	}
	e.values = e.values.Exclude(min, max)
	e.mu.Unlock()
}

// size returns the size of this entry in bytes.
func (e *entry) size() int {
	e.mu.RLock()
	sz := e.values.Size()
	e.mu.RUnlock()
	return sz
}

// InfluxQLType returns for the entry the data type of its values.
func (e *entry) InfluxQLType() (influxql.DataType, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.values.InfluxQLType()
}

// storer is the interface that descibes a cache's store.
type storer interface {
	entry(key []byte) *entry                        // Get an entry by its key.
	write(key []byte, values Values) (bool, error)  // Write an entry to the store.
	add(key []byte, entry *entry)                   // Add a new entry to the store.
	remove(key []byte)                              // Remove an entry from the store.
	keys(sorted bool) [][]byte                      // Return an optionally sorted slice of entry keys.
	apply(f func([]byte, *entry) error) error       // Apply f to all entries in the store in parallel.
	applySerial(f func([]byte, *entry) error) error // Apply f to all entries in serial.
	reset()                                         // Reset the store to an initial unused state.
	split(n int) []storer                           // Split splits the store into n stores
	count() int                                     // Count returns the number of keys in the store
}

// Cache maintains an in-memory store of Values for a set of keys.
type Cache struct {
	_       uint64 // Padding for 32 bit struct alignment
	mu      sync.RWMutex
	store   storer
	maxSize uint64

	// snapshots are the cache objects that are currently being written to tsm files
	// they're kept in memory while flushing so they can be queried along with the cache.
	// they are read only and should never be modified
	snapshot     *Cache
	snapshotting bool

	tracker       *cacheTracker
	lastSnapshot  time.Time
	lastWriteTime time.Time

	// A one time synchronization used to initial the cache with a store.  Since the store can allocate a
	// a large amount memory across shards, we lazily create it.
	initialize       atomic.Value
	initializedCount uint32
}

// NewCache returns an instance of a cache which will use a maximum of maxSize bytes of memory.
// Only used for engine caches, never for snapshots.
func NewCache(maxSize uint64) *Cache {
	c := &Cache{
		maxSize:      maxSize,
		store:        emptyStore{},
		lastSnapshot: time.Now(),
		tracker:      newCacheTracker(newCacheMetrics(nil), nil),
	}
	c.initialize.Store(&sync.Once{})
	return c
}

// init initializes the cache and allocates the underlying store.  Once initialized,
// the store re-used until Freed.
func (c *Cache) init() {
	if !atomic.CompareAndSwapUint32(&c.initializedCount, 0, 1) {
		return
	}

	c.mu.Lock()
	c.store, _ = newring(ringShards)
	c.mu.Unlock()
}

// Free releases the underlying store and memory held by the Cache.
func (c *Cache) Free() {
	if !atomic.CompareAndSwapUint32(&c.initializedCount, 1, 0) {
		return
	}

	c.mu.Lock()
	c.store = emptyStore{}
	c.mu.Unlock()
}

// Write writes the set of values for the key to the cache. This function is goroutine-safe.
// It returns an error if the cache will exceed its max size by adding the new values.
func (c *Cache) Write(key []byte, values []Value) error {
	c.init()
	addedSize := uint64(Values(values).Size())

	// Enough room in the cache?
	limit := c.maxSize
	n := c.Size() + addedSize

	if limit > 0 && n > limit {
		c.tracker.IncWritesErr()
		c.tracker.AddWrittenBytesDrop(uint64(addedSize))
		return ErrCacheMemorySizeLimitExceeded(n, limit)
	}

	newKey, err := c.store.write(key, values)
	if err != nil {
		c.tracker.IncWritesErr()
		c.tracker.AddWrittenBytesErr(uint64(addedSize))
		return err
	}

	if newKey {
		addedSize += uint64(len(key))
	}
	// Update the cache size and the memory size stat.
	c.tracker.IncCacheSize(addedSize)
	c.tracker.AddMemBytes(addedSize)
	c.tracker.AddWrittenBytesOK(uint64(addedSize))
	c.tracker.IncWritesOK()

	return nil
}

// WriteMulti writes the map of keys and associated values to the cache. This
// function is goroutine-safe. It returns an error if the cache will exceeded
// its max size by adding the new values.  The write attempts to write as many
// values as possible.  If one key fails, the others can still succeed and an
// error will be returned.
func (c *Cache) WriteMulti(values map[string][]Value) error {
	c.init()
	var addedSize uint64
	for _, v := range values {
		addedSize += uint64(Values(v).Size())
	}

	// Enough room in the cache?
	limit := c.maxSize // maxSize is safe for reading without a lock.
	n := c.Size() + addedSize
	if limit > 0 && n > limit {
		c.tracker.IncWritesErr()
		c.tracker.AddWrittenBytesDrop(uint64(addedSize))
		return ErrCacheMemorySizeLimitExceeded(n, limit)
	}

	var werr error
	c.mu.RLock()
	store := c.store
	c.mu.RUnlock()

	var bytesWrittenErr uint64

	// We'll optimistically set size here, and then decrement it for write errors.
	for k, v := range values {
		newKey, err := store.write([]byte(k), v)
		if err != nil {
			// The write failed, hold onto the error and adjust the size delta.
			werr = err
			addedSize -= uint64(Values(v).Size())
			bytesWrittenErr += uint64(Values(v).Size())
		}

		if newKey {
			addedSize += uint64(len(k))
		}
	}

	// Some points in the batch were dropped.  An error is returned so
	// error stat is incremented as well.
	if werr != nil {
		c.tracker.IncWritesErr()
		c.tracker.IncWritesDrop()
		c.tracker.AddWrittenBytesErr(bytesWrittenErr)
	}

	// Update the memory size stat
	c.tracker.IncCacheSize(addedSize)
	c.tracker.AddMemBytes(addedSize)
	c.tracker.IncWritesOK()
	c.tracker.AddWrittenBytesOK(addedSize)

	c.mu.Lock()
	c.lastWriteTime = time.Now()
	c.mu.Unlock()

	return werr
}

// Snapshot takes a snapshot of the current cache, adds it to the slice of caches that
// are being flushed, and resets the current cache with new values.
func (c *Cache) Snapshot() (*Cache, error) {
	c.init()

	c.mu.Lock()
	defer c.mu.Unlock()

	if c.snapshotting {
		return nil, ErrSnapshotInProgress
	}

	c.snapshotting = true
	c.tracker.IncSnapshotsActive() // increment the number of times we tried to do this

	// If no snapshot exists, create a new one, otherwise update the existing snapshot
	if c.snapshot == nil {
		store, err := newring(ringShards)
		if err != nil {
			return nil, err
		}

		c.snapshot = &Cache{
			store:   store,
			tracker: newCacheTracker(c.tracker.metrics, c.tracker.labels),
		}
	}

	// Did a prior snapshot exist that failed?  If so, return the existing
	// snapshot to retry.
	if c.snapshot.Size() > 0 {
		return c.snapshot, nil
	}

	c.snapshot.store, c.store = c.store, c.snapshot.store
	snapshotSize := c.Size()

	c.snapshot.tracker.SetSnapshotSize(snapshotSize) // Save the size of the snapshot on the snapshot cache
	c.tracker.SetSnapshotSize(snapshotSize)          // Save the size of the snapshot on the live cache

	// Reset the cache's store.
	c.store.reset()
	c.tracker.SetCacheSize(0)
	c.lastSnapshot = time.Now()

	c.tracker.AddSnapshottedBytes(snapshotSize) // increment the number of bytes added to the snapshot
	c.tracker.SetDiskBytes(0)
	c.tracker.SetSnapshotsActive(0)

	return c.snapshot, nil
}

// Deduplicate sorts the snapshot before returning it. The compactor and any queries
// coming in while it writes will need the values sorted.
func (c *Cache) Deduplicate() {
	c.mu.RLock()
	store := c.store
	c.mu.RUnlock()

	// Apply a function that simply calls deduplicate on each entry in the ring.
	// apply cannot return an error in this invocation.
	_ = store.apply(func(_ []byte, e *entry) error { e.deduplicate(); return nil })
}

// ClearSnapshot removes the snapshot cache from the list of flushing caches and
// adjusts the size.
func (c *Cache) ClearSnapshot(success bool) {
	c.init()

	c.mu.RLock()
	snapStore := c.snapshot.store
	c.mu.RUnlock()

	// reset the snapshot store outside of the write lock
	if success {
		snapStore.reset()
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	c.snapshotting = false

	if success {
		snapshotSize := c.tracker.SnapshotSize()
		c.tracker.SetSnapshotsActive(0)
		c.tracker.SubMemBytes(snapshotSize) // decrement the number of bytes in cache

		// Reset the snapshot to a fresh Cache.
		c.snapshot = &Cache{
			store:   c.snapshot.store,
			tracker: newCacheTracker(c.tracker.metrics, c.tracker.labels),
		}

		c.tracker.SetSnapshotSize(0)
		c.tracker.SetDiskBytes(0)
		c.tracker.SetSnapshotsActive(0)
	}
}

// Size returns the number of point-calcuated bytes the cache currently uses.
func (c *Cache) Size() uint64 {
	return c.tracker.CacheSize() + c.tracker.SnapshotSize()
}

// MaxSize returns the maximum number of bytes the cache may consume.
func (c *Cache) MaxSize() uint64 {
	return c.maxSize
}

func (c *Cache) Count() int {
	c.mu.RLock()
	n := c.store.count()
	c.mu.RUnlock()
	return n
}

// Keys returns a sorted slice of all keys under management by the cache.
func (c *Cache) Keys() [][]byte {
	c.mu.RLock()
	store := c.store
	c.mu.RUnlock()
	return store.keys(true)
}

func (c *Cache) Split(n int) []*Cache {
	if n == 1 {
		return []*Cache{c}
	}

	caches := make([]*Cache, n)
	storers := c.store.split(n)
	for i := 0; i < n; i++ {
		caches[i] = &Cache{
			store: storers[i],
		}
	}
	return caches
}

// Type returns the series type for a key.
func (c *Cache) Type(key []byte) (models.FieldType, error) {
	c.mu.RLock()
	e := c.store.entry(key)
	if e == nil && c.snapshot != nil {
		e = c.snapshot.store.entry(key)
	}
	c.mu.RUnlock()

	if e != nil {
		typ, err := e.InfluxQLType()
		if err != nil {
			return models.Empty, tsdb.ErrUnknownFieldType
		}

		switch typ {
		case influxql.Float:
			return models.Float, nil
		case influxql.Integer:
			return models.Integer, nil
		case influxql.Unsigned:
			return models.Unsigned, nil
		case influxql.Boolean:
			return models.Boolean, nil
		case influxql.String:
			return models.String, nil
		}
	}

	return models.Empty, tsdb.ErrUnknownFieldType
}

// Values returns a copy of all values, deduped and sorted, for the given key.
func (c *Cache) Values(key []byte) Values {
	var snapshotEntries *entry

	c.mu.RLock()
	e := c.store.entry(key)
	if c.snapshot != nil {
		snapshotEntries = c.snapshot.store.entry(key)
	}
	c.mu.RUnlock()

	if e == nil {
		if snapshotEntries == nil {
			// No values in hot cache or snapshots.
			return nil
		}
	} else {
		e.deduplicate()
	}

	// Build the sequence of entries that will be returned, in the correct order.
	// Calculate the required size of the destination buffer.
	var entries []*entry
	sz := 0

	if snapshotEntries != nil {
		snapshotEntries.deduplicate() // guarantee we are deduplicated
		entries = append(entries, snapshotEntries)
		sz += snapshotEntries.count()
	}

	if e != nil {
		entries = append(entries, e)
		sz += e.count()
	}

	// Any entries? If not, return.
	if sz == 0 {
		return nil
	}

	// Create the buffer, and copy all hot values and snapshots. Individual
	// entries are sorted at this point, so now the code has to check if the
	// resultant buffer will be sorted from start to finish.
	values := make(Values, sz)
	n := 0
	for _, e := range entries {
		e.mu.RLock()
		n += copy(values[n:], e.values)
		e.mu.RUnlock()
	}
	values = values[:n]
	values = values.Deduplicate()

	return values
}

// Delete removes all values for the given keys from the cache.
func (c *Cache) Delete(keys [][]byte) {
	c.DeleteRange(keys, math.MinInt64, math.MaxInt64)
}

// DeleteRange removes the values for all keys containing points
// with timestamps between between min and max from the cache.
//
// TODO(edd): Lock usage could possibly be optimised if necessary.
func (c *Cache) DeleteRange(keys [][]byte, min, max int64) {
	c.init()

	c.mu.Lock()
	defer c.mu.Unlock()

	var total uint64
	for _, k := range keys {
		// Make sure key exist in the cache, skip if it does not
		e := c.store.entry(k)
		if e == nil {
			continue
		}

		total += uint64(e.size())
		// Everything is being deleted.
		if min == math.MinInt64 && max == math.MaxInt64 {
			total += uint64(len(k)) // all entries and the key.
			c.store.remove(k)
			continue
		}

		// Filter what to delete by time range.
		e.filter(min, max)
		if e.count() == 0 {
			// Nothing left in cache for that key
			total += uint64(len(k)) // all entries and the key.
			c.store.remove(k)
			continue
		}

		// Just update what is being deleted by the size of the filtered entries.
		total -= uint64(e.size())
	}
	c.tracker.DecCacheSize(total) // Decrease the live cache size.
	c.tracker.SetMemBytes(uint64(c.Size()))
}

// SetMaxSize updates the memory limit of the cache.
func (c *Cache) SetMaxSize(size uint64) {
	c.mu.Lock()
	c.maxSize = size
	c.mu.Unlock()
}

// values returns the values for the key. It assumes the data is already sorted.
// It doesn't lock the cache but it does read-lock the entry if there is one for the key.
// values should only be used in compact.go in the CacheKeyIterator.
func (c *Cache) values(key []byte) Values {
	e := c.store.entry(key)
	if e == nil {
		return nil
	}
	e.mu.RLock()
	v := e.values
	e.mu.RUnlock()
	return v
}

// ApplyEntryFn applies the function f to each entry in the Cache.
// ApplyEntryFn calls f on each entry in turn, within the same goroutine.
// It is safe for use by multiple goroutines.
func (c *Cache) ApplyEntryFn(f func(key []byte, entry *entry) error) error {
	c.mu.RLock()
	store := c.store
	c.mu.RUnlock()
	return store.applySerial(f)
}

// CacheLoader processes a set of WAL segment files, and loads a cache with the data
// contained within those files.  Processing of the supplied files take place in the
// order they exist in the files slice.
type CacheLoader struct {
	files []string

	Logger *zap.Logger
}

// NewCacheLoader returns a new instance of a CacheLoader.
func NewCacheLoader(files []string) *CacheLoader {
	return &CacheLoader{
		files:  files,
		Logger: zap.NewNop(),
	}
}

// Load returns a cache loaded with the data contained within the segment files.
// If, during reading of a segment file, corruption is encountered, that segment
// file is truncated up to and including the last valid byte, and processing
// continues with the next segment file.
func (cl *CacheLoader) Load(cache *Cache) error {

	var r *WALSegmentReader
	for _, fn := range cl.files {
		if err := func() error {
			f, err := os.OpenFile(fn, os.O_CREATE|os.O_RDWR, 0666)
			if err != nil {
				return err
			}
			defer f.Close()

			// Log some information about the segments.
			stat, err := os.Stat(f.Name())
			if err != nil {
				return err
			}
			cl.Logger.Info("Reading file", zap.String("path", f.Name()), zap.Int64("size", stat.Size()))

			// Nothing to read, skip it
			if stat.Size() == 0 {
				return nil
			}

			if r == nil {
				r = NewWALSegmentReader(f)
				defer r.Close()
			} else {
				r.Reset(f)
			}

			for r.Next() {
				entry, err := r.Read()
				if err != nil {
					n := r.Count()
					cl.Logger.Info("File corrupt", zap.Error(err), zap.String("path", f.Name()), zap.Int64("pos", n))
					if err := f.Truncate(n); err != nil {
						return err
					}
					break
				}

				switch t := entry.(type) {
				case *WriteWALEntry:
					if err := cache.WriteMulti(t.Values); err != nil {
						return err
					}
				case *DeleteRangeWALEntry:
					cache.DeleteRange(t.Keys, t.Min, t.Max)
				case *DeleteWALEntry:
					cache.Delete(t.Keys)
				}
			}

			return r.Close()
		}(); err != nil {
			return err
		}
	}
	return nil
}

// WithLogger sets the logger on the CacheLoader.
func (cl *CacheLoader) WithLogger(log *zap.Logger) {
	cl.Logger = log.With(zap.String("service", "cacheloader"))
}

func (c *Cache) LastWriteTime() time.Time {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.lastWriteTime
}

// UpdateAge updates the age statistic based on the current time.
func (c *Cache) UpdateAge() {
	c.mu.RLock()
	defer c.mu.RUnlock()
	c.tracker.SetAge(time.Since(c.lastSnapshot))
}

// cacheTracker tracks writes to the cache and snapshots.
//
// As well as being responsible for providing atomic reads and writes to the
// statistics, cacheTracker also mirrors any changes to the external prometheus
// metrics, which the Engine exposes.
//
// *NOTE* - cacheTracker fields should not be directory modified. Doing so
// could result in the Engine exposing inaccurate metrics.
type cacheTracker struct {
	metrics         *cacheMetrics
	labels          prometheus.Labels
	snapshotsActive uint64
	snapshotSize    uint64
	cacheSize       uint64

	// Used in testing.
	memSizeBytes     uint64
	snapshottedBytes uint64
	writesDropped    uint64
	writesErr        uint64
}

func newCacheTracker(metrics *cacheMetrics, defaultLabels prometheus.Labels) *cacheTracker {
	return &cacheTracker{metrics: metrics, labels: defaultLabels}
}

// Labels returns a copy of the default labels used by the tracker's metrics.
// The returned map is safe for modification.
func (t *cacheTracker) Labels() prometheus.Labels {
	labels := make(prometheus.Labels, len(t.labels))
	for k, v := range t.labels {
		labels[k] = v
	}
	return labels
}

// AddMemBytes increases the number of in-memory cache bytes.
func (t *cacheTracker) AddMemBytes(bytes uint64) {
	atomic.AddUint64(&t.memSizeBytes, bytes)

	labels := t.labels
	t.metrics.MemSize.With(labels).Add(float64(bytes))
}

// SubMemBytes decreases the number of in-memory cache bytes.
func (t *cacheTracker) SubMemBytes(bytes uint64) {
	atomic.AddUint64(&t.memSizeBytes, ^(bytes - 1))

	labels := t.labels
	t.metrics.MemSize.With(labels).Sub(float64(bytes))
}

// SetMemBytes sets the number of in-memory cache bytes.
func (t *cacheTracker) SetMemBytes(bytes uint64) {
	atomic.StoreUint64(&t.memSizeBytes, bytes)

	labels := t.labels
	t.metrics.MemSize.With(labels).Set(float64(bytes))
}

// AddBytesWritten increases the number of bytes written to the cache.
func (t *cacheTracker) AddBytesWritten(bytes uint64) {
	labels := t.labels
	t.metrics.MemSize.With(labels).Add(float64(bytes))
}

// AddSnapshottedBytes increases the number of bytes snapshotted.
func (t *cacheTracker) AddSnapshottedBytes(bytes uint64) {
	atomic.AddUint64(&t.snapshottedBytes, bytes)

	labels := t.labels
	t.metrics.SnapshottedBytes.With(labels).Add(float64(bytes))
}

// SetDiskBytes sets the number of bytes on disk used by snapshot data.
func (t *cacheTracker) SetDiskBytes(bytes uint64) {
	labels := t.labels
	t.metrics.DiskSize.With(labels).Set(float64(bytes))
}

// IncSnapshotsActive increases the number of active snapshots.
func (t *cacheTracker) IncSnapshotsActive() {
	atomic.AddUint64(&t.snapshotsActive, 1)

	labels := t.labels
	t.metrics.SnapshotsActive.With(labels).Inc()
}

// SetSnapshotsActive sets the number of bytes on disk used by snapshot data.
func (t *cacheTracker) SetSnapshotsActive(n uint64) {
	atomic.StoreUint64(&t.snapshotsActive, n)

	labels := t.labels
	t.metrics.SnapshotsActive.With(labels).Set(float64(n))
}

// AddWrittenBytes increases the number of bytes written to the cache, with a required status.
func (t *cacheTracker) AddWrittenBytes(status string, bytes uint64) {
	labels := t.Labels()
	labels["status"] = status
	t.metrics.WrittenBytes.With(labels).Add(float64(bytes))
}

// AddWrittenBytesOK increments the number of successful writes.
func (t *cacheTracker) AddWrittenBytesOK(bytes uint64) { t.AddWrittenBytes("ok", bytes) }

// AddWrittenBytesError increments the number of writes that encountered an error.
func (t *cacheTracker) AddWrittenBytesErr(bytes uint64) { t.AddWrittenBytes("error", bytes) }

// AddWrittenBytesDrop increments the number of writes that were dropped.
func (t *cacheTracker) AddWrittenBytesDrop(bytes uint64) { t.AddWrittenBytes("dropped", bytes) }

// IncWrites increments the number of writes to the cache, with a required status.
func (t *cacheTracker) IncWrites(status string) {
	labels := t.Labels()
	labels["status"] = status
	t.metrics.Writes.With(labels).Inc()
}

// IncWritesOK increments the number of successful writes.
func (t *cacheTracker) IncWritesOK() { t.IncWrites("ok") }

// IncWritesError increments the number of writes that encountered an error.
func (t *cacheTracker) IncWritesErr() {
	atomic.AddUint64(&t.writesErr, 1)

	t.IncWrites("error")
}

// IncWritesDrop increments the number of writes that were dropped.
func (t *cacheTracker) IncWritesDrop() {
	atomic.AddUint64(&t.writesDropped, 1)

	t.IncWrites("dropped")
}

// CacheSize returns the live cache size.
func (t *cacheTracker) CacheSize() uint64 { return atomic.LoadUint64(&t.cacheSize) }

// IncCacheSize increases the live cache size by sz bytes.
func (t *cacheTracker) IncCacheSize(sz uint64) { atomic.AddUint64(&t.cacheSize, sz) }

// DecCacheSize decreases the live cache size by sz bytes.
func (t *cacheTracker) DecCacheSize(sz uint64) { atomic.AddUint64(&t.cacheSize, ^(sz - 1)) }

// SetCacheSize sets the live cache size to sz.
func (t *cacheTracker) SetCacheSize(sz uint64) { atomic.StoreUint64(&t.cacheSize, sz) }

// SetSnapshotSize sets the last successful snapshot size.
func (t *cacheTracker) SetSnapshotSize(sz uint64) { atomic.StoreUint64(&t.snapshotSize, sz) }

// SnapshotSize returns the last successful snapshot size.
func (t *cacheTracker) SnapshotSize() uint64 { return atomic.LoadUint64(&t.snapshotSize) }

// SetAge sets the time since the last successful snapshot
func (t *cacheTracker) SetAge(d time.Duration) {
	labels := t.Labels()
	t.metrics.Age.With(labels).Set(d.Seconds())
}

func valueType(v Value) byte {
	switch v.(type) {
	case FloatValue:
		return 1
	case IntegerValue:
		return 2
	case StringValue:
		return 3
	case BooleanValue:
		return 4
	default:
		return 0
	}
}

type emptyStore struct{}

func (e emptyStore) entry(key []byte) *entry                        { return nil }
func (e emptyStore) write(key []byte, values Values) (bool, error)  { return false, nil }
func (e emptyStore) add(key []byte, entry *entry)                   {}
func (e emptyStore) remove(key []byte)                              {}
func (e emptyStore) keys(sorted bool) [][]byte                      { return nil }
func (e emptyStore) apply(f func([]byte, *entry) error) error       { return nil }
func (e emptyStore) applySerial(f func([]byte, *entry) error) error { return nil }
func (e emptyStore) reset()                                         {}
func (e emptyStore) split(n int) []storer                           { return nil }
func (e emptyStore) count() int                                     { return 0 }

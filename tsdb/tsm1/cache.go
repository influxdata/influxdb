package tsm1

import (
	"context"
	"fmt"
	"math"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/influxdata/influxdb/v2/kit/tracing"
	"github.com/influxdata/influxdb/v2/models"
	"github.com/influxdata/influxdb/v2/storage/wal"
	"github.com/influxdata/influxdb/v2/tsdb"
	"github.com/influxdata/influxql"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

var (
	// ErrSnapshotInProgress is returned if a snapshot is attempted while one is already running.
	ErrSnapshotInProgress = fmt.Errorf("snapshot in progress")
)

// CacheMemorySizeLimitExceededError is the type of error returned from the cache when
// a write would place it over its size limit.
type CacheMemorySizeLimitExceededError struct {
	Size  uint64
	Limit uint64
}

func (c CacheMemorySizeLimitExceededError) Error() string {
	return fmt.Sprintf("cache-max-memory-size exceeded: (%d/%d)", c.Size, c.Limit)
}

// ErrCacheMemorySizeLimitExceeded returns an error indicating an operation
// could not be completed due to exceeding the cache-max-memory-size setting.
func ErrCacheMemorySizeLimitExceeded(n, limit uint64) error {
	return CacheMemorySizeLimitExceededError{Size: n, Limit: limit}
}

// Cache maintains an in-memory store of Values for a set of keys.
type Cache struct {
	mu      sync.RWMutex
	store   *ring
	maxSize uint64

	// snapshots are the cache objects that are currently being written to tsm files
	// they're kept in memory while flushing so they can be queried along with the cache.
	// they are read only and should never be modified
	snapshot     *Cache
	snapshotting bool

	tracker       *cacheTracker
	lastSnapshot  time.Time
	lastWriteTime time.Time
}

// NewCache returns an instance of a cache which will use a maximum of maxSize bytes of memory.
// Only used for engine caches, never for snapshots.
func NewCache(maxSize uint64) *Cache {
	return &Cache{
		maxSize:      maxSize,
		store:        newRing(),
		lastSnapshot: time.Now(),
		tracker:      newCacheTracker(newCacheMetrics(nil), nil),
	}
}

// Write writes the set of values for the key to the cache. This function is goroutine-safe.
// It returns an error if the cache will exceed its max size by adding the new values.
func (c *Cache) Write(key []byte, values []Value) error {
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
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.snapshotting {
		return nil, ErrSnapshotInProgress
	}

	c.snapshotting = true
	c.tracker.IncSnapshotsActive() // increment the number of times we tried to do this

	// If no snapshot exists, create a new one, otherwise update the existing snapshot
	if c.snapshot == nil {
		c.snapshot = &Cache{
			store:   newRing(),
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
			return models.Empty, errUnknownFieldType
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

	return models.Empty, errUnknownFieldType
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

// DeleteBucketRange removes values for all keys containing points
// with timestamps between min and max contained in the bucket identified
// by name from the cache.
func (c *Cache) DeleteBucketRange(ctx context.Context, name string, min, max int64, pred Predicate) {
	span, _ := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	// TODO(edd/jeff): find a way to optimize lock usage
	c.mu.Lock()
	defer c.mu.Unlock()

	var toDelete []string
	var total uint64

	// applySerial only errors if the closure returns an error.
	_ = c.store.applySerial(func(k string, e *entry) error {
		if !strings.HasPrefix(k, name) {
			return nil
		}
		// TODO(edd): either use an unsafe conversion to []byte, or add a MatchesString
		// method to tsm1.Predicate.
		if pred != nil && !pred.Matches([]byte(k)) {
			return nil
		}

		total += uint64(e.size())

		// if everything is being deleted, just stage it to be deleted and move on.
		if min == math.MinInt64 && max == math.MaxInt64 {
			toDelete = append(toDelete, k)
			return nil
		}

		// filter the values and subtract out the remaining bytes from the reduction.
		e.filter(min, max)
		total -= uint64(e.size())

		// if it has no entries left, flag it to be deleted.
		if e.count() == 0 {
			toDelete = append(toDelete, k)
		}

		return nil
	})

	for _, k := range toDelete {
		total += uint64(len(k))
		// TODO(edd): either use unsafe conversion to []byte or add a removeString method.
		c.store.remove([]byte(k))
	}

	c.tracker.DecCacheSize(total)
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
func (c *Cache) ApplyEntryFn(f func(key string, entry *entry) error) error {
	c.mu.RLock()
	store := c.store
	c.mu.RUnlock()
	return store.applySerial(f)
}

// CacheLoader processes a set of WAL segment files, and loads a cache with the data
// contained within those files.
type CacheLoader struct {
	reader *wal.WALReader
}

// NewCacheLoader returns a new instance of a CacheLoader.
func NewCacheLoader(files []string) *CacheLoader {
	return &CacheLoader{
		reader: wal.NewWALReader(files),
	}
}

// Load returns a cache loaded with the data contained within the segment files.
func (cl *CacheLoader) Load(cache *Cache) error {
	return cl.reader.Read(func(entry wal.WALEntry) error {
		switch en := entry.(type) {
		case *wal.WriteWALEntry:
			return cache.WriteMulti(en.Values)

		case *wal.DeleteBucketRangeWALEntry:
			var pred Predicate
			if len(en.Predicate) > 0 {
				var err error
				pred, err = UnmarshalPredicate(en.Predicate)
				if err != nil {
					return err
				}
			}

			// TODO(edd): we need to clean up how we're encoding the prefix so that we
			// don't have to remember to get it right everywhere we need to touch TSM data.
			encoded := tsdb.EncodeName(en.OrgID, en.BucketID)
			name := models.EscapeMeasurement(encoded[:])

			cache.DeleteBucketRange(context.Background(), string(name), en.Min, en.Max, pred)
			return nil
		}

		return nil
	})
}

// WithLogger sets the logger on the CacheLoader.
func (cl *CacheLoader) WithLogger(logger *zap.Logger) {
	cl.reader.WithLogger(logger.With(zap.String("service", "cacheloader")))
}

// LastWriteTime returns the time that the cache was last written to.
func (c *Cache) LastWriteTime() time.Time {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.lastWriteTime
}

// Age returns the age of the cache, which is the duration since it was last
// snapshotted.
func (c *Cache) Age() time.Duration {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return time.Since(c.lastSnapshot)
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

const (
	valueTypeUndefined = 0
	valueTypeFloat64   = 1
	valueTypeInteger   = 2
	valueTypeString    = 3
	valueTypeBoolean   = 4
	valueTypeUnsigned  = 5
)

func valueType(v Value) byte {
	switch v.(type) {
	case FloatValue:
		return valueTypeFloat64
	case IntegerValue:
		return valueTypeInteger
	case StringValue:
		return valueTypeString
	case BooleanValue:
		return valueTypeBoolean
	case UnsignedValue:
		return valueTypeUnsigned
	default:
		return valueTypeUndefined
	}
}

var (
	valueTypeBlockType = [8]byte{
		valueTypeUndefined: blockUndefined,
		valueTypeFloat64:   BlockFloat64,
		valueTypeInteger:   BlockInteger,
		valueTypeString:    BlockString,
		valueTypeBoolean:   BlockBoolean,
		valueTypeUnsigned:  BlockUnsigned,
		6:                  blockUndefined,
		7:                  blockUndefined,
	}
)

func valueTypeToBlockType(typ byte) byte { return valueTypeBlockType[typ&7] }

package tsm1

// Compactions are the process of creating read-optimized TSM files.
// The files are created by converting write-optimized WAL entries
// to read-optimized TSM format.  They can also be created from existing
// TSM files when there are tombstone records that neeed to be removed, points
// that were overwritten by later writes and need to updated, or multiple
// smaller TSM files need to be merged to reduce file counts and improve
// compression ratios.
//
// The the compaction process is stream-oriented using multiple readers and
// iterators.  The resulting stream is written sorted and chunked to allow for
// one-pass writing of a new TSM file.

import (
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"time"
)

// maxCompactionSegments is the maximum number of segments that can be
// compaction at one time.  A lower value would shorten
// compaction times and memory requirements, but produce more TSM files
// with lower compression ratios.  A higher value increases compaction times
// and memory usage but produces more dense TSM files.
const maxCompactionSegments = 10

const maxTSMFileSize = 250 * 1024 * 1024
const rolloverTSMFileSize = 5 * 1025 * 1024

var errMaxFileExceeded = fmt.Errorf("max file exceeded")

var (
	MaxTime = time.Unix(0, math.MaxInt64)
	MinTime = time.Unix(0, 0)
)

// CompactionPlanner determines what TSM files and WAL segments to include in a
// given compaction run.
type CompactionPlanner interface {
	Plan() ([]string, []string, error)
}

// DefaultPlanner implements CompactionPlanner using a strategy to minimize
// the number of closed WAL segments as well as rewriting existing files for
// improve compression ratios.  It prefers compacting WAL segments over TSM
// files to allow cached points to be evicted more quickly.  When looking at
// TSM files, it will pull in TSM files that need to be rewritten to ensure
// points exist in only one file.  Reclaiming space is lower priority while
// there are multiple WAL segments still on disk (e.g. deleting tombstones,
// combining smaller TSM files, etc..)
//
// It prioritizes WAL segments and TSM files as follows:
//
// 1) If there are more than 10 closed WAL segments, it will use the 10 oldest
// 2) If there are any TSM files that contain points that would be overwritten
//    by a WAL segment, those TSM files are included
// 3) If there are fewer than 10 WAL segments and no TSM files are required to be
//    re-written, any TSM files containing tombstones are included.
// 4) If thare are still fewer than 10 WAL segments and no TSM files included, any
//    TSM files less than the max file size are included.
type DefaultPlanner struct {
	WAL interface {
		ClosedSegments() ([]SegmentStat, error)
	}

	Cache interface {
		CheckpointRange() (uint64, uint64)
	}

	FileStore interface {
		Stats() []FileStat
	}
}

// Plan returns a set of TSM files to rewrite, and WAL segments to compact
func (c *DefaultPlanner) Plan() (tsmFiles, walSegments []string, err error) {
	wal, err := c.WAL.ClosedSegments()
	if err != nil {
		return nil, nil, err
	}

	if len(wal) > maxCompactionSegments {
		wal = wal[:maxCompactionSegments]
	}
	// Check the cache to see what segments are currently cached.  If they are
	// not in the cache, we can't compact them and we don't want to include them
	// in walSegments because they will be deleted after the compaction runs.
	minCheck, maxCheck := c.Cache.CheckpointRange()
	var cachedWal []SegmentStat
	for _, w := range wal {
		if uint64(w.ID) >= minCheck && uint64(w.ID) <= maxCheck {
			cachedWal = append(cachedWal, w)
		}
	}
	wal = cachedWal

	tsmStats := c.FileStore.Stats()

	var walPaths []string
	var tsmPaths []string

	// Since TSM files can be added from different conditions, this is used to ensure
	// they are added only once.
	uniqueTsmPaths := map[string]struct{}{}

	// We need to rewrite any TSM files that could contain points for any keys in the
	// WAL segments.
	for _, w := range wal {
		for _, t := range tsmStats {
			if t.OverlapsTimeRange(w.MinTime, w.MaxTime) && t.OverlapsKeyRange(w.MinKey, w.MaxKey) {
				uniqueTsmPaths[t.Path] = struct{}{}
			}
		}

		walPaths = append(walPaths, w.Path)
	}

	if len(wal) < maxCompactionSegments && len(tsmPaths) == 0 {
		for _, tsm := range tsmStats {
			if tsm.HasTombstone {
				uniqueTsmPaths[tsm.Path] = struct{}{}
			}
		}
	}

	// Only look to rollup TSM files if we don't have any already identified to be
	// re-written and we have less than the max WAL segments.
	if len(wal) < maxCompactionSegments && len(tsmPaths) == 0 && len(tsmStats) > 1 {
		for _, tsm := range tsmStats {
			if tsm.Size > maxTSMFileSize {
				continue
			}
			uniqueTsmPaths[tsm.Path] = struct{}{}
		}
	}

	for k := range uniqueTsmPaths {
		tsmPaths = append(tsmPaths, k)
	}
	sort.Strings(tsmPaths)

	if len(tsmPaths) == 1 && len(walPaths) == 0 {
		return nil, nil, nil
	}
	return tsmPaths, walPaths, nil
}

// Compactor merges multiple WAL segments and TSM files into one or more
// new TSM files.
type Compactor struct {
	Dir         string
	MaxFileSize int
	currentID   int

	FileStore interface {
		NextID() int
	}

	Cache *Cache

	merge *MergeIterator
}

// Compact converts WAL segements and TSM files into new TSM files.
func (c *Compactor) Compact(tsmFiles, walSegments []string) ([]string, error) {
	var wal KeyIterator
	var err error
	if len(walSegments) > 0 {
		ids := SegmentPaths(walSegments).IDs()
		min, max := uint64(ids[0]), uint64(ids[len(ids)-1])

		// WALKeyIterator allows all the segments to be ordered by key and
		// sorted values during compaction.
		wal = NewCacheKeyIterator(c.Cache, min, max)
	}

	// For each TSM file, create a TSM reader
	var trs []*TSMReader
	for _, file := range tsmFiles {
		f, err := os.Open(file)
		if err != nil {
			return nil, err
		}

		tr, err := NewTSMReaderWithOptions(
			TSMReaderOptions{
				MMAPFile: f,
			})
		if err != nil {
			return nil, err
		}
		trs = append(trs, tr)
	}

	var tsm KeyIterator
	if len(trs) > 0 {
		tsm, err = NewTSMKeyIterator(trs...)
		if err != nil {
			return nil, err
		}
	}

	// Merge iterator combines the WAL and TSM iterators (note: TSM iteration is
	// not in place yet).  It will also chunk the values into 1000 element blocks.
	c.merge = NewMergeIterator(tsm, wal, 1000, MinTime, MaxTime)
	defer c.merge.Close()

	// These are the new TSM files written
	var files []string

	for {
		c.currentID = c.FileStore.NextID()

		// New TSM files are written to a temp file and renamed when fully completed.
		fileName := filepath.Join(c.Dir, fmt.Sprintf("%07d.%s.tmp", c.currentID, "tsm1dev"))

		rollover := rolloverTSMFileSize
		if len(walSegments) == 0 {
			rollover = maxTSMFileSize
		}
		// Write as much as possible to this file
		err := c.write(fileName, rollover)

		// We've hit the max file limit and there is more to write.  Create a new file
		// and continue.
		if err == errMaxFileExceeded {
			files = append(files, fileName)
			continue
		}

		// We hit an error but didn't finish the compaction.  Remove the temp file and abort.
		if err != nil {
			return nil, err
		}

		files = append(files, fileName)
		break
	}
	c.merge = nil
	return files, nil
}

func (c *Compactor) write(path string, rollover int) error {
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		return fmt.Errorf("%v already file exists. aborting", path)
	}

	fd, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return err
	}

	// Create the write for the new TSM file.
	w, err := NewTSMWriter(fd)
	if err != nil {
		return err
	}

	for c.merge.Next() {
		// Each call to read returns the next sorted key (or the prior one if there are
		// more values to write).  The size of values will be less than or equal to our
		// chunk size (1000)
		key, values, err := c.merge.Read()
		if err != nil {
			return err
		}

		// Write the key and value
		if err := w.Write(key, values); err != nil {
			return err
		}

		// If we have a max file size configured and we're over it, close out the file
		// and return the error.
		if rollover != 0 && w.Size() > rollover {
			if err := w.WriteIndex(); err != nil {
				return err
			}

			if err := w.Close(); err != nil {
				return err
			}

			return errMaxFileExceeded
		}
	}

	// We're all done.  Close out the file.
	if err := w.WriteIndex(); err != nil {
		return err
	}

	if err := w.Close(); err != nil {
		return err
	}

	return nil
}

// MergeIterator merges multiple KeyIterators while chunking each read call
// into a fixed size.  Each iteration, the lowest lexicographically ordered
// key is returned with the next set of values for that key ordered by time.  Values
// with identical times are overwitten by the WAL KeyIterator.
//
// Moving through the full iteration cycle will result in sorted, unique, chunks of values
// up to a max size. Each key returned will be greater than or equal to the prior
// key returned.
type MergeIterator struct {
	// wal is the iterator for multiple WAL segments combined
	wal KeyIterator

	// tsm is the iterator for multiple TSM files combined
	tsm KeyIterator

	// size is the maximum value of a chunk to return
	size int

	// key is the current iteration series key
	key, walKey, tsmKey string

	// walBuf is the remaining values from the last wal Read call
	walBuf map[string][]Value

	// tsmBuf is the remaining values from the last tsm Read call
	tsmBuf map[string][]Value

	// values is the current set of values that will be returned by Read.
	values []Value

	// err is any error returned by an underlying iterator to be returned by Read
	err error

	minTime, maxTime time.Time
}

func NewMergeIterator(TSM KeyIterator, WAL KeyIterator, size int, minTime, maxTime time.Time) *MergeIterator {
	m := &MergeIterator{
		wal:     WAL,
		tsm:     TSM,
		walBuf:  map[string][]Value{},
		tsmBuf:  map[string][]Value{},
		size:    size,
		minTime: minTime,
		maxTime: maxTime,
	}
	return m
}
func (m *MergeIterator) Next() bool {
	for {
		// If we still have values in the current chunk, slice off up to size of them
		if m.size < len(m.values) {
			m.values = m.values[:m.size]
		} else {
			m.values = m.values[:0]
		}

		// We still have values.
		if len(m.values) > 0 {
			return true
		}

		// Prime the tsm buffers if possible
		if len(m.tsmBuf) == 0 && m.tsm != nil && m.tsm.Next() {
			k, v, err := m.tsm.Read()
			m.err = err
			m.tsmKey = k
			if len(v) > 0 {
				m.tsmBuf[k] = v
			}
		}

		// Prime the wal buffer if possible
		if len(m.walBuf) == 0 && m.wal != nil && m.wal.Next() {
			k, v, err := m.wal.Read()
			m.err = err
			m.walKey = k
			if len(v) > 0 {
				m.walBuf[k] = v
			}
		}

		m.key = m.currentKey()

		// No more keys, we're done.
		if m.key == "" {
			return false
		}

		// Otherwise, append the wal values to the tsm values and sort, dedup.  We want
		// the wal values to overwrite any tsm values so they are append second.
		var dedup bool
		if len(m.walBuf[m.key]) > 0 && len(m.tsmBuf[m.key]) > 0 {
			tsmVals := m.tsmBuf[m.key]
			walPoint := m.walBuf[m.key][0].Time()
			tsmPoint := tsmVals[len(tsmVals)-1].Time()

			if walPoint.Before(tsmPoint) || walPoint.Equal(tsmPoint) {
				dedup = true
			}
		}

		// Only have wal values? Use the wal buf
		if len(m.tsmBuf[m.key]) == 0 && len(m.walBuf[m.key]) > 0 {
			m.values = m.walBuf[m.key]
			m.walKey = ""
			// Only have tsm values? Use the wal buf
		} else if len(m.walBuf[m.key]) == 0 && len(m.tsmBuf[m.key]) > 0 {
			m.values = m.tsmBuf[m.key]
			m.tsmKey = ""
		} else {
			m.values = append(m.tsmBuf[m.key], m.walBuf[m.key]...)
			m.walKey = ""
			m.tsmKey = ""
		}

		if dedup {
			m.values = Values(m.values).Deduplicate()
		}

		// We need to find the index of the min and max values that are within
		// the minTime/maxTime filters.
		minIndex, maxIndex := 0, len(m.values)
		for i, v := range m.values {
			if v.Time().Before(m.minTime) {
				minIndex = i
			}
			if v.Time().Equal(m.minTime) {
				minIndex = i
			}

			if m.maxTime.Before(v.Time()) {
				maxIndex = i + 1
			}

			if m.maxTime.Equal(v.Time()) {
				maxIndex = i
			}
		}
		m.values = m.values[minIndex:maxIndex]

		// Remove the values from our buffer since they are all moved into the current chunk
		delete(m.tsmBuf, m.key)
		delete(m.walBuf, m.key)

		// If we have any values, return true.  Otherwise, continue looping until we exhaust
		// all the keys or finds some values.
		if len(m.values) > 0 {
			return true
		}
	}
}

func (m *MergeIterator) Read() (string, []Value, error) {
	if len(m.values) >= m.size {
		return m.key, m.values[:m.size], m.err
	}
	return m.key, m.values, m.err
}

func (m *MergeIterator) Close() error {
	m.walBuf = nil
	m.values = nil
	if m.wal != nil {
		m.wal.Close()
	}

	if m.tsm != nil {
		m.tsm.Close()
	}
	return nil
}

func (m *MergeIterator) currentKey() string {
	var key string
	// This is the smallest key across the wal and tsm maps
	if m.tsmKey == "" && m.walKey == "" {
		key = ""
	} else if m.tsmKey != "" && m.walKey == "" {
		key = m.tsmKey
	} else if m.walKey != "" && m.tsmKey == "" {
		key = m.walKey
	} else if m.walKey <= m.tsmKey {
		key = m.walKey
	} else {
		key = m.tsmKey
	}

	return key
}

// KeyIterator allows iteration over set of keys and values in sorted order.
type KeyIterator interface {
	Next() bool
	Read() (string, []Value, error)
	Close() error
}

// walKeyIterator allows WAL segments to be iterated over in sorted order.
type walKeyIterator struct {
	k      string
	Order  []string
	Series map[string]Values
}

func (k *walKeyIterator) Next() bool {
	if len(k.Order) == 0 {
		return false
	}
	k.k = k.Order[0]
	k.Order = k.Order[1:]
	return true
}

func (k *walKeyIterator) Read() (string, []Value, error) {
	return k.k, k.Series[k.k], nil
}

func (k *walKeyIterator) Close() error {
	k.Order = nil
	k.Series = nil
	return nil
}

func NewWALKeyIterator(readers ...*WALSegmentReader) (KeyIterator, error) {
	series := map[string]Values{}
	order := []string{}

	// Iterate over each reader in order.  Later readers will overwrite earlier ones if values
	// overlap.
	for _, r := range readers {
		for r.Next() {
			entry, err := r.Read()
			if err != nil {
				return nil, err
			}

			switch t := entry.(type) {
			case *WriteWALEntry:
				// Each point needs to be decomposed from a time with multiple fields, to a time, value tuple
				for k, v := range t.Values {
					// Just append each point as we see it.  Dedup and sorting happens later.
					series[k] = append(series[k], v...)
				}

			case *DeleteWALEntry:
				// Each key is a series, measurement + tagset string
				for _, k := range t.Keys {
					// seriesKey is specific to a field, measurment + tagset string + sep + field name
					for seriesKey := range series {
						//  If the delete series key matches the portion before the separator, we delete what we have
						if k == seriesKey {
							delete(series, seriesKey)
						}
					}
				}
			}
		}
	}

	// Need to create the order that we'll iterate over (sorted key), as well as
	// sort and dedup all the points for each key.
	for k, v := range series {
		order = append(order, k)
		series[k] = v.Deduplicate(true)
	}
	sort.Strings(order)

	return &walKeyIterator{
		Series: series,
		Order:  order,
	}, nil
}

// tsmKeyIterator implements the KeyIterator for set of TSMReaders.  Iteration produces
// keys in sorted order and the values between the keys sorted and deduped.  If any of
// the readers have associated tombstone entries, they are returned as part of iteration.
type tsmKeyIterator struct {
	// readers is the set of readers it produce a sorted key run with
	readers []*TSMReader

	// values is the temporary buffers for each key that is returned by a reader
	values map[string][]Value

	// pos is the current key postion within the corresponding readers slice.  A value of
	// pos[0] = 1, means the reader[0] is currently at key 1 in its ordered index.
	pos []int

	keys []string

	// err is any error we received while iterating values.
	err error

	// key is the current key lowest key across all readers that has not be fully exhausted
	// of values.
	key string
}

func NewTSMKeyIterator(readers ...*TSMReader) (KeyIterator, error) {
	return &tsmKeyIterator{
		readers: readers,
		values:  map[string][]Value{},
		pos:     make([]int, len(readers)),
		keys:    make([]string, len(readers)),
	}, nil
}

func (k *tsmKeyIterator) Next() bool {
	// If we have a key from the prior iteration, purge it and it's values from the
	// values map.  We're done with it.
	if k.key != "" {
		delete(k.values, k.key)
		for i, readerKey := range k.keys {
			if readerKey == k.key {
				k.keys[i] = ""
			}
		}
	}

	var skipSearch bool
	// For each iterator, group up all the values for their current key.
	for i, r := range k.readers {
		if k.keys[i] != "" {
			continue
		}

		// Grab the key for this reader
		key := r.Key(k.pos[i])
		k.keys[i] = key

		if key != "" && key <= k.key {
			k.key = key
			skipSearch = true
		}

		// Bump it to the next key
		k.pos[i]++

		// If it return a key, grab all the values for it.
		if key != "" {
			// Note: this could be made more efficient to just grab chunks of values instead of
			// all for the key.
			values, err := r.ReadAll(key)
			if err != nil {
				k.err = err
			}

			if len(values) > 0 {
				existing := k.values[key]

				if len(existing) == 0 {
					k.values[key] = values
				} else if values[0].Time().After(existing[len(existing)-1].Time()) {
					k.values[key] = append(existing, values...)
				} else if values[len(values)-1].Time().Before(existing[0].Time()) {
					k.values[key] = append(values, existing...)
				} else {
					k.values[key] = Values(append(existing, values...)).Deduplicate()
				}
			}
		}
	}

	if !skipSearch {
		// Determine our current key which is the smallest key in the values map
		k.key = k.currentKey()
	}

	return len(k.values) > 0
}

func (k *tsmKeyIterator) currentKey() string {
	var key string
	for searchKey := range k.values {
		if key == "" || searchKey <= key {
			key = searchKey
		}
	}

	return key
}

func (k *tsmKeyIterator) Read() (string, []Value, error) {
	if k.key == "" {
		return "", nil, k.err
	}

	return k.key, k.values[k.key], k.err
}

func (k *tsmKeyIterator) Close() error {
	k.values = nil
	k.pos = nil
	for _, r := range k.readers {
		if err := r.Close(); err != nil {
			return err
		}
	}
	return nil
}

type cacheKeyIterator struct {
	cache    *Cache
	min, max uint64

	k     string
	order []string
}

func NewCacheKeyIterator(cache *Cache, min, max uint64) KeyIterator {
	keys := cache.KeyRange(min, max)

	return &cacheKeyIterator{
		cache: cache,
		order: keys,
		min:   min,
		max:   max,
	}
}

func (c *cacheKeyIterator) Next() bool {
	if len(c.order) == 0 {
		return false
	}
	c.k = c.order[0]
	c.order = c.order[1:]
	return true
}

func (c *cacheKeyIterator) Read() (string, []Value, error) {
	return c.k, c.cache.ValuesRange(c.k, c.min, c.max), nil
}

func (c *cacheKeyIterator) Close() error {
	return nil
}

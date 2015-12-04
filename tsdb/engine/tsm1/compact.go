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
	Plan() []string
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
	FileStore interface {
		Stats() []FileStat
	}
}

// Plan returns a set of TSM files to rewrite
func (c *DefaultPlanner) Plan() []string {
	tsmStats := c.FileStore.Stats()

	var tsmPaths []string

	var hasDeletes bool
	for _, tsm := range tsmStats {
		if tsm.HasTombstone {
			tsmPaths = append(tsmPaths, tsm.Path)
			hasDeletes = true
			continue
		}

		if tsm.Size > rolloverTSMFileSize {
			continue
		}
		tsmPaths = append(tsmPaths, tsm.Path)
	}

	sort.Strings(tsmPaths)

	if !hasDeletes && len(tsmPaths) <= 1 {
		return nil
	}

	return tsmPaths
}

// Compactor merges multiple TSM files into new files or
// writes a Cache into 1 or more TSM files
type Compactor struct {
	Dir         string
	MaxFileSize int

	FileStore interface {
		NextGeneration() int
	}
}

// WriteSnapshot will write a Cache snapshot to a new TSM files.
func (c *Compactor) WriteSnapshot(cache *Cache) ([]string, error) {
	iter := NewCacheKeyIterator(cache)

	return c.writeNewFiles(iter)
}

// Compact will write multiple smaller TSM files into 1 or more larger files
func (c *Compactor) Compact(tsmFiles []string) ([]string, error) {
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

	if len(trs) == 0 {
		return nil, nil
	}

	tsm, err := NewTSMKeyIterator(trs...)
	if err != nil {
		return nil, err
	}

	return c.writeNewFiles(tsm)
}

// Clone will return a new compactor that can be used even if the engine is closed
func (c *Compactor) Clone() *Compactor {
	return &Compactor{
		Dir:         c.Dir,
		MaxFileSize: c.MaxFileSize,
		FileStore:   c.FileStore,
	}
}

// writeNewFiles will write from the iterator into new TSM files, rotating
// to a new file when we've reached the max TSM file size
func (c *Compactor) writeNewFiles(iter KeyIterator) ([]string, error) {
	// These are the new TSM files written
	var files []string

	for {
		currentID := c.FileStore.NextGeneration()

		// New TSM files are written to a temp file and renamed when fully completed.
		fileName := filepath.Join(c.Dir, fmt.Sprintf("%09d-%09d.%s.tmp", currentID, 1, "tsm1dev"))

		// Write as much as possible to this file
		err := c.write(fileName, iter)

		// We've hit the max file limit and there is more to write.  Create a new file
		// and continue.
		if err == errMaxFileExceeded {
			files = append(files, fileName)
			continue
		} else if err == ErrNoValues {
			// If the file only contained tombstoned entries, then it would be a 0 length
			// file that we can drop.
			if err := os.RemoveAll(fileName); err != nil {
				return nil, err
			}
			break
		}

		// We hit an error but didn't finish the compaction.  Remove the temp file and abort.
		if err != nil {
			return nil, err
		}

		files = append(files, fileName)
		break
	}

	return files, nil
}

func (c *Compactor) write(path string, iter KeyIterator) error {
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
	defer w.Close()

	for iter.Next() {
		// Each call to read returns the next sorted key (or the prior one if there are
		// more values to write).  The size of values will be less than or equal to our
		// chunk size (1000)
		key, values, err := iter.Read()
		if err != nil {
			return err
		}

		// Write the key and value
		if err := w.Write(key, values); err != nil {
			return err
		}

		// If we have a max file size configured and we're over it, close out the file
		// and return the error.
		if w.Size() > c.MaxFileSize {
			if err := w.WriteIndex(); err != nil {
				return err
			}

			return errMaxFileExceeded
		}
	}

	// We're all done.  Close out the file.
	if err := w.WriteIndex(); err != nil {
		return err
	}

	return nil
}

// KeyIterator allows iteration over set of keys and values in sorted order.
type KeyIterator interface {
	Next() bool
	Read() (string, []Value, error)
	Close() error
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
	cache *Cache

	k     string
	order []string
}

func NewCacheKeyIterator(cache *Cache) KeyIterator {
	keys := cache.Keys()

	return &cacheKeyIterator{
		cache: cache,
		order: keys,
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
	return c.k, c.cache.values(c.k), nil
}

func (c *cacheKeyIterator) Close() error {
	return nil
}

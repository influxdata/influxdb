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

	"github.com/influxdb/influxdb/tsdb"
)

const maxTSMFileSize = uint32(2048 * 1024 * 1024) // 2GB

const (
	CompactionTempExtension = "tmp"
	TSMFileExtension        = "tsm"
)

var errMaxFileExceeded = fmt.Errorf("max file exceeded")

var (
	MaxTime = time.Unix(0, math.MaxInt64)
	MinTime = time.Unix(0, 0)
)

// compactionSteps are the sizes of files to roll up into before combining.
var compactionSteps = []uint32{
	32 * 1024 * 1024,
	128 * 1024 * 1024,
	512 * 1024 * 1024,
	2048 * 1024 * 1024,
}

// compactionLevel takes a size and returns the index of the compaction step
// that the size falls into
func compactionLevel(size uint64) int {
	for i, step := range compactionSteps {
		if size < uint64(step) {
			return i
		}
	}

	return len(compactionSteps)
}

// CompactionPlanner determines what TSM files and WAL segments to include in a
// given compaction run.
type CompactionPlanner interface {
	Plan(lastWrite time.Time) []string
}

// DefaultPlanner implements CompactionPlanner using a strategy to roll up
// multiple generations of TSM files into larger files in stages.  It attempts
// to minimize the number of TSM files on disk while rolling up a bounder number
// of files.
type DefaultPlanner struct {
	FileStore interface {
		Stats() []FileStat
		LastModified() time.Time
	}

	MinCompactionFileCount int

	// CompactFullWriteColdDuration specifies the length of time after
	// which if no writes have been committed to the WAL, the engine will
	// do a full compaction of the TSM files in this shard. This duration
	// should always be greater than the CacheFlushWriteColdDuraion
	CompactFullWriteColdDuration time.Duration

	// lastPlanCompactedFull will be true if the last time
	// Plan was called, all files were over the max size
	// or there was only one file
	lastPlanCompactedFull bool

	// lastPlanCheck is the last time Plan was called
	lastPlanCheck time.Time
}

// tsmGeneration represents the TSM files within a generation.
// 000001-01.tsm, 000001-02.tsm would be in the same generation
// 000001 each with different sequence numbers.
type tsmGeneration struct {
	id    int
	files []FileStat
}

// size returns the total size of the generation
func (t *tsmGeneration) size() uint64 {
	var n uint64
	for _, f := range t.files {
		n += uint64(f.Size)
	}
	return n
}

func (t *tsmGeneration) lastModified() time.Time {
	var max time.Time
	for _, f := range t.files {
		if f.LastModified.After(max) {
			max = f.LastModified
		}
	}
	return max
}

// count return then number of files in the generation
func (t *tsmGeneration) count() int {
	return len(t.files)
}

// Plan returns a set of TSM files to rewrite
func (c *DefaultPlanner) Plan(lastWrite time.Time) []string {
	// first check if we should be doing a full compaction because nothing has been written in a long time
	if !c.lastPlanCompactedFull && c.CompactFullWriteColdDuration > 0 && time.Now().Sub(lastWrite) > c.CompactFullWriteColdDuration {
		var tsmFiles []string
		for _, group := range c.findGenerations() {
			// If the generation size is less the max size
			if group.size() < uint64(maxTSMFileSize) {
				for _, f := range group.files {
					tsmFiles = append(tsmFiles, f.Path)
				}
			}
		}
		sort.Strings(tsmFiles)

		c.lastPlanCompactedFull = true

		if len(tsmFiles) <= 1 {
			return nil
		}

		return tsmFiles
	}

	// don't plan if nothing has changed in the filestore
	if c.lastPlanCheck.After(c.FileStore.LastModified()) {
		return nil
	}

	// Determine the generations from all files on disk.  We need to treat
	// a generation conceptually as a single file even though it may be
	// split across several files in sequence.
	generations := c.findGenerations()

	c.lastPlanCheck = time.Now()

	if len(generations) <= 1 {
		return nil
	}

	// Loop through the generations (they're in decending order) and find the newest generations
	// that have the min compaction file count in the same compaction step size
	startIndex := 0
	endIndex := len(generations)
	currentLevel := compactionLevel(generations[0].size())
	count := 0
	for i, g := range generations {
		level := compactionLevel(g.size())
		count += 1

		if level != currentLevel {
			if count >= c.MinCompactionFileCount {
				endIndex = i
				break
			}
			currentLevel = level
			startIndex = i
			count = 0
			continue
		}
	}

	if currentLevel == len(compactionSteps) {
		return nil
	}

	generations = generations[startIndex:endIndex]

	// if we don't have enough generations to compact, return
	if len(generations) < c.MinCompactionFileCount {
		return nil
	}

	// All the files to be compacted must be compacted in order
	var tsmFiles []string
	for _, group := range generations {
		for _, f := range group.files {
			tsmFiles = append(tsmFiles, f.Path)
		}
	}
	sort.Strings(tsmFiles)

	// Only one, we can't improve on that so nothing to do
	if len(tsmFiles) == 1 {
		return nil
	}

	c.lastPlanCompactedFull = false

	return tsmFiles
}

// findGenerations groups all the TSM files by they generation based
// on their filename then returns the generations in descending order (newest first)
func (c *DefaultPlanner) findGenerations() tsmGenerations {
	generations := map[int]*tsmGeneration{}

	tsmStats := c.FileStore.Stats()
	for _, f := range tsmStats {
		gen, _, _ := ParseTSMFileName(f.Path)

		group := generations[gen]
		if group == nil {
			group = &tsmGeneration{
				id: gen,
			}
			generations[gen] = group
		}
		group.files = append(group.files, f)
	}

	orderedGenerations := make(tsmGenerations, 0, len(generations))
	for _, g := range generations {
		orderedGenerations = append(orderedGenerations, g)
	}
	sort.Sort(sort.Reverse(orderedGenerations))
	return orderedGenerations
}

// Compactor merges multiple TSM files into new files or
// writes a Cache into 1 or more TSM files
type Compactor struct {
	Dir    string
	Cancel chan struct{}

	FileStore interface {
		NextGeneration() int
	}
}

// WriteSnapshot will write a Cache snapshot to a new TSM files.
func (c *Compactor) WriteSnapshot(cache *Cache) ([]string, error) {
	iter := NewCacheKeyIterator(cache, tsdb.DefaultMaxPointsPerBlock)
	return c.writeNewFiles(c.FileStore.NextGeneration(), 1, iter)
}

// Compact will write multiple smaller TSM files into 1 or more larger files
func (c *Compactor) Compact(tsmFiles []string) ([]string, error) {
	// The new compacted files need to added to the max generation in the
	// set.  We need to find that max generation as well as the max sequence
	// number to ensure we write to the next unique location.
	var maxGeneration, maxSequence int
	for _, f := range tsmFiles {
		gen, seq, err := ParseTSMFileName(f)
		if err != nil {
			return nil, err
		}

		if gen > maxGeneration {
			maxGeneration = gen
			maxSequence = seq
		}

		if gen == maxGeneration && seq > maxSequence {
			maxSequence = seq
		}
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
		defer tr.Close()
		trs = append(trs, tr)
	}

	if len(trs) == 0 {
		return nil, nil
	}

	tsm, err := NewTSMKeyIterator(tsdb.DefaultMaxPointsPerBlock, trs...)
	if err != nil {
		return nil, err
	}

	return c.writeNewFiles(maxGeneration, maxSequence, tsm)
}

// Clone will return a new compactor that can be used even if the engine is closed
func (c *Compactor) Clone() *Compactor {
	return &Compactor{
		Dir:       c.Dir,
		FileStore: c.FileStore,
		Cancel:    c.Cancel,
	}
}

// writeNewFiles will write from the iterator into new TSM files, rotating
// to a new file when we've reached the max TSM file size
func (c *Compactor) writeNewFiles(generation, sequence int, iter KeyIterator) ([]string, error) {
	// These are the new TSM files written
	var files []string

	for {
		sequence++
		// New TSM files are written to a temp file and renamed when fully completed.
		fileName := filepath.Join(c.Dir, fmt.Sprintf("%09d-%09d.%s.tmp", generation, sequence, TSMFileExtension))

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
			if err := os.Remove(fileName); err != nil {
				return nil, err
			}
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
		select {
		case <-c.Cancel:
			return fmt.Errorf("compaction aborted")
		default:
		}

		// Each call to read returns the next sorted key (or the prior one if there are
		// more values to write).  The size of values will be less than or equal to our
		// chunk size (1000)
		key, minTime, maxTime, block, err := iter.Read()
		if err != nil {
			return err
		}

		// Write the key and value
		if err := w.WriteBlock(key, minTime, maxTime, block); err != nil {
			return err
		}

		// If we have a max file size configured and we're over it, close out the file
		// and return the error.
		if w.Size() > maxTSMFileSize {
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
	Read() (string, time.Time, time.Time, []byte, error)
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

	size int
	// key is the current key lowest key across all readers that has not be fully exhausted
	// of values.
	key string

	iterators []*BlockIterator
	blocks    blocks

	buf blocks
}

type block struct {
	key              string
	minTime, maxTime time.Time
	b                []byte
}

type blocks []*block

func (a blocks) Len() int { return len(a) }

func (a blocks) Less(i, j int) bool {
	if a[i].key == a[j].key {
		return a[i].minTime.Before(a[j].minTime)
	}
	return a[i].key < a[j].key
}

func (a blocks) Swap(i, j int) { a[i], a[j] = a[j], a[i] }

func NewTSMKeyIterator(size int, readers ...*TSMReader) (KeyIterator, error) {
	var iter []*BlockIterator
	for _, r := range readers {
		iter = append(iter, r.BlockIterator())
	}

	return &tsmKeyIterator{
		readers:   readers,
		values:    map[string][]Value{},
		pos:       make([]int, len(readers)),
		keys:      make([]string, len(readers)),
		size:      size,
		iterators: iter,
		buf:       make([]*block, len(iter)),
	}, nil
}

func (k *tsmKeyIterator) Next() bool {
	// If we still have blocks from the last read, slice off the current one
	// and return
	if len(k.blocks) > 0 {
		k.blocks = k.blocks[1:]
		if len(k.blocks) > 0 {
			return true
		}
	}

	// Read the next block from each TSM iterator
	for i, v := range k.buf {
		if v == nil {
			iter := k.iterators[i]
			if iter.Next() {
				key, minTime, maxTime, b, err := iter.Read()
				if err != nil {
					k.err = err
				}

				k.buf[i] = &block{
					minTime: minTime,
					maxTime: maxTime,
					key:     key,
					b:       b,
				}
			}
		}
	}

	// Each reader could have a different key that it's currently at, need to find
	// the next smallest one to keep the sort ordering.
	var minKey string
	for _, b := range k.buf {
		// block could be nil if the iterator has been exhausted for that file
		if b == nil {
			continue
		}
		if minKey == "" || b.key < minKey {
			minKey = b.key
		}
	}

	// Now we need to find all blocks that match the min key so we can combine and dedup
	// the blocks if necessary
	for i, b := range k.buf {
		if b == nil {
			continue
		}
		if b.key == minKey {
			k.blocks = append(k.blocks, b)
			k.buf[i] = nil
		}
	}

	// If we have more than one block, we many need to dedup
	if len(k.blocks) > 1 {
		var decoded Values
		var dedup bool

		// Quickly scan each block to see if any overlap with the first block
		for i := 1; i < len(k.blocks); i++ {
			if k.blocks[i].minTime.Equal(k.blocks[0].maxTime) || k.blocks[i].minTime.Before(k.blocks[0].maxTime) {
				dedup = true
				break
			}
		}

		if dedup {
			// We have some overlapping blocks so decode all, append in order and then dedup
			for i := 0; i < len(k.blocks); i++ {
				v, err := DecodeBlock(k.blocks[i].b, nil)
				if err != nil {
					k.err = err
					return true
				}
				decoded = append(decoded, v...)
			}
			decoded = decoded.Deduplicate()

			// Since we combined multiple blocks, we could have more values than we should put into
			// a single block.  We need to chunk them up into groups and re-encode them.
			var chunked blocks
			for len(decoded) > k.size {
				cb, err := Values(decoded[:k.size]).Encode(nil)
				if err != nil {
					k.err = err
					return true
				}

				chunked = append(chunked, &block{
					minTime: decoded[0].Time(),
					maxTime: decoded[k.size].Time(),
					key:     k.blocks[0].key,
					b:       cb,
				})
				decoded = decoded[k.size:]
			}

			// Re-encode the remaining values into the last block
			if len(decoded) > 0 {
				cb, err := Values(decoded).Encode(nil)
				if err != nil {
					k.err = err
					return true
				}

				chunked = append(chunked, &block{
					minTime: decoded[0].Time(),
					maxTime: decoded[len(decoded)-1].Time(),
					key:     k.blocks[0].key,
					b:       cb,
				})

			}
			k.blocks = chunked
		}
	}

	return len(k.blocks) > 0
}

func (k *tsmKeyIterator) Read() (string, time.Time, time.Time, []byte, error) {
	if len(k.blocks) == 0 {
		return "", time.Unix(0, 0), time.Unix(0, 0), nil, k.err
	}

	block := k.blocks[0]
	return block.key, block.minTime, block.maxTime, block.b, k.err
}

func (k *tsmKeyIterator) Close() error {
	k.values = nil
	k.pos = nil
	k.iterators = nil
	for _, r := range k.readers {
		if err := r.Close(); err != nil {
			return err
		}
	}
	return nil
}

type cacheKeyIterator struct {
	cache *Cache
	size  int

	k                string
	order            []string
	values           []Value
	block            []byte
	minTime, maxTime time.Time
	err              error
}

func NewCacheKeyIterator(cache *Cache, size int) KeyIterator {
	keys := cache.Keys()

	return &cacheKeyIterator{
		size:  size,
		cache: cache,
		order: keys,
	}
}

func (c *cacheKeyIterator) Next() bool {
	if len(c.values) > c.size {
		c.values = c.values[c.size:]
		return true
	}

	if len(c.order) == 0 {
		return false
	}
	c.k = c.order[0]
	c.order = c.order[1:]
	c.values = c.cache.values(c.k)
	return true
}

func (c *cacheKeyIterator) Read() (string, time.Time, time.Time, []byte, error) {
	minTime, maxTime := c.values[0].Time(), c.values[len(c.values)-1].Time()
	var b []byte
	var err error
	if len(c.values) > c.size {
		maxTime = c.values[c.size].Time()
		b, err = Values(c.values[:c.size]).Encode(nil)
	} else {
		b, err = Values(c.values).Encode(nil)
	}

	return c.k, minTime, maxTime, b, err
}

func (c *cacheKeyIterator) Close() error {
	return nil
}

type tsmGenerations []*tsmGeneration

func (a tsmGenerations) Len() int           { return len(a) }
func (a tsmGenerations) Less(i, j int) bool { return a[i].id < a[j].id }
func (a tsmGenerations) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }

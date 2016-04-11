package tsm1

import (
	"expvar"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/tsdb"
)

type TSMFile interface {
	// Path returns the underlying file path for the TSMFile.  If the file
	// has not be written or loaded from disk, the zero value is returne.
	Path() string

	// Read returns all the values in the block where time t resides
	Read(key string, t int64) ([]Value, error)

	// ReadAt returns all the values in the block identified by entry.
	ReadAt(entry *IndexEntry, values []Value) ([]Value, error)
	ReadFloatBlockAt(entry *IndexEntry, values []FloatValue) ([]FloatValue, error)
	ReadIntegerBlockAt(entry *IndexEntry, values []IntegerValue) ([]IntegerValue, error)
	ReadStringBlockAt(entry *IndexEntry, values []StringValue) ([]StringValue, error)
	ReadBooleanBlockAt(entry *IndexEntry, values []BooleanValue) ([]BooleanValue, error)

	// Entries returns the index entries for all blocks for the given key.
	Entries(key string) []IndexEntry
	ReadEntries(key string, entries *[]IndexEntry)

	// Returns true if the TSMFile may contain a value with the specified
	// key and time
	ContainsValue(key string, t int64) bool

	// Contains returns true if the file contains any values for the given
	// key.
	Contains(key string) bool

	// TimeRange returns the min and max time across all keys in the file.
	TimeRange() (int64, int64)

	// KeyRange returns the min and max keys in the file.
	KeyRange() (string, string)

	// Keys returns all keys contained in the file.
	Keys() []string

	// KeyCount returns the number of distict keys in the file.
	KeyCount() int

	// KeyAt returns the key located at index position idx
	KeyAt(idx int) (string, byte)

	// Type returns the block type of the values stored for the key.  Returns one of
	// BlockFloat64, BlockInt64, BlockBoolean, BlockString.  If key does not exist,
	// an error is returned.
	Type(key string) (byte, error)

	// Delete removes the keys from the set of keys available in this file.
	Delete(keys []string) error

	// HasTombstones returns true if file contains values that have been deleted.
	HasTombstones() bool

	// TombstoneFiles returns the tombstone filestats if there are any tombstones
	// written for this file.
	TombstoneFiles() []FileStat

	// Close the underlying file resources
	Close() error

	// Size returns the size of the file on disk in bytes.
	Size() uint32

	// Remove deletes the file from the filesystem
	Remove() error

	// Stats returns summary information about the TSM file.
	Stats() FileStat

	// BlockIterator returns an iterator pointing to the first block in the file and
	// allows sequential iteration to each every block.
	BlockIterator() *BlockIterator
}

// Statistics gathered by the FileStore.
const (
	statFileStoreBytes = "diskBytes"
)

type FileStore struct {
	mu           sync.RWMutex
	lastModified time.Time

	currentGeneration int
	dir               string

	files []TSMFile

	Logger       *log.Logger
	traceLogging bool

	statMap *expvar.Map
}

type FileStat struct {
	Path             string
	HasTombstone     bool
	Size             uint32
	LastModified     int64
	MinTime, MaxTime int64
	MinKey, MaxKey   string
}

func (f FileStat) OverlapsTimeRange(min, max int64) bool {
	return f.MinTime <= max && f.MaxTime >= min
}

func (f FileStat) OverlapsKeyRange(min, max string) bool {
	return min != "" && max != "" && f.MinKey <= max && f.MaxKey >= min
}

func (f FileStat) ContainsKey(key string) bool {
	return f.MinKey >= key || key <= f.MaxKey
}

func NewFileStore(dir string) *FileStore {
	db, rp := tsdb.DecodeStorePath(dir)
	return &FileStore{
		dir:          dir,
		lastModified: time.Now(),
		Logger:       log.New(os.Stderr, "[filestore] ", log.LstdFlags),
		statMap: influxdb.NewStatistics(
			"tsm1_filestore:"+dir,
			"tsm1_filestore",
			map[string]string{"path": dir, "database": db, "retentionPolicy": rp},
		),
	}
}

// Returns the number of TSM files currently loaded
func (f *FileStore) Count() int {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return len(f.files)
}

// Files returns TSM files currently loaded.
func (f *FileStore) Files() []TSMFile {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.files
}

// CurrentGeneration returns the current generation of the TSM files
func (f *FileStore) CurrentGeneration() int {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.currentGeneration
}

// NextGeneration returns the max file ID + 1
func (f *FileStore) NextGeneration() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.currentGeneration++
	return f.currentGeneration
}

func (f *FileStore) Add(files ...TSMFile) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, file := range files {
		f.statMap.Add(statFileStoreBytes, int64(file.Size()))
	}
	f.files = append(f.files, files...)
	sort.Sort(tsmReaders(f.files))
}

// Remove removes the files with matching paths from the set of active files.  It does
// not remove the paths from disk.
func (f *FileStore) Remove(paths ...string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var active []TSMFile
	for _, file := range f.files {
		keep := true
		for _, remove := range paths {
			if remove == file.Path() {
				keep = false
				break
			}
		}

		if keep {
			active = append(active, file)
		} else {
			// Removing the file, remove the file size from the total file store bytes
			f.statMap.Add(statFileStoreBytes, -int64(file.Size()))
		}
	}
	f.files = active
	sort.Sort(tsmReaders(f.files))
}

// WalkKeys calls fn for every key in every TSM file known to the FileStore.  If the key
// exists in multiple files, it will be invoked for each file.
func (f *FileStore) WalkKeys(fn func(key string, typ byte) error) error {
	f.mu.RLock()
	defer f.mu.RUnlock()

	for _, f := range f.files {
		for i := 0; i < f.KeyCount(); i++ {
			key, typ := f.KeyAt(i)
			if err := fn(key, typ); err != nil {
				return err
			}
		}
	}
	return nil
}

// Keys returns all keys and types for all files
func (f *FileStore) Keys() map[string]byte {
	f.mu.RLock()
	defer f.mu.RUnlock()

	uniqueKeys := map[string]byte{}
	for _, f := range f.files {
		for i := 0; i < f.KeyCount(); i++ {
			key, typ := f.KeyAt(i)
			uniqueKeys[key] = typ
		}
	}

	return uniqueKeys
}

func (f *FileStore) Type(key string) (byte, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	for _, f := range f.files {
		if f.Contains(key) {
			return f.Type(key)
		}
	}
	return 0, fmt.Errorf("unknown type for %v", key)
}

func (f *FileStore) Delete(keys []string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.lastModified = time.Now()

	for _, file := range f.files {
		if err := file.Delete(keys); err != nil {
			return err
		}
	}
	return nil
}

func (f *FileStore) Open() error {
	f.mu.Lock()
	defer f.mu.Unlock()

	// Not loading files from disk so nothing to do
	if f.dir == "" {
		return nil
	}

	files, err := filepath.Glob(filepath.Join(f.dir, fmt.Sprintf("*.%s", TSMFileExtension)))
	if err != nil {
		return err
	}

	// struct to hold the result of opening each reader in a goroutine
	type res struct {
		r   *TSMReader
		err error
	}

	readerC := make(chan *res)
	for i, fn := range files {
		// Keep track of the latest ID
		generation, _, err := ParseTSMFileName(fn)
		if err != nil {
			return err
		}

		if generation >= f.currentGeneration {
			f.currentGeneration = generation + 1
		}

		file, err := os.OpenFile(fn, os.O_RDONLY, 0666)
		if err != nil {
			return fmt.Errorf("error opening file %s: %v", fn, err)
		}

		// Accumulate file store size stat
		if fi, err := file.Stat(); err == nil {
			f.statMap.Add(statFileStoreBytes, fi.Size())
		}

		go func(idx int, file *os.File) {
			start := time.Now()
			df, err := NewTSMReaderWithOptions(TSMReaderOptions{
				MMAPFile: file,
			})
			if f.traceLogging {
				f.Logger.Printf("%s (#%d) opened in %v", file.Name(), idx, time.Now().Sub(start))
			}

			if err != nil {
				readerC <- &res{r: df, err: fmt.Errorf("error opening memory map for file %s: %v", file.Name(), err)}
				return
			}
			readerC <- &res{r: df}
		}(i, file)
	}

	for range files {
		res := <-readerC
		if res.err != nil {

			return res.err
		}
		f.files = append(f.files, res.r)
	}
	close(readerC)

	sort.Sort(tsmReaders(f.files))
	return nil
}

func (f *FileStore) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()

	for _, f := range f.files {
		f.Close()
	}

	f.files = nil
	return nil
}

func (f *FileStore) Read(key string, t int64) ([]Value, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	for _, f := range f.files {
		// Can this file possibly contain this key and timestamp?
		if !f.Contains(key) {
			continue
		}

		// May have the key and time we are looking for so try to find
		v, err := f.Read(key, t)
		if err != nil {
			return nil, err
		}

		if len(v) > 0 {
			return v, nil
		}
	}
	return nil, nil
}

func (f *FileStore) KeyCursor(key string, t int64, ascending bool) *KeyCursor {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return newKeyCursor(f, key, t, ascending)
}

func (f *FileStore) Stats() []FileStat {
	f.mu.RLock()
	defer f.mu.RUnlock()
	stats := make([]FileStat, len(f.files))
	for i, fd := range f.files {
		stats[i] = fd.Stats()
	}

	return stats
}

func (f *FileStore) Replace(oldFiles, newFiles []string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.lastModified = time.Now()

	// Copy the current set of active files while we rename
	// and load the new files.  We copy the pointers here to minimize
	// the time that locks are held as well as to ensure that the replacement
	// is atomic.©
	var updated []TSMFile
	for _, t := range f.files {
		updated = append(updated, t)
	}

	// Rename all the new files to make them live on restart
	for _, file := range newFiles {
		var newName = file
		if strings.HasSuffix(file, ".tmp") {
			// The new TSM files have a tmp extension.  First rename them.
			newName = file[:len(file)-4]
			if err := os.Rename(file, newName); err != nil {
				return err
			}
		}

		fd, err := os.Open(newName)
		if err != nil {
			return err
		}

		tsm, err := NewTSMReaderWithOptions(TSMReaderOptions{
			MMAPFile: fd,
		})
		if err != nil {
			return err
		}
		updated = append(updated, tsm)
	}

	// We need to prune our set of active files now
	var active []TSMFile
	for _, file := range updated {
		keep := true
		for _, remove := range oldFiles {
			if remove == file.Path() {
				keep = false
				if err := file.Close(); err != nil {
					return err
				}

				if err := file.Remove(); err != nil {
					return err
				}
				break
			}
		}

		if keep {
			active = append(active, file)
		}
	}

	if err := syncDir(f.dir); err != nil {
		return err
	}

	f.files = active
	sort.Sort(tsmReaders(f.files))

	// Recalculate the disk size stat
	var totalSize int64
	for _, file := range f.files {
		totalSize += int64(file.Size())
	}
	sizeStat := new(expvar.Int)
	sizeStat.Set(totalSize)
	f.statMap.Set(statFileStoreBytes, sizeStat)

	return nil
}

// LastModified returns the last time the file store was updated with new
// TSM files or a delete
func (f *FileStore) LastModified() time.Time {
	f.mu.RLock()
	defer f.mu.RUnlock()

	return f.lastModified
}

// BlockCount returns number of values stored in the block at location idx
// in the file at path.  If path does not match any file in the store, 0 is
// returned.  If idx is out of range for the number of blocks in the file,
// 0 is returned.
func (f *FileStore) BlockCount(path string, idx int) int {
	f.mu.RLock()
	defer f.mu.RUnlock()

	if idx < 0 {
		return 0
	}

	for _, fd := range f.files {
		if fd.Path() == path {
			iter := fd.BlockIterator()
			for i := 0; i < idx; i++ {
				if !iter.Next() {
					return 0
				}
			}
			_, _, _, block, _ := iter.Read()
			return BlockCount(block)
		}
	}
	return 0
}

// locations returns the files and index blocks for a key and time.  ascending indicates
// whether the key will be scan in ascending time order or descenging time order.
// This function assumes the read-lock has been taken.
func (f *FileStore) locations(key string, t int64, ascending bool) []location {
	var locations []location

	filesSnapshot := make([]TSMFile, len(f.files))
	for i := range f.files {
		filesSnapshot[i] = f.files[i]
	}

	var entries []IndexEntry
	for _, fd := range filesSnapshot {
		minTime, maxTime := fd.TimeRange()

		// If we ascending and the max time of the file is before where we want to start
		// skip it.
		if ascending && maxTime < t {
			continue
			// If we are descending and the min time fo the file is after where we want to start,
			// then skip it.
		} else if !ascending && minTime > t {
			continue
		}

		// This file could potential contain points we are looking for so find the blocks for
		// the given key.
		fd.ReadEntries(key, &entries)
		for _, ie := range entries {
			// If we ascending and the max time of a block is before where we are looking, skip
			// it since the data is out of our range
			if ascending && ie.MaxTime < t {
				continue
				// If we descending and the min time of a block is after where we are looking, skip
				// it since the data is out of our range
			} else if !ascending && ie.MinTime > t {
				continue
			}

			// Otherwise, add this file and block location
			locations = append(locations, location{
				r:     fd,
				entry: ie,
			})
		}
	}
	return locations
}

// ParseTSMFileName parses the generation and sequence from a TSM file name.
func ParseTSMFileName(name string) (int, int, error) {
	base := filepath.Base(name)
	idx := strings.Index(base, ".")
	if idx == -1 {
		return 0, 0, fmt.Errorf("file %s is named incorrectly", name)
	}

	id := base[:idx]

	parts := strings.Split(id, "-")
	if len(parts) != 2 {
		return 0, 0, fmt.Errorf("file %s is named incorrectly", name)
	}

	generation, err := strconv.ParseUint(parts[0], 10, 32)
	sequence, err := strconv.ParseUint(parts[1], 10, 32)

	return int(generation), int(sequence), err
}

type KeyCursor struct {
	key string
	fs  *FileStore

	// seeks is all the file locations that we need to return during iteration.
	seeks []location

	// current is the set of blocks possibly containing the next set of points.
	// Normally this is just one entry, but there may be multiple if points have
	// been overwritten.
	current []location
	buf     []Value

	// pos is the index within seeks.  Based on ascending, it will increment or
	// decrement through the size of seeks slice.
	pos       int
	ascending bool

	// duplicates is a hint that there are overlapping blocks for this key in
	// multiple files (e.g. points have been overwritten but not fully compacted)
	// If this is true, we need to scan the duplicate blocks and dedup the points
	// as query time until they are compacted.
	duplicates bool
}

type location struct {
	r     TSMFile
	entry IndexEntry

	// Has this location been read before
	read bool
}

// newKeyCursor returns a new instance of KeyCursor.
// This function assumes the read-lock has been taken.
func newKeyCursor(fs *FileStore, key string, t int64, ascending bool) *KeyCursor {
	c := &KeyCursor{
		key:       key,
		fs:        fs,
		seeks:     fs.locations(key, t, ascending),
		ascending: ascending,
	}
	c.duplicates = c.hasOverlappingBlocks()
	c.seek(t)
	return c
}

// Close removes all references on the cursor.
func (c *KeyCursor) Close() {
	c.buf = nil
	c.seeks = nil
	c.fs = nil
	c.current = nil
}

// hasOverlappingBlocks returns true if blocks have overlapping time ranges.
// This result is computed once and stored as the "duplicates" field.
func (c *KeyCursor) hasOverlappingBlocks() bool {
	if len(c.seeks) == 0 {
		return false
	}

	for i := 1; i < len(c.seeks); i++ {
		prev := c.seeks[i-1]
		cur := c.seeks[i]
		if prev.entry.MaxTime >= cur.entry.MinTime {
			return true
		}
	}
	return false
}

// seek positions the cursor at the given time.
func (c *KeyCursor) seek(t int64) {
	if len(c.seeks) == 0 {
		return
	}
	c.current = nil

	if c.ascending {
		c.seekAscending(t)
	} else {
		c.seekDescending(t)
	}
}

func (c *KeyCursor) seekAscending(t int64) {
	for i, e := range c.seeks {
		if t < e.entry.MinTime || e.entry.Contains(t) {
			// Record the position of the first block matching our seek time
			if len(c.current) == 0 {
				c.pos = i
			}

			c.current = append(c.current, e)

			// Exit if we don't have duplicates.
			// Otherwise, keep looking for additional blocks containing this point.
			if !c.duplicates {
				return
			}
		}
	}
}

func (c *KeyCursor) seekDescending(t int64) {
	for i := len(c.seeks) - 1; i >= 0; i-- {
		e := c.seeks[i]
		if t > e.entry.MaxTime || e.entry.Contains(t) {
			// Record the position of the first block matching our seek time
			if len(c.current) == 0 {
				c.pos = i
			}
			c.current = append(c.current, e)

			// Exit if we don't have duplicates.
			// Otherwise, keep looking for additional blocks containing this point.
			if !c.duplicates {
				return
			}
		}
	}
}

// Next moves the cursor to the next position.
// Data should be read by the ReadBlock functions.
func (c *KeyCursor) Next() {
	c.current = c.current[:0]
	if c.ascending {
		c.nextAscending()
	} else {
		c.nextDescending()
	}
}

func (c *KeyCursor) nextAscending() {
	for {
		c.pos++
		if c.pos >= len(c.seeks) {
			return
		} else if !c.seeks[c.pos].read {
			break
		}
	}

	// Append the first matching block
	c.current = []location{c.seeks[c.pos]}

	// We're done if there are no overlapping blocks.
	if !c.duplicates {
		return
	}

	// If we have ovelapping blocks, append all their values so we can dedup
	first := c.seeks[c.pos]
	for i := c.pos + 1; i < len(c.seeks); i++ {
		if c.seeks[i].read {
			continue
		}

		if c.seeks[i].entry.MinTime <= first.entry.MaxTime {
			c.current = append(c.current, c.seeks[i])
		}
	}
}

func (c *KeyCursor) nextDescending() {
	for {
		c.pos--
		if c.pos < 0 {
			return
		} else if !c.seeks[c.pos].read {
			break
		}
	}

	// Append the first matching block
	c.current = []location{c.seeks[c.pos]}

	// We're done if there are no overlapping blocks.
	if !c.duplicates {
		return
	}

	// If we have ovelapping blocks, append all their values so we can dedup
	first := c.seeks[c.pos]
	for i := c.pos; i >= 0; i-- {
		if c.seeks[i].read {
			continue
		}
		if c.seeks[i].entry.MaxTime >= first.entry.MinTime {
			c.current = append(c.current, c.seeks[i])
		}
	}
}

// ReadFloatBlock reads the next block as a set of float values.
func (c *KeyCursor) ReadFloatBlock(buf []FloatValue) ([]FloatValue, error) {
	// No matching blocks to decode
	if len(c.current) == 0 {
		return nil, nil
	}

	// First block is the oldest block containing the points we're search for.
	first := c.current[0]
	values, err := first.r.ReadFloatBlockAt(&first.entry, buf[:0])
	first.read = true

	// Only one block with this key and time range so return it
	if len(c.current) == 1 {
		return values, err
	}

	// Otherwise, search the remaining blocks that overlap and append their values so we can
	// dedup them.
	for i := 1; i < len(c.current); i++ {
		cur := c.current[i]
		if c.ascending && !cur.read {
			cur.read = true
			c.pos++
			v, err := cur.r.ReadFloatBlockAt(&cur.entry, nil)
			if err != nil {
				return nil, err
			}
			values = append(values, v...)
		} else if !c.ascending && !cur.read {
			cur.read = true
			c.pos--

			v, err := cur.r.ReadFloatBlockAt(&cur.entry, nil)
			if err != nil {
				return nil, err
			}
			values = append(v, values...)
		}
	}

	return FloatValues(values).Deduplicate(), err
}

// ReadIntegerBlock reads the next block as a set of integer values.
func (c *KeyCursor) ReadIntegerBlock(buf []IntegerValue) ([]IntegerValue, error) {
	// No matching blocks to decode
	if len(c.current) == 0 {
		return nil, nil
	}

	// First block is the oldest block containing the points we're search for.
	first := c.current[0]
	values, err := first.r.ReadIntegerBlockAt(&first.entry, buf[:0])
	first.read = true

	// Only one block with this key and time range so return it
	if len(c.current) == 1 {
		return values, err
	}

	// Otherwise, search the remaining blocks that overlap and append their values so we can
	// dedup them.
	for i := 1; i < len(c.current); i++ {
		cur := c.current[i]
		if c.ascending && !cur.read {
			cur.read = true
			c.pos++
			v, err := cur.r.ReadIntegerBlockAt(&cur.entry, nil)
			if err != nil {
				return nil, err
			}
			values = append(values, v...)
		} else if !c.ascending && !cur.read {
			cur.read = true
			c.pos--

			v, err := cur.r.ReadIntegerBlockAt(&cur.entry, nil)
			if err != nil {
				return nil, err
			}
			values = append(v, values...)
		}
	}

	return IntegerValues(values).Deduplicate(), err
}

// ReadStringBlock reads the next block as a set of string values.
func (c *KeyCursor) ReadStringBlock(buf []StringValue) ([]StringValue, error) {
	// No matching blocks to decode
	if len(c.current) == 0 {
		return nil, nil
	}

	// First block is the oldest block containing the points we're search for.
	first := c.current[0]
	values, err := first.r.ReadStringBlockAt(&first.entry, buf[:0])
	first.read = true

	// Only one block with this key and time range so return it
	if len(c.current) == 1 {
		return values, err
	}

	// Otherwise, search the remaining blocks that overlap and append their values so we can
	// dedup them.
	for i := 1; i < len(c.current); i++ {
		cur := c.current[i]
		if c.ascending && !cur.read {
			cur.read = true
			c.pos++
			v, err := cur.r.ReadStringBlockAt(&cur.entry, nil)
			if err != nil {
				return nil, err
			}
			values = append(values, v...)
		} else if !c.ascending && !cur.read {
			cur.read = true
			c.pos--

			v, err := cur.r.ReadStringBlockAt(&cur.entry, nil)
			if err != nil {
				return nil, err
			}
			values = append(v, values...)
		}
	}

	return StringValues(values).Deduplicate(), err
}

// ReadBooleanBlock reads the next block as a set of boolean values.
func (c *KeyCursor) ReadBooleanBlock(buf []BooleanValue) ([]BooleanValue, error) {
	// No matching blocks to decode
	if len(c.current) == 0 {
		return nil, nil
	}

	// First block is the oldest block containing the points we're search for.
	first := c.current[0]
	values, err := first.r.ReadBooleanBlockAt(&first.entry, buf[:0])
	first.read = true

	// Only one block with this key and time range so return it
	if len(c.current) == 1 {
		return values, err
	}

	// Otherwise, search the remaining blocks that overlap and append their values so we can
	// dedup them.
	for i := 1; i < len(c.current); i++ {
		cur := c.current[i]
		if c.ascending && !cur.read {
			cur.read = true
			c.pos++
			v, err := cur.r.ReadBooleanBlockAt(&cur.entry, nil)
			if err != nil {
				return nil, err
			}
			values = append(values, v...)
		} else if !c.ascending && !cur.read {
			cur.read = true
			c.pos--

			v, err := cur.r.ReadBooleanBlockAt(&cur.entry, nil)
			if err != nil {
				return nil, err
			}
			values = append(v, values...)
		}
	}

	return BooleanValues(values).Deduplicate(), err
}

type tsmReaders []TSMFile

func (a tsmReaders) Len() int           { return len(a) }
func (a tsmReaders) Less(i, j int) bool { return a[i].Path() < a[j].Path() }
func (a tsmReaders) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }

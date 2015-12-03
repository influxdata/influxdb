package tsm1

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

type TSMFile interface {
	// Path returns the underlying file path for the TSMFile.  If the file
	// has not be written or loaded from disk, the zero value is returne.
	Path() string

	// Read returns all the values in the block where time t resides
	Read(key string, t time.Time) ([]Value, error)

	// Next returns all the values in the block after the block where time t resides
	Next(key string, t time.Time) ([]Value, error)

	// Returns true if the TSMFile may contain a value with the specified
	// key and time
	ContainsValue(key string, t time.Time) bool

	// Contains returns true if the file contains any values for the given
	// key.
	Contains(key string) bool

	// TimeRange returns the min and max time across all keys in the file.
	TimeRange() (time.Time, time.Time)

	// KeyRange returns the min and max keys in the file.
	KeyRange() (string, string)

	// Keys returns all keys contained in the file.
	Keys() []string

	// Type returns the block type of the values stored for the key.  Returns one of
	// BlockFloat64, BlockInt64, BlockBool, BlockString.  If key does not exist,
	// an error is returned.
	Type(key string) (byte, error)

	// Delete removes the key from the set of keys available in this file.
	Delete(key string) error

	// Close the underlying file resources
	Close() error

	// Size returns the size of the file on disk in bytes.
	Size() int
}

type FileStore struct {
	mu sync.RWMutex

	currentFileID int
	dir           string

	files []TSMFile

	stats []FileStat
}

type FileStat struct {
	Path             string
	HasTombstone     bool
	Size             int
	MinTime, MaxTime time.Time
	MinKey, MaxKey   string
}

func (f FileStat) OverlapsTimeRange(min, max time.Time) bool {
	return (f.MinTime.Equal(max) || f.MinTime.Before(max)) &&
		(f.MaxTime.Equal(min) || f.MaxTime.After(min))
}

func (f FileStat) OverlapsKeyRange(min, max string) bool {
	return min != "" && max != "" && f.MinKey <= max && f.MaxKey >= min
}

func (f FileStat) ContainsKey(key string) bool {
	return f.MinKey >= key || key <= f.MaxKey
}

func NewFileStore(dir string) *FileStore {
	return &FileStore{
		dir: dir,
	}
}

// Returns the number of TSM files currently loaded
func (f *FileStore) Count() int {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return len(f.files)
}

// CurrentID returns the max file ID + 1
func (f *FileStore) CurrentID() int {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.currentFileID
}

// NextID returns the max file ID + 1
func (f *FileStore) NextID() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.currentFileID++
	return f.currentFileID
}

func (f *FileStore) Add(files ...TSMFile) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.files = append(f.files, files...)
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
		}
	}
	f.files = active
}

func (f *FileStore) Keys() []string {
	f.mu.RLock()
	defer f.mu.RUnlock()

	uniqueKeys := map[string]struct{}{}
	for _, f := range f.files {
		for _, key := range f.Keys() {
			uniqueKeys[key] = struct{}{}
		}
	}

	var keys []string
	for key := range uniqueKeys {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
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

func (f *FileStore) Delete(key string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	for _, file := range f.files {
		if file.Contains(key) {
			if err := file.Delete(key); err != nil {
				return err
			}
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

	files, err := filepath.Glob(filepath.Join(f.dir, fmt.Sprintf("*.%s", "tsm1dev")))
	if err != nil {
		return err
	}

	for _, fn := range files {
		// Keep track of the latest ID
		id, err := f.idFromFileName(fn)
		if err != nil {
			return err
		}

		if id >= f.currentFileID {
			f.currentFileID = id + 1
		}

		file, err := os.OpenFile(fn, os.O_RDONLY, 0666)
		if err != nil {
			return fmt.Errorf("error opening file %s: %v", fn, err)
		}

		df, err := NewTSMReaderWithOptions(TSMReaderOptions{
			MMAPFile: file,
		})
		if err != nil {
			return fmt.Errorf("error opening memory map for file %s: %v", fn, err)
		}

		f.files = append(f.files, df)
	}
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

func (f *FileStore) Read(key string, t time.Time) ([]Value, error) {
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

func (f *FileStore) Next(key string, t time.Time) ([]Value, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	for _, f := range f.files {
		// Can this file possibly contain this key and timestamp?
		if !f.Contains(key) {
			continue
		}

		// May have the key and time we are looking for so try to find
		v, err := f.Next(key, t)
		if err != nil {
			return nil, err
		}

		if len(v) > 0 {
			return v, nil
		}
	}
	return nil, nil
}

func (f *FileStore) Stats() []FileStat {
	f.mu.RLock()
	if f.stats == nil {
		f.mu.RUnlock()
		f.mu.Lock()
		defer f.mu.Unlock()

		var paths []FileStat
		for _, fd := range f.files {
			minTime, maxTime := fd.TimeRange()
			minKey, maxKey := fd.KeyRange()

			paths = append(paths, FileStat{
				Path:    fd.Path(),
				Size:    fd.Size(),
				MinTime: minTime,
				MaxTime: maxTime,
				MinKey:  minKey,
				MaxKey:  maxKey,
			})
		}
		f.stats = paths
		return f.stats
	}
	defer f.mu.RUnlock()

	return f.stats
}

func (f *FileStore) Replace(oldFiles, newFiles []string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
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
			os.Rename(file, newName)
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

				break
			}
		}

		if keep {
			active = append(active, file)
		}
	}

	f.stats = nil
	f.files = active

	// The old files should be removed since they have been replace by new files
	for _, f := range oldFiles {
		os.RemoveAll(f)
	}

	return nil
}

// idFromFileName parses the segment file ID from its name
func (f *FileStore) idFromFileName(name string) (int, error) {
	parts := strings.Split(filepath.Base(name), ".")
	if len(parts) != 2 {
		return 0, fmt.Errorf("file %s is named incorrectly", name)
	}

	id, err := strconv.ParseUint(parts[0], 10, 32)

	return int(id), err
}

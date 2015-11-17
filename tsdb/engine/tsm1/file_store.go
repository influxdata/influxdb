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

	// Returns true if the TSMFile may contain a value with the specified
	// key and time
	ContainsValue(key string, t time.Time) bool

	// Contains returns true if the file contains any values for the given
	// key.
	Contains(key string) bool

	// Keys returns all keys contained in the file.
	Keys() []string

	// Delete removes the key from the set of keys available in this file.
	Delete(key string) error

	// Close the underlying file resources
	Close() error
}

type FileStore struct {
	mu sync.RWMutex

	currentFileID int
	dir           string

	files []TSMFile
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

	files, err := filepath.Glob(filepath.Join(f.dir, fmt.Sprintf("*.%s", Format)))
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

		df, err := NewTSMReader(file)
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
		if !f.ContainsValue(key, t) {
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

// idFromFileName parses the segment file ID from its name
func (f *FileStore) idFromFileName(name string) (int, error) {
	parts := strings.Split(filepath.Base(name), ".")
	if len(parts) != 2 {
		return 0, fmt.Errorf("file %s is named incorrectly", name)
	}

	id, err := strconv.ParseUint(parts[0], 10, 32)

	return int(id), err
}

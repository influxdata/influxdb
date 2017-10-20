package tsm1

import (
	"bufio"
	"compress/gzip"
	"encoding/binary"
	"io"
	"io/ioutil"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

const (
	headerSize = 4
	v2header   = 0x1502
	v3header   = 0x1503
	v4header   = 0x1504
)

// Tombstoner records tombstones when entries are deleted.
type Tombstoner struct {
	mu sync.RWMutex

	// Path is the location of the file to record tombstone. This should be the
	// full path to a TSM file.
	Path string

	// cache of the stats for this tombstone
	fileStats []FileStat
	// indicates that the stats may be out of sync with what is on disk and they
	// should be refreshed.
	statsLoaded bool

	// Tombstones that have been written but not flushed to disk yet.
	tombstones []Tombstone
}

// Tombstone represents an individual deletion.
type Tombstone struct {
	// Key is the tombstoned series key.
	Key []byte

	// Min and Max are the min and max unix nanosecond time ranges of Key that are deleted.  If
	// the full range is deleted, both values are -1.
	Min, Max int64
}

// Add adds the all keys, across all timestamps, to the tombstone.
func (t *Tombstoner) Add(keys [][]byte) error {
	return t.AddRange(keys, math.MinInt64, math.MaxInt64)
}

// AddRange adds all keys to the tombstone specifying only the data between min and max to be removed.
func (t *Tombstoner) AddRange(keys [][]byte, min, max int64) error {
	if len(keys) == 0 {
		return nil
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	// If this TSMFile has not been written (mainly in tests), don't write a
	// tombstone because the keys will not be written when it's actually saved.
	if t.Path == "" {
		return nil
	}

	t.statsLoaded = false

	if cap(t.tombstones) < len(t.tombstones)+len(keys) {
		ts := make([]Tombstone, len(t.tombstones), len(t.tombstones)+len(keys))
		copy(ts, t.tombstones)
		t.tombstones = ts
	}

	for _, k := range keys {
		t.tombstones = append(t.tombstones, Tombstone{
			Key: k,
			Min: min,
			Max: max,
		})
	}

	return nil
}

func (t *Tombstoner) Flush() error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if len(t.tombstones) == 0 {
		return nil
	}

	if err := t.writeTombstoneV4(t.tombstones); err != nil {
		return err
	}

	t.tombstones = t.tombstones[:0]
	return nil
}

// ReadAll returns all the tombstones in the Tombstoner's directory.
func (t *Tombstoner) ReadAll() ([]Tombstone, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	tombstones, err := t.readTombstone()
	if err != nil {
		return nil, err
	}

	if len(t.tombstones) == 0 {
		return tombstones, nil
	}
	return append(tombstones, t.tombstones...), nil
}

// Delete removes all the tombstone files from disk.
func (t *Tombstoner) Delete() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if err := os.RemoveAll(t.tombstonePath()); err != nil {
		return err
	}
	t.statsLoaded = false
	return nil
}

// HasTombstones return true if there are any tombstone entries recorded.
func (t *Tombstoner) HasTombstones() bool {
	files := t.TombstoneFiles()
	t.mu.RLock()
	n := len(t.tombstones)
	t.mu.RUnlock()

	return len(files) > 0 && files[0].Size > 0 || n > 0
}

// TombstoneFiles returns any tombstone files associated with Tombstoner's TSM file.
func (t *Tombstoner) TombstoneFiles() []FileStat {
	t.mu.RLock()
	if t.statsLoaded {
		stats := t.fileStats
		t.mu.RUnlock()
		return stats
	}
	t.mu.RUnlock()

	stat, err := os.Stat(t.tombstonePath())
	if os.IsNotExist(err) || err != nil {
		t.mu.Lock()
		// The file doesn't exist so record that we tried to load it so
		// we don't continue to keep trying.  This is the common case.
		t.statsLoaded = os.IsNotExist(err)
		t.fileStats = t.fileStats[:0]
		t.mu.Unlock()
		return nil
	}

	t.mu.Lock()
	t.fileStats = append(t.fileStats[:0], FileStat{
		Path:         t.tombstonePath(),
		LastModified: stat.ModTime().UnixNano(),
		Size:         uint32(stat.Size()),
	})
	t.statsLoaded = true
	stats := t.fileStats
	t.mu.Unlock()

	return stats
}

// Walk calls fn for every Tombstone under the Tombstoner.
func (t *Tombstoner) Walk(fn func(t Tombstone) error) error {
	f, err := os.Open(t.tombstonePath())
	if os.IsNotExist(err) {
		return nil
	} else if err != nil {
		return err
	}
	defer f.Close()

	var b [4]byte
	if _, err := f.Read(b[:]); err != nil {
		// Might be a zero length file which should not exist, but
		// an old bug allowed them to occur.  Treat it as an empty
		// v1 tombstone file so we don't abort loading the TSM file.
		return t.readTombstoneV1(f, fn)
	}

	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return err
	}

	header := binary.BigEndian.Uint32(b[:])
	if header == v4header {
		return t.readTombstoneV4(f, fn)
	} else if header == v3header {
		return t.readTombstoneV3(f, fn)
	} else if header == v2header {
		return t.readTombstoneV2(f, fn)
	}
	return t.readTombstoneV1(f, fn)
}

func (t *Tombstoner) writeTombstoneV3(tombstones []Tombstone) error {
	tmp, err := ioutil.TempFile(filepath.Dir(t.Path), "tombstone")
	if err != nil {
		return err
	}
	defer tmp.Close()

	var b [8]byte

	bw := bufio.NewWriterSize(tmp, 1024*1024)

	binary.BigEndian.PutUint32(b[:4], v3header)
	if _, err := bw.Write(b[:4]); err != nil {
		return err
	}

	gz := gzip.NewWriter(bw)
	for _, ts := range tombstones {
		if err := t.writeTombstone(gz, ts); err != nil {
			return err
		}
	}

	if err := gz.Close(); err != nil {
		return err
	}

	if err := bw.Flush(); err != nil {
		return err
	}

	// fsync the file to flush the write
	if err := tmp.Sync(); err != nil {
		return err
	}

	tmpFilename := tmp.Name()
	tmp.Close()

	if err := renameFile(tmpFilename, t.tombstonePath()); err != nil {
		return err
	}

	return syncDir(filepath.Dir(t.tombstonePath()))
}

// writeTombstoneV4 writes v3 files that are concatenated together.  A v4 header is
// written to indicated this is a v4 file.
func (t *Tombstoner) writeTombstoneV4(tombstones []Tombstone) error {
	tmp, err := ioutil.TempFile(filepath.Dir(t.Path), "tombstone")
	if err != nil {
		return err
	}
	defer tmp.Close()

	// Copy the existing v4 file if it exists
	f, err := os.Open(t.tombstonePath())
	if !os.IsNotExist(err) {
		var b [4]byte
		if n, err := f.Read(b[:]); n == 4 && err == nil {
			header := binary.BigEndian.Uint32(b[:])
			// There is an existing tombstone on disk and it's not a v3.  Just rewrite it as a v3
			// version again.
			if header != v4header {
				return t.writeTombstoneV3(tombstones)
			}

			// Seek back to the beginning we copy the header
			if _, err := f.Seek(0, io.SeekStart); err != nil {
				return err
			}

			// Copy the while file
			if _, err := io.Copy(tmp, f); err != nil {
				f.Close()
				return err
			}
		}
	} else if err != nil && !os.IsNotExist(err) {
		return err
	}
	f.Close()

	var b [8]byte
	bw := bufio.NewWriterSize(tmp, 1024*1024)

	// Write the header only if the file is new
	if os.IsNotExist(err) {
		binary.BigEndian.PutUint32(b[:4], v4header)
		if _, err := bw.Write(b[:4]); err != nil {
			return err
		}
	}

	// Write the tombstones
	gz := gzip.NewWriter(bw)
	for _, ts := range tombstones {
		if err := t.writeTombstone(gz, ts); err != nil {
			return err
		}
	}

	if err := gz.Close(); err != nil {
		return err
	}

	if err := bw.Flush(); err != nil {
		return err
	}

	// fsync the file to flush the write
	if err := tmp.Sync(); err != nil {
		return err
	}

	tmpFilename := tmp.Name()
	tmp.Close()

	if err := renameFile(tmpFilename, t.tombstonePath()); err != nil {
		return err
	}

	return syncDir(filepath.Dir(t.tombstonePath()))
}

func (t *Tombstoner) readTombstone() ([]Tombstone, error) {
	var tombstones []Tombstone

	if err := t.Walk(func(t Tombstone) error {
		tombstones = append(tombstones, t)
		return nil
	}); err != nil {
		return nil, err
	}
	return tombstones, nil
}

// readTombstoneV1 reads the first version of tombstone files that were not
// capable of storing a min and max time for a key.  This is used for backwards
// compatibility with versions prior to 0.13.  This format is a simple newline
// separated text file.
func (t *Tombstoner) readTombstoneV1(f *os.File, fn func(t Tombstone) error) error {
	r := bufio.NewScanner(f)
	for r.Scan() {
		line := r.Text()
		if line == "" {
			continue
		}
		if err := fn(Tombstone{
			Key: []byte(line),
			Min: math.MinInt64,
			Max: math.MaxInt64,
		}); err != nil {
			return err
		}
	}

	if err := r.Err(); err != nil {
		return err
	}

	for _, t := range t.tombstones {
		if err := fn(t); err != nil {
			return err
		}
	}
	return nil
}

// readTombstoneV2 reads the second version of tombstone files that are capable
// of storing keys and the range of time for the key that points were deleted. This
// format is binary.
func (t *Tombstoner) readTombstoneV2(f *os.File, fn func(t Tombstone) error) error {
	// Skip header, already checked earlier
	if _, err := f.Seek(headerSize, io.SeekStart); err != nil {
		return err
	}
	n := int64(4)

	fi, err := f.Stat()
	if err != nil {
		return err
	}
	size := fi.Size()

	var (
		min, max int64
		key      []byte
	)
	b := make([]byte, 4096)
	for {
		if n >= size {
			break
		}

		if _, err = f.Read(b[:4]); err != nil {
			return err
		}
		n += 4

		keyLen := int(binary.BigEndian.Uint32(b[:4]))
		if keyLen > len(b) {
			b = make([]byte, keyLen)
		}

		if _, err := f.Read(b[:keyLen]); err != nil {
			return err
		}
		key = b[:keyLen]
		n += int64(keyLen)

		if _, err := f.Read(b[:8]); err != nil {
			return err
		}
		n += 8

		min = int64(binary.BigEndian.Uint64(b[:8]))

		if _, err := f.Read(b[:8]); err != nil {
			return err
		}
		n += 8
		max = int64(binary.BigEndian.Uint64(b[:8]))

		if err := fn(Tombstone{
			Key: key,
			Min: min,
			Max: max,
		}); err != nil {
			return err
		}
	}

	for _, t := range t.tombstones {
		if err := fn(t); err != nil {
			return err
		}
	}
	return nil
}

// readTombstoneV3 reads the third version of tombstone files that are capable
// of storing keys and the range of time for the key that points were deleted. This
// format is a binary and compressed with gzip.
func (t *Tombstoner) readTombstoneV3(f *os.File, fn func(t Tombstone) error) error {
	// Skip header, already checked earlier
	if _, err := f.Seek(headerSize, io.SeekStart); err != nil {
		return err
	}

	var (
		min, max int64
		key      []byte
	)

	gr, err := gzip.NewReader(bufio.NewReader(f))
	if err != nil {
		return err
	}
	defer gr.Close()

	b := make([]byte, 4096)
	for {
		if _, err = io.ReadFull(gr, b[:4]); err == io.EOF || err == io.ErrUnexpectedEOF {
			break
		} else if err != nil {
			return err
		}

		keyLen := int(binary.BigEndian.Uint32(b[:4]))
		if keyLen > len(b) {
			b = make([]byte, keyLen)
		}

		if _, err := io.ReadFull(gr, b[:keyLen]); err != nil {
			return err
		}

		// Copy the key since b is re-used
		key = make([]byte, keyLen)
		copy(key, b[:keyLen])

		if _, err := io.ReadFull(gr, b[:8]); err != nil {
			return err
		}

		min = int64(binary.BigEndian.Uint64(b[:8]))

		if _, err := io.ReadFull(gr, b[:8]); err != nil {
			return err
		}

		max = int64(binary.BigEndian.Uint64(b[:8]))

		if err := fn(Tombstone{
			Key: key,
			Min: min,
			Max: max,
		}); err != nil {
			return err
		}
	}

	for _, t := range t.tombstones {
		if err := fn(t); err != nil {
			return err
		}
	}
	return nil
}

// readTombstoneV4 reads the fourth version of tombstone files that are capable
// of storing multiple v3 files appended together.
func (t *Tombstoner) readTombstoneV4(f *os.File, fn func(t Tombstone) error) error {
	// Skip header, already checked earlier
	if _, err := f.Seek(headerSize, io.SeekStart); err != nil {
		return err
	}

	var (
		min, max int64
		key      []byte
	)

	br := bufio.NewReader(f)
	gr, err := gzip.NewReader(br)
	if err != nil {
		return err
	}
	defer gr.Close()

	b := make([]byte, 4096)
	for {
		gr.Multistream(false)
		if err := func() error {
			for {
				if _, err = io.ReadFull(gr, b[:4]); err == io.EOF || err == io.ErrUnexpectedEOF {
					return nil
				} else if err != nil {
					return err
				}

				keyLen := int(binary.BigEndian.Uint32(b[:4]))
				if keyLen > len(b) {
					b = make([]byte, keyLen)
				}

				if _, err := io.ReadFull(gr, b[:keyLen]); err != nil {
					return err
				}

				// Copy the key since b is re-used
				key = make([]byte, keyLen)
				copy(key, b[:keyLen])

				if _, err := io.ReadFull(gr, b[:8]); err != nil {
					return err
				}

				min = int64(binary.BigEndian.Uint64(b[:8]))

				if _, err := io.ReadFull(gr, b[:8]); err != nil {
					return err
				}

				max = int64(binary.BigEndian.Uint64(b[:8]))

				if err := fn(Tombstone{
					Key: key,
					Min: min,
					Max: max,
				}); err != nil {
					return err
				}
			}
		}(); err != nil {
			return err
		}

		for _, t := range t.tombstones {
			if err := fn(t); err != nil {
				return err
			}
		}

		err = gr.Reset(br)
		if err == io.EOF {
			return nil
		}
	}
}

func (t *Tombstoner) tombstonePath() string {
	if strings.HasSuffix(t.Path, "tombstone") {
		return t.Path
	}

	// Filename is 0000001.tsm1
	filename := filepath.Base(t.Path)

	// Strip off the tsm1
	ext := filepath.Ext(filename)
	if ext != "" {
		filename = strings.TrimSuffix(filename, ext)
	}

	// Append the "tombstone" suffix to create a 0000001.tombstone file
	return filepath.Join(filepath.Dir(t.Path), filename+".tombstone")
}

func (t *Tombstoner) writeTombstone(dst io.Writer, ts Tombstone) error {
	var b [8]byte
	binary.BigEndian.PutUint32(b[:4], uint32(len(ts.Key)))
	if _, err := dst.Write(b[:4]); err != nil {
		return err
	}
	if _, err := dst.Write([]byte(ts.Key)); err != nil {
		return err
	}
	binary.BigEndian.PutUint64(b[:], uint64(ts.Min))
	if _, err := dst.Write(b[:]); err != nil {
		return err
	}

	binary.BigEndian.PutUint64(b[:], uint64(ts.Max))
	if _, err := dst.Write(b[:]); err != nil {
		return err
	}
	return nil
}

package tsm1

/*
Tombstone file format:

╔═══════════════════════════════════════════Tombstone File════════════════════════════════════════════╗
║ ┌─────────────┐┌──────────────────────────────────────────────────────────────────────────────────┐ ║
║ │             ││                                                                                  │ ║
║ │             ││                                                                                  │ ║
║ │             ││                                                                                  │ ║
║ │             ││                                                                                  │ ║
║ │             ││                                                                                  │ ║
║ │   Header    ││                                                                                  │ ║
║ │   4 bytes   ││                                Tombstone Entries                                 │ ║
║ │             ││                                                                                  │ ║
║ │             ││                                                                                  │ ║
║ │             ││                                                                                  │ ║
║ │             ││                                                                                  │ ║
║ │             ││                                                                                  │ ║
║ │             ││                                                                                  │ ║
║ └─────────────┘└──────────────────────────────────────────────────────────────────────────────────┘ ║
╚═════════════════════════════════════════════════════════════════════════════════════════════════════╝

╔═══════════════════════════════════════════Tombstone Entry═══════════════════════════════════════════╗
║ ┌──────┐┌───────────────┐┌────────────┐┌────────────────────────┐┌───────────────┐┌───────────────┐ ║
║ │      ││               ││            ││                        ││               ││               │ ║
║ │      ││               ││            ││                        ││               ││               │ ║
║ │      ││               ││            ││                        ││               ││               │ ║
║ │      ││               ││            ││                        ││               ││               │ ║
║ │      ││               ││            ││                        ││               ││               │ ║
║ │Prefix││   Reserved    ││ Key Length ││          Key           ││   Min Time    ││   Max Time    │ ║
║ │ Bit  ││    7 bits     ││  24 bits   ││        N bytes         ││    8 bytes    ││    8 bytes    │ ║
║ │      ││               ││            ││                        ││               ││               │ ║
║ │      ││               ││            ││                        ││               ││               │ ║
║ │      ││               ││            ││                        ││               ││               │ ║
║ │      ││               ││            ││                        ││               ││               │ ║
║ │      ││               ││            ││                        ││               ││               │ ║
║ │      ││               ││            ││                        ││               ││               │ ║
║ └──────┘└───────────────┘└────────────┘└────────────────────────┘└───────────────┘└───────────────┘ ║
╚═════════════════════════════════════════════════════════════════════════════════════════════════════╝

NOTE: v1, v2 and v3 tombstone supports have been dropped from 2.x. Only v4 is now
supported.
*/

import (
	"bufio"
	"compress/gzip"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/influxdata/influxdb/v2/pkg/fs"
)

const (
	headerSize = 4
	v4header   = 0x1504
)

var errIncompatibleV4Version = errors.New("incompatible v4 version")

// Tombstoner records tombstones when entries are deleted.
type Tombstoner struct {
	mu sync.RWMutex

	// Path is the location of the file to record tombstone. This should be the
	// full path to a TSM file.
	Path string

	FilterFn func(k []byte) bool

	// cache of the stats for this tombstone
	fileStats []FileStat
	// indicates that the stats may be out of sync with what is on disk and they
	// should be refreshed.
	statsLoaded bool

	// Tombstones that have been written but not flushed to disk yet.
	tombstones []Tombstone

	// These are references used for pending writes that have not been committed.  If
	// these are nil, then no pending writes are in progress.
	gz                *gzip.Writer
	bw                *bufio.Writer
	pendingFile       *os.File
	tmp               [8]byte
	lastAppliedOffset int64

	// Optional observer for when tombstone files are written.
	obs FileStoreObserver
}

// NewTombstoner constructs a Tombstoner for the given path. FilterFn can be nil.
func NewTombstoner(path string, filterFn func(k []byte) bool) *Tombstoner {
	return &Tombstoner{
		Path:     path,
		FilterFn: filterFn,
		obs:      noFileStoreObserver{},
	}
}

// Tombstone represents an individual deletion.
type Tombstone struct {
	// Key is the tombstoned series key.
	Key []byte

	// Prefix indicates if this tombstone entry is a prefix key, meaning all
	// keys with a prefix matching Key should be removed for the [Min, Max] range.
	Prefix bool

	// Min and Max are the min and max unix nanosecond time ranges of Key that are deleted.
	Min, Max int64

	// Predicate stores the marshaled form of some predicate for matching keys.
	Predicate []byte
}

func (t Tombstone) String() string {
	prefix := "Key"
	if t.Prefix {
		prefix = "Prefix"
	}
	return fmt.Sprintf("%s: %q, [%d, %d] pred:%v", prefix, t.Key, t.Min, t.Max, len(t.Predicate) > 0)
}

// WithObserver sets a FileStoreObserver for when the tombstone file is written.
func (t *Tombstoner) WithObserver(obs FileStoreObserver) {
	if obs == nil {
		obs = noFileStoreObserver{}
	}
	t.obs = obs
}

// AddPrefixRange adds a prefix-based tombstone key with an explicit range.
func (t *Tombstoner) AddPrefixRange(key []byte, min, max int64, predicate []byte) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	// If this TSMFile has not been written (mainly in tests), don't write a
	// tombstone because the keys will not be written when it's actually saved.
	if t.Path == "" {
		return nil
	}

	t.statsLoaded = false

	if err := t.prepareLatest(); err != nil {
		return err
	}

	return t.writeTombstoneV4(t.gz, Tombstone{
		Key:       key,
		Min:       min,
		Max:       max,
		Prefix:    true,
		Predicate: predicate,
	})
}

// Add adds the all keys, across all timestamps, to the tombstone.
func (t *Tombstoner) Add(keys [][]byte) error {
	return t.AddRange(keys, math.MinInt64, math.MaxInt64)
}

// AddRange adds all keys to the tombstone specifying only the data between min and max to be removed.
func (t *Tombstoner) AddRange(keys [][]byte, min, max int64) error {
	for t.FilterFn != nil && len(keys) > 0 && !t.FilterFn(keys[0]) {
		keys = keys[1:]
	}

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

	if err := t.prepareLatest(); err != nil {
		return err
	}

	for _, k := range keys {
		if t.FilterFn != nil && !t.FilterFn(k) {
			continue
		}

		if err := t.writeTombstoneV4(t.gz, Tombstone{
			Key:    k,
			Min:    min,
			Max:    max,
			Prefix: false,
		}); err != nil {
			return err
		}
	}
	return nil
}

func (t *Tombstoner) Flush() error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if err := t.commit(); err != nil {
		// Reset our temp references and clean up.
		_ = t.rollback()
		return err
	}
	return nil
}

func (t *Tombstoner) Rollback() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.rollback()
}

// Delete removes all the tombstone files from disk.
func (t *Tombstoner) Delete() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if err := os.RemoveAll(t.tombstonePath()); err != nil {
		return err
	}
	t.statsLoaded = false
	t.lastAppliedOffset = 0

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
		CreatedAt:    stat.ModTime().UnixNano(),
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
	t.mu.Lock()
	defer t.mu.Unlock()

	f, err := os.Open(t.tombstonePath())
	if os.IsNotExist(err) {
		return nil
	} else if err != nil {
		return err
	}
	defer f.Close()

	var b [4]byte
	if _, err := f.Read(b[:]); err != nil {
		return errors.New("unable to read header")
	}

	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return err
	}

	header := binary.BigEndian.Uint32(b[:])
	if header == v4header {
		return t.readTombstoneV4(f, fn)
	}
	return errors.New("invalid tombstone file")
}

func (t *Tombstoner) prepareLatest() error {
	if t.pendingFile != nil { // There is already a pending tombstone file open.
		return nil
	}

	tmpPath := fmt.Sprintf("%s.%s", t.tombstonePath(), CompactionTempExtension)
	tmp, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_RDWR|os.O_EXCL, 0666)
	if err != nil {
		return err
	}

	removeTmp := func() {
		tmp.Close()
		os.Remove(tmp.Name())
	}

	// Copy the existing v4 file if it exists
	f, err := os.Open(t.tombstonePath())
	if err != nil && !os.IsNotExist(err) {
		// An unexpected error should be returned
		removeTmp()
		return err
	} else if err == nil {
		// No error so load the tombstone file.
		defer f.Close()
		var b [4]byte
		if n, err := f.Read(b[:]); n == 4 && err == nil {
			header := binary.BigEndian.Uint32(b[:])
			// There is an existing tombstone on disk and it's not a v4.
			// We can't support it.
			if header != v4header {
				removeTmp()
				return errIncompatibleV4Version
			}

			// Seek back to the beginning we copy the header
			if _, err := f.Seek(0, io.SeekStart); err != nil {
				removeTmp()
				return err
			}

			// Copy the whole file
			if _, err := io.Copy(tmp, f); err != nil {
				f.Close()
				removeTmp()
				return err
			}
		}
	}

	// Else, the error was that the file does not exist. Create a new one.
	var b [8]byte
	bw := bufio.NewWriterSize(tmp, 64*1024)

	// Write the header only if the file is new
	if os.IsNotExist(err) {
		binary.BigEndian.PutUint32(b[:4], v4header)
		if _, err := bw.Write(b[:4]); err != nil {
			removeTmp()
			return err
		}
	}

	// Write the tombstones
	gz := gzip.NewWriter(bw)

	t.pendingFile = tmp
	t.gz = gz
	t.bw = bw

	return nil
}

func (t *Tombstoner) commit() error {
	// No pending writes
	if t.pendingFile == nil {
		return nil
	}

	if err := t.gz.Close(); err != nil {
		return err
	}

	if err := t.bw.Flush(); err != nil {
		return err
	}

	// fsync the file to flush the write
	if err := t.pendingFile.Sync(); err != nil {
		return err
	}

	tmpFilename := t.pendingFile.Name()
	t.pendingFile.Close()

	if err := t.obs.FileFinishing(tmpFilename); err != nil {
		return err
	}

	if err := fs.RenameFileWithReplacement(tmpFilename, t.tombstonePath()); err != nil {
		return err
	}

	if err := fs.SyncDir(filepath.Dir(t.tombstonePath())); err != nil {
		return err
	}

	t.pendingFile = nil
	t.bw = nil
	t.gz = nil

	return nil
}

func (t *Tombstoner) rollback() error {
	if t.pendingFile == nil {
		return nil
	}

	tmpFilename := t.pendingFile.Name()
	t.pendingFile.Close()
	t.gz = nil
	t.bw = nil
	t.pendingFile = nil
	return os.Remove(tmpFilename)
}

// readTombstoneV4 reads the fourth version of tombstone files that are capable
// of storing multiple v3 files appended together.
func (t *Tombstoner) readTombstoneV4(f *os.File, fn func(t Tombstone) error) error {
	// Skip header, already checked earlier
	if t.lastAppliedOffset != 0 {
		if _, err := f.Seek(t.lastAppliedOffset, io.SeekStart); err != nil {
			return err
		}
	} else {
		if _, err := f.Seek(headerSize, io.SeekStart); err != nil {
			return err
		}
	}

	const kmask = int64(0xff000000) // Mask for non key-length bits

	br := bufio.NewReaderSize(f, 64*1024)
	gr, err := gzip.NewReader(br)
	if err == io.EOF {
		return nil
	} else if err != nil {
		return err
	}
	defer gr.Close()

	var ( // save these buffers across loop iterations to avoid allocations
		keyBuf  []byte
		predBuf []byte
	)

	for {
		gr.Multistream(false)
		if err := func() error {
			for {
				var buf [8]byte

				if _, err = io.ReadFull(gr, buf[:4]); err == io.EOF || err == io.ErrUnexpectedEOF {
					return nil
				} else if err != nil {
					return err
				}

				keyLen := int64(binary.BigEndian.Uint32(buf[:4]))
				prefix := keyLen>>31&1 == 1 // Prefix is set according to whether the highest bit is set.
				hasPred := keyLen>>30&1 == 1

				// Remove 8 MSB to get correct length.
				keyLen &^= kmask

				if int64(len(keyBuf)) < keyLen {
					keyBuf = make([]byte, keyLen)
				}
				// cap slice protects against invalid usages of append in callback
				key := keyBuf[:keyLen:keyLen]

				if _, err := io.ReadFull(gr, key); err != nil {
					return err
				}

				if _, err := io.ReadFull(gr, buf[:8]); err != nil {
					return err
				}
				min := int64(binary.BigEndian.Uint64(buf[:8]))

				if _, err := io.ReadFull(gr, buf[:8]); err != nil {
					return err
				}
				max := int64(binary.BigEndian.Uint64(buf[:8]))

				var predicate []byte
				if hasPred {
					if _, err := io.ReadFull(gr, buf[:8]); err != nil {
						return err
					}
					predLen := binary.BigEndian.Uint64(buf[:8])

					if uint64(len(predBuf)) < predLen {
						predBuf = make([]byte, predLen)
					}
					// cap slice protects against invalid usages of append in callback
					predicate = predBuf[:predLen:predLen]

					if _, err := io.ReadFull(gr, predicate); err != nil {
						return err
					}
				}

				if err := fn(Tombstone{
					Key:       key,
					Min:       min,
					Max:       max,
					Prefix:    prefix,
					Predicate: predicate,
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
			break
		}
	}

	// Save the position of tombstone file so we don't re-apply the same set again if there are
	// more deletes.
	pos, err := f.Seek(0, io.SeekCurrent)
	if err != nil {
		return err
	}
	t.lastAppliedOffset = pos
	return nil
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

func (t *Tombstoner) writeTombstoneV4(dst io.Writer, ts Tombstone) error {
	maxKeyLen := 0x00ffffff // 24 bit key length. Top 8 bits for other information.

	// Maximum key length. Leaves 8 spare bits.
	if len(ts.Key) > maxKeyLen {
		return fmt.Errorf("key has length %d, maximum allowed key length %d", len(ts.Key), maxKeyLen)
	}

	l := uint32(len(ts.Key))
	if ts.Prefix {
		// A mask to set the prefix bit on a tombstone.
		l |= 1 << 31
	}
	if len(ts.Predicate) > 0 {
		// A mask to set the predicate bit on a tombstone
		l |= 1 << 30
	}

	binary.BigEndian.PutUint32(t.tmp[:4], l)
	if _, err := dst.Write(t.tmp[:4]); err != nil {
		return err
	}
	if _, err := dst.Write([]byte(ts.Key)); err != nil {
		return err
	}

	binary.BigEndian.PutUint64(t.tmp[:], uint64(ts.Min))
	if _, err := dst.Write(t.tmp[:]); err != nil {
		return err
	}

	binary.BigEndian.PutUint64(t.tmp[:], uint64(ts.Max))
	if _, err := dst.Write(t.tmp[:]); err != nil {
		return err
	}

	if len(ts.Predicate) > 0 {
		binary.BigEndian.PutUint64(t.tmp[:], uint64(len(ts.Predicate)))
		if _, err := dst.Write(t.tmp[:]); err != nil {
			return err
		}

		if _, err := dst.Write(ts.Predicate); err != nil {
			return err
		}
	}

	return nil
}

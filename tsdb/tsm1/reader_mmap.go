package tsm1

import (
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"sync"
	"sync/atomic"

	"github.com/influxdata/influxdb/v2/pkg/fs"
	"github.com/influxdata/influxdb/v2/pkg/mincore"
	"go.uber.org/zap"
)

// mmapAccess is mmap based block accessor.  It access blocks through an
// MMAP file interface.
type mmapAccessor struct {
	accessCount uint64 // Counter incremented everytime the mmapAccessor is accessed
	freeCount   uint64 // Counter to determine whether the accessor can free its resources

	logger       *zap.Logger
	mmapWillNeed bool // If true then mmap advise value MADV_WILLNEED will be provided the kernel for b.

	mu    sync.RWMutex
	b     []byte
	f     *os.File
	_path string // If the underlying file is renamed then this gets updated

	pageFaultLimiter *mincore.Limiter // limits page fault accesses

	index *indirectIndex
}

func (m *mmapAccessor) init() (*indirectIndex, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Set the path explicitly.
	m._path = m.f.Name()

	if err := verifyVersion(m.f); err != nil {
		return nil, err
	}

	var err error

	if _, err := m.f.Seek(0, 0); err != nil {
		return nil, err
	}

	stat, err := m.f.Stat()
	if err != nil {
		return nil, err
	}

	m.b, err = mmap(m.f, 0, int(stat.Size()))
	if err != nil {
		return nil, err
	}
	if len(m.b) < 8 {
		return nil, fmt.Errorf("mmapAccessor: byte slice too small for indirectIndex")
	}

	// Hint to the kernel that we will be reading the file.  It would be better to hint
	// that we will be reading the index section, but that's not been
	// implemented as yet.
	if m.mmapWillNeed {
		if err := madviseWillNeed(m.b); err != nil {
			return nil, err
		}
	}

	indexOfsPos := len(m.b) - 8
	indexStart := binary.BigEndian.Uint64(m.b[indexOfsPos : indexOfsPos+8])
	if indexStart >= uint64(indexOfsPos) {
		return nil, fmt.Errorf("mmapAccessor: invalid indexStart")
	}

	m.index = NewIndirectIndex()
	if err := m.index.UnmarshalBinary(m.b[indexStart:indexOfsPos]); err != nil {
		return nil, err
	}
	m.index.logger = m.logger

	// Allow resources to be freed immediately if requested
	m.incAccess()
	atomic.StoreUint64(&m.freeCount, 1)

	return m.index, nil
}

func (m *mmapAccessor) free() error {
	accessCount := atomic.LoadUint64(&m.accessCount)
	freeCount := atomic.LoadUint64(&m.freeCount)

	// Already freed everything.
	if freeCount == 0 && accessCount == 0 {
		return nil
	}

	// Were there accesses after the last time we tried to free?
	// If so, don't free anything and record the access count that we
	// see now for the next check.
	if accessCount != freeCount {
		atomic.StoreUint64(&m.freeCount, accessCount)
		return nil
	}

	// Reset both counters to zero to indicate that we have freed everything.
	atomic.StoreUint64(&m.accessCount, 0)
	atomic.StoreUint64(&m.freeCount, 0)

	m.mu.RLock()
	defer m.mu.RUnlock()

	return madviseDontNeed(m.b)
}

func (m *mmapAccessor) incAccess() {
	atomic.AddUint64(&m.accessCount, 1)
}

func (m *mmapAccessor) rename(path string) error {
	m.incAccess()

	m.mu.Lock()
	defer m.mu.Unlock()

	if err := fs.RenameFileWithReplacement(m._path, path); err != nil {
		return err
	}
	m._path = path
	return nil
}

func (m *mmapAccessor) read(key []byte, timestamp int64) ([]Value, error) {
	entry := m.index.Entry(key, timestamp)
	if entry == nil {
		return nil, nil
	}

	return m.readBlock(entry, nil)
}

func (m *mmapAccessor) readBlock(entry *IndexEntry, values []Value) ([]Value, error) {
	m.incAccess()

	m.mu.RLock()
	defer m.mu.RUnlock()

	if int64(len(m.b)) < entry.Offset+int64(entry.Size) {
		return nil, ErrTSMClosed
	}
	//TODO: Validate checksum
	var err error
	b := m.b[entry.Offset+4 : entry.Offset+int64(entry.Size)]
	values, err = DecodeBlock(b, values)
	if err != nil {
		return nil, err
	}

	// Rate limit page faults.
	if err := m.wait(b); err != nil {
		return nil, err
	}

	return values, nil
}

func (m *mmapAccessor) readBytes(entry *IndexEntry, b []byte) (uint32, []byte, error) {
	m.incAccess()

	m.mu.RLock()
	if int64(len(m.b)) < entry.Offset+int64(entry.Size) {
		m.mu.RUnlock()
		return 0, nil, ErrTSMClosed
	}

	// return the bytes after the 4 byte checksum
	crc, block := binary.BigEndian.Uint32(m.b[entry.Offset:entry.Offset+4]), m.b[entry.Offset+4:entry.Offset+int64(entry.Size)]
	m.mu.RUnlock()

	// Rate limit page faults.
	if err := m.wait(m.b[entry.Offset : entry.Offset+4]); err != nil {
		return 0, nil, err
	} else if err := m.wait(block); err != nil {
		return 0, nil, err
	}

	return crc, block, nil
}

// readAll returns all values for a key in all blocks.
func (m *mmapAccessor) readAll(key []byte) ([]Value, error) {
	m.incAccess()

	blocks, err := m.index.ReadEntries(key, nil)
	if len(blocks) == 0 || err != nil {
		return nil, err
	}

	tombstones := m.index.TombstoneRange(key, nil)

	m.mu.RLock()
	defer m.mu.RUnlock()

	var temp []Value
	var values []Value
	for _, block := range blocks {
		var skip bool
		for _, t := range tombstones {
			// Should we skip this block because it contains points that have been deleted
			if t.Min <= block.MinTime && t.Max >= block.MaxTime {
				skip = true
				break
			}
		}

		if skip {
			continue
		}
		//TODO: Validate checksum
		temp = temp[:0]
		// The +4 is the 4 byte checksum length
		temp, err = DecodeBlock(m.b[block.Offset+4:block.Offset+int64(block.Size)], temp)
		if err != nil {
			return nil, err
		} else if err := m.wait(m.b[block.Offset+4 : block.Offset+int64(block.Size)]); err != nil {
			return nil, err
		}

		// Filter out any values that were deleted
		for _, t := range tombstones {
			temp = Values(temp).Exclude(t.Min, t.Max)
		}

		values = append(values, temp...)
	}

	return values, nil
}

func (m *mmapAccessor) path() string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m._path
}

func (m *mmapAccessor) close() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.b == nil {
		return nil
	}

	err := munmap(m.b)
	if err != nil {
		return err
	}

	m.b = nil
	return m.f.Close()
}

// wait rate limits page faults to the underlying data. Skipped if limiter is not set.
func (m *mmapAccessor) wait(b []byte) error {
	if m.pageFaultLimiter == nil {
		return nil
	}
	return m.pageFaultLimiter.WaitRange(context.Background(), b)
}

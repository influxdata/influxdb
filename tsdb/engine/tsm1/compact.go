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
// iterators.  The resulting stream is written sorted and chunk to allow for
// one-pass writing of a new TSM file.

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
)

var errMaxFileExceeded = fmt.Errorf("max file exceeded")

// Compactor merges multiple WAL segments and TSM files into one or more
// new TSM files.
type Compactor struct {
	Dir         string
	MaxFileSize int
	currentID   int

	merge *MergeIterator
}

func (c *Compactor) Compact(walSegments []string) ([]string, error) {
	var walReaders []*WALSegmentReader

	// For each segment, create a reader to iterate over each WAL entry
	for _, path := range walSegments {
		f, err := os.Open(path)
		if err != nil {
			return nil, err
		}
		r := NewWALSegmentReader(f)
		defer r.Close()

		walReaders = append(walReaders, r)
	}

	// WALKeyIterator allows all the segments to be ordered by key and
	// sorted values during compaction.
	walKeyIterator, err := NewWALKeyIterator(walReaders...)
	if err != nil {
		return nil, err
	}

	// Merge iterator combines the WAL and TSM iterators (note: TSM iteration is
	// not in place yet).  It will also chunk the values into 1000 element blocks.
	c.merge = NewMergeIterator(walKeyIterator, 1000)
	defer c.merge.Close()

	// These are the new TSM files written
	var files []string

	for {
		// TODO: this needs to be intialized based on the existing files on disk
		c.currentID++

		// New TSM files are written to a temp file and renamed when fully completed.
		fileName := filepath.Join(c.Dir, fmt.Sprintf("%07d.%s.tmp", c.currentID, Format))

		// Write as much as possible to this file
		err := c.write(fileName)

		// We've hit the max file limit and there is more to write.  Create a new file
		// and continue.
		if err == errMaxFileExceeded {
			files = append(files, fileName)
			continue
		}

		// We hit an error but didn't finish the compaction.  Remove the temp file and abort.
		if err != nil {
			os.RemoveAll(fileName)
			return nil, err
		}

		files = append(files, fileName)
		break
	}
	c.merge = nil
	return files, nil
}

func (c *Compactor) write(path string) error {
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
		if c.MaxFileSize != 0 && w.Size() > c.MaxFileSize {
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

	// size is the maximum value of a chunk to return
	size int

	// key is the current iteration series key
	key string

	// walBuf is the remaining values from the last wal Read call
	walBuf []Value

	// chunk is the curren set of values that will be returned by Read
	chunk []Value

	// err is any error returned by an underlying iterator to be returned
	// by Read
	err error
}

func (m *MergeIterator) Next() bool {
	// Prime the wal buffer if possible
	if len(m.walBuf) == 0 && m.wal.Next() {
		k, v, err := m.wal.Read()
		m.key = k
		m.err = err
		m.walBuf = v
	}

	// Move size elements into the current chunk and slice the same
	// amount off of the wal buffer.
	if m.size < len(m.walBuf) {
		m.chunk = m.walBuf[:m.size]
		m.walBuf = m.walBuf[m.size:]
	} else {
		m.chunk = m.walBuf
		m.walBuf = m.walBuf[:0]
	}

	return len(m.chunk) > 0
}

func (m *MergeIterator) Read() (string, []Value, error) {
	return m.key, m.chunk, m.err
}

func (m *MergeIterator) Close() error {
	m.walBuf = nil
	m.chunk = nil
	return m.wal.Close()
}

func NewMergeIterator(WAL KeyIterator, size int) *MergeIterator {
	m := &MergeIterator{
		wal:  WAL,
		size: size,
	}
	return m
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
		series[k] = v.Deduplicate()
	}
	sort.Strings(order)

	return &walKeyIterator{
		Series: series,
		Order:  order,
	}, nil
}

package tsm1

import (
	"fmt"
	"sort"
	"strings"
)

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
				for _, p := range t.Points {
					// Create a series key for each field
					for k, v := range p.Fields() {
						key := fmt.Sprintf("%s%s%s", p.Key(), keyFieldSeparator, k)

						// Just append each point as we see it.  Dedup and sorting happens later.
						series[key] = append(series[key], NewValue(p.Time(), v))
					}
				}

			case *DeleteWALEntry:
				// Each key is a series, measurement + tagset string
				for _, k := range t.Keys {
					// seriesKey is specific to a field, measurment + tagset string + sep + field name
					for seriesKey := range series {
						key := seriesKey[:strings.Index(seriesKey, keyFieldSeparator)]
						//  If the delete series key matches the portion before the separator, we delete what we have
						if key == k {
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

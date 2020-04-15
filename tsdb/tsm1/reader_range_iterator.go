package tsm1

import (
	"github.com/influxdata/influxdb/v2/tsdb/cursors"
)

// TimeRangeIterator will iterate over the keys of a TSM file, starting at
// the provided key. It is used to determine if each key has data which exists
// within a specified time interval.
type TimeRangeIterator struct {
	timeRangeBlockReader
}

// Next advances the iterator and reports if it is still valid.
func (b *TimeRangeIterator) Next() bool {
	if b.Err() != nil {
		return false
	}

	return b.iter.Next()
}

// Seek points the iterator at the smallest key greater than or equal to the
// given key, returning true if it was an exact match. It returns false for
// ok if the key does not exist.
func (b *TimeRangeIterator) Seek(key []byte) (exact, ok bool) {
	if b.Err() != nil {
		return false, false
	}

	return b.iter.Seek(key)
}

// HasData reports true if the current key has data for the time range.
func (b *TimeRangeIterator) HasData() bool {
	if b.Err() != nil {
		return false
	}

	e, ts := b.getEntriesAndTombstones()
	if len(e) == 0 {
		return false
	}

	if len(ts) == 0 {
		// no tombstones, fast path will avoid decoding blocks
		// if queried time interval intersects with one of the entries
		if intersectsEntry(e, b.tr) {
			return true
		}
	}

	for i := range e {
		if !b.readBlock(&e[i]) {
			return false
		}

		// remove tombstoned timestamps
		for i := range ts {
			b.a.Exclude(ts[i].Min, ts[i].Max)
		}

		if b.a.Contains(b.tr.Min, b.tr.Max) {
			return true
		}
	}

	return false
}

// The timeRangeBlockReader provides common behavior
// for enumerating keys over a given time range and
// accumulating statistics.
type timeRangeBlockReader struct {
	r     *TSMReader
	iter  *TSMIndexIterator
	tr    TimeRange
	err   error
	stats cursors.CursorStats

	// temporary storage
	trbuf []TimeRange
	buf   []byte
	a     cursors.TimestampArray
}

func (b *timeRangeBlockReader) Err() error {
	if b.err != nil {
		return b.err
	}
	return b.iter.Err()
}

// Key reports the current key.
func (b *timeRangeBlockReader) Key() []byte {
	return b.iter.Key()
}

// Type reports the current block type.
func (b *timeRangeBlockReader) Type() byte {
	return b.iter.Type()
}

func (b *timeRangeBlockReader) getEntriesAndTombstones() ([]IndexEntry, []TimeRange) {
	if b.err != nil {
		return nil, nil
	}

	e := excludeEntries(b.iter.Entries(), b.tr)
	if len(e) == 0 {
		return nil, nil
	}

	b.trbuf = b.r.TombstoneRange(b.iter.Key(), b.trbuf[:0])
	var ts []TimeRange
	if len(b.trbuf) > 0 {
		ts = excludeTimeRanges(b.trbuf, b.tr)
	}

	return e, ts
}

// readBlock reads the block identified by IndexEntry e and accumulates
// statistics. readBlock returns true on success.
func (b *timeRangeBlockReader) readBlock(e *IndexEntry) bool {
	_, b.buf, b.err = b.r.ReadBytes(e, b.buf)
	if b.err != nil {
		return false
	}

	b.err = DecodeTimestampArrayBlock(b.buf, &b.a)
	if b.err != nil {
		return false
	}

	b.stats.ScannedBytes += b.a.Len() * 8 // sizeof Timestamp (int64)
	b.stats.ScannedValues += b.a.Len()
	return true
}

// Stats returns statistics accumulated by the iterator for any block reads.
func (b *timeRangeBlockReader) Stats() cursors.CursorStats {
	return b.stats
}

/*
intersectsEntry determines whether the range [min, max]
intersects one or both boundaries of IndexEntry.

          +------------------+
          |    IndexEntry    |
+---------+------------------+---------+
|  RANGE  |                  |  RANGE  |
+-+-------+-+           +----+----+----+
  |  RANGE  |           |  RANGE  |
  +----+----+-----------+---------+
       |          RANGE           |
       +--------------------------+
*/

// intersectsEntry determines if tr overlaps one or both boundaries
// of at least one element of e. If that is the case,
// and the block has no tombstones, the block timestamps do not
// need to be decoded.
func intersectsEntry(e []IndexEntry, tr TimeRange) bool {
	for i := range e {
		min, max := e[i].MinTime, e[i].MaxTime
		if tr.Overlaps(min, max) && !tr.Within(min, max) {
			return true
		}
	}
	return false
}

// excludeEntries returns a slice which excludes leading and trailing
// elements of e that are outside the time range specified by tr.
func excludeEntries(e []IndexEntry, tr TimeRange) []IndexEntry {
	for i := range e {
		if e[i].OverlapsTimeRange(tr.Min, tr.Max) {
			e = e[i:]
			break
		}
	}

	for i := range e {
		if !e[i].OverlapsTimeRange(tr.Min, tr.Max) {
			e = e[:i]
			break
		}
	}

	return e
}

// excludeTimeRanges returns a slice which excludes leading and trailing
// elements of e that are outside the time range specified by tr.
func excludeTimeRanges(e []TimeRange, tr TimeRange) []TimeRange {
	for i := range e {
		if e[i].Overlaps(tr.Min, tr.Max) {
			e = e[i:]
			break
		}
	}

	for i := range e {
		if !e[i].Overlaps(tr.Min, tr.Max) {
			e = e[:i]
			break
		}
	}

	return e
}

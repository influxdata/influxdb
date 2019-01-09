package tsm1

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"sort"
	"sync"

	"go.uber.org/zap"
)

// TSMIndex represent the index section of a TSM file.  The index records all
// blocks, their locations, sizes, min and max times.
type TSMIndex interface {
	// Delete removes the given keys from the index. Returns true if there were any changes.
	Delete(keys [][]byte) bool

	// DeleteRange removes the given keys with data between minTime and maxTime from the index.
	// Returns true if there were any changes.
	DeleteRange(keys [][]byte, minTime, maxTime int64) bool

	// DeletePrefix removes keys that begin with the given prefix with data between minTime and
	// maxTime from the index. Returns true if there were any changes. It calls dead with any
	// keys that became dead as a result of this call.
	DeletePrefix(prefix []byte, minTime, maxTime int64, dead func([]byte)) bool

	// MaybeContainsKey returns true if the given key may exist in the index. This is faster than
	// Contains but, may return false positives.
	MaybeContainsKey(key []byte) bool

	// Contains return true if the given key exists in the index.
	Contains(key []byte) bool

	// MaybeContainsValue returns true if key and time might exist in this file. This function
	// could return true even though the actual point does not exists. For example, the key may
	// exist in this file, but not have a point exactly at time t.
	MaybeContainsValue(key []byte, timestamp int64) bool

	// ReadEntries reads the index entries for key into entries.
	ReadEntries(key []byte, entries []IndexEntry) ([]IndexEntry, error)

	// Entry returns the index entry for the specified key and timestamp.  If no entry
	// matches the key and timestamp, nil is returned.
	Entry(key []byte, timestamp int64) *IndexEntry

	// KeyCount returns the count of unique keys in the index.
	KeyCount() int

	// Iterator returns an iterator over the keys starting at the provided key. You must
	// call Next before calling any of the accessors.
	Iterator([]byte) *TSMIndexIterator

	// OverlapsTimeRange returns true if the time range of the file intersect min and max.
	OverlapsTimeRange(min, max int64) bool

	// OverlapsKeyRange returns true if the min and max keys of the file overlap the arguments min and max.
	OverlapsKeyRange(min, max []byte) bool

	// Size returns the size of the current index in bytes.
	Size() uint32

	// TimeRange returns the min and max time across all keys in the file.
	TimeRange() (int64, int64)

	// TombstoneRange returns ranges of time that are deleted for the given key.
	TombstoneRange(key []byte, buf []TimeRange) []TimeRange

	// KeyRange returns the min and max keys in the file.
	KeyRange() ([]byte, []byte)

	// Type returns the block type of the values stored for the key.  Returns one of
	// BlockFloat64, BlockInt64, BlockBool, BlockString.  If key does not exist,
	// an error is returned.
	Type(key []byte) (byte, error)

	// UnmarshalBinary populates an index from an encoded byte slice
	// representation of an index.
	UnmarshalBinary(b []byte) error

	// Close closes the index and releases any resources.
	Close() error
}

// indirectIndex is a TSMIndex that uses a raw byte slice representation of an index.  This
// implementation can be used for indexes that may be MMAPed into memory.
type indirectIndex struct {
	mu     sync.RWMutex
	logger *zap.Logger

	// indirectIndex works a follows.  Assuming we have an index structure in memory as
	// the diagram below:
	//
	// ┌────────────────────────────────────────────────────────────────────┐
	// │                               Index                                │
	// ├─┬──────────────────────┬──┬───────────────────────┬───┬────────────┘
	// │0│                      │62│                       │145│
	// ├─┴───────┬─────────┬────┼──┴──────┬─────────┬──────┼───┴─────┬──────┐
	// │Key 1 Len│   Key   │... │Key 2 Len│  Key 2  │ ...  │  Key 3  │ ...  │
	// │ 2 bytes │ N bytes │    │ 2 bytes │ N bytes │      │ 2 bytes │      │
	// └─────────┴─────────┴────┴─────────┴─────────┴──────┴─────────┴──────┘

	// We would build an `offsets` slices where each element pointers to the byte location
	// for the first key in the index slice.

	// ┌────────────────────────────────────────────────────────────────────┐
	// │                              Offsets                               │
	// ├────┬────┬────┬─────────────────────────────────────────────────────┘
	// │ 0  │ 62 │145 │
	// └────┴────┴────┘

	// Using this offset slice we can find `Key 2` by doing a binary search
	// over the offsets slice.  Instead of comparing the value in the offsets
	// (e.g. `62`), we use that as an index into the underlying index to
	// retrieve the key at postion `62` and perform our comparisons with that.

	// When we have identified the correct position in the index for a given
	// key, we could perform another binary search or a linear scan.  This
	// should be fast as well since each index entry is 28 bytes and all
	// contiguous in memory.  The current implementation uses a linear scan since the
	// number of block entries is expected to be < 100 per key.

	// b is the underlying index byte slice.  This could be a copy on the heap or an MMAP
	// slice reference
	b faultBuffer

	// ro contains the positions in b for each key as well as the first bytes of each key
	// to avoid disk seeks.
	ro readerOffsets

	// minKey, maxKey are the minium and maximum (lexicographically sorted) contained in the
	// file
	minKey, maxKey []byte

	// minTime, maxTime are the minimum and maximum times contained in the file across all
	// series.
	minTime, maxTime int64

	// tombstones contains only the tombstoned keys with subset of time values deleted.  An
	// entry would exist here if a subset of the points for a key were deleted and the file
	// had not be re-compacted to remove the points on disk.
	tombstones map[uint32][]TimeRange

	// prefixTombstones contains the tombestoned keys with a subset of the values deleted that
	// all share the same prefix.
	prefixTombstones *prefixTree
}

// NewIndirectIndex returns a new indirect index.
func NewIndirectIndex() *indirectIndex {
	return &indirectIndex{
		tombstones:       make(map[uint32][]TimeRange),
		prefixTombstones: newPrefixTree(),
	}
}

// MaybeContainsKey returns true of key may exist in this index.
func (d *indirectIndex) MaybeContainsKey(key []byte) bool {
	return bytes.Compare(key, d.minKey) >= 0 && bytes.Compare(key, d.maxKey) <= 0
}

// ReadEntries returns all index entries for a key.
func (d *indirectIndex) ReadEntries(key []byte, entries []IndexEntry) ([]IndexEntry, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	iter := d.ro.Iterator()
	exact, _ := iter.Seek(key, &d.b)
	if !exact {
		return nil, nil
	}

	entries, err := readEntries(d.b.access(iter.EntryOffset(&d.b), 0), entries)
	if err != nil {
		return nil, err
	}

	return entries, nil
}

// Entry returns the index entry for the specified key and timestamp.  If no entry
// matches the key an timestamp, nil is returned.
func (d *indirectIndex) Entry(key []byte, timestamp int64) *IndexEntry {
	entries, err := d.ReadEntries(key, nil)
	if err != nil {
		d.logger.Error("error reading tsm index key", zap.String("key", fmt.Sprintf("%q", key)))
		return nil
	}
	for _, entry := range entries {
		if entry.Contains(timestamp) {
			return &entry
		}
	}
	return nil
}

// KeyCount returns the count of unique keys in the index.
func (d *indirectIndex) KeyCount() int {
	d.mu.RLock()
	n := len(d.ro.offsets)
	d.mu.RUnlock()
	return n
}

// Iterator returns an iterator over the keys starting at the provided key. You must
// call Next before calling any of the accessors.
func (d *indirectIndex) Iterator(key []byte) *TSMIndexIterator {
	d.mu.RLock()
	iter := d.ro.Iterator()
	_, ok := iter.Seek(key, &d.b)
	ti := &TSMIndexIterator{
		d:     d,
		n:     int(len(d.ro.offsets)),
		b:     &d.b,
		iter:  &iter,
		first: true,
		ok:    ok,
	}
	d.mu.RUnlock()

	return ti
}

// Delete removes the given keys from the index.
func (d *indirectIndex) Delete(keys [][]byte) bool {
	if len(keys) == 0 {
		return false
	}

	d.mu.RLock()
	iter := d.ro.Iterator()
	for _, key := range keys {
		if !iter.Next() || !bytes.Equal(iter.Key(&d.b), key) {
			if exact, _ := iter.Seek(key, &d.b); !exact {
				continue
			}
		}

		delete(d.tombstones, iter.Offset())
		iter.Delete()
	}
	d.mu.RUnlock()

	if !iter.HasDeletes() {
		return false
	}

	d.mu.Lock()
	iter.Done()
	d.mu.Unlock()

	return true
}

// insertTimeRange adds a time range described by the minTime and maxTime into ts.
func insertTimeRange(ts []TimeRange, minTime, maxTime int64) []TimeRange {
	n := sort.Search(len(ts), func(i int) bool {
		if ts[i].Min == minTime {
			return ts[i].Max >= maxTime
		}
		return ts[i].Min > minTime
	})

	ts = append(ts, TimeRange{})
	copy(ts[n+1:], ts[n:])
	ts[n] = TimeRange{Min: minTime, Max: maxTime}
	return ts
}

// pendingTombstone is a type that describes a pending insertion of a tombstone.
type pendingTombstone struct {
	Key         int
	Index       int
	Offset      uint32
	EntryOffset uint32
	Tombstones  int
}

// coversEntries checks if all of the stored tombstones including one for minTime and maxTime cover
// all of the index entries. It mutates the entries slice to do the work, so be sure to make a copy
// if you must.
func (d *indirectIndex) coversEntries(offset uint32, key []byte, buf []TimeRange,
	entries []IndexEntry, minTime, maxTime int64) ([]TimeRange, bool) {

	// grab the tombstones from the prefixes. these come out unsorted, so we sort
	// them and place them in the merger section named unsorted.
	buf = d.prefixTombstones.Search(key, buf[:0])
	if len(buf) > 1 {
		sort.Slice(buf, func(i, j int) bool { return buf[i].Less(buf[j]) })
	}

	// create the merger with the other tombstone entries: the ones for the specific
	// key and the one we have proposed to add.
	merger := timeRangeMerger{
		sorted:   d.tombstones[offset],
		unsorted: buf,
		single:   TimeRange{Min: minTime, Max: maxTime},
		used:     false,
	}

	return buf, timeRangesCoverEntries(merger, entries)
}

// DeleteRange removes the given keys with data between minTime and maxTime from the index.
func (d *indirectIndex) DeleteRange(keys [][]byte, minTime, maxTime int64) bool {
	// If we're deleting everything, we won't need to worry about partial deletes.
	if minTime <= d.minTime && maxTime >= d.maxTime {
		return d.Delete(keys)
	}

	// Is the range passed in outside of the time range for the file?
	if minTime > d.maxTime || maxTime < d.minTime {
		return false
	}

	// General outline:
	// Under the read lock, determine the set of actions we need to
	// take and on what keys to take them. Then, under the write
	// lock, perform those actions. We keep track of some state
	// during the read lock to make double checking under the
	// write lock cheap.

	d.mu.RLock()
	iter := d.ro.Iterator()
	var (
		ok      bool
		trbuf   []TimeRange
		entries []IndexEntry
		pending []pendingTombstone
		err     error
	)

	for i, key := range keys {
		if !iter.Next() || !bytes.Equal(iter.Key(&d.b), key) {
			if exact, _ := iter.Seek(key, &d.b); !exact {
				continue
			}
		}

		entryOffset := iter.EntryOffset(&d.b)
		entries, err = readEntriesTimes(d.b.access(entryOffset, 0), entries)
		if err != nil {
			// If we have an error reading the entries for a key, we should just pretend
			// the whole key is deleted. Maybe a better idea is to report this up somehow
			// but that's for another time.
			iter.Delete()
			continue
		}

		// Is the time range passed outside of the time range we have stored for this key?
		min, max := entries[0].MinTime, entries[len(entries)-1].MaxTime
		if minTime > max || maxTime < min {
			continue
		}

		// Does the range passed in cover every value for the key?
		if minTime <= min && maxTime >= max {
			iter.Delete()
			continue
		}

		// Does adding the minTime and maxTime cover the entries?
		offset := iter.Offset()
		trbuf, ok = d.coversEntries(offset, key, trbuf, entries, minTime, maxTime)
		if ok {
			iter.Delete()
			continue
		}

		// Save that we should add a tombstone for this key, and how many tombstones
		// already existed to avoid double checks.
		pending = append(pending, pendingTombstone{
			Key:         i,
			Index:       iter.Index(),
			Offset:      offset,
			EntryOffset: entryOffset,
			Tombstones:  len(d.tombstones[offset]),
		})
	}

	d.mu.RUnlock()

	if len(pending) == 0 && !iter.HasDeletes() {
		return false
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	for _, p := range pending {
		// Check the existing tombstones. If the length did not/ change, then we know
		// that we don't need to double check coverage, since we only ever increase the
		// number of tombstones for a key.
		if trs := d.tombstones[p.Offset]; p.Tombstones == len(trs) {
			d.tombstones[p.Offset] = insertTimeRange(trs, minTime, maxTime)
			continue
		}

		// Since the length changed, we have to do the expensive overlap check again.
		// We re-read the entries again under the write lock because this should be
		// rare and only during concurrent deletes to the same key. We could make
		// a copy of the entries before getting here, but that penalizes the common
		// no-concurrent case.
		entries, err = readEntriesTimes(d.b.access(p.EntryOffset, 0), entries)
		if err != nil {
			// If we have an error reading the entries for a key, we should just pretend
			// the whole key is deleted. Maybe a better idea is to report this up somehow
			// but that's for another time.
			delete(d.tombstones, p.Offset)
			iter.SetIndex(p.Index)
			if iter.Offset() == p.Offset {
				iter.Delete()
			}
			continue
		}

		trbuf, ok = d.coversEntries(p.Offset, keys[p.Key], trbuf, entries, minTime, maxTime)
		if ok {
			delete(d.tombstones, p.Offset)
			iter.SetIndex(p.Index)
			if iter.Offset() == p.Offset {
				iter.Delete()
			}
			continue
		}

		// Append the TimeRange into the tombstones.
		trs := d.tombstones[p.Offset]
		d.tombstones[p.Offset] = insertTimeRange(trs, minTime, maxTime)
	}

	iter.Done()
	return true
}

// DeletePrefix removes keys that begin with the given prefix with data between minTime and
// maxTime from the index. Returns true if there were any changes. It calls dead with any
// keys that became dead as a result of this call.
func (d *indirectIndex) DeletePrefix(prefix []byte, minTime, maxTime int64, dead func([]byte)) bool {
	if dead == nil {
		dead = func([]byte) {}
	}

	// If we're deleting everything, we won't need to worry about partial deletes.
	partial := !(minTime <= d.minTime && maxTime >= d.maxTime)

	// Is the range passed in outside of the time range for the file?
	if minTime > d.maxTime || maxTime < d.minTime {
		return false
	}

	d.mu.RLock()
	var (
		ok        bool
		trbuf     []TimeRange
		entries   []IndexEntry
		err       error
		mustTrack bool
	)

	// seek to the earliest key with the prefix, and start iterating. we can't call
	// next until after we've checked the key, so keep a "first" flag.
	first := true
	iter := d.ro.Iterator()
	for {
		if first {
			if _, ok := iter.Seek(prefix, &d.b); !ok {
				break
			}
		} else if !iter.Next() {
			break
		}

		first = false
		key := iter.Key(&d.b)
		if !bytes.HasPrefix(key, prefix) {
			break
		}

		// if we're not doing a partial delete, we don't need to read the entries and
		// can just delete the key and move on.
		if !partial {
			dead(key)
			iter.Delete()
			continue
		}

		entryOffset := iter.EntryOffset(&d.b)
		entries, err = readEntriesTimes(d.b.access(entryOffset, 0), entries)
		if err != nil {
			// If we have an error reading the entries for a key, we should just pretend
			// the whole key is deleted. Maybe a better idea is to report this up somehow
			// but that's for another time.
			dead(key)
			iter.Delete()
			continue
		}

		// Is the time range passed outside the range we have stored for the key?
		min, max := entries[0].MinTime, entries[len(entries)-1].MaxTime
		if minTime > max || maxTime < min {
			continue
		}

		// Does the range passed cover every value for the key?
		if minTime <= min && maxTime >= max {
			dead(key)
			iter.Delete()
			continue
		}

		// Does adding the minTime and maxTime cover the entries?
		trbuf, ok = d.coversEntries(iter.Offset(), iter.Key(&d.b), trbuf, entries, minTime, maxTime)
		if ok {
			dead(key)
			iter.Delete()
			continue
		}

		// Otherwise, we have to track it in the prefix tombstones list.
		mustTrack = true
	}
	d.mu.RUnlock()

	// Check and abort if nothing needs to be done.
	if !mustTrack && !iter.HasDeletes() {
		return false
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	if mustTrack {
		d.prefixTombstones.Append(prefix, TimeRange{Min: minTime, Max: maxTime})
	}

	if iter.HasDeletes() {
		iter.Done()
	}

	return true
}

// TombstoneRange returns ranges of time that are deleted for the given key.
func (d *indirectIndex) TombstoneRange(key []byte, buf []TimeRange) []TimeRange {
	d.mu.RLock()
	rs := d.prefixTombstones.Search(key, buf[:0])
	iter := d.ro.Iterator()
	exact, _ := iter.Seek(key, &d.b)
	if exact {
		rs = append(rs, d.tombstones[iter.Offset()]...)
	}
	d.mu.RUnlock()
	return rs
}

// Contains return true if the given key exists in the index.
func (d *indirectIndex) Contains(key []byte) bool {
	d.mu.RLock()
	iter := d.ro.Iterator()
	exact, _ := iter.Seek(key, &d.b)
	d.mu.RUnlock()
	return exact
}

// MaybeContainsValue returns true if key and time might exist in this file.
func (d *indirectIndex) MaybeContainsValue(key []byte, timestamp int64) bool {
	d.mu.RLock()
	defer d.mu.RUnlock()

	iter := d.ro.Iterator()
	exact, _ := iter.Seek(key, &d.b)
	if !exact {
		return false
	}

	for _, t := range d.tombstones[iter.Offset()] {
		if t.Min <= timestamp && timestamp <= t.Max {
			return false
		}
	}

	if d.prefixTombstones.checkOverlap(key, timestamp) {
		return false
	}

	entries, err := d.ReadEntries(key, nil)
	if err != nil {
		d.logger.Error("error reading tsm index key", zap.String("key", fmt.Sprintf("%q", key)))
		return false
	}

	for _, entry := range entries {
		if entry.Contains(timestamp) {
			return true
		}
	}

	return false
}

// Type returns the block type of the values stored for the key.
func (d *indirectIndex) Type(key []byte) (byte, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	iter := d.ro.Iterator()
	exact, _ := iter.Seek(key, &d.b)
	if !exact {
		return 0, errors.New("key does not exist")
	}

	return d.b.access(iter.EntryOffset(&d.b), 1)[0], nil
}

// OverlapsTimeRange returns true if the time range of the file intersect min and max.
func (d *indirectIndex) OverlapsTimeRange(min, max int64) bool {
	return d.minTime <= max && d.maxTime >= min
}

// OverlapsKeyRange returns true if the min and max keys of the file overlap the arguments min and max.
func (d *indirectIndex) OverlapsKeyRange(min, max []byte) bool {
	return bytes.Compare(d.minKey, max) <= 0 && bytes.Compare(d.maxKey, min) >= 0
}

// KeyRange returns the min and max keys in the index.
func (d *indirectIndex) KeyRange() ([]byte, []byte) {
	return d.minKey, d.maxKey
}

// TimeRange returns the min and max time across all keys in the index.
func (d *indirectIndex) TimeRange() (int64, int64) {
	return d.minTime, d.maxTime
}

// MarshalBinary returns a byte slice encoded version of the index.
func (d *indirectIndex) MarshalBinary() ([]byte, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	return d.b.b, nil
}

// UnmarshalBinary populates an index from an encoded byte slice
// representation of an index.
func (d *indirectIndex) UnmarshalBinary(b []byte) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	// Keep a reference to the actual index bytes
	d.b = faultBuffer{b: b}
	if len(b) == 0 {
		return nil
	}

	// make sure a uint32 is sufficient to store any offset into the index.
	if uint64(len(b)) != uint64(uint32(len(b))) {
		return fmt.Errorf("indirectIndex: too large to open")
	}

	var minTime, maxTime int64 = math.MaxInt64, math.MinInt64

	// To create our "indirect" index, we need to find the location of all the keys in
	// the raw byte slice.  The keys are listed once each (in sorted order).  Following
	// each key is a time ordered list of index entry blocks for that key.  The loop below
	// basically skips across the slice keeping track of the counter when we are at a key
	// field.
	var i uint32
	var ro readerOffsets

	iMax := uint32(len(b))
	if iMax > math.MaxInt32 {
		return fmt.Errorf("indirectIndex: too large to store offsets")
	}

	for i < iMax {
		offset := i // save for when we add to the data structure

		// Skip to the start of the values
		// key length value (2) + type (1) + length of key
		if i+2 >= iMax {
			return fmt.Errorf("indirectIndex: not enough data for key length value")
		}
		keyLength := uint32(binary.BigEndian.Uint16(b[i : i+2]))
		i += 2

		if i+keyLength+indexTypeSize >= iMax {
			return fmt.Errorf("indirectIndex: not enough data for key and type")
		}
		ro.AddKey(offset, b[i:i+keyLength])
		i += keyLength + indexTypeSize

		// count of index entries
		if i+indexCountSize >= iMax {
			return fmt.Errorf("indirectIndex: not enough data for index entries count")
		}
		count := uint32(binary.BigEndian.Uint16(b[i : i+indexCountSize]))
		if count == 0 {
			return fmt.Errorf("indirectIndex: key exits with no entries")
		}
		i += indexCountSize

		// Find the min time for the block
		if i+8 >= iMax {
			return fmt.Errorf("indirectIndex: not enough data for min time")
		}
		minT := int64(binary.BigEndian.Uint64(b[i : i+8]))
		if minT < minTime {
			minTime = minT
		}

		i += (count - 1) * indexEntrySize

		// Find the max time for the block
		if i+16 >= iMax {
			return fmt.Errorf("indirectIndex: not enough data for max time")
		}
		maxT := int64(binary.BigEndian.Uint64(b[i+8 : i+16]))
		if maxT > maxTime {
			maxTime = maxT
		}

		i += indexEntrySize
	}

	ro.Done()

	firstOfs := ro.offsets[0]
	key := readKey(b[firstOfs:])
	d.minKey = key

	lastOfs := ro.offsets[len(ro.offsets)-1]
	key = readKey(b[lastOfs:])
	d.maxKey = key

	d.minTime = minTime
	d.maxTime = maxTime
	d.ro = ro

	return nil
}

// Size returns the size of the current index in bytes.
func (d *indirectIndex) Size() uint32 {
	d.mu.RLock()
	defer d.mu.RUnlock()

	return d.b.len()
}

func (d *indirectIndex) Close() error {
	return nil
}

func readKey(b []byte) (key []byte) {
	size := binary.BigEndian.Uint16(b[:2])
	return b[2 : 2+size]
}

func readEntries(b []byte, entries []IndexEntry) ([]IndexEntry, error) {
	if len(b) < indexTypeSize+indexCountSize {
		return entries[:0], errors.New("readEntries: data too short for headers")
	}

	count := int(binary.BigEndian.Uint16(b[indexTypeSize : indexTypeSize+indexCountSize]))
	if cap(entries) < count {
		entries = make([]IndexEntry, count)
	} else {
		entries = entries[:count]
	}
	b = b[indexTypeSize+indexCountSize:]

	for i := range entries {
		if err := entries[i].UnmarshalBinary(b); err != nil {
			return entries[:0], err
		}
		b = b[indexEntrySize:]
	}

	return entries, nil
}

// readEntriesTimes is a helper function to read entries at the provided buffer but
// only reading in the min and max times.
func readEntriesTimes(b []byte, entries []IndexEntry) ([]IndexEntry, error) {
	if len(b) < indexTypeSize+indexCountSize {
		return entries[:0], errors.New("readEntries: data too short for headers")
	}

	count := int(binary.BigEndian.Uint16(b[indexTypeSize : indexTypeSize+indexCountSize]))
	if cap(entries) < count {
		entries = make([]IndexEntry, count)
	} else {
		entries = entries[:count]
	}
	b = b[indexTypeSize+indexCountSize:]

	for i := range entries {
		if len(b) < indexEntrySize {
			return entries[:0], errors.New("readEntries: stream too short for entry")
		}
		entries[i].MinTime = int64(binary.BigEndian.Uint64(b[0:8]))
		entries[i].MaxTime = int64(binary.BigEndian.Uint64(b[8:16]))
		b = b[indexEntrySize:]
	}

	return entries, nil
}

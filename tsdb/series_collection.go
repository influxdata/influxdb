package tsdb

import (
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/influxdata/platform/models"
	"github.com/influxdata/platform/pkg/bytesutil"
)

// SeriesCollection is a struct of arrays representation of a collection of series that allows
// for efficient filtering.
type SeriesCollection struct {
	Points     []models.Point
	Keys       [][]byte
	SeriesKeys [][]byte
	Names      [][]byte
	Tags       []models.Tags
	Types      []models.FieldType
	SeriesIDs  []SeriesID

	// Keeps track of invalid entries.
	Dropped     uint64
	DroppedKeys [][]byte
	Reason      string

	// Used by the concurrent iterators to stage drops. Inefficient, but should be
	// very infrequently used.
	state *seriesCollectionState
}

// seriesCollectionState keeps track of concurrent iterator state.
type seriesCollectionState struct {
	mu     sync.Mutex
	reason string
	index  map[int]struct{}
}

// NewSeriesCollection builds a SeriesCollection from a slice of points. It does some filtering
// of invalid points.
func NewSeriesCollection(points []models.Point) *SeriesCollection {
	out := &SeriesCollection{
		Points: append([]models.Point(nil), points...),
		Keys:   make([][]byte, 0, len(points)),
		Names:  make([][]byte, 0, len(points)),
		Tags:   make([]models.Tags, 0, len(points)),
		Types:  make([]models.FieldType, 0, len(points)),
	}

	for _, pt := range points {
		out.Keys = append(out.Keys, pt.Key())
		out.Names = append(out.Names, pt.Name())
		out.Tags = append(out.Tags, pt.Tags())

		fi := pt.FieldIterator()
		fi.Next()
		out.Types = append(out.Types, fi.Type())
	}

	return out
}

// Duplicate returns a copy of the SeriesCollection. The slices are shared. Appending to any of
// them may or may not be reflected.
func (s SeriesCollection) Duplicate() *SeriesCollection { return &s }

// Length returns the length of the first non-nil slice in the collection, or 0 if there is no
// non-nil slice.
func (s *SeriesCollection) Length() int {
	switch {
	case s.Points != nil:
		return len(s.Points)
	case s.Keys != nil:
		return len(s.Keys)
	case s.SeriesKeys != nil:
		return len(s.SeriesKeys)
	case s.Names != nil:
		return len(s.Names)
	case s.Tags != nil:
		return len(s.Tags)
	case s.Types != nil:
		return len(s.Types)
	case s.SeriesIDs != nil:
		return len(s.SeriesIDs)
	default:
		return 0
	}
}

// Copy will copy the element at src into dst in all slices that can: x[dst] = x[src].
func (s *SeriesCollection) Copy(dst, src int) {
	if dst == src {
		return
	}
	udst, usrc := uint(dst), uint(src)
	if n := uint(len(s.Points)); udst < n && usrc < n {
		s.Points[udst] = s.Points[usrc]
	}
	if n := uint(len(s.Keys)); udst < n && usrc < n {
		s.Keys[udst] = s.Keys[usrc]
	}
	if n := uint(len(s.SeriesKeys)); udst < n && usrc < n {
		s.SeriesKeys[udst] = s.SeriesKeys[usrc]
	}
	if n := uint(len(s.Names)); udst < n && usrc < n {
		s.Names[udst] = s.Names[usrc]
	}
	if n := uint(len(s.Tags)); udst < n && usrc < n {
		s.Tags[udst] = s.Tags[usrc]
	}
	if n := uint(len(s.Types)); udst < n && usrc < n {
		s.Types[udst] = s.Types[usrc]
	}
	if n := uint(len(s.SeriesIDs)); udst < n && usrc < n {
		s.SeriesIDs[udst] = s.SeriesIDs[usrc]
	}
}

// Swap will swap the elements at i and j in all slices that can: x[i], x[j] = x[j], x[i].
func (s *SeriesCollection) Swap(i, j int) {
	if i == j {
		return
	}
	ui, uj := uint(i), uint(j)
	if n := uint(len(s.Points)); ui < n && uj < n {
		s.Points[ui], s.Points[uj] = s.Points[uj], s.Points[ui]
	}
	if n := uint(len(s.Keys)); ui < n && uj < n {
		s.Keys[ui], s.Keys[uj] = s.Keys[uj], s.Keys[ui]
	}
	if n := uint(len(s.SeriesKeys)); ui < n && uj < n {
		s.SeriesKeys[ui], s.SeriesKeys[uj] = s.SeriesKeys[uj], s.SeriesKeys[ui]
	}
	if n := uint(len(s.Names)); ui < n && uj < n {
		s.Names[ui], s.Names[uj] = s.Names[uj], s.Names[ui]
	}
	if n := uint(len(s.Tags)); ui < n && uj < n {
		s.Tags[ui], s.Tags[uj] = s.Tags[uj], s.Tags[ui]
	}
	if n := uint(len(s.Types)); ui < n && uj < n {
		s.Types[ui], s.Types[uj] = s.Types[uj], s.Types[ui]
	}
	if n := uint(len(s.SeriesIDs)); ui < n && uj < n {
		s.SeriesIDs[ui], s.SeriesIDs[uj] = s.SeriesIDs[uj], s.SeriesIDs[ui]
	}
}

// Truncate will truncate all of the slices that can down to length: x = x[:length].
func (s *SeriesCollection) Truncate(length int) {
	ulength := uint(length)
	if ulength < uint(len(s.Points)) {
		s.Points = s.Points[:ulength]
	}
	if ulength < uint(len(s.Keys)) {
		s.Keys = s.Keys[:ulength]
	}
	if ulength < uint(len(s.SeriesKeys)) {
		s.SeriesKeys = s.SeriesKeys[:ulength]
	}
	if ulength < uint(len(s.Names)) {
		s.Names = s.Names[:ulength]
	}
	if ulength < uint(len(s.Tags)) {
		s.Tags = s.Tags[:ulength]
	}
	if ulength < uint(len(s.Types)) {
		s.Types = s.Types[:ulength]
	}
	if ulength < uint(len(s.SeriesIDs)) {
		s.SeriesIDs = s.SeriesIDs[:ulength]
	}
}

// Advance will advance all of the slices that can length elements: x = x[length:].
func (s *SeriesCollection) Advance(length int) {
	ulength := uint(length)
	if ulength < uint(len(s.Points)) {
		s.Points = s.Points[ulength:]
	}
	if ulength < uint(len(s.Keys)) {
		s.Keys = s.Keys[ulength:]
	}
	if ulength < uint(len(s.SeriesKeys)) {
		s.SeriesKeys = s.SeriesKeys[ulength:]
	}
	if ulength < uint(len(s.Names)) {
		s.Names = s.Names[ulength:]
	}
	if ulength < uint(len(s.Tags)) {
		s.Tags = s.Tags[ulength:]
	}
	if ulength < uint(len(s.Types)) {
		s.Types = s.Types[ulength:]
	}
	if ulength < uint(len(s.SeriesIDs)) {
		s.SeriesIDs = s.SeriesIDs[ulength:]
	}
}

// InvalidateAll causes all of the entries to become invalid.
func (s *SeriesCollection) InvalidateAll(reason string) {
	if s.Reason == "" {
		s.Reason = reason
	}
	s.Dropped += uint64(len(s.Keys))
	s.DroppedKeys = append(s.DroppedKeys, s.Keys...)
	s.Truncate(0)
}

// ApplyConcurrentDrops will remove all of the dropped values during concurrent iteration. It should
// not be called concurrently with any calls to Invalid.
func (s *SeriesCollection) ApplyConcurrentDrops() {
	state := s.getState(false)
	if state == nil {
		return
	}

	length, j := s.Length(), 0
	for i := 0; i < length; i++ {
		if _, ok := state.index[i]; ok {
			s.Dropped++

			if i < len(s.Keys) {
				s.DroppedKeys = append(s.DroppedKeys, s.Keys[i])
			}

			continue
		}

		s.Copy(j, i)
		j++
	}
	s.Truncate(j)

	if s.Reason == "" {
		s.Reason = state.reason
	}

	// clear concurrent state
	atomic.StorePointer((*unsafe.Pointer)(unsafe.Pointer(&s.state)), nil)
}

// getState returns the SeriesCollection's concurrent state. If alloc is true and there
// is no state, it will attempt to allocate one and set it. It is safe to call concurrently, but
// not with ApplyConcurrentDrops.
func (s *SeriesCollection) getState(alloc bool) *seriesCollectionState {
	addr := (*unsafe.Pointer)(unsafe.Pointer(&s.state))

	// fast path: load pointer and it already exists. always return the result if we can't alloc.
	if ptr := atomic.LoadPointer(addr); ptr != nil || !alloc {
		return (*seriesCollectionState)(ptr)
	}

	// nothing there. make a new state and try to swap it in.
	atomic.CompareAndSwapPointer(addr, nil, unsafe.Pointer(new(seriesCollectionState)))

	// reload the pointer. this way we always end up with the winner of the race.
	return (*seriesCollectionState)(atomic.LoadPointer(addr))
}

// invalidIndex stages the index as invalid with the reason. It will be removed when
// ApplyConcurrentDrops is called.
func (s *SeriesCollection) invalidIndex(index int, reason string) {
	state := s.getState(true)

	state.mu.Lock()
	if state.index == nil {
		state.index = make(map[int]struct{})
	}
	state.index[index] = struct{}{}
	if state.reason == "" {
		state.reason = reason
	}
	state.mu.Unlock()
}

// PartialWriteError returns a PartialWriteError if any entries have been marked as invalid. It
// returns an error to avoid `return collection.PartialWriteError()` always being non-nil.
func (s *SeriesCollection) PartialWriteError() error {
	if s.Dropped == 0 {
		return nil
	}
	droppedKeys := bytesutil.SortDedup(s.DroppedKeys)
	return PartialWriteError{
		Reason:      s.Reason,
		Dropped:     len(droppedKeys),
		DroppedKeys: droppedKeys,
	}
}

// Iterator returns a new iterator over the entries in the collection. Multiple iterators
// can exist at the same time. Marking entries as invalid/skipped is more expensive, but thread
// safe. You must call ApplyConcurrentDrops after all of the iterators are finished.
func (s *SeriesCollection) Iterator() SeriesCollectionIterator {
	return SeriesCollectionIterator{
		s:      s,
		length: s.Length(),
		index:  -1,
	}
}

// SeriesCollectionIterator is an iterator over the collection of series.
type SeriesCollectionIterator struct {
	s      *SeriesCollection
	length int
	index  int
}

// Next advances the iterator and returns false if it's done.
func (i *SeriesCollectionIterator) Next() bool {
	i.index++
	return i.index < i.length
}

// Helpers that return the current state of the iterator.

func (i SeriesCollectionIterator) Index() int             { return i.index }
func (i SeriesCollectionIterator) Length() int            { return i.length }
func (i SeriesCollectionIterator) Point() models.Point    { return i.s.Points[i.index] }
func (i SeriesCollectionIterator) Key() []byte            { return i.s.Keys[i.index] }
func (i SeriesCollectionIterator) SeriesKey() []byte      { return i.s.SeriesKeys[i.index] }
func (i SeriesCollectionIterator) Name() []byte           { return i.s.Names[i.index] }
func (i SeriesCollectionIterator) Tags() models.Tags      { return i.s.Tags[i.index] }
func (i SeriesCollectionIterator) Type() models.FieldType { return i.s.Types[i.index] }
func (i SeriesCollectionIterator) SeriesID() SeriesID     { return i.s.SeriesIDs[i.index] }

// Invalid flags the current entry as invalid, including it in the set of dropped keys and
// recording a reason. Only the first reason is kept. This is safe for concurrent callers,
// but ApplyConcurrentDrops must be called after all iterators are finished.
func (i *SeriesCollectionIterator) Invalid(reason string) {
	i.s.invalidIndex(i.index, reason)
}

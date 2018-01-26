package tsdb

import (
	"io"
	"sync"

	"github.com/RoaringBitmap/roaring"
)

// SeriesIDSet represents a lockable bitmap of series ids.
type SeriesIDSet struct {
	sync.RWMutex
	bitmap *roaring.Bitmap
}

// NewSeriesIDSet returns a new instance of SeriesIDSet.
func NewSeriesIDSet() *SeriesIDSet {
	return &SeriesIDSet{
		bitmap: roaring.NewBitmap(),
	}
}

// Add adds the series id to the set.
func (s *SeriesIDSet) Add(id uint64) {
	s.Lock()
	defer s.Unlock()
	s.AddNoLock(id)
}

// AddNoLock adds the series id to the set. Add is not safe for use from multiple
// goroutines. Callers must manage synchronization.
func (s *SeriesIDSet) AddNoLock(id uint64) {
	s.bitmap.Add(uint32(id))
}

// Contains returns true if the id exists in the set.
func (s *SeriesIDSet) Contains(id uint64) bool {
	s.RLock()
	x := s.ContainsNoLock(id)
	s.RUnlock()
	return x
}

// ContainsNoLock returns true if the id exists in the set. ContainsNoLock is
// not safe for use from multiple goroutines. The caller must manage synchronization.
func (s *SeriesIDSet) ContainsNoLock(id uint64) bool {
	return s.bitmap.Contains(uint32(id))
}

// Remove removes the id from the set.
func (s *SeriesIDSet) Remove(id uint64) {
	s.Lock()
	defer s.Unlock()
	s.RemoveNoLock(id)
}

// RemoveNoLock removes the id from the set. RemoveNoLock is not safe for use
// from multiple goroutines. The caller must manage synchronization.
func (s *SeriesIDSet) RemoveNoLock(id uint64) {
	s.bitmap.Remove(uint32(id))
}

// Cardinality returns the cardinality of the SeriesIDSet.
func (s *SeriesIDSet) Cardinality() uint64 {
	s.RLock()
	defer s.RUnlock()
	return s.bitmap.GetCardinality()
}

// Merge merged the contents of others into s. The caller does not need to
// provide s as an argument, and the contents of s will always be present in s
// after Merge returns.
func (s *SeriesIDSet) Merge(others ...*SeriesIDSet) {
	bms := make([]*roaring.Bitmap, 0, len(others)+1)

	s.RLock()
	bms = append(bms, s.bitmap) // Add ourself.

	// Add other bitsets.
	for _, other := range others {
		other.RLock()
		defer other.RUnlock() // Hold until we have merged all the bitmaps
		bms = append(bms, other.bitmap)
	}

	result := roaring.FastOr(bms...)
	s.RUnlock()

	s.Lock()
	s.bitmap = result
	s.Unlock()
}

// Equals returns true if other and s are the same set of ids.
func (s *SeriesIDSet) Equals(other *SeriesIDSet) bool {
	if s == other {
		return true
	}

	s.RLock()
	defer s.RUnlock()
	other.RLock()
	defer other.RUnlock()
	return s.bitmap.Equals(other.bitmap)
}

// AndNot returns a new SeriesIDSet containing elements that were present in s,
// but not present in other.
func (s *SeriesIDSet) AndNot(other *SeriesIDSet) *SeriesIDSet {
	s.RLock()
	defer s.RUnlock()
	other.RLock()
	defer other.RUnlock()

	return &SeriesIDSet{bitmap: roaring.AndNot(s.bitmap, other.bitmap)}
}

// ForEach calls f for each id in the set.
func (s *SeriesIDSet) ForEach(f func(id uint64)) {
	s.RLock()
	defer s.RUnlock()
	itr := s.bitmap.Iterator()
	for itr.HasNext() {
		f(uint64(itr.Next()))
	}
}

func (s *SeriesIDSet) String() string {
	s.RLock()
	defer s.RUnlock()
	return s.bitmap.String()
}

// Diff removes from s any elements also present in other.
func (s *SeriesIDSet) Diff(other *SeriesIDSet) {
	other.RLock()
	defer other.RUnlock()

	s.Lock()
	defer s.Unlock()
	s.bitmap = roaring.AndNot(s.bitmap, other.bitmap)
}

// UnmarshalBinary unmarshals data into the set.
func (s *SeriesIDSet) UnmarshalBinary(data []byte) error {
	s.Lock()
	defer s.Unlock()
	return s.bitmap.UnmarshalBinary(data)
}

// WriteTo writes the set to w.
func (s *SeriesIDSet) WriteTo(w io.Writer) (int64, error) {
	s.RLock()
	defer s.RUnlock()
	return s.bitmap.WriteTo(w)
}

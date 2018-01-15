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
	defer s.RUnlock()
	return s.ContainsNoLock(id)
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

// Merge merged the contents of others into s.
func (s *SeriesIDSet) Merge(others ...*SeriesIDSet) {
	bms := make([]*roaring.Bitmap, 0, len(others))
	for _, other := range others {
		other.RLock()
		bms = append(bms, other.bitmap)
		other.RUnlock()
	}

	s.Lock()
	defer s.Unlock()
	s.bitmap = roaring.FastOr(bms...)
}

// Diff deletes any bits set in other.
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

package tsdb

import (
	"io"
	"sync"
	"unsafe"

	"github.com/RoaringBitmap/roaring"
)

// SeriesIDSet represents a lockable bitmap of series ids.
type SeriesIDSet struct {
	sync.RWMutex
	bitmap *roaring.Bitmap
}

// NewSeriesIDSet returns a new instance of SeriesIDSet.
func NewSeriesIDSet(a ...SeriesID) *SeriesIDSet {
	ss := &SeriesIDSet{bitmap: roaring.NewBitmap()}
	if len(a) > 0 {
		a32 := make([]uint32, len(a))
		for i := range a {
			a32[i] = uint32(a[i].RawID())
		}
		ss.bitmap.AddMany(a32)
	}
	return ss
}

// NewSeriesIDSetNegate returns a new SeriesIDSet containing all the elements in a
// that are not present in b. That is, the set difference between a and b.
func NewSeriesIDSetNegate(a, b *SeriesIDSet) *SeriesIDSet {
	a.RLock()
	b.RLock()
	result := &SeriesIDSet{bitmap: roaring.AndNot(a.bitmap, b.bitmap)}
	a.RUnlock()
	b.RUnlock()
	return result
}

// Bytes estimates the memory footprint of this SeriesIDSet, in bytes.
func (s *SeriesIDSet) Bytes() int {
	var b int
	s.RLock()
	b += 24 // mu RWMutex is 24 bytes
	b += int(unsafe.Sizeof(s.bitmap)) + int(s.bitmap.GetSizeInBytes())
	s.RUnlock()
	return b
}

// Add adds the series id to the set.
func (s *SeriesIDSet) Add(id SeriesID) {
	s.Lock()
	s.AddNoLock(id)
	s.Unlock()
}

// AddNoLock adds the series id to the set. Add is not safe for use from multiple
// goroutines. Callers must manage synchronization.
func (s *SeriesIDSet) AddNoLock(id SeriesID) {
	s.bitmap.Add(uint32(id.RawID()))
}

// AddMany adds multiple ids to the SeriesIDSet. AddMany takes a lock, so may not be
// optimal to call many times with few ids.
func (s *SeriesIDSet) AddMany(ids ...SeriesID) {
	if len(ids) == 0 {
		return
	}

	a32 := make([]uint32, len(ids))
	for i := range ids {
		a32[i] = uint32(ids[i].RawID())
	}

	s.Lock()
	s.bitmap.AddMany(a32)
	s.Unlock()
}

// Contains returns true if the id exists in the set.
func (s *SeriesIDSet) Contains(id SeriesID) bool {
	s.RLock()
	contains := s.ContainsNoLock(id)
	s.RUnlock()
	return contains
}

// ContainsNoLock returns true if the id exists in the set. ContainsNoLock is
// not safe for use from multiple goroutines. The caller must manage synchronization.
func (s *SeriesIDSet) ContainsNoLock(id SeriesID) bool {
	return s.bitmap.Contains(uint32(id.RawID()))
}

// Remove removes the id from the set.
func (s *SeriesIDSet) Remove(id SeriesID) {
	s.Lock()
	s.RemoveNoLock(id)
	s.Unlock()
}

// RemoveNoLock removes the id from the set. RemoveNoLock is not safe for use
// from multiple goroutines. The caller must manage synchronization.
func (s *SeriesIDSet) RemoveNoLock(id SeriesID) {
	s.bitmap.Remove(uint32(id.RawID()))
}

// Cardinality returns the cardinality of the SeriesIDSet.
func (s *SeriesIDSet) Cardinality() uint64 {
	s.RLock()
	cardinality := s.bitmap.GetCardinality()
	s.RUnlock()
	return cardinality
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
		bms = append(bms, other.bitmap)
	}

	result := roaring.FastOr(bms...)

	for _, other := range others {
		other.RUnlock()
	}
	s.RUnlock()

	s.Lock()
	s.bitmap = result
	s.Unlock()
}

// MergeInPlace merges other into s, modifying s in the process.
func (s *SeriesIDSet) MergeInPlace(other *SeriesIDSet) {
	if s == other {
		return
	}

	other.RLock()
	s.Lock()
	s.bitmap.Or(other.bitmap)
	s.Unlock()
	other.RUnlock()
}

// Equals returns true if other and s are the same set of ids.
func (s *SeriesIDSet) Equals(other *SeriesIDSet) bool {
	if s == other {
		return true
	}

	s.RLock()
	other.RLock()
	equals := s.bitmap.Equals(other.bitmap)
	s.RUnlock()
	other.RUnlock()
	return equals
}

// And returns a new SeriesIDSet containing elements that were present in s and other.
func (s *SeriesIDSet) And(other *SeriesIDSet) *SeriesIDSet {
	s.RLock()
	other.RLock()
	result := &SeriesIDSet{bitmap: roaring.And(s.bitmap, other.bitmap)}
	s.RUnlock()
	other.RUnlock()
	return result
}

// RemoveSet removes all values in other from s, if they exist.
func (s *SeriesIDSet) RemoveSet(other *SeriesIDSet) {
	s.RLock()
	other.RLock()
	s.bitmap.AndNot(other.bitmap)
	s.RUnlock()
	other.RUnlock()
}

// ForEach calls f for each id in the set. The function is applied to the IDs
// in ascending order.
func (s *SeriesIDSet) ForEach(f func(id SeriesID)) {
	s.RLock()
	s.ForEachNoLock(f)
	s.RUnlock()
}

// ForEachNoLock calls f for each id in the set without taking a lock.
func (s *SeriesIDSet) ForEachNoLock(f func(id SeriesID)) {
	itr := s.bitmap.Iterator()
	for itr.HasNext() {
		f(NewSeriesID(uint64(itr.Next())))
	}
}

func (s *SeriesIDSet) String() string {
	s.RLock()
	strValue := s.bitmap.String()
	s.RUnlock()
	return strValue
}

// Diff removes from s any elements also present in other.
func (s *SeriesIDSet) Diff(other *SeriesIDSet) {
	other.RLock()
	s.Lock()
	s.bitmap = roaring.AndNot(s.bitmap, other.bitmap)
	s.Unlock()
	other.RUnlock()
}

// Clone returns a new SeriesIDSet with a deep copy of the underlying bitmap.
func (s *SeriesIDSet) Clone() *SeriesIDSet {
	// Cloning the SeriesIDSet involves cloning s's bitmap.
	// Unfortunately, if the bitmap is set to COW, the original bitmap is modified during clone,
	// so we have to take a write lock rather than a read lock.
	// For now, we'll just hold a write lock for clone; if this shows up as a bottleneck later,
	// we can conditionally RLock if we are not COW.
	s.Lock()
	clone := s.CloneNoLock()
	s.Unlock()
	return clone
}

// CloneNoLock calls Clone without taking a lock.
func (s *SeriesIDSet) CloneNoLock() *SeriesIDSet {
	new := NewSeriesIDSet()
	new.bitmap = s.bitmap.Clone()
	return new
}

// Iterator returns an iterator to the underlying bitmap.
// This iterator is not protected by a lock.
func (s *SeriesIDSet) Iterator() SeriesIDSetIterable {
	return s.bitmap.Iterator()
}

// UnmarshalBinary unmarshals data into the set.
func (s *SeriesIDSet) UnmarshalBinary(data []byte) error {
	s.Lock()
	err := s.bitmap.UnmarshalBinary(data)
	s.Unlock()
	return err
}

// UnmarshalBinaryUnsafe unmarshals data into the set.
// References to the underlying data are used so data should not be reused by caller.
func (s *SeriesIDSet) UnmarshalBinaryUnsafe(data []byte) error {
	s.Lock()
	_, err := s.bitmap.FromBuffer(data)
	s.Unlock()
	return err
}

// WriteTo writes the set to w.
func (s *SeriesIDSet) WriteTo(w io.Writer) (int64, error) {
	s.RLock()
	n, err := s.bitmap.WriteTo(w)
	s.RUnlock()
	return n, err
}

// Clear clears the underlying bitmap for re-use. Clear is safe for use by multiple goroutines.
func (s *SeriesIDSet) Clear() {
	s.Lock()
	s.ClearNoLock()
	s.Unlock()
}

// ClearNoLock clears the underlying bitmap for re-use without taking a lock.
func (s *SeriesIDSet) ClearNoLock() {
	s.bitmap.Clear()
}

// Slice returns a slice of series ids.
func (s *SeriesIDSet) Slice() []uint64 {
	s.RLock()
	a := make([]uint64, 0, s.bitmap.GetCardinality())
	for _, seriesID := range s.bitmap.ToArray() {
		a = append(a, uint64(seriesID))
	}
	s.RUnlock()
	return a
}

type SeriesIDSetIterable interface {
	HasNext() bool
	Next() uint32
}

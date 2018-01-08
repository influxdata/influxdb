package tsdb

import (
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
	s.bitmap.Add(uint32(id))
}

// Contains returns true if the id exists in the set.
func (s *SeriesIDSet) Contains(id uint64) bool {
	return s.bitmap.Contains(uint32(id))
}

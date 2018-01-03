package tsi1

import (
	"sync"

	"github.com/RoaringBitmap/roaring"
)

// SeriesSet represents a lockable bitmap of series ids.
type SeriesSet struct {
	sync.RWMutex
	bitmap *roaring.Bitmap
}

// NewSeriesSet returns a new instance of SeriesSet.
func NewSeriesSet() *SeriesSet {
	return &SeriesSet{
		bitmap: roaring.NewBitmap(),
	}
}

// Add adds the series id to the set.
func (s *SeriesSet) Add(id uint64) {
	s.bitmap.Add(uint32(id))
}

// Contains returns true if the id exists in the set.
func (s *SeriesSet) Contains(id uint64) bool {
	return s.bitmap.Contains(uint32(id))
}

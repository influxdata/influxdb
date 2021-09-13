package cursors

// Int64Iterator describes the behavior for enumerating a sequence of int64
// values.
type Int64Iterator interface {
	// Next advances the Int64Iterator to the next value. It returns false when
	// there are no more values.
	Next() bool

	// Value returns the current value.
	Value() int64

	Stats() CursorStats
}

// EmptyInt64Iterator is an implementation of Int64Iterator that returns no
// values.
var EmptyInt64Iterator Int64Iterator = &int64Iterator{}

type int64Iterator struct{}

func (*int64Iterator) Next() bool         { return false }
func (*int64Iterator) Value() int64       { return 0 }
func (*int64Iterator) Stats() CursorStats { return CursorStats{} }

type Int64SliceIterator struct {
	s     []int64
	v     int64
	i     int
	stats CursorStats
}

func NewInt64SliceIterator(s []int64) *Int64SliceIterator {
	return &Int64SliceIterator{s: s, i: 0}
}

func NewInt64SliceIteratorWithStats(s []int64, stats CursorStats) *Int64SliceIterator {
	return &Int64SliceIterator{s: s, i: 0, stats: stats}
}

func (s *Int64SliceIterator) Next() bool {
	if s.i < len(s.s) {
		s.v = s.s[s.i]
		s.i++
		return true
	}
	s.v = 0
	return false
}

func (s *Int64SliceIterator) Value() int64 {
	return s.v
}

func (s *Int64SliceIterator) Stats() CursorStats {
	return s.stats
}

func (s *Int64SliceIterator) toSlice() []int64 {
	if s.i < len(s.s) {
		return s.s[s.i:]
	}
	return nil
}

// Int64SliceIteratorToSlice reads the remainder of i into a slice and returns
// the result.
func Int64SliceIteratorToSlice(i Int64Iterator) []int64 {
	if i == nil {
		return nil
	}

	if si, ok := i.(*Int64SliceIterator); ok {
		return si.toSlice()
	}
	var a []int64
	for i.Next() {
		a = append(a, i.Value())
	}
	return a
}

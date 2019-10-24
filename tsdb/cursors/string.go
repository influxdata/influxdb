package cursors

// StringIterator describes the behavior for enumerating a sequence of
// string values.
type StringIterator interface {
	// Next advances the StringIterator to the next value. It returns false
	// when there are no more values.
	Next() bool

	// Value returns the current value.
	Value() string

	Stats() CursorStats
}

// EmptyStringIterator is an implementation of StringIterator that returns
// no values.
var EmptyStringIterator StringIterator = &stringIterator{}

type stringIterator struct{}

func (*stringIterator) Next() bool         { return false }
func (*stringIterator) Value() string      { return "" }
func (*stringIterator) Stats() CursorStats { return CursorStats{} }

type StringSliceIterator struct {
	s     []string
	v     string
	i     int
	stats CursorStats
}

func NewStringSliceIterator(s []string) *StringSliceIterator {
	return &StringSliceIterator{s: s, i: 0}
}

func NewStringSliceIteratorWithStats(s []string, stats CursorStats) *StringSliceIterator {
	return &StringSliceIterator{s: s, i: 0, stats: stats}
}

func (s *StringSliceIterator) Next() bool {
	if s.i < len(s.s) {
		s.v = s.s[s.i]
		s.i++
		return true
	}
	s.v = ""
	return false
}

func (s *StringSliceIterator) Value() string {
	return s.v
}

func (s *StringSliceIterator) Stats() CursorStats {
	return s.stats
}

func (s *StringSliceIterator) toSlice() []string {
	if s.i < len(s.s) {
		return s.s[s.i:]
	}
	return nil
}

// StringIteratorToSlice reads the remainder of i into a slice and
// returns the result.
func StringIteratorToSlice(i StringIterator) []string {
	if i == nil {
		return nil
	}

	if si, ok := i.(*StringSliceIterator); ok {
		return si.toSlice()
	}
	var a []string
	for i.Next() {
		a = append(a, i.Value())
	}
	return a
}

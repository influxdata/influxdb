package gen

import (
	"fmt"
	"math"
)

type Sequence interface {
	Next() bool
	Value() string
	Count() int
}

type CounterByteSequence struct {
	format string
	nfmt   string
	val    string
	s      int
	v      int
	end    int
}

func NewCounterByteSequenceCount(n int) *CounterByteSequence {
	return NewCounterByteSequence("value%s", 0, n)
}

func NewCounterByteSequence(format string, start, end int) *CounterByteSequence {
	s := &CounterByteSequence{
		format: format,
		nfmt:   fmt.Sprintf("%%0%dd", int(math.Ceil(math.Log10(float64(end))))),
		s:      start,
		v:      start,
		end:    end,
	}
	s.update()
	return s
}

func (s *CounterByteSequence) Next() bool {
	s.v++
	if s.v >= s.end {
		s.v = s.s
	}
	s.update()
	return true
}

func (s *CounterByteSequence) update() {
	s.val = fmt.Sprintf(s.format, fmt.Sprintf(s.nfmt, s.v))
}

func (s *CounterByteSequence) Count() int    { return s.end - s.s }
func (s *CounterByteSequence) Value() string { return s.val }

type ConstantStringSequence string

func (ConstantStringSequence) Next() bool      { return true }
func (s ConstantStringSequence) Value() string { return string(s) }
func (ConstantStringSequence) Count() int      { return 1 }

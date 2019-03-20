package gen

import (
	"fmt"
	"math"
)

type Sequence interface {
	Next() bool
	Value() string
}

type CountableSequence interface {
	Sequence
	Count() int
}

type CounterByteSequence struct {
	format string
	nfmt   string
	val    string
	s      int
	i      int
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
		i:      start,
		end:    end,
	}
	s.update()
	return s
}

func (s *CounterByteSequence) Next() bool {
	s.i++
	if s.i >= s.end {
		s.i = s.s
	}
	s.update()
	return true
}

func (s *CounterByteSequence) update() {
	s.val = fmt.Sprintf(s.format, fmt.Sprintf(s.nfmt, s.i))
}

func (s *CounterByteSequence) Value() string { return s.val }
func (s *CounterByteSequence) Count() int    { return s.end - s.s }

type StringArraySequence struct {
	vals []string
	c    int
	i    int
}

func NewStringArraySequence(vals []string) *StringArraySequence {
	return &StringArraySequence{vals: sortDedupStrings(vals)}
}

func (s *StringArraySequence) Next() bool {
	s.i++
	if s.i == len(s.vals) {
		s.i = 0
	}
	s.c = s.i
	return true
}

func (s *StringArraySequence) Value() string {
	return s.vals[s.c]
}

func (s *StringArraySequence) Count() int {
	return len(s.vals)
}

type StringConstantSequence struct {
	val string
}

func NewStringConstantSequence(val string) *StringConstantSequence {
	return &StringConstantSequence{val: val}
}

func (s *StringConstantSequence) Next() bool    { return true }
func (s *StringConstantSequence) Value() string { return s.val }
func (s *StringConstantSequence) Count() int    { return 1 }

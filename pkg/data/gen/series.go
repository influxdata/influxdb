package gen

import (
	"bytes"
)

type seriesKeyField interface {
	// Key returns the series key.
	// The returned value may be cached.
	Key() []byte

	// Field returns the name of the field.
	// The returned value may be modified by a subsequent call to Next.
	Field() []byte
}

type constSeries struct {
	key   []byte
	field []byte
}

func (s *constSeries) Key() []byte   { return s.key }
func (s *constSeries) Field() []byte { return s.field }

var nilSeries seriesKeyField = &constSeries{}

// Compare returns an integer comparing two SeriesGenerator instances
// lexicographically.
// The result will be 0 if a==b, -1 if a < b, and +1 if a > b.
// A nil argument is equivalent to an empty SeriesGenerator.
func CompareSeries(a, b seriesKeyField) int {
	if a == nil {
		a = nilSeries
	}
	if b == nil {
		b = nilSeries
	}

	switch res := bytes.Compare(a.Key(), b.Key()); res {
	case 0:
		return bytes.Compare(a.Field(), b.Field())
	default:
		return res
	}
}

func (s *constSeries) CopyFrom(a seriesKeyField) {
	key := a.Key()
	if cap(s.key) < len(key) {
		s.key = make([]byte, len(key))
	} else {
		s.key = s.key[:len(key)]
	}
	copy(s.key, key)

	field := a.Field()
	if cap(s.field) < len(field) {
		s.field = make([]byte, len(field))
	} else {
		s.field = s.field[:len(field)]
	}
	copy(s.field, field)
}

package tsi1

import (
	"encoding/binary"
	"sort"

	"github.com/influxdata/influxdb/models"
)

// TermList represents a list of terms sorted by frequency.
type TermList struct {
	m map[string]int // terms by index
	a []termListElem // sorted terms
}

// NewTermList computes a term list based on a map of term frequency.
func NewTermList(m map[string]int) *TermList {
	if len(m) == 0 {
		return &TermList{}
	}

	l := &TermList{
		a: make([]termListElem, 0, len(m)),
		m: make(map[string]int, len(m)),
	}

	// Insert elements into slice.
	for term, freq := range m {
		l.a = append(l.a, termListElem{term: term, freq: freq})
	}
	sort.Sort(termListElems(l.a))

	// Create lookup of terms to indices.
	for i, e := range l.a {
		l.m[e.term] = i
	}

	return l
}

// Len returns the length of the list.
func (l *TermList) Len() int { return len(l.a) }

// Offset returns the offset for a given term. Returns zero if term doesn't exist.
func (l *TermList) Offset(v []byte) uint32 {
	i, ok := l.m[string(v)]
	if !ok {
		return 0
	}
	return l.a[i].offset
}

// OffsetString returns the offset for a given term. Returns zero if term doesn't exist.
func (l *TermList) OffsetString(v string) uint32 {
	i, ok := l.m[v]
	if !ok {
		return 0
	}
	return l.a[i].offset
}

// AppendEncodedSeries dictionary encodes a series and appends it to the buffer.
func (l *TermList) AppendEncodedSeries(dst []byte, name []byte, tags models.Tags) []byte {
	var buf [binary.MaxVarintLen32]byte

	// Encode name.
	offset := l.Offset(name)
	if offset == 0 {
		panic("name not in term list: " + string(name))
	}
	n := binary.PutUvarint(buf[:], uint64(offset))
	dst = append(dst, buf[:n]...)

	// Encode tag count.
	n = binary.PutUvarint(buf[:], uint64(len(tags)))
	dst = append(dst, buf[:n]...)

	// Encode tags.
	for _, t := range tags {
		// Encode tag key.
		offset := l.Offset(t.Key)
		if offset == 0 {
			panic("tag key not in term list: " + string(t.Key))
		}
		n := binary.PutUvarint(buf[:], uint64(offset))
		dst = append(dst, buf[:n]...)

		// Encode tag value.
		offset = l.Offset(t.Value)
		if offset == 0 {
			panic("tag value not in term list: " + string(t.Value))
		}
		n = binary.PutUvarint(buf[:], uint64(offset))
		dst = append(dst, buf[:n]...)
	}

	return dst
}

// termListElem represents an element in a term list.
type termListElem struct {
	term   string // term value
	freq   int    // term frequency
	offset uint32 // position in file
}

// termListElems represents a list of elements sorted by descending frequency.
type termListElems []termListElem

func (a termListElems) Len() int      { return len(a) }
func (a termListElems) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a termListElems) Less(i, j int) bool {
	if a[i].freq != a[j].freq {
		return a[i].freq > a[i].freq
	}
	return a[i].term < a[j].term
}

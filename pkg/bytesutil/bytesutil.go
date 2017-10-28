package bytesutil

import (
	"bytes"
	"fmt"
	"sort"
)

// Sort sorts a slice of byte slices.
func Sort(a [][]byte) {
	sort.Sort(byteSlices(a))
}

// IsSorted determines if a slice is sorte
func IsSorted(a [][]byte) bool {
	return sort.IsSorted(byteSlices(a))
}

// SearchBytes searches through a slice
func SearchBytes(a [][]byte, x []byte) int {
	return sort.Search(len(a), func(i int) bool { return bytes.Compare(a[i], x) >= 0 })
}

// SearchBytesFixed searches a for x using a binary search.  The size of a must be a multiple of
// of x or else the function panics.  There returned value is the index within a where x should
// exist.  The caller should ensure that x does exist at this index.
func SearchBytesFixed(a []byte, sz int, fn func(x []byte) bool) int {
	if len(a)%sz != 0 {
		panic(fmt.Sprintf("x is not a multiple of a: %d %d", len(a), sz))
	}

	i, j := 0, len(a)-sz
	for i < j {
		h := int(uint(i+j) >> 1)
		h -= h % sz
		if !fn(a[h : h+sz]) {
			i = h + sz
		} else {
			j = h
		}
	}

	return i
}

// Union returns the union of a & b in sorted order.
func Union(a, b [][]byte) [][]byte {
	n := len(b)
	if len(a) > len(b) {
		n = len(a)
	}
	other := make([][]byte, 0, n)

	for {
		if len(a) > 0 && len(b) > 0 {
			if cmp := bytes.Compare(a[0], b[0]); cmp == 0 {
				other, a, b = append(other, a[0]), a[1:], b[1:]
			} else if cmp == -1 {
				other, a = append(other, a[0]), a[1:]
			} else {
				other, b = append(other, b[0]), b[1:]
			}
		} else if len(a) > 0 {
			other, a = append(other, a[0]), a[1:]
		} else if len(b) > 0 {
			other, b = append(other, b[0]), b[1:]
		} else {
			return other
		}
	}
}

// Intersect returns the intersection of a & b in sorted order.
func Intersect(a, b [][]byte) [][]byte {
	n := len(b)
	if len(a) > len(b) {
		n = len(a)
	}
	other := make([][]byte, 0, n)

	for len(a) > 0 && len(b) > 0 {
		if cmp := bytes.Compare(a[0], b[0]); cmp == 0 {
			other, a, b = append(other, a[0]), a[1:], b[1:]
		} else if cmp == -1 {
			a = a[1:]
		} else {
			b = b[1:]
		}
	}
	return other
}

// Clone returns a copy of b.
func Clone(b []byte) []byte {
	if b == nil {
		return nil
	}
	buf := make([]byte, len(b))
	copy(buf, b)
	return buf
}

// CloneSlice returns a copy of a slice of byte slices.
func CloneSlice(a [][]byte) [][]byte {
	other := make([][]byte, len(a))
	for i := range a {
		other[i] = Clone(a[i])
	}
	return other
}

type byteSlices [][]byte

func (a byteSlices) Len() int           { return len(a) }
func (a byteSlices) Less(i, j int) bool { return bytes.Compare(a[i], a[j]) == -1 }
func (a byteSlices) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }

// Implement a heap structure by providing three methods, Initialize,
// BubbleDown and BubbleUp. Initialize is O(n), while both Bubble
// methods are O(log n). The methods take any type that implement the
// sort.Interface interface. You can get a reverse Heap (a Max heap)
// by using sort.Reverse() on a sort.Interface to reverse the result
// of the Less() method.
package heap

import "sort"

func Initialize(s sort.Interface) {
	l := s.Len()
	// start with the right most node
	for n := l - 1; n >= 0; n-- {
		BubbleDown(s, n)
	}
}

func parent(s sort.Interface, i int) int {
	if i%2 == 0 {
		return (i - 1) / 2
	}

	return i / 2
}

func BubbleUp(s sort.Interface, i int) {
	p := parent(s, i)
	if p < 0 {
		return
	}

	if s.Less(i, p) {
		s.Swap(i, p)
		BubbleUp(s, p)
	}
}

func BubbleDown(s sort.Interface, i int) {
	// length
	l := s.Len()
	lc := i*2 + 1
	if lc >= l {
		return
	}

	rc := i*2 + 2
	// the smaller child
	sc := lc
	if rc < l && s.Less(rc, lc) {
		sc = rc
	}

	if s.Less(sc, i) {
		s.Swap(sc, i)
		BubbleDown(s, sc)
	}
}

package execute

import (
	"fmt"
	"sync/atomic"
)

const (
	boolSize    = 1
	int64Size   = 8
	uint64Size  = 8
	float64Size = 8
	stringSize  = 16
	timeSize    = 8
)

// Allocator tracks the amount of memory being consumed by a query.
// The allocator provides methods similar to make and append, to allocate large slices of data.
// The allocator also provides a Free method to account for when memory will be freed.
type Allocator struct {
	Limit          int64
	bytesAllocated int64
	maxAllocated   int64
}

func (a *Allocator) count(n, size int) (c int64) {
	c = atomic.AddInt64(&a.bytesAllocated, int64(n*size))
	for max := atomic.LoadInt64(&a.maxAllocated); c > max; max = atomic.LoadInt64(&a.maxAllocated) {
		if atomic.CompareAndSwapInt64(&a.maxAllocated, max, c) {
			return
		}
	}
	return
}

// Free informs the allocator that memory has been freed.
func (a *Allocator) Free(n, size int) {
	a.count(-n, size)
}

// Max reports the maximum amount of allocated memory at any point in the query.
func (a *Allocator) Max() int64 {
	return atomic.LoadInt64(&a.maxAllocated)
}

func (a *Allocator) account(n, size int) {
	if want := a.count(n, size); want > a.Limit {
		allocated := a.count(-n, size)
		panic(AllocError{
			Limit:     a.Limit,
			Allocated: allocated,
			Wanted:    want - allocated,
		})
	}
}

// Bools makes a slice of bool values.
func (a *Allocator) Bools(l, c int) []bool {
	a.account(c, boolSize)
	return make([]bool, l, c)
}

// AppendBools appends bools to a slice
func (a *Allocator) AppendBools(slice []bool, vs ...bool) []bool {
	if cap(slice)-len(slice) > len(vs) {
		return append(slice, vs...)
	}
	s := append(slice, vs...)
	diff := cap(s) - cap(slice)
	a.account(diff, boolSize)
	return s
}

func (a *Allocator) GrowBools(slice []bool, n int) []bool {
	newCap := len(slice) + n
	if newCap < cap(slice) {
		return slice[:newCap]
	}
	// grow capacity same way as built-in append
	newCap = newCap*3/2 + 1
	s := make([]bool, len(slice)+n, newCap)
	copy(s, slice)
	diff := cap(s) - cap(slice)
	a.account(diff, boolSize)
	return s
}

// Ints makes a slice of int64 values.
func (a *Allocator) Ints(l, c int) []int64 {
	a.account(c, int64Size)
	return make([]int64, l, c)
}

// AppendInts appends int64s to a slice
func (a *Allocator) AppendInts(slice []int64, vs ...int64) []int64 {
	if cap(slice)-len(slice) > len(vs) {
		return append(slice, vs...)
	}
	s := append(slice, vs...)
	diff := cap(s) - cap(slice)
	a.account(diff, int64Size)
	return s
}

func (a *Allocator) GrowInts(slice []int64, n int) []int64 {
	newCap := len(slice) + n
	if newCap < cap(slice) {
		return slice[:newCap]
	}
	// grow capacity same way as built-in append
	newCap = newCap*3/2 + 1
	s := make([]int64, len(slice)+n, newCap)
	copy(s, slice)
	diff := cap(s) - cap(slice)
	a.account(diff, int64Size)
	return s
}

// UInts makes a slice of uint64 values.
func (a *Allocator) UInts(l, c int) []uint64 {
	a.account(c, uint64Size)
	return make([]uint64, l, c)
}

// AppendUInts appends uint64s to a slice
func (a *Allocator) AppendUInts(slice []uint64, vs ...uint64) []uint64 {
	if cap(slice)-len(slice) > len(vs) {
		return append(slice, vs...)
	}
	s := append(slice, vs...)
	diff := cap(s) - cap(slice)
	a.account(diff, uint64Size)
	return s
}

func (a *Allocator) GrowUInts(slice []uint64, n int) []uint64 {
	newCap := len(slice) + n
	if newCap < cap(slice) {
		return slice[:newCap]
	}
	// grow capacity same way as built-in append
	newCap = newCap*3/2 + 1
	s := make([]uint64, len(slice)+n, newCap)
	copy(s, slice)
	diff := cap(s) - cap(slice)
	a.account(diff, uint64Size)
	return s
}

// Floats makes a slice of float64 values.
func (a *Allocator) Floats(l, c int) []float64 {
	a.account(c, float64Size)
	return make([]float64, l, c)
}

// AppendFloats appends float64s to a slice
func (a *Allocator) AppendFloats(slice []float64, vs ...float64) []float64 {
	if cap(slice)-len(slice) > len(vs) {
		return append(slice, vs...)
	}
	s := append(slice, vs...)
	diff := cap(s) - cap(slice)
	a.account(diff, float64Size)
	return s
}

func (a *Allocator) GrowFloats(slice []float64, n int) []float64 {
	newCap := len(slice) + n
	if newCap < cap(slice) {
		return slice[:newCap]
	}
	// grow capacity same way as built-in append
	newCap = newCap*3/2 + 1
	s := make([]float64, len(slice)+n, newCap)
	copy(s, slice)
	diff := cap(s) - cap(slice)
	a.account(diff, float64Size)
	return s
}

// Strings makes a slice of string values.
// Only the string headers are accounted for.
func (a *Allocator) Strings(l, c int) []string {
	a.account(c, stringSize)
	return make([]string, l, c)
}

// AppendStrings appends strings to a slice.
// Only the string headers are accounted for.
func (a *Allocator) AppendStrings(slice []string, vs ...string) []string {
	//TODO(nathanielc): Account for actual size of strings
	if cap(slice)-len(slice) > len(vs) {
		return append(slice, vs...)
	}
	s := append(slice, vs...)
	diff := cap(s) - cap(slice)
	a.account(diff, stringSize)
	return s
}

func (a *Allocator) GrowStrings(slice []string, n int) []string {
	newCap := len(slice) + n
	if newCap < cap(slice) {
		return slice[:newCap]
	}
	// grow capacity same way as built-in append
	newCap = newCap*3/2 + 1
	s := make([]string, len(slice)+n, newCap)
	copy(s, slice)
	diff := cap(s) - cap(slice)
	a.account(diff, stringSize)
	return s
}

// Times makes a slice of Time values.
func (a *Allocator) Times(l, c int) []Time {
	a.account(c, timeSize)
	return make([]Time, l, c)
}

// AppendTimes appends Times to a slice
func (a *Allocator) AppendTimes(slice []Time, vs ...Time) []Time {
	if cap(slice)-len(slice) > len(vs) {
		return append(slice, vs...)
	}
	s := append(slice, vs...)
	diff := cap(s) - cap(slice)
	a.account(diff, timeSize)
	return s
}

func (a *Allocator) GrowTimes(slice []Time, n int) []Time {
	newCap := len(slice) + n
	if newCap < cap(slice) {
		return slice[:newCap]
	}
	// grow capacity same way as built-in append
	newCap = newCap*3/2 + 1
	s := make([]Time, len(slice)+n, newCap)
	copy(s, slice)
	diff := cap(s) - cap(slice)
	a.account(diff, timeSize)
	return s
}

type AllocError struct {
	Limit     int64
	Allocated int64
	Wanted    int64
}

func (a AllocError) Error() string {
	return fmt.Sprintf("allocation limit reached: limit %d, allocated: %d, wanted: %d", a.Limit, a.Allocated, a.Wanted)
}

// Portions of this file from github.com/shawnsmithdev/zermelo under the MIT license.
//
// The MIT License (MIT)
//
// Copyright (c) 2014 Shawn Smith
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

package radix

import (
	"sort"
)

const (
	minSize      = 256
	radix   uint = 8
	bitSize uint = 64
)

// SortUint64s sorts a slice of uint64s.
func SortUint64s(x []uint64) {
	if len(x) < 2 {
		return
	} else if len(x) < minSize {
		sort.Slice(x, func(i, j int) bool { return x[i] < x[j] })
	} else {
		doSort(x)
	}
}

func doSort(x []uint64) {
	// Each pass processes a byte offset, copying back and forth between slices
	from := x
	to := make([]uint64, len(x))
	var key uint8
	var offset [256]int // Keep track of where groups start

	for keyOffset := uint(0); keyOffset < bitSize; keyOffset += radix {
		keyMask := uint64(0xFF << keyOffset) // Current 'digit' to look at
		var counts [256]int                  // Keep track of the number of elements for each kind of byte
		sorted := true                       // Check for already sorted
		prev := uint64(0)                    // if elem is always >= prev it is already sorted
		for _, elem := range from {
			key = uint8((elem & keyMask) >> keyOffset) // fetch the byte at current 'digit'
			counts[key]++                              // count of elems to put in this digit's bucket

			if sorted { // Detect sorted
				sorted = elem >= prev
				prev = elem
			}
		}

		if sorted { // Short-circuit sorted
			if (keyOffset/radix)%2 == 1 {
				copy(to, from)
			}
			return
		}

		// Find target bucket offsets
		offset[0] = 0
		for i := 1; i < len(offset); i++ {
			offset[i] = offset[i-1] + counts[i-1]
		}

		// Rebucket while copying to other buffer
		for _, elem := range from {
			key = uint8((elem & keyMask) >> keyOffset) // Get the digit
			to[offset[key]] = elem                     // Copy the element to the digit's bucket
			offset[key]++                              // One less space, move the offset
		}
		// On next pass copy data the other way
		to, from = from, to
	}
}

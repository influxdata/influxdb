// +build go1.9

package bits

import (
	"math/bits"
)

// LeadingZeros64 returns the number of leading zero bits in x; the result is
// 64 for x == 0.
func LeadingZeros64(x uint64) uint64 {
	return uint64(bits.LeadingZeros64(x))
}

// LeadingZeros32 returns the number of leading zero bits in x; the result is
// 32 for x == 0.
func LeadingZeros32(x uint32) uint64 {
	return uint64(bits.LeadingZeros32(x))
}

// TrailingZeros64 returns the number of trailing zero bits in x; the result is
// 64 for x == 0.
func TrailingZeros64(x uint64) uint64 {
	return uint64(bits.TrailingZeros64(x))
}

// +build !go1.9

package bits

import (
	"github.com/dgryski/go-bits"
)

// LeadingZeros64 counts leading zeroes.
var LeadingZeros64 = bits.Clz

// TrailingZeros64 counts trailing zeroes.
var TrailingZeros64 = bits.Ctz

// LeadingZeros32 uses a 64-bit clz implementation and
// reduces the result by 32, to get a clz for a word.
func LeadingZeros32(x uint32) uint64 {
	return bits.Clz(uint64(x)) - 32
}

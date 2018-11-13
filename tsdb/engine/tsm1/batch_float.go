package tsm1

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"math/bits"
	"unsafe"
)

// FloatArrayEncodeAll encodes src into b, returning b and any error encountered.
// The returned slice may be of a different length and capactity to b.
//
// Currently only the float compression scheme used in Facebook's Gorilla is
// supported, so this method implements a batch oriented version of that.
func FloatArrayEncodeAll(src []float64, b []byte) ([]byte, error) {
	if cap(b) < 9 {
		b = make([]byte, 0, 9) // Enough room for the header and one value.
	}

	b = b[:1]
	b[0] = floatCompressedGorilla << 4

	var first float64
	var finished bool
	if len(src) > 0 && math.IsNaN(src[0]) {
		return nil, fmt.Errorf("unsupported value: NaN")
	} else if len(src) == 0 {
		first = math.NaN() // Write sentinal value to terminate batch.
		finished = true
	} else {
		first = src[0]
		src = src[1:]
	}

	b = b[:9]
	n := uint64(8 + 64) // Number of bits written.
	prev := math.Float64bits(first)

	// Write first value.
	binary.BigEndian.PutUint64(b[1:], prev)

	prevLeading, prevTrailing := ^uint64(0), uint64(0)
	var leading, trailing uint64
	var mask uint64
	var sum float64

	// Encode remaining values.
	for i := 0; !finished; i++ {
		var x float64
		if i < len(src) {
			x = src[i]
			sum += x
		} else {
			// Encode sentinal value to terminate batch
			x = math.NaN()
			finished = true
		}

		{
			cur := math.Float64bits(x)
			vDelta := cur ^ prev
			if vDelta == 0 {
				n++ // Write a zero bit. Nothing else to do.
				prev = cur
				continue
			}

			// First the current bit of the current byte is set to indicate we're
			// writing a delta value to the stream.
			for n>>3 >= uint64(len(b)) { // Keep growing b until we can fit all bits in.
				b = append(b, byte(0))
			}

			// n&7 - current bit in current byte.
			// n>>3 - the current byte.
			b[n>>3] |= 128 >> (n & 7) // Sets the current bit of the current byte.
			n++

			// Write the delta to b.

			// Determine the leading and trailing zeros.
			leading = uint64(bits.LeadingZeros64(vDelta))
			trailing = uint64(bits.TrailingZeros64(vDelta))

			// Clamp number of leading zeros to avoid overflow when encoding
			leading &= 0x1F
			if leading >= 32 {
				leading = 31
			}

			// At least 2 further bits will be required.
			if (n+2)>>3 >= uint64(len(b)) {
				b = append(b, byte(0))
			}

			if prevLeading != ^uint64(0) && leading >= prevLeading && trailing >= prevTrailing {
				n++ // Write a zero bit.

				// Write the l least significant bits of vDelta to b, most significant
				// bit first.
				l := uint64(64 - prevLeading - prevTrailing)
				for (n+l)>>3 >= uint64(len(b)) { // Keep growing b until we can fit all bits in.
					b = append(b, byte(0))
				}

				// Full value to write.
				v := (vDelta >> prevTrailing) << (64 - l) // l least signifciant bits of v.

				var m = n & 7 // Current bit in current byte.
				var written uint64
				if m > 0 { // In this case the current byte is not full.
					written = 8 - m
					if l < written {
						written = l
					}
					mask = v >> 56 // Move 8 MSB to 8 LSB
					b[n>>3] |= byte(mask >> m)
					n += written

					if l-written == 0 {
						prev = cur
						continue
					}
				}

				vv := v << written // Move written bits out of the way.

				// TODO(edd): Optimise this. It's unlikely we actually have 8 bytes to write.
				if (n>>3)+8 >= uint64(len(b)) {
					b = append(b, 0, 0, 0, 0, 0, 0, 0, 0)
				}
				binary.BigEndian.PutUint64(b[n>>3:], vv)
				n += (l - written)
			} else {
				prevLeading, prevTrailing = leading, trailing

				// Set a single bit to indicate a value will follow.
				b[n>>3] |= 128 >> (n & 7) // Set current bit on current byte
				n++

				// Write 5 bits of leading.
				if (n+5)>>3 >= uint64(len(b)) {
					b = append(b, byte(0))
				}

				// Enough room to write the 5 bits in the current byte?
				var m = n & 7
				l := uint64(5)
				v := leading << 59 // 5 LSB of leading.
				mask = v >> 56     // Move 5 MSB to 8 LSB

				if m <= 3 { // 5 bits fit into current byte.
					b[n>>3] |= byte(mask >> m)
					n += l
				} else { // In this case there are fewer than 5 bits available in current byte.
					// First step is to fill current byte
					written := 8 - m
					b[n>>3] |= byte(mask >> m) // Some of mask will get lost.
					n += written

					// Second step is to write the lost part of mask into the next byte.
					mask = v << written // Move written bits in previous byte out of way.
					mask >>= 56

					m = n & 7 // Recompute current bit.
					b[n>>3] |= byte(mask >> m)
					n += (l - written)
				}

				// Note that if leading == trailing == 0, then sigbits == 64.  But that
				// value doesn't actually fit into the 6 bits we have.
				// Luckily, we never need to encode 0 significant bits, since that would
				// put us in the other case (vdelta == 0).  So instead we write out a 0 and
				// adjust it back to 64 on unpacking.
				sigbits := 64 - leading - trailing

				if (n+6)>>3 >= uint64(len(b)) {
					b = append(b, byte(0))
				}

				m = n & 7
				l = uint64(6)
				v = sigbits << 58 // Move 6 LSB of sigbits to MSB
				mask = v >> 56    // Move 6 MSB to 8 LSB
				if m <= 2 {
					// The 6 bits fit into the current byte.
					b[n>>3] |= byte(mask >> m)
					n += l
				} else { // In this case there are fewer than 6 bits available in current byte.
					// First step is to fill the current byte.
					written := 8 - m
					b[n>>3] |= byte(mask >> m) // Write to the current bit.
					n += written

					// Second step is to write the lost part of mask into the next byte.
					// Write l remaining bits into current byte.
					mask = v << written // Remove bits written in previous byte out of way.
					mask >>= 56

					m = n & 7 // Recompute current bit.
					b[n>>3] |= byte(mask >> m)
					n += l - written
				}

				// Write final value.
				m = n & 7
				l = sigbits
				v = (vDelta >> trailing) << (64 - l) // Move l LSB into MSB
				for (n+l)>>3 >= uint64(len(b)) {     // Keep growing b until we can fit all bits in.
					b = append(b, byte(0))
				}

				var written uint64
				if m > 0 { // In this case the current byte is not full.
					written = 8 - m
					if l < written {
						written = l
					}
					mask = v >> 56 // Move 8 MSB to 8 LSB
					b[n>>3] |= byte(mask >> m)
					n += written

					if l-written == 0 {
						prev = cur
						continue
					}
				}

				// Shift remaining bits and write out in one go.
				vv := v << written // Remove bits written in previous byte.
				// TODO(edd): Optimise this.
				if (n>>3)+8 >= uint64(len(b)) {
					b = append(b, 0, 0, 0, 0, 0, 0, 0, 0)
				}

				binary.BigEndian.PutUint64(b[n>>3:], vv)
				n += (l - written)
			}
			prev = cur
		}
	}

	if math.IsNaN(sum) {
		return nil, fmt.Errorf("unsupported value: NaN")
	}

	length := n >> 3
	if n&7 > 0 {
		length++ // Add an extra byte to capture overflowing bits.
	}
	return b[:length], nil
}

// bitMask contains a lookup table where the index is the number of bits
// and the value is a mask. The table is always read by ANDing the index
// with 0x3f, such that if the index is 64, position 0 will be read, which
// is a 0xffffffffffffffff, thus returning all bits.
//
// 00 = 0xffffffffffffffff
// 01 = 0x0000000000000001
// 02 = 0x0000000000000003
// 03 = 0x0000000000000007
// ...
// 62 = 0x3fffffffffffffff
// 63 = 0x7fffffffffffffff
var bitMask [64]uint64

func init() {
	v := uint64(1)
	for i := 1; i <= 64; i++ {
		bitMask[i&0x3f] = v
		v = v<<1 | 1
	}
}

func FloatArrayDecodeAll(b []byte, buf []float64) ([]float64, error) {
	if len(b) < 9 {
		return []float64{}, nil
	}

	var (
		val         uint64      // current value
		trailingN   uint8       // trailing zero count
		meaningfulN uint8  = 64 // meaningful bit count
	)

	// first byte is the compression type; always Gorilla
	b = b[1:]

	val = binary.BigEndian.Uint64(b)
	if val == uvnan {
		if buf == nil {
			var tmp [1]float64
			buf = tmp[:0]
		}
		// special case: there were no values to decode
		return buf[:0], nil
	}

	buf = buf[:0]
	// convert the []float64 to []uint64 to avoid calling math.Float64Frombits,
	// which results in unnecessary moves between Xn registers before moving
	// the value into the float64 slice. This change increased performance from
	// 320 MB/s to 340 MB/s on an Intel(R) Core(TM) i7-6920HQ CPU @ 2.90GHz
	dst := *(*[]uint64)(unsafe.Pointer(&buf))
	dst = append(dst, val)

	b = b[8:]

	// The bit reader code uses brCachedVal to store up to the next 8 bytes
	// of MSB data read from b. brValidBits stores the number of remaining unread
	// bits starting from the MSB. Before N bits are read from brCachedVal,
	// they are left-rotated N bits, such that they end up in the left-most position.
	// Using bits.RotateLeft64 results in a single instruction on many CPU architectures.
	// This approach permits simple tests, such as for the two control bits:
	//
	//    brCachedVal&1 > 0
	//
	// The alternative was to leave brCachedValue alone and perform shifts and
	// masks to read specific bits. The original approach looked like the
	// following:
	//
	//    brCachedVal&(1<<(brValidBits&0x3f)) > 0
	//
	var (
		brCachedVal = uint64(0) // a buffer of up to the next 8 bytes read from b in MSB order
		brValidBits = uint8(0)  // the number of unread bits remaining in brCachedVal
	)

	// Refill brCachedVal, reading up to 8 bytes from b
	if len(b) >= 8 {
		// fast path reads 8 bytes directly
		brCachedVal = binary.BigEndian.Uint64(b)
		brValidBits = 64
		b = b[8:]
	} else if len(b) > 0 {
		brCachedVal = 0
		brValidBits = uint8(len(b) * 8)
		for i := range b {
			brCachedVal = (brCachedVal << 8) | uint64(b[i])
		}
		brCachedVal = bits.RotateLeft64(brCachedVal, -int(brValidBits))
		b = b[:0]
	} else {
		goto ERROR
	}

	// The expected exit condition is for a uvnan to be decoded.
	// Any other error (EOF) indicates a truncated stream.
	for {
		if brValidBits > 0 {
			// brValidBits > 0 is impossible to predict, so we place the
			// most likely case inside the if and immediately jump, keeping
			// the instruction pipeline consistently full.
			// This is a similar approach to using the GCC __builtin_expect
			// intrinsic, which modifies the order of branches such that the
			// likely case follows the conditional jump.
			//
			// Written as if brValidBits == 0 and placing the Refill brCachedVal
			// code inside reduces benchmarks from 318 MB/s to 260 MB/s on an
			// Intel(R) Core(TM) i7-6920HQ CPU @ 2.90GHz
			goto READ0
		}

		// Refill brCachedVal, reading up to 8 bytes from b
		if len(b) >= 8 {
			brCachedVal = binary.BigEndian.Uint64(b)
			brValidBits = 64
			b = b[8:]
		} else if len(b) > 0 {
			brCachedVal = 0
			brValidBits = uint8(len(b) * 8)
			for i := range b {
				brCachedVal = (brCachedVal << 8) | uint64(b[i])
			}
			brCachedVal = bits.RotateLeft64(brCachedVal, -int(brValidBits))
			b = b[:0]
		} else {
			goto ERROR
		}

	READ0:
		// read control bit 0
		brValidBits -= 1
		brCachedVal = bits.RotateLeft64(brCachedVal, 1)
		if brCachedVal&1 > 0 {
			if brValidBits > 0 {
				goto READ1
			}

			// Refill brCachedVal, reading up to 8 bytes from b
			if len(b) >= 8 {
				brCachedVal = binary.BigEndian.Uint64(b)
				brValidBits = 64
				b = b[8:]
			} else if len(b) > 0 {
				brCachedVal = 0
				brValidBits = uint8(len(b) * 8)
				for i := range b {
					brCachedVal = (brCachedVal << 8) | uint64(b[i])
				}
				brCachedVal = bits.RotateLeft64(brCachedVal, -int(brValidBits))
				b = b[:0]
			} else {
				goto ERROR
			}

		READ1:
			// read control bit 1
			brValidBits -= 1
			brCachedVal = bits.RotateLeft64(brCachedVal, 1)
			if brCachedVal&1 > 0 {
				// read 5 bits for leading zero count and 6 bits for the meaningful data count
				const leadingTrailingBitCount = 11
				var lmBits uint64 // leading + meaningful data counts
				if brValidBits >= leadingTrailingBitCount {
					// decode 5 bits leading + 6 bits meaningful for a total of 11 bits
					brValidBits -= leadingTrailingBitCount
					brCachedVal = bits.RotateLeft64(brCachedVal, leadingTrailingBitCount)
					lmBits = brCachedVal
				} else {
					bits01 := uint8(11)
					if brValidBits > 0 {
						bits01 -= brValidBits
						lmBits = bits.RotateLeft64(brCachedVal, 11)
					}

					// Refill brCachedVal, reading up to 8 bytes from b
					if len(b) >= 8 {
						brCachedVal = binary.BigEndian.Uint64(b)
						brValidBits = 64
						b = b[8:]
					} else if len(b) > 0 {
						brCachedVal = 0
						brValidBits = uint8(len(b) * 8)
						for i := range b {
							brCachedVal = (brCachedVal << 8) | uint64(b[i])
						}
						brCachedVal = bits.RotateLeft64(brCachedVal, -int(brValidBits))
						b = b[:0]
					} else {
						goto ERROR
					}
					brCachedVal = bits.RotateLeft64(brCachedVal, int(bits01))
					brValidBits -= bits01
					lmBits &^= bitMask[bits01&0x3f]
					lmBits |= brCachedVal & bitMask[bits01&0x3f]
				}

				lmBits &= 0x7ff
				leadingN := uint8((lmBits >> 6) & 0x1f) // 5 bits leading
				meaningfulN = uint8(lmBits & 0x3f)      // 6 bits meaningful
				if meaningfulN > 0 {
					trailingN = 64 - leadingN - meaningfulN
				} else {
					// meaningfulN == 0 is a special case, such that all bits
					// are meaningful
					trailingN = 0
					meaningfulN = 64
				}
			}

			var sBits uint64 // significant bits
			if brValidBits >= meaningfulN {
				brValidBits -= meaningfulN
				brCachedVal = bits.RotateLeft64(brCachedVal, int(meaningfulN))
				sBits = brCachedVal
			} else {
				mBits := meaningfulN
				if brValidBits > 0 {
					mBits -= brValidBits
					sBits = bits.RotateLeft64(brCachedVal, int(meaningfulN))
				}

				// Refill brCachedVal, reading up to 8 bytes from b
				if len(b) >= 8 {
					brCachedVal = binary.BigEndian.Uint64(b)
					brValidBits = 64
					b = b[8:]
				} else if len(b) > 0 {
					brCachedVal = 0
					brValidBits = uint8(len(b) * 8)
					for i := range b {
						brCachedVal = (brCachedVal << 8) | uint64(b[i])
					}
					brCachedVal = bits.RotateLeft64(brCachedVal, -int(brValidBits))
					b = b[:0]
				} else {
					goto ERROR
				}
				brCachedVal = bits.RotateLeft64(brCachedVal, int(mBits))
				brValidBits -= mBits
				sBits &^= bitMask[mBits&0x3f]
				sBits |= brCachedVal & bitMask[mBits&0x3f]
			}
			sBits &= bitMask[meaningfulN&0x3f]

			val ^= sBits << (trailingN & 0x3f)
			if val == uvnan {
				// IsNaN, eof
				break
			}
		}

		dst = append(dst, val)
	}

	return *(*[]float64)(unsafe.Pointer(&dst)), nil

ERROR:
	return (*(*[]float64)(unsafe.Pointer(&dst)))[:0], io.EOF
}

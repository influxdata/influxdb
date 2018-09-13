package tsm1

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"math/bits"
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
			if n>>3 >= uint64(len(b)) { // Grow b — no room in current byte.
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
					// The 6 bits fir into the current byte.
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

func FloatArrayDecodeAll(b []byte, dst []float64) ([]float64, error) {
	if len(b) == 0 {
		return []float64{}, nil
	}

	sz := cap(dst)
	if sz == 0 {
		sz = 64
		dst = make([]float64, sz)
	} else {
		dst = dst[:sz]
	}

	var (
		val      uint64 // current value
		leading  uint64
		trailing uint64
		bit      bool
		br       BatchBitReader
	)

	j := 0

	// first byte is the compression type.
	// we currently just have gorilla compression.
	br.Reset(b[1:])
	val = br.ReadBits(64)
	if val == uvnan {
		// special case: there were no values to decode
		return dst[:0], nil
	}

	dst[j] = math.Float64frombits(val)
	j++

	// The expected exit condition is for `uvnan` to be decoded.
	// Any other error (EOF) indicates a truncated stream.
	for br.Err() == nil {
		// read compressed value
		if br.CanReadBitFast() {
			bit = br.ReadBitFast()
		} else {
			bit = br.ReadBit()
		}

		if bit {
			if br.CanReadBitFast() {
				bit = br.ReadBitFast()
			} else {
				bit = br.ReadBit()
			}

			if bit {
				leading = br.ReadBits(5)
				mbits := br.ReadBits(6)
				if mbits == 0 {
					mbits = 64
				}
				trailing = 64 - leading - mbits
			}

			mbits := uint(64 - leading - trailing)
			bits := br.ReadBits(mbits)
			val ^= bits << trailing
			if val == uvnan { // IsNaN, eof
				break
			}
		}

		f := math.Float64frombits(val)
		if j < len(dst) {
			dst[j] = f
		} else {
			dst = append(dst, f) // force a resize
			dst = dst[:cap(dst)]
		}
		j++
	}

	return dst[:j], br.Err()
}

// BatchBitReader reads bits from an io.Reader.
type BatchBitReader struct {
	data []byte

	buf struct {
		v uint64 // bit buffer
		n uint   // available bits
	}
	err error
}

// NewBatchBitReader returns a new instance of BatchBitReader that reads from data.
func NewBatchBitReader(data []byte) *BatchBitReader {
	b := new(BatchBitReader)
	b.Reset(data)
	return b
}

// Reset sets the underlying reader on b and reinitializes.
func (r *BatchBitReader) Reset(data []byte) {
	r.data = data
	r.buf.v, r.buf.n, r.err = 0, 0, nil
	r.readBuf()
}

func (r *BatchBitReader) Err() error { return r.err }

// CanReadBitFast returns true if calling ReadBitFast() is allowed.
// Fast bit reads are allowed when at least 2 values are in the buffer.
// This is because it is not required to refilled the buffer and the caller
// can inline the calls.
func (r *BatchBitReader) CanReadBitFast() bool { return r.buf.n > 1 }

// ReadBitFast is an optimized bit read.
// IMPORTANT: Only allowed if CanReadFastBit() is true!
func (r *BatchBitReader) ReadBitFast() bool {
	v := r.buf.v&(1<<63) != 0
	r.buf.v <<= 1
	r.buf.n -= 1
	return v
}

// ReadBit returns the next bit from the underlying data.
func (r *BatchBitReader) ReadBit() bool {
	return r.ReadBits(1) != 0
}

// ReadBits reads nbits from the underlying data into a uint64.
// nbits must be from 1 to 64, inclusive.
func (r *BatchBitReader) ReadBits(nbits uint) uint64 {
	// Return EOF if there is no more data.
	if r.buf.n == 0 {
		r.err = io.EOF
		return 0
	}

	// Return bits from buffer if less than available bits.
	if nbits <= r.buf.n {
		// Return all bits, if requested.
		if nbits == 64 {
			v := r.buf.v
			r.buf.v, r.buf.n = 0, 0
			r.readBuf()
			return v
		}

		// Otherwise mask returned bits.
		v := r.buf.v >> (64 - nbits)
		r.buf.v <<= nbits
		r.buf.n -= nbits

		if r.buf.n == 0 {
			r.readBuf()
		}
		return v
	}

	// Otherwise read all available bits in current buffer.
	v, n := r.buf.v, r.buf.n

	// Read new buffer.
	r.buf.v, r.buf.n = 0, 0
	r.readBuf()

	// Append new buffer to previous buffer and shift to remove unnecessary bits.
	v |= r.buf.v >> n
	v >>= 64 - nbits

	// Remove used bits from new buffer.
	bufN := nbits - n
	if bufN > r.buf.n {
		bufN = r.buf.n
	}
	r.buf.v <<= bufN
	r.buf.n -= bufN

	if r.buf.n == 0 {
		r.readBuf()
	}

	return v
}

func (r *BatchBitReader) readBuf() {
	// Determine number of bytes to read to fill buffer.
	byteN := 8 - (r.buf.n / 8)

	// Limit to the length of our data.
	if n := uint(len(r.data)); byteN > n {
		byteN = n
	}

	// Optimized 8-byte read.
	if byteN == 8 {
		r.buf.v = binary.BigEndian.Uint64(r.data)
		r.buf.n = 64
		r.data = r.data[8:]
		return
	}

	i := uint(0)

	if byteN > 3 {
		r.buf.n += 32
		r.buf.v |= uint64(binary.BigEndian.Uint32(r.data)) << (64 - r.buf.n)
		i += 4
	}

	// Otherwise append bytes to buffer.
	for ; i < byteN; i++ {
		r.buf.n += 8
		r.buf.v |= uint64(r.data[i]) << (64 - r.buf.n)
	}

	// Move data forward.
	r.data = r.data[byteN:]
}

package tsm1

import (
	"encoding/binary"
	"io"
	"math"
)

func FloatBatchDecodeAll(b []byte, dst []float64) ([]float64, error) {
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

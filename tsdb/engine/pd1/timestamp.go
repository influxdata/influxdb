// Package timestamp provides structs and functions for converting streams of timestamps
// to byte slices.
//
// The encoding is adapative based on structure of the timestamps that are encoded.  By default,
// a bit-packed format that compresses multiple 64bit timestamps into a single 64bit word is used.
// If the values are too large to be compressed using the bit-packed format, it will fall back to
// a raw 8byte per timestamp format.  If the the values can be run-length encoded, based on the
// differences between consectutive values, a shorter, variable sized RLE format is used.
package pd1

import (
	"encoding/binary"
	"math"
	"time"

	"github.com/jwilder/encoding/simple8b"
)

const (
	// EncodingPacked is a bit-packed format
	EncodingPacked = 0
	// EncodingRLE is a run-length encoded format
	EncodingRLE = 1
	// EncodingRAW is a non-compressed format
	EncodingRaw = 2
)

// TimeEncoder encodes time.Time to byte slices.
type TimeEncoder interface {
	Write(t time.Time)
	Bytes() ([]byte, error)
}

// TimeEncoder decodes byte slices to time.Time values.
type TimeDecoder interface {
	Next() bool
	Read() time.Time
}

type encoder struct {
	ts []int64
}

// NewTimeEncoder returns a TimeEncoder
func NewTimeEncoder() TimeEncoder {
	return &encoder{}
}

// Write adds a time.Time to the compressed stream.
func (e *encoder) Write(t time.Time) {
	e.ts = append(e.ts, t.UnixNano())
}

func (e *encoder) reduce() (min, max, divisor int64, rle bool, deltas []int64) {
	// We make a copy of the timestamps so that if we end up using using RAW encoding,
	// we still have the original values to encode.
	deltas = make([]int64, len(e.ts))
	copy(deltas, e.ts)

	// Starting values for a min, max and divisor
	min, max, divisor = e.ts[0], 0, 1e12

	// First differential encode the values in place
	for i := len(deltas) - 1; i > 0; i-- {
		deltas[i] = deltas[i] - deltas[i-1]

		// We also want to keep track of the min, max and divisor so we don't
		// have to loop again
		v := deltas[i]
		if v < min {
			min = v
		}

		if v > max {
			max = v
		}

		for {
			// If our value is divisible by 10, break.  Otherwise, try the next smallest divisor.
			if v%divisor == 0 {
				break
			}
			divisor /= 10
		}
	}

	// Are the deltas able to be run-length encoded?
	rle = true
	for i := 1; i < len(deltas); i++ {
		deltas[i] = (deltas[i] - min) / divisor
		// Skip the first value || see if prev = curr.  The deltas can be RLE if the are all equal.
		rle = i == 1 || rle && (deltas[i-1] == deltas[i])
	}

	// No point RLE encoding 1 value
	rle = rle && len(deltas) > 1
	return
}

// Bytes returns the encoded bytes of all written times.
func (e *encoder) Bytes() ([]byte, error) {
	if len(e.ts) == 0 {
		return []byte{}, nil
	}

	// Minimum, maxim and largest common divisor.  rle is true if dts (the delta timestamps),
	// are all the same.
	min, max, div, rle, dts := e.reduce()

	// The deltas are all the same, so we can run-length encode them
	if rle && len(e.ts) > 60 {
		return e.encodeRLE(e.ts[0], e.ts[1]-e.ts[0], div, len(e.ts))
	}

	// We can't compress this time-range, the deltas exceed 1 << 60.  That would mean that two
	// adjacent timestamps are nanosecond resolution and ~36.5yr apart.
	if max > simple8b.MaxValue {
		return e.encodeRaw()
	}

	// Otherwise, encode them in a compressed format
	return e.encodePacked(min, div, dts)
}

func (e *encoder) encodePacked(min, div int64, dts []int64) ([]byte, error) {
	enc := simple8b.NewEncoder()
	for _, v := range dts[1:] {
		enc.Write(uint64(v))
	}

	b := make([]byte, 8*2+1)

	// 4 high bits used for the encoding type
	b[0] = byte(EncodingPacked) << 4
	// 4 low bits are the log10 divisor
	b[0] |= byte(math.Log10(float64(div)))

	// The minimum timestamp value
	binary.BigEndian.PutUint64(b[1:9], uint64(min))

	// The first delta value
	binary.BigEndian.PutUint64(b[9:17], uint64(dts[0]))

	// The compressed deltas
	deltas, err := enc.Bytes()
	if err != nil {
		return nil, err
	}

	return append(b, deltas...), nil
}

func (e *encoder) encodeRaw() ([]byte, error) {
	b := make([]byte, 1+len(e.ts)*8)
	b[0] = byte(EncodingRaw) << 4
	for i, v := range e.ts {
		binary.BigEndian.PutUint64(b[1+i*8:1+i*8+8], uint64(v))
	}
	return b, nil
}

func (e *encoder) encodeRLE(first, delta, div int64, n int) ([]byte, error) {
	// Large varints can take up to 10 bytes
	b := make([]byte, 1+10*3)

	// 4 high bits used for the encoding type
	b[0] = byte(EncodingRLE) << 4
	// 4 low bits are the log10 divisor
	b[0] |= byte(math.Log10(float64(div)))

	i := 1
	// The first timestamp
	binary.BigEndian.PutUint64(b[i:], uint64(first))
	i += 8
	// The first delta
	i += binary.PutUvarint(b[i:], uint64(delta/div))
	// The number of times the delta is repeated
	i += binary.PutUvarint(b[i:], uint64(n))

	return b[:i], nil
}

type decoder struct {
	v  time.Time
	ts []int64
}

func NewTimeDecoder(b []byte) TimeDecoder {
	d := &decoder{}
	d.decode(b)
	return d
}

func (d *decoder) Next() bool {
	if len(d.ts) == 0 {
		return false
	}
	d.v = time.Unix(0, d.ts[0])
	d.ts = d.ts[1:]
	return true
}

func (d *decoder) Read() time.Time {
	return d.v
}

func (d *decoder) decode(b []byte) {
	if len(b) == 0 {
		return
	}

	// Encoding type is stored in the 4 high bits of the first byte
	encoding := b[0] >> 4
	switch encoding {
	case EncodingRaw:
		d.decodeRaw(b[1:])
	case EncodingRLE:
		d.decodeRLE(b)
	default:
		d.decodePacked(b)
	}
}

func (d *decoder) decodePacked(b []byte) {
	div := int64(math.Pow10(int(b[0] & 0xF)))
	min := int64(binary.BigEndian.Uint64(b[1:9]))
	first := int64(binary.BigEndian.Uint64(b[9:17]))

	enc := simple8b.NewDecoder(b[17:])

	deltas := []int64{first}
	for enc.Next() {
		deltas = append(deltas, int64(enc.Read()))
	}

	// Compute the prefix sum and scale the deltas back up
	for i := 1; i < len(deltas); i++ {
		deltas[i] = (deltas[i] * div) + min
		deltas[i] = deltas[i-1] + deltas[i]
	}

	d.ts = deltas
}

func (d *decoder) decodeRLE(b []byte) {
	var i, n int

	// Lower 4 bits hold the 10 based exponent so we can scale the values back up
	div := int64(math.Pow10(int(b[i] & 0xF)))
	i += 1

	// Next 8 bytes is the starting timestamp
	first := binary.BigEndian.Uint64(b[i : i+8])
	i += 8

	// Next 1-10 bytes is our (scaled down by factor of 10) run length values
	value, n := binary.Uvarint(b[i:])

	// Scale the value back up
	value *= uint64(div)
	i += n

	// Last 1-10 bytes is how many times the value repeats
	count, n := binary.Uvarint(b[i:])

	// Rebuild construct the original values now
	deltas := make([]int64, count)
	for i := range deltas {
		deltas[i] = int64(value)
	}

	// Reverse the delta-encoding
	deltas[0] = int64(first)
	for i := 1; i < len(deltas); i++ {
		deltas[i] = deltas[i-1] + deltas[i]
	}

	d.ts = deltas
}

func (d *decoder) decodeRaw(b []byte) {
	d.ts = make([]int64, len(b)/8)
	for i := range d.ts {
		d.ts[i] = int64(binary.BigEndian.Uint64(b[i*8 : i*8+8]))
	}
}

package tsm1

// NOTE(benbjohnson)
// Copied from: github.com/dgryski/go-bitstream
// Required copy so that the underlying reader could be reused without alloc.

import "io"

// A BitReader reads bits from an io.Reader
type BitReader struct {
	r     io.Reader
	b     [1]byte
	count uint8
}

// NewReader returns a BitReader that returns a single bit at a time from 'r'
func NewReader(r io.Reader) *BitReader {
	b := new(BitReader)
	b.r = r
	return b
}

// Reset sets the underlying reader on b and reinitializes.
func (b *BitReader) Reset(r io.Reader) {
	b.r = r
	b.b[0] = 0
	b.count = 0
}

// ReadBit returns the next bit from the stream, reading a new byte from the underlying reader if required.
func (b *BitReader) ReadBit() (bool, error) {
	if b.count == 0 {
		if n, err := b.r.Read(b.b[:]); n != 1 || (err != nil && err != io.EOF) {
			return false, err
		}
		b.count = 8
	}
	b.count--
	d := (b.b[0] & 0x80)
	b.b[0] <<= 1
	return d != 0, nil
}

// ReadByte reads a single byte from the stream, regardless of alignment
func (b *BitReader) ReadByte() (byte, error) {
	if b.count == 0 {
		n, err := b.r.Read(b.b[:])
		if n == 0 {
			b.b[0] = 0
		}
		return b.b[0], err
	}

	byt := b.b[0]

	var n int
	var err error
	n, err = b.r.Read(b.b[:])
	if n != 1 || (err != nil && err != io.EOF) {
		return 0, err
	}

	byt |= b.b[0] >> b.count

	b.b[0] <<= (8 - b.count)

	return byt, err
}

// ReadBits reads  nbits from the stream
func (b *BitReader) ReadBits(nbits int) (uint64, error) {
	var u uint64
	for nbits >= 8 {
		byt, err := b.ReadByte()
		if err != nil {
			return 0, err
		}

		u = (u << 8) | uint64(byt)
		nbits -= 8
	}

	var err error
	for nbits > 0 && err != io.EOF {
		byt, err := b.ReadBit()
		if err != nil {
			return 0, err
		}
		u <<= 1
		if byt {
			u |= 1
		}
		nbits--
	}

	return u, nil
}

package tsi1

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"

	"github.com/cespare/xxhash"
)

// writeTo writes write v into w. Updates n.
func writeTo(w io.Writer, v []byte, n *int64) error {
	nn, err := w.Write(v)
	*n += int64(nn)
	return err
}

// writeUint8To writes write v into w. Updates n.
func writeUint8To(w io.Writer, v uint8, n *int64) error {
	nn, err := w.Write([]byte{v})
	*n += int64(nn)
	return err
}

// writeUint16To writes write v into w using big endian encoding. Updates n.
func writeUint16To(w io.Writer, v uint16, n *int64) error {
	var buf [2]byte
	binary.BigEndian.PutUint16(buf[:], v)
	nn, err := w.Write(buf[:])
	*n += int64(nn)
	return err
}

// writeUint32To writes write v into w using big endian encoding. Updates n.
func writeUint32To(w io.Writer, v uint32, n *int64) error {
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], v)
	nn, err := w.Write(buf[:])
	*n += int64(nn)
	return err
}

// writeUint64To writes write v into w using big endian encoding. Updates n.
func writeUint64To(w io.Writer, v uint64, n *int64) error {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], v)
	nn, err := w.Write(buf[:])
	*n += int64(nn)
	return err
}

// writeUvarintTo writes write v into w using variable length encoding. Updates n.
func writeUvarintTo(w io.Writer, v uint64, n *int64) error {
	var buf [binary.MaxVarintLen64]byte
	i := binary.PutUvarint(buf[:], v)
	nn, err := w.Write(buf[:i])
	*n += int64(nn)
	return err
}

func Hexdump(data []byte) {
	addr := 0
	for len(data) > 0 {
		n := len(data)
		if n > 16 {
			n = 16
		}

		fmt.Fprintf(os.Stderr, "%07x % x\n", addr, data[:n])

		data = data[n:]
		addr += n
	}
	fmt.Fprintln(os.Stderr, "")
}

// hashKey hashes a key using murmur3.
func hashKey(key []byte) uint32 {
	h := xxhash.Sum64(key)
	if h == 0 {
		h = 1
	}
	return uint32(h)
}

// dist returns the probe distance for a hash in a slot index.
func dist(hash uint32, i, capacity int) int {
	return (i + capacity - (int(hash) % capacity)) % capacity
}

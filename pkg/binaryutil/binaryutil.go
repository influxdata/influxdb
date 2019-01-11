package binaryutil

// VarintSize returns the number of bytes to varint encode x.
// This code is copied from encoding/binary.PutVarint() with the buffer removed.
func VarintSize(x int64) int {
	ux := uint64(x) << 1
	if x < 0 {
		ux = ^ux
	}
	return UvarintSize(ux)
}

// UvarintSize returns the number of bytes to uvarint encode x.
// This code is copied from encoding/binary.PutUvarint() with the buffer removed.
func UvarintSize(x uint64) int {
	i := 0
	for x >= 0x80 {
		x >>= 7
		i++
	}
	return i + 1
}

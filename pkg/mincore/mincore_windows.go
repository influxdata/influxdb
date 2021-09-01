//go:build windows
// +build windows

package mincore

// Mincore returns a zero-length vector.
func Mincore(data []byte) ([]byte, error) {
	return make([]byte, 0), nil
}

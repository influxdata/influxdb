package radix

import (
	"bytes"
	"math/rand"
	"testing"
)

func TestBuffer(t *testing.T) {
	var buf buffer

	for i := 0; i < 1000; i++ {
		x1 := make([]byte, rand.Intn(32)+1)
		for j := range x1 {
			x1[j] = byte(i + j)
		}

		x2 := buf.Copy(x1)
		if !bytes.Equal(x2, x1) {
			t.Fatal("bad copy")
		}

		x1[0] += 1
		if bytes.Equal(x2, x1) {
			t.Fatal("bad copy")
		}
	}
}

func TestBufferAppend(t *testing.T) {
	var buf buffer
	x1 := buf.Copy(make([]byte, 1))
	x2 := buf.Copy(make([]byte, 1))

	_ = append(x1, 1)
	if x2[0] != 0 {
		t.Fatal("append wrote past")
	}
}

func TestBufferLarge(t *testing.T) {
	var buf buffer

	x1 := make([]byte, bufferSize+1)
	x2 := buf.Copy(x1)

	if !bytes.Equal(x1, x2) {
		t.Fatal("bad copy")
	}

	x1[0] += 1
	if bytes.Equal(x1, x2) {
		t.Fatal("bad copy")
	}
}

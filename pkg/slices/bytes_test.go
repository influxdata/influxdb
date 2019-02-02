package slices

import (
	"math"
	"reflect"
	"testing"
	"unsafe"
)

func TestCopyChunkedByteSlices_oneChunk(t *testing.T) {
	src := [][]byte{
		[]byte("influx"),
		[]byte("data"),
	}

	dst := CopyChunkedByteSlices(src, 3)
	if !reflect.DeepEqual(src, dst) {
		t.Errorf("destination should match source src: %v dst: %v", src, dst)
	}

	dst[0][1] = 'z'
	if reflect.DeepEqual(src, dst) {
		t.Error("destination should not match source")
	}
}

func TestCopyChunkedByteSlices_multipleChunks(t *testing.T) {
	src := [][]byte{
		[]byte("influx"),
		[]byte("data"),
		[]byte("is"),
		[]byte("the"),
		[]byte("best"),
		[]byte("time"),
		[]byte("series"),
		[]byte("database"),
		[]byte("in"),
		[]byte("the"),
		[]byte("whole"),
		[]byte("wide"),
		[]byte("world"),
		[]byte(":-)"),
	}

	chunkSize := 4
	dst := CopyChunkedByteSlices(src, chunkSize)
	if !reflect.DeepEqual(src, dst) {
		t.Errorf("destination should match source src: %v dst: %v", src, dst)
	}

	for i := 0; i < int(math.Ceil(float64(len(src))/float64(chunkSize))); i++ {
		thisChunkSize := chunkSize
		if len(src)-thisChunkSize*i < thisChunkSize {
			thisChunkSize = len(src) - thisChunkSize*i
		}

		chunk := dst[i*thisChunkSize : (i+1)*thisChunkSize]

		for j := 0; j < thisChunkSize-1; j++ {
			a := (*reflect.SliceHeader)(unsafe.Pointer(&chunk[j]))
			b := (*reflect.SliceHeader)(unsafe.Pointer(&chunk[j+1]))
			if b.Data-a.Data != uintptr(a.Len) {
				t.Error("chunk elements do not appear to be adjacent, so not part of one chunk")
			}
			if a.Cap != a.Len {
				t.Errorf("slice length != capacity; %d vs %d", a.Len, a.Cap)
			}
			if b.Cap != b.Len {
				t.Errorf("slice length != capacity; %d vs %d", b.Len, b.Cap)
			}
		}
	}

	dst[0][5] = 'z'
	if reflect.DeepEqual(src, dst) {
		t.Error("destination should not match source")
	}
}

package hll

import (
	crand "crypto/rand"
	"encoding/binary"
	"fmt"
	"math"
	"math/rand"
	"reflect"
	"testing"
	"unsafe"

	"github.com/davecgh/go-spew/spew"
)

func nopHash(buf []byte) uint64 {
	if len(buf) != 8 {
		panic(fmt.Sprintf("unexpected size buffer: %d", len(buf)))
	}
	return binary.BigEndian.Uint64(buf)
}

func toByte(v uint64) []byte {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], v)
	return buf[:]
}

func TestPlus_Bytes(t *testing.T) {
	testCases := []struct {
		p      uint8
		normal bool
	}{
		{4, false},
		{5, false},
		{4, true},
		{5, true},
	}

	for i, testCase := range testCases {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			h := NewTestPlus(testCase.p)

			plusStructOverhead := int(unsafe.Sizeof(*h))
			compressedListOverhead := int(unsafe.Sizeof(*h.sparseList))

			var expectedDenseListCapacity, expectedSparseListCapacity int

			if testCase.normal {
				h.toNormal()
				// denseList has capacity for 2^p elements, one byte each
				expectedDenseListCapacity = int(math.Pow(2, float64(testCase.p)))
				if expectedDenseListCapacity != cap(h.denseList) {
					t.Errorf("denseList capacity: want %d got %d", expectedDenseListCapacity, cap(h.denseList))
				}
			} else {
				// sparseList has capacity for 2^p elements, one byte each
				expectedSparseListCapacity = int(math.Pow(2, float64(testCase.p)))
				if expectedSparseListCapacity != cap(h.sparseList.b) {
					t.Errorf("sparseList capacity: want %d got %d", expectedSparseListCapacity, cap(h.sparseList.b))
				}
				expectedSparseListCapacity += compressedListOverhead
			}

			expectedSize := plusStructOverhead + expectedDenseListCapacity + expectedSparseListCapacity
			if expectedSize != h.Bytes() {
				t.Errorf("Bytes(): want %d got %d", expectedSize, h.Bytes())
			}
		})
	}
}

func TestPlus_Add_NoSparse(t *testing.T) {
	h := NewTestPlus(16)
	h.toNormal()

	h.Add(toByte(0x00010fffffffffff))
	n := h.denseList[1]
	if n != 5 {
		t.Error(n)
	}

	h.Add(toByte(0x0002ffffffffffff))
	n = h.denseList[2]
	if n != 1 {
		t.Error(n)
	}

	h.Add(toByte(0x0003000000000000))
	n = h.denseList[3]
	if n != 49 {
		t.Error(n)
	}

	h.Add(toByte(0x0003000000000001))
	n = h.denseList[3]
	if n != 49 {
		t.Error(n)
	}

	h.Add(toByte(0xff03700000000000))
	n = h.denseList[0xff03]
	if n != 2 {
		t.Error(n)
	}

	h.Add(toByte(0xff03080000000000))
	n = h.denseList[0xff03]
	if n != 5 {
		t.Error(n)
	}
}

func TestPlusPrecision_NoSparse(t *testing.T) {
	h := NewTestPlus(4)
	h.toNormal()

	h.Add(toByte(0x1fffffffffffffff))
	n := h.denseList[1]
	if n != 1 {
		t.Error(n)
	}

	h.Add(toByte(0xffffffffffffffff))
	n = h.denseList[0xf]
	if n != 1 {
		t.Error(n)
	}

	h.Add(toByte(0x00ffffffffffffff))
	n = h.denseList[0]
	if n != 5 {
		t.Error(n)
	}
}

func TestPlus_toNormal(t *testing.T) {
	h := NewTestPlus(16)
	h.Add(toByte(0x00010fffffffffff))
	h.toNormal()
	c := h.Count()
	if c != 1 {
		t.Error(c)
	}

	if h.sparse {
		t.Error("toNormal should convert to normal")
	}

	h = NewTestPlus(16)
	h.hash = nopHash
	h.Add(toByte(0x00010fffffffffff))
	h.Add(toByte(0x0002ffffffffffff))
	h.Add(toByte(0x0003000000000000))
	h.Add(toByte(0x0003000000000001))
	h.Add(toByte(0xff03700000000000))
	h.Add(toByte(0xff03080000000000))
	h.mergeSparse()
	h.toNormal()

	n := h.denseList[1]
	if n != 5 {
		t.Error(n)
	}
	n = h.denseList[2]
	if n != 1 {
		t.Error(n)
	}
	n = h.denseList[3]
	if n != 49 {
		t.Error(n)
	}
	n = h.denseList[0xff03]
	if n != 5 {
		t.Error(n)
	}
}

func TestPlusCount(t *testing.T) {
	h := NewTestPlus(16)

	n := h.Count()
	if n != 0 {
		t.Error(n)
	}

	h.Add(toByte(0x00010fffffffffff))
	h.Add(toByte(0x00020fffffffffff))
	h.Add(toByte(0x00030fffffffffff))
	h.Add(toByte(0x00040fffffffffff))
	h.Add(toByte(0x00050fffffffffff))
	h.Add(toByte(0x00050fffffffffff))

	n = h.Count()
	if n != 5 {
		t.Error(n)
	}

	// not mutated, still returns correct count
	n = h.Count()
	if n != 5 {
		t.Error(n)
	}

	h.Add(toByte(0x00060fffffffffff))

	// mutated
	n = h.Count()
	if n != 6 {
		t.Error(n)
	}
}

func TestPlus_Merge_Error(t *testing.T) {
	h := NewTestPlus(16)
	h2 := NewTestPlus(10)

	err := h.Merge(h2)
	if err == nil {
		t.Error("different precision should return error")
	}
}

func TestHLL_Merge_Sparse(t *testing.T) {
	h := NewTestPlus(16)
	h.Add(toByte(0x00010fffffffffff))
	h.Add(toByte(0x00020fffffffffff))
	h.Add(toByte(0x00030fffffffffff))
	h.Add(toByte(0x00040fffffffffff))
	h.Add(toByte(0x00050fffffffffff))
	h.Add(toByte(0x00050fffffffffff))

	h2 := NewTestPlus(16)
	h2.Merge(h)
	n := h2.Count()
	if n != 5 {
		t.Error(n)
	}

	if h2.sparse {
		t.Error("Merge should convert to normal")
	}

	if !h.sparse {
		t.Error("Merge should not modify argument")
	}

	h2.Merge(h)
	n = h2.Count()
	if n != 5 {
		t.Error(n)
	}

	h.Add(toByte(0x00060fffffffffff))
	h.Add(toByte(0x00070fffffffffff))
	h.Add(toByte(0x00080fffffffffff))
	h.Add(toByte(0x00090fffffffffff))
	h.Add(toByte(0x000a0fffffffffff))
	h.Add(toByte(0x000a0fffffffffff))
	n = h.Count()
	if n != 10 {
		t.Error(n)
	}

	h2.Merge(h)
	n = h2.Count()
	if n != 10 {
		t.Error(n)
	}
}

func TestHLL_Merge_Normal(t *testing.T) {
	h := NewTestPlus(16)
	h.toNormal()
	h.Add(toByte(0x00010fffffffffff))
	h.Add(toByte(0x00020fffffffffff))
	h.Add(toByte(0x00030fffffffffff))
	h.Add(toByte(0x00040fffffffffff))
	h.Add(toByte(0x00050fffffffffff))
	h.Add(toByte(0x00050fffffffffff))

	h2 := NewTestPlus(16)
	h2.toNormal()
	h2.Merge(h)
	n := h2.Count()
	if n != 5 {
		t.Error(n)
	}

	h2.Merge(h)
	n = h2.Count()
	if n != 5 {
		t.Error(n)
	}

	h.Add(toByte(0x00060fffffffffff))
	h.Add(toByte(0x00070fffffffffff))
	h.Add(toByte(0x00080fffffffffff))
	h.Add(toByte(0x00090fffffffffff))
	h.Add(toByte(0x000a0fffffffffff))
	h.Add(toByte(0x000a0fffffffffff))
	n = h.Count()
	if n != 10 {
		t.Error(n)
	}

	h2.Merge(h)
	n = h2.Count()
	if n != 10 {
		t.Error(n)
	}
}

func TestPlus_Merge(t *testing.T) {
	h := NewTestPlus(16)

	k1 := uint64(0xf000017000000000)
	h.Add(toByte(k1))
	if !h.tmpSet.has(h.encodeHash(k1)) {
		t.Error("key not in hash")
	}

	k2 := uint64(0x000fff8f00000000)
	h.Add(toByte(k2))
	if !h.tmpSet.has(h.encodeHash(k2)) {
		t.Error("key not in hash")
	}

	if len(h.tmpSet) != 2 {
		t.Error(h.tmpSet)
	}

	h.mergeSparse()
	if len(h.tmpSet) != 0 {
		t.Error(h.tmpSet)
	}
	if h.sparseList.count != 2 {
		t.Error(h.sparseList)
	}

	iter := h.sparseList.Iter()
	n := iter.Next()
	if n != h.encodeHash(k2) {
		t.Error(n)
	}
	n = iter.Next()
	if n != h.encodeHash(k1) {
		t.Error(n)
	}

	k3 := uint64(0x0f00017000000000)
	h.Add(toByte(k3))
	if !h.tmpSet.has(h.encodeHash(k3)) {
		t.Error("key not in hash")
	}

	h.mergeSparse()
	if len(h.tmpSet) != 0 {
		t.Error(h.tmpSet)
	}
	if h.sparseList.count != 3 {
		t.Error(h.sparseList)
	}

	iter = h.sparseList.Iter()
	n = iter.Next()
	if n != h.encodeHash(k2) {
		t.Error(n)
	}
	n = iter.Next()
	if n != h.encodeHash(k3) {
		t.Error(n)
	}
	n = iter.Next()
	if n != h.encodeHash(k1) {
		t.Error(n)
	}

	h.Add(toByte(k1))
	if !h.tmpSet.has(h.encodeHash(k1)) {
		t.Error("key not in hash")
	}

	h.mergeSparse()
	if len(h.tmpSet) != 0 {
		t.Error(h.tmpSet)
	}
	if h.sparseList.count != 3 {
		t.Error(h.sparseList)
	}

	iter = h.sparseList.Iter()
	n = iter.Next()
	if n != h.encodeHash(k2) {
		t.Error(n)
	}
	n = iter.Next()
	if n != h.encodeHash(k3) {
		t.Error(n)
	}
	n = iter.Next()
	if n != h.encodeHash(k1) {
		t.Error(n)
	}
}

func TestPlus_EncodeDecode(t *testing.T) {
	h := NewTestPlus(8)
	i, r := h.decodeHash(h.encodeHash(0xffffff8000000000))
	if i != 0xff {
		t.Error(i)
	}
	if r != 1 {
		t.Error(r)
	}

	i, r = h.decodeHash(h.encodeHash(0xff00000000000000))
	if i != 0xff {
		t.Error(i)
	}
	if r != 57 {
		t.Error(r)
	}

	i, r = h.decodeHash(h.encodeHash(0xff30000000000000))
	if i != 0xff {
		t.Error(i)
	}
	if r != 3 {
		t.Error(r)
	}

	i, r = h.decodeHash(h.encodeHash(0xaa10000000000000))
	if i != 0xaa {
		t.Error(i)
	}
	if r != 4 {
		t.Error(r)
	}

	i, r = h.decodeHash(h.encodeHash(0xaa0f000000000000))
	if i != 0xaa {
		t.Error(i)
	}
	if r != 5 {
		t.Error(r)
	}
}

func TestPlus_Error(t *testing.T) {
	_, err := NewPlus(3)
	if err == nil {
		t.Error("precision 3 should return error")
	}

	_, err = NewPlus(18)
	if err != nil {
		t.Error(err)
	}

	_, err = NewPlus(19)
	if err == nil {
		t.Error("precision 17 should return error")
	}
}

func TestPlus_Marshal_Unmarshal_Sparse(t *testing.T) {
	h, _ := NewPlus(4)
	h.sparse = true
	h.tmpSet = map[uint32]struct{}{26: struct{}{}, 40: struct{}{}}

	// Add a bunch of values to the sparse representation.
	for i := 0; i < 10; i++ {
		h.sparseList.Append(uint32(rand.Int()))
	}

	data, err := h.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}

	// Peeking at the first byte should reveal the version.
	if got, exp := data[0], byte(2); got != exp {
		t.Fatalf("got byte %v, expected %v", got, exp)
	}

	var res Plus
	if err := res.UnmarshalBinary(data); err != nil {
		t.Fatal(err)
	}

	// reflect.DeepEqual will always return false when comparing non-nil
	// functions, so we'll set them to nil.
	h.hash, res.hash = nil, nil
	if got, exp := &res, h; !reflect.DeepEqual(got, exp) {
		t.Fatalf("got %v, wanted %v", spew.Sdump(got), spew.Sdump(exp))
	}
}

func TestPlus_Marshal_Unmarshal_Dense(t *testing.T) {
	h, _ := NewPlus(4)
	h.sparse = false

	// Add a bunch of values to the dense representation.
	for i := 0; i < 10; i++ {
		h.denseList = append(h.denseList, uint8(rand.Int()))
	}

	data, err := h.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}

	// Peeking at the first byte should reveal the version.
	if got, exp := data[0], byte(2); got != exp {
		t.Fatalf("got byte %v, expected %v", got, exp)
	}

	var res Plus
	if err := res.UnmarshalBinary(data); err != nil {
		t.Fatal(err)
	}

	// reflect.DeepEqual will always return false when comparing non-nil
	// functions, so we'll set them to nil.
	h.hash, res.hash = nil, nil
	if got, exp := &res, h; !reflect.DeepEqual(got, exp) {
		t.Fatalf("got %v, wanted %v", spew.Sdump(got), spew.Sdump(exp))
	}
}

// Tests that a sketch can be serialised / unserialised and keep an accurate
// cardinality estimate.
func TestPlus_Marshal_Unmarshal_Count(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping test in short mode")
	}

	count := make(map[string]struct{}, 1000000)
	h, _ := NewPlus(16)

	buf := make([]byte, 8)
	for i := 0; i < 1000000; i++ {
		if _, err := crand.Read(buf); err != nil {
			panic(err)
		}

		count[string(buf)] = struct{}{}

		// Add to the sketch.
		h.Add(buf)
	}

	gotC := h.Count()
	epsilon := 15000 // 1.5%
	if got, exp := math.Abs(float64(int(gotC)-len(count))), epsilon; int(got) > exp {
		t.Fatalf("error was %v for estimation %d and true cardinality %d", got, gotC, len(count))
	}

	// Serialise the sketch.
	sketch, err := h.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}

	// Deserialise.
	h = &Plus{}
	if err := h.UnmarshalBinary(sketch); err != nil {
		t.Fatal(err)
	}

	// The count should be the same
	oldC := gotC
	if got, exp := h.Count(), oldC; got != exp {
		t.Fatalf("got %d, expected %d", got, exp)
	}

	// Add some more values.
	for i := 0; i < 1000000; i++ {
		if _, err := crand.Read(buf); err != nil {
			panic(err)
		}

		count[string(buf)] = struct{}{}

		// Add to the sketch.
		h.Add(buf)
	}

	// The sketch should still be working correctly.
	gotC = h.Count()
	epsilon = 30000 // 1.5%
	if got, exp := math.Abs(float64(int(gotC)-len(count))), epsilon; int(got) > exp {
		t.Fatalf("error was %v for estimation %d and true cardinality %d", got, gotC, len(count))
	}
}

func NewTestPlus(p uint8) *Plus {
	h, err := NewPlus(p)
	if err != nil {
		panic(err)
	}
	h.hash = nopHash
	return h
}

// Generate random data to add to the sketch.
func genData(n int) [][]byte {
	out := make([][]byte, 0, n)
	buf := make([]byte, 8)

	for i := 0; i < n; i++ {
		// generate 8 random bytes
		n, err := rand.Read(buf)
		if err != nil {
			panic(err)
		} else if n != 8 {
			panic(fmt.Errorf("only %d bytes generated", n))
		}

		out = append(out, buf)
	}
	if len(out) != n {
		panic(fmt.Sprintf("wrong size slice: %d", n))
	}
	return out
}

// Memoises values to be added to a sketch during a benchmark.
var benchdata = map[int][][]byte{}

func benchmarkPlusAdd(b *testing.B, h *Plus, n int) {
	blobs, ok := benchdata[n]
	if !ok {
		// Generate it.
		benchdata[n] = genData(n)
		blobs = benchdata[n]
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := 0; j < len(blobs); j++ {
			h.Add(blobs[j])
		}
	}
	b.StopTimer()
}

func BenchmarkPlus_Add_100(b *testing.B) {
	h, _ := NewPlus(16)
	benchmarkPlusAdd(b, h, 100)
}

func BenchmarkPlus_Add_1000(b *testing.B) {
	h, _ := NewPlus(16)
	benchmarkPlusAdd(b, h, 1000)
}

func BenchmarkPlus_Add_10000(b *testing.B) {
	h, _ := NewPlus(16)
	benchmarkPlusAdd(b, h, 10000)
}

func BenchmarkPlus_Add_100000(b *testing.B) {
	h, _ := NewPlus(16)
	benchmarkPlusAdd(b, h, 100000)
}

func BenchmarkPlus_Add_1000000(b *testing.B) {
	h, _ := NewPlus(16)
	benchmarkPlusAdd(b, h, 1000000)
}

func BenchmarkPlus_Add_10000000(b *testing.B) {
	h, _ := NewPlus(16)
	benchmarkPlusAdd(b, h, 10000000)
}

func BenchmarkPlus_Add_100000000(b *testing.B) {
	h, _ := NewPlus(16)
	benchmarkPlusAdd(b, h, 100000000)
}

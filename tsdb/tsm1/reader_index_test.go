package tsm1

import (
	"fmt"
	"math"
	"reflect"
	"sync/atomic"
	"testing"
)

func loadIndex(tb testing.TB, w IndexWriter) *indirectIndex {
	tb.Helper()

	b, err := w.MarshalBinary()
	fatalIfErr(tb, "marshaling index", err)

	indir := NewIndirectIndex()
	fatalIfErr(tb, "unmarshaling index", indir.UnmarshalBinary(b))

	return indir
}

func TestIndirectIndex_Entries_NonExistent(t *testing.T) {
	index := NewIndexWriter()
	index.Add([]byte("cpu"), BlockFloat64, 0, 1, 10, 100)
	index.Add([]byte("cpu"), BlockFloat64, 2, 3, 20, 200)
	ind := loadIndex(t, index)

	// mem has not been added to the index so we should get no entries back
	// for both
	exp := index.Entries([]byte("mem"))
	entries, err := ind.ReadEntries([]byte("mem"), nil)
	if err != nil {
		t.Fatal(err)
	}

	if got, exp := len(entries), len(exp); got != exp && exp != 0 {
		t.Fatalf("entries length mismatch: got %v, exp %v", got, exp)
	}
}

func TestIndirectIndex_Type(t *testing.T) {
	index := NewIndexWriter()
	index.Add([]byte("cpu"), BlockInteger, 0, 1, 10, 20)
	ind := loadIndex(t, index)

	typ, err := ind.Type([]byte("cpu"))
	if err != nil {
		fatal(t, "reading type", err)
	}

	if got, exp := typ, BlockInteger; got != exp {
		t.Fatalf("type mismatch: got %v, exp %v", got, exp)
	}
}

func TestIndirectIndex_Delete(t *testing.T) {
	check := func(t *testing.T, got, exp bool) {
		t.Helper()
		if exp != got {
			t.Fatalf("expected: %v but got: %v", exp, got)
		}
	}

	index := NewIndexWriter()
	index.Add([]byte("cpu1"), BlockInteger, 0, 10, 10, 20)
	index.Add([]byte("cpu1"), BlockInteger, 10, 20, 10, 20)
	index.Add([]byte("cpu2"), BlockInteger, 0, 10, 10, 20)
	index.Add([]byte("cpu2"), BlockInteger, 10, 20, 10, 20)
	index.Add([]byte("mem"), BlockInteger, 0, 10, 10, 20)
	ind := loadIndex(t, index)

	ind.Delete([][]byte{[]byte("cpu1")})

	check(t, ind.Contains([]byte("mem")), true)
	check(t, ind.Contains([]byte("cpu1")), false)
	check(t, ind.Contains([]byte("cpu2")), true)

	ind.Delete([][]byte{[]byte("cpu1"), []byte("cpu2")})

	check(t, ind.Contains([]byte("mem")), true)
	check(t, ind.Contains([]byte("cpu1")), false)
	check(t, ind.Contains([]byte("cpu2")), false)

	ind.Delete([][]byte{[]byte("mem")})

	check(t, ind.Contains([]byte("mem")), false)
	check(t, ind.Contains([]byte("cpu1")), false)
	check(t, ind.Contains([]byte("cpu2")), false)
}

func TestIndirectIndex_DeleteRange(t *testing.T) {
	check := func(t *testing.T, got, exp bool) {
		t.Helper()
		if exp != got {
			t.Fatalf("expected: %v but got: %v", exp, got)
		}
	}

	index := NewIndexWriter()
	index.Add([]byte("cpu1"), BlockInteger, 0, 10, 10, 20)
	index.Add([]byte("cpu1"), BlockInteger, 10, 20, 10, 20)
	index.Add([]byte("cpu2"), BlockInteger, 0, 10, 10, 20)
	index.Add([]byte("cpu2"), BlockInteger, 10, 20, 10, 20)
	index.Add([]byte("mem"), BlockInteger, 0, 10, 10, 20)
	ind := loadIndex(t, index)

	ind.DeleteRange([][]byte{[]byte("cpu1")}, 5, 15)

	check(t, ind.Contains([]byte("mem")), true)
	check(t, ind.Contains([]byte("cpu1")), true)
	check(t, ind.MaybeContainsValue([]byte("cpu1"), 4), true)
	check(t, ind.MaybeContainsValue([]byte("cpu1"), 5), false)
	check(t, ind.MaybeContainsValue([]byte("cpu1"), 10), false)
	check(t, ind.MaybeContainsValue([]byte("cpu1"), 15), false)
	check(t, ind.MaybeContainsValue([]byte("cpu1"), 16), true)
	check(t, ind.Contains([]byte("cpu2")), true)
	check(t, ind.MaybeContainsValue([]byte("cpu2"), 4), true)
	check(t, ind.MaybeContainsValue([]byte("cpu2"), 5), true)
	check(t, ind.MaybeContainsValue([]byte("cpu2"), 10), true)
	check(t, ind.MaybeContainsValue([]byte("cpu2"), 15), true)
	check(t, ind.MaybeContainsValue([]byte("cpu2"), 16), true)

	ind.DeleteRange([][]byte{[]byte("cpu1"), []byte("cpu2")}, 0, 5)

	check(t, ind.Contains([]byte("mem")), true)
	check(t, ind.Contains([]byte("cpu1")), true)
	check(t, ind.MaybeContainsValue([]byte("cpu1"), 4), false)
	check(t, ind.MaybeContainsValue([]byte("cpu1"), 5), false)
	check(t, ind.MaybeContainsValue([]byte("cpu1"), 10), false)
	check(t, ind.MaybeContainsValue([]byte("cpu1"), 15), false)
	check(t, ind.MaybeContainsValue([]byte("cpu1"), 16), true)
	check(t, ind.Contains([]byte("cpu2")), true)
	check(t, ind.MaybeContainsValue([]byte("cpu2"), 4), false)
	check(t, ind.MaybeContainsValue([]byte("cpu2"), 5), false)
	check(t, ind.MaybeContainsValue([]byte("cpu2"), 10), true)
	check(t, ind.MaybeContainsValue([]byte("cpu2"), 15), true)
	check(t, ind.MaybeContainsValue([]byte("cpu2"), 16), true)

	ind.DeleteRange([][]byte{[]byte("cpu1"), []byte("cpu2")}, 15, 20)

	check(t, ind.Contains([]byte("mem")), true)
	check(t, ind.Contains([]byte("cpu1")), false)
	check(t, ind.MaybeContainsValue([]byte("cpu1"), 4), false)
	check(t, ind.MaybeContainsValue([]byte("cpu1"), 5), false)
	check(t, ind.MaybeContainsValue([]byte("cpu1"), 10), false)
	check(t, ind.MaybeContainsValue([]byte("cpu1"), 15), false)
	check(t, ind.MaybeContainsValue([]byte("cpu1"), 16), false)
	check(t, ind.Contains([]byte("cpu2")), true)
	check(t, ind.MaybeContainsValue([]byte("cpu2"), 4), false)
	check(t, ind.MaybeContainsValue([]byte("cpu2"), 5), false)
	check(t, ind.MaybeContainsValue([]byte("cpu2"), 10), true)
	check(t, ind.MaybeContainsValue([]byte("cpu2"), 15), false)
	check(t, ind.MaybeContainsValue([]byte("cpu2"), 16), false)
}

func TestIndirectIndex_DeletePrefix(t *testing.T) {
	check := func(t *testing.T, got, exp bool) {
		t.Helper()
		if exp != got {
			t.Fatalf("expected: %v but got: %v", exp, got)
		}
	}

	index := NewIndexWriter()
	index.Add([]byte("cpu1"), BlockInteger, 0, 10, 10, 20)
	index.Add([]byte("cpu1"), BlockInteger, 10, 20, 10, 20)
	index.Add([]byte("cpu2"), BlockInteger, 0, 10, 10, 20)
	index.Add([]byte("cpu2"), BlockInteger, 10, 20, 10, 20)
	index.Add([]byte("mem"), BlockInteger, 0, 10, 10, 20)
	ind := loadIndex(t, index)

	ind.DeletePrefix([]byte("c"), 5, 15, nil)

	check(t, ind.Contains([]byte("mem")), true)
	check(t, ind.Contains([]byte("cpu1")), true)
	check(t, ind.MaybeContainsValue([]byte("cpu1"), 4), true)
	check(t, ind.MaybeContainsValue([]byte("cpu1"), 5), false)
	check(t, ind.MaybeContainsValue([]byte("cpu1"), 10), false)
	check(t, ind.MaybeContainsValue([]byte("cpu1"), 15), false)
	check(t, ind.MaybeContainsValue([]byte("cpu1"), 16), true)
	check(t, ind.Contains([]byte("cpu2")), true)
	check(t, ind.MaybeContainsValue([]byte("cpu2"), 4), true)
	check(t, ind.MaybeContainsValue([]byte("cpu2"), 5), false)
	check(t, ind.MaybeContainsValue([]byte("cpu2"), 10), false)
	check(t, ind.MaybeContainsValue([]byte("cpu2"), 15), false)
	check(t, ind.MaybeContainsValue([]byte("cpu2"), 16), true)

	ind.DeletePrefix([]byte("cp"), 0, 5, nil)

	check(t, ind.Contains([]byte("mem")), true)
	check(t, ind.Contains([]byte("cpu1")), true)
	check(t, ind.MaybeContainsValue([]byte("cpu1"), 4), false)
	check(t, ind.MaybeContainsValue([]byte("cpu1"), 5), false)
	check(t, ind.MaybeContainsValue([]byte("cpu1"), 10), false)
	check(t, ind.MaybeContainsValue([]byte("cpu1"), 15), false)
	check(t, ind.MaybeContainsValue([]byte("cpu1"), 16), true)
	check(t, ind.Contains([]byte("cpu2")), true)
	check(t, ind.MaybeContainsValue([]byte("cpu2"), 4), false)
	check(t, ind.MaybeContainsValue([]byte("cpu2"), 5), false)
	check(t, ind.MaybeContainsValue([]byte("cpu2"), 10), false)
	check(t, ind.MaybeContainsValue([]byte("cpu2"), 15), false)
	check(t, ind.MaybeContainsValue([]byte("cpu2"), 16), true)

	ind.DeletePrefix([]byte("cpu"), 15, 20, nil)

	check(t, ind.Contains([]byte("mem")), true)
	check(t, ind.Contains([]byte("cpu1")), false)
	check(t, ind.MaybeContainsValue([]byte("cpu1"), 4), false)
	check(t, ind.MaybeContainsValue([]byte("cpu1"), 5), false)
	check(t, ind.MaybeContainsValue([]byte("cpu1"), 10), false)
	check(t, ind.MaybeContainsValue([]byte("cpu1"), 15), false)
	check(t, ind.MaybeContainsValue([]byte("cpu1"), 16), false)
	check(t, ind.Contains([]byte("cpu2")), false)
	check(t, ind.MaybeContainsValue([]byte("cpu2"), 4), false)
	check(t, ind.MaybeContainsValue([]byte("cpu2"), 5), false)
	check(t, ind.MaybeContainsValue([]byte("cpu2"), 10), false)
	check(t, ind.MaybeContainsValue([]byte("cpu2"), 15), false)
	check(t, ind.MaybeContainsValue([]byte("cpu2"), 16), false)
}

func TestIndirectIndex_DeletePrefix_NoMatch(t *testing.T) {
	check := func(t *testing.T, got, exp bool) {
		t.Helper()
		if exp != got {
			t.Fatalf("expected: %v but got: %v", exp, got)
		}
	}

	index := NewIndexWriter()
	index.Add([]byte("cpu"), BlockInteger, 0, 10, 10, 20)
	ind := loadIndex(t, index)

	ind.DeletePrefix([]byte("b"), 5, 5, nil)
	ind.DeletePrefix([]byte("d"), 5, 5, nil)

	check(t, ind.Contains([]byte("cpu")), true)
	check(t, ind.MaybeContainsValue([]byte("cpu"), 5), true)
}

func TestIndirectIndex_DeletePrefix_Dead(t *testing.T) {
	check := func(t *testing.T, got, exp interface{}) {
		t.Helper()
		if !reflect.DeepEqual(exp, got) {
			t.Fatalf("expected: %q but got: %q", exp, got)
		}
	}

	var keys [][]byte
	dead := func(key []byte) { keys = append(keys, append([]byte(nil), key...)) }

	b := func(keys ...string) (out [][]byte) {
		for _, key := range keys {
			out = append(out, []byte(key))
		}
		return out
	}

	index := NewIndexWriter()
	index.Add([]byte("cpu"), BlockInteger, 0, 10, 10, 20)
	index.Add([]byte("dpu"), BlockInteger, 0, 10, 10, 20)
	ind := loadIndex(t, index)

	ind.DeletePrefix([]byte("b"), 5, 5, dead)
	check(t, keys, b())

	ind.DeletePrefix([]byte("c"), 0, 9, dead)
	check(t, keys, b())

	ind.DeletePrefix([]byte("c"), 9, 10, dead)
	check(t, keys, b("cpu"))

	ind.DeletePrefix([]byte("d"), -50, 50, dead)
	check(t, keys, b("cpu", "dpu"))
}

//
// indirectIndex benchmarks
//

const (
	indexKeyCount   = 500000
	indexBlockCount = 100
)

type indexCacheInfo struct {
	index    *indirectIndex
	offsets  []uint32
	prefixes []prefixEntry
	allKeys  [][]byte
	bytes    []byte
}

func (i *indexCacheInfo) reset() {
	i.index.ro.offsets = append([]uint32(nil), i.offsets...)
	i.index.ro.prefixes = append([]prefixEntry(nil), i.prefixes...)
	i.index.tombstones = make(map[uint32][]TimeRange)
	i.index.prefixTombstones = newPrefixTree()
	resetFaults(i.index)
}

var (
	indexCache = map[string]*indexCacheInfo{}
	indexSizes = map[string][2]int{
		"large": {500000, 100},
		"med":   {1000, 1000},
		"small": {5000, 2},
	}
)

func getFaults(indirect *indirectIndex) int64 {
	return int64(atomic.LoadUint64(&indirect.b.faults))
}

func resetFaults(indirect *indirectIndex) {
	if indirect != nil {
		indirect.b = faultBuffer{b: indirect.b.b}
	}
}

func getIndex(tb testing.TB, name string) (*indirectIndex, *indexCacheInfo) {
	info, ok := indexCache[name]
	if ok {
		info.reset()
		return info.index, info
	}
	info = new(indexCacheInfo)

	sizes, ok := indexSizes[name]
	if !ok {
		sizes = [2]int{indexKeyCount, indexBlockCount}
	}
	keys, blocks := sizes[0], sizes[1]

	writer := NewIndexWriter()

	// add a ballast key that starts at -1 so that we don't trigger optimizations
	// when deleting [0, MaxInt]
	writer.Add([]byte("ballast"), BlockFloat64, -1, 1, 0, 100)

	for i := 0; i < keys; i++ {
		key := []byte(fmt.Sprintf("cpu-%08d", i))
		info.allKeys = append(info.allKeys, key)
		for j := 0; j < blocks; j++ {
			writer.Add(key, BlockFloat64, 0, 100, 10, 100)
		}
	}

	var err error
	info.bytes, err = writer.MarshalBinary()
	if err != nil {
		tb.Fatalf("unexpected error marshaling index: %v", err)
	}

	info.index = NewIndirectIndex()
	if err = info.index.UnmarshalBinary(info.bytes); err != nil {
		tb.Fatalf("unexpected error unmarshaling index: %v", err)
	}
	info.offsets = append([]uint32(nil), info.index.ro.offsets...)
	info.prefixes = append([]prefixEntry(nil), info.index.ro.prefixes...)

	indexCache[name] = info
	return info.index, info
}

func BenchmarkIndirectIndex_UnmarshalBinary(b *testing.B) {
	indirect, info := getIndex(b, "large")
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if err := indirect.UnmarshalBinary(info.bytes); err != nil {
			b.Fatalf("unexpected error unmarshaling index: %v", err)
		}
	}
}

func BenchmarkIndirectIndex_Entries(b *testing.B) {
	indirect, _ := getIndex(b, "med")
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		resetFaults(indirect)
		indirect.ReadEntries([]byte("cpu-00000001"), nil)
	}

	if faultBufferEnabled {
		b.SetBytes(getFaults(indirect) * 4096)
		b.Log("recorded faults:", getFaults(indirect))
	}
}

func BenchmarkIndirectIndex_ReadEntries(b *testing.B) {
	var entries []IndexEntry
	indirect, _ := getIndex(b, "med")
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		resetFaults(indirect)
		entries, _ = indirect.ReadEntries([]byte("cpu-00000001"), entries)
	}

	if faultBufferEnabled {
		b.SetBytes(getFaults(indirect) * 4096)
		b.Log("recorded faults:", getFaults(indirect))
	}
}

func BenchmarkBlockIterator_Next(b *testing.B) {
	indirect, _ := getIndex(b, "med")
	r := TSMReader{index: indirect}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		resetFaults(indirect)
		bi := r.BlockIterator()
		for bi.Next() {
		}
	}

	if faultBufferEnabled {
		b.SetBytes(getFaults(indirect) * 4096)
		b.Log("recorded faults:", getFaults(indirect))
	}
}

func BenchmarkIndirectIndex_DeleteRangeLast(b *testing.B) {
	indirect, _ := getIndex(b, "large")
	keys := [][]byte{[]byte("cpu-00999999")}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		resetFaults(indirect)
		indirect.DeleteRange(keys, 10, 50)
	}

	if faultBufferEnabled {
		b.SetBytes(getFaults(indirect) * 4096)
		b.Log("recorded faults:", getFaults(indirect))
	}
}

func BenchmarkIndirectIndex_DeleteRangeFull(b *testing.B) {
	run := func(b *testing.B, name string) {
		indirect, _ := getIndex(b, name)
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			b.StopTimer()
			var info *indexCacheInfo
			indirect, info = getIndex(b, name)
			b.StartTimer()

			for i := 0; i < len(info.allKeys); i += 4096 {
				n := i + 4096
				if n > len(info.allKeys) {
					n = len(info.allKeys)
				}
				indirect.DeleteRange(info.allKeys[i:n], 10, 50)
			}
		}

		if faultBufferEnabled {
			b.SetBytes(getFaults(indirect) * 4096)
			b.Log("recorded faults:", getFaults(indirect))
		}
	}

	b.Run("Large", func(b *testing.B) { run(b, "large") })
	b.Run("Small", func(b *testing.B) { run(b, "small") })
}

func BenchmarkIndirectIndex_DeleteRangeFull_Covered(b *testing.B) {
	run := func(b *testing.B, name string) {
		indirect, _ := getIndex(b, name)
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			b.StopTimer()
			var info *indexCacheInfo
			indirect, info = getIndex(b, name)
			b.StartTimer()

			for i := 0; i < len(info.allKeys); i += 4096 {
				n := i + 4096
				if n > len(info.allKeys) {
					n = len(info.allKeys)
				}
				indirect.DeleteRange(info.allKeys[i:n], 0, math.MaxInt64)
			}
		}

		if faultBufferEnabled {
			b.SetBytes(getFaults(indirect) * 4096)
			b.Log("recorded faults:", getFaults(indirect))
		}
	}

	b.Run("Large", func(b *testing.B) { run(b, "large") })
	b.Run("Small", func(b *testing.B) { run(b, "small") })
}

func BenchmarkIndirectIndex_Delete(b *testing.B) {
	run := func(b *testing.B, name string) {
		indirect, _ := getIndex(b, name)
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			b.StopTimer()
			var info *indexCacheInfo
			indirect, info = getIndex(b, name)
			b.StartTimer()

			for i := 0; i < len(info.allKeys); i += 4096 {
				n := i + 4096
				if n > len(info.allKeys) {
					n = len(info.allKeys)
				}
				indirect.Delete(info.allKeys[i:n])
			}
		}

		if faultBufferEnabled {
			b.SetBytes(getFaults(indirect) * 4096)
			b.Log("recorded faults:", getFaults(indirect))
		}
	}

	b.Run("Large", func(b *testing.B) { run(b, "large") })
	b.Run("Small", func(b *testing.B) { run(b, "small") })
}

func BenchmarkIndirectIndex_DeletePrefixFull(b *testing.B) {
	prefix := []byte("cpu-")
	run := func(b *testing.B, name string) {
		indirect, _ := getIndex(b, name)
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			b.StopTimer()
			indirect, _ = getIndex(b, name)
			b.StartTimer()

			indirect.DeletePrefix(prefix, 10, 50, nil)
		}

		if faultBufferEnabled {
			b.SetBytes(getFaults(indirect) * 4096)
			b.Log("recorded faults:", getFaults(indirect))
		}
	}

	b.Run("Large", func(b *testing.B) { run(b, "large") })
	b.Run("Small", func(b *testing.B) { run(b, "small") })
}

func BenchmarkIndirectIndex_DeletePrefixFull_Covered(b *testing.B) {
	prefix := []byte("cpu-")
	run := func(b *testing.B, name string) {
		indirect, _ := getIndex(b, name)
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			b.StopTimer()
			indirect, _ = getIndex(b, name)
			b.StartTimer()

			indirect.DeletePrefix(prefix, 0, math.MaxInt64, nil)
		}

		if faultBufferEnabled {
			b.SetBytes(getFaults(indirect) * 4096)
			b.Log("recorded faults:", getFaults(indirect))
		}
	}

	b.Run("Large", func(b *testing.B) { run(b, "large") })
	b.Run("Small", func(b *testing.B) { run(b, "small") })
}

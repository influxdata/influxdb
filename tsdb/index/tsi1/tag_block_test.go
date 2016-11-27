package tsi1_test

import (
	"bytes"
	"reflect"
	"strconv"
	"testing"

	"github.com/influxdata/influxdb/tsdb/index/tsi1"
)

// Ensure tag blocks can be written and opened.
func TestTagBlockWriter(t *testing.T) {
	// Write 3 series to writer.
	tsw := tsi1.NewTagBlockWriter()
	tsw.AddTagValue([]byte("region"), []byte("us-east"), false, []uint32{1, 2})
	tsw.AddTagValue([]byte("region"), []byte("us-west"), false, []uint32{3})
	tsw.AddTagValue([]byte("host"), []byte("server0"), false, []uint32{1})
	tsw.AddTagValue([]byte("host"), []byte("server1"), false, []uint32{2})
	tsw.AddTagValue([]byte("host"), []byte("server2"), false, []uint32{3})

	// Encode into buffer.
	var buf bytes.Buffer
	if n, err := tsw.WriteTo(&buf); err != nil {
		t.Fatal(err)
	} else if int(n) != buf.Len() {
		t.Fatalf("bytes written mismatch: %d, expected %d", n, buf.Len())
	}

	// Unmarshal into a block.
	var blk tsi1.TagBlock
	if err := blk.UnmarshalBinary(buf.Bytes()); err != nil {
		t.Fatal(err)
	}

	// Verify data.
	if e, _ := blk.TagValueElem([]byte("region"), []byte("us-east")); e == nil {
		t.Fatal("expected element")
	} else if a := e.(*tsi1.TagBlockValueElem).SeriesIDs(); !reflect.DeepEqual(a, []uint32{1, 2}) {
		t.Fatalf("unexpected series ids: %#v", a)
	}

	if e, _ := blk.TagValueElem([]byte("region"), []byte("us-west")); e == nil {
		t.Fatal("expected element")
	} else if a := e.(*tsi1.TagBlockValueElem).SeriesIDs(); !reflect.DeepEqual(a, []uint32{3}) {
		t.Fatalf("unexpected series ids: %#v", a)
	}
	if e, _ := blk.TagValueElem([]byte("host"), []byte("server0")); e == nil {
		t.Fatal("expected element")
	} else if a := e.(*tsi1.TagBlockValueElem).SeriesIDs(); !reflect.DeepEqual(a, []uint32{1}) {
		t.Fatalf("unexpected series ids: %#v", a)
	}
	if e, _ := blk.TagValueElem([]byte("host"), []byte("server1")); e == nil {
		t.Fatal("expected element")
	} else if a := e.(*tsi1.TagBlockValueElem).SeriesIDs(); !reflect.DeepEqual(a, []uint32{2}) {
		t.Fatalf("unexpected series ids: %#v", a)
	}
	if e, _ := blk.TagValueElem([]byte("host"), []byte("server2")); e == nil {
		t.Fatal("expected element")
	} else if a := e.(*tsi1.TagBlockValueElem).SeriesIDs(); !reflect.DeepEqual(a, []uint32{3}) {
		t.Fatalf("unexpected series ids: %#v", a)
	}
}

var benchmarkTagBlock10x1000 *tsi1.TagBlock
var benchmarkTagBlock100x1000 *tsi1.TagBlock
var benchmarkTagBlock1000x1000 *tsi1.TagBlock
var benchmarkTagBlock1x1000000 *tsi1.TagBlock

func BenchmarkTagBlock_SeriesN_10_1000(b *testing.B) {
	benchmarkTagBlock_SeriesN(b, 10, 1000, &benchmarkTagBlock10x1000)
}
func BenchmarkTagBlock_SeriesN_100_1000(b *testing.B) {
	benchmarkTagBlock_SeriesN(b, 100, 1000, &benchmarkTagBlock100x1000)
}
func BenchmarkTagBlock_SeriesN_1000_1000(b *testing.B) {
	benchmarkTagBlock_SeriesN(b, 1000, 1000, &benchmarkTagBlock1000x1000)
}
func BenchmarkTagBlock_SeriesN_1_1000000(b *testing.B) {
	benchmarkTagBlock_SeriesN(b, 1, 1000000, &benchmarkTagBlock1x1000000)
}

func benchmarkTagBlock_SeriesN(b *testing.B, tagN, valueN int, blk **tsi1.TagBlock) {
	if (*blk) == nil {
		tw := tsi1.NewTagBlockWriter()

		// Write block.
		var kbuf, vbuf [20]byte
		for i := 0; i < tagN; i++ {
			for j := 0; j < valueN; j++ {
				k := strconv.AppendInt(kbuf[:0], int64(i), 10)
				v := strconv.AppendInt(vbuf[:0], int64(j), 10)
				tw.AddTagValue(k, v, false, []uint32{1})
			}
		}

		// Encode into buffer.
		var buf bytes.Buffer
		if _, err := tw.WriteTo(&buf); err != nil {
			b.Fatal(err)
		}
		b.Log("size", buf.Len())

		// Unmarshal into a block.
		*blk = &tsi1.TagBlock{}
		if err := (*blk).UnmarshalBinary(buf.Bytes()); err != nil {
			b.Fatal(err)
		}
	}

	// Benchmark lookups.
	b.ReportAllocs()
	b.ResetTimer()

	key, value := []byte("0"), []byte("0")
	for i := 0; i < b.N; i++ {
		if e, _ := (*blk).TagValueElem(key, value); e == nil {
			b.Fatal("expected element")
		} else if n := e.(*tsi1.TagBlockValueElem).SeriesN(); n != 1 {
			b.Fatalf("unexpected series count: %d", n)
		}
	}
}

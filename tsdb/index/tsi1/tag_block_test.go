package tsi1_test

import (
	"bytes"
	"fmt"
	"reflect"
	"testing"

	"github.com/influxdata/influxdb/tsdb"
	"github.com/influxdata/influxdb/tsdb/index/tsi1"
)

// Ensure tag blocks can be written and opened.
func TestTagBlockWriter(t *testing.T) {
	// Write 3 series to writer.
	var buf bytes.Buffer
	enc := tsi1.NewTagBlockEncoder(&buf)

	if err := enc.EncodeKey([]byte("host"), false); err != nil {
		t.Fatal(err)
	} else if err := enc.EncodeValue([]byte("server0"), false, tsdb.NewSeriesIDSet(1)); err != nil {
		t.Fatal(err)
	} else if err := enc.EncodeValue([]byte("server1"), false, tsdb.NewSeriesIDSet(2)); err != nil {
		t.Fatal(err)
	} else if err := enc.EncodeValue([]byte("server2"), false, tsdb.NewSeriesIDSet(3)); err != nil {
		t.Fatal(err)
	}

	if err := enc.EncodeKey([]byte("region"), false); err != nil {
		t.Fatal(err)
	} else if err := enc.EncodeValue([]byte("us-east"), false, tsdb.NewSeriesIDSet(1, 2)); err != nil {
		t.Fatal(err)
	} else if err := enc.EncodeValue([]byte("us-west"), false, tsdb.NewSeriesIDSet(3)); err != nil {
		t.Fatal(err)
	}

	// Flush encoder.
	if err := enc.Close(); err != nil {
		t.Fatal(err)
	} else if int(enc.N()) != buf.Len() {
		t.Fatalf("bytes written mismatch: %d, expected %d", enc.N(), buf.Len())
	}

	// Unmarshal into a block.
	var blk tsi1.TagBlock
	if err := blk.UnmarshalBinary(buf.Bytes()); err != nil {
		t.Fatal(err)
	}

	// Verify data.
	if e := blk.TagValueElem([]byte("region"), []byte("us-east")); e == nil {
		t.Fatal("expected element")
	} else if a, err := e.(*tsi1.TagBlockValueElem).SeriesIDs(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	} else if !reflect.DeepEqual(a, []uint64{1, 2}) {
		t.Fatalf("unexpected series ids: %#v", a)
	}

	if e := blk.TagValueElem([]byte("region"), []byte("us-west")); e == nil {
		t.Fatal("expected element")
	} else if a, err := e.(*tsi1.TagBlockValueElem).SeriesIDs(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	} else if !reflect.DeepEqual(a, []uint64{3}) {
		t.Fatalf("unexpected series ids: %#v", a)
	}
	if e := blk.TagValueElem([]byte("host"), []byte("server0")); e == nil {
		t.Fatal("expected element")
	} else if a, err := e.(*tsi1.TagBlockValueElem).SeriesIDs(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	} else if !reflect.DeepEqual(a, []uint64{1}) {
		t.Fatalf("unexpected series ids: %#v", a)
	}
	if e := blk.TagValueElem([]byte("host"), []byte("server1")); e == nil {
		t.Fatal("expected element")
	} else if a, err := e.(*tsi1.TagBlockValueElem).SeriesIDs(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	} else if !reflect.DeepEqual(a, []uint64{2}) {
		t.Fatalf("unexpected series ids: %#v", a)
	}
	if e := blk.TagValueElem([]byte("host"), []byte("server2")); e == nil {
		t.Fatal("expected element")
	} else if a, err := e.(*tsi1.TagBlockValueElem).SeriesIDs(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	} else if !reflect.DeepEqual(a, []uint64{3}) {
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
		var buf bytes.Buffer
		enc := tsi1.NewTagBlockEncoder(&buf)

		// Write block.
		for i := 0; i < tagN; i++ {
			if err := enc.EncodeKey([]byte(fmt.Sprintf("%08d", i)), false); err != nil {
				b.Fatal(err)
			}

			for j := 0; j < valueN; j++ {
				if err := enc.EncodeValue([]byte(fmt.Sprintf("%08d", j)), false, tsdb.NewSeriesIDSet(1)); err != nil {
					b.Fatal(err)
				}
			}
		}

		// Flush encoder.
		if err := enc.Close(); err != nil {
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
		if e := (*blk).TagValueElem(key, value); e == nil {
			b.Fatal("expected element")
		} else if n := e.(*tsi1.TagBlockValueElem).SeriesN(); n != 1 {
			b.Fatalf("unexpected series count: %d", n)
		}
	}
}

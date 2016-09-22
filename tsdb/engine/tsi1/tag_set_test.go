package tsi1_test

import (
	"bytes"
	"reflect"
	"strconv"
	"testing"

	"github.com/influxdata/influxdb/tsdb/engine/tsi1"
)

// Ensure tag sets can be written and opened.
func TestTagSetWriter(t *testing.T) {
	// Write 3 series to writer.
	tsw := tsi1.NewTagSetWriter()
	tsw.AddSeries(map[string]string{"region": "us-east", "host": "server0"}, 1)
	tsw.AddSeries(map[string]string{"region": "us-east", "host": "server1"}, 2)
	tsw.AddSeries(map[string]string{"region": "us-west", "host": "server2"}, 3)

	// Encode into buffer.
	var buf bytes.Buffer
	if n, err := tsw.WriteTo(&buf); err != nil {
		t.Fatal(err)
	} else if n == 0 {
		t.Fatal("expected bytes written")
	}

	// Unmarshal into a TagSet.
	var ts tsi1.TagSet
	if err := ts.UnmarshalBinary(buf.Bytes()); err != nil {
		t.Fatal(err)
	}

	// Verify data in TagSet.
	if a := ts.TagValueSeriesIDs([]byte("region"), []byte("us-east")); !reflect.DeepEqual(a, []uint32{1, 2}) {
		t.Fatalf("unexpected series ids: %#v", a)
	} else if a := ts.TagValueSeriesIDs([]byte("region"), []byte("us-west")); !reflect.DeepEqual(a, []uint32{3}) {
		t.Fatalf("unexpected series ids: %#v", a)
	}
	if a := ts.TagValueSeriesIDs([]byte("host"), []byte("server0")); !reflect.DeepEqual(a, []uint32{1}) {
		t.Fatalf("unexpected series ids: %#v", a)
	} else if a := ts.TagValueSeriesIDs([]byte("host"), []byte("server1")); !reflect.DeepEqual(a, []uint32{2}) {
		t.Fatalf("unexpected series ids: %#v", a)
	} else if a := ts.TagValueSeriesIDs([]byte("host"), []byte("server2")); !reflect.DeepEqual(a, []uint32{3}) {
		t.Fatalf("unexpected series ids: %#v", a)
	}
}

var benchmarkTagSet10x1000 *tsi1.TagSet
var benchmarkTagSet100x1000 *tsi1.TagSet
var benchmarkTagSet1000x1000 *tsi1.TagSet
var benchmarkTagSet1x1000000 *tsi1.TagSet

func BenchmarkTagSet_SeriesN_10_1000(b *testing.B) {
	benchmarkTagSet_SeriesN(b, 10, 1000, &benchmarkTagSet10x1000)
}
func BenchmarkTagSet_SeriesN_100_1000(b *testing.B) {
	benchmarkTagSet_SeriesN(b, 100, 1000, &benchmarkTagSet100x1000)
}
func BenchmarkTagSet_SeriesN_1000_1000(b *testing.B) {
	benchmarkTagSet_SeriesN(b, 1000, 1000, &benchmarkTagSet1000x1000)
}
func BenchmarkTagSet_SeriesN_1_1000000(b *testing.B) {
	benchmarkTagSet_SeriesN(b, 1, 1000000, &benchmarkTagSet1x1000000)
}

func benchmarkTagSet_SeriesN(b *testing.B, tagN, valueN int, ts **tsi1.TagSet) {
	if (*ts) == nil {
		tsw := tsi1.NewTagSetWriter()

		// Write tagset block.
		var kbuf, vbuf [20]byte
		for i := 0; i < tagN; i++ {
			for j := 0; j < valueN; j++ {
				k := strconv.AppendInt(kbuf[:0], int64(i), 10)
				v := strconv.AppendInt(vbuf[:0], int64(j), 10)
				tsw.AddTagValueSeries(k, v, 1)
			}
		}
		tsw.AddSeries(map[string]string{"region": "us-east", "host": "server0"}, 1)
		tsw.AddSeries(map[string]string{"region": "us-east", "host": "server1"}, 2)
		tsw.AddSeries(map[string]string{"region": "us-west", "host": "server2"}, 3)

		// Encode into buffer.
		var buf bytes.Buffer
		if _, err := tsw.WriteTo(&buf); err != nil {
			b.Fatal(err)
		}
		b.Log("size", buf.Len())

		// Unmarshal into a TagSet.
		*ts = &tsi1.TagSet{}
		if err := (*ts).UnmarshalBinary(buf.Bytes()); err != nil {
			b.Fatal(err)
		}
	}

	// Benchmark lookups.
	b.ReportAllocs()
	b.ResetTimer()

	key, value := []byte("0"), []byte("0")
	for i := 0; i < b.N; i++ {
		if n := (*ts).TagValueSeriesN(key, value); n != 1 {
			b.Fatalf("unexpected series count: %d", n)
		}
	}
}

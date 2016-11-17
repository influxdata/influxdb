package tsi1_test

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"reflect"
	"testing"

	"github.com/influxdata/influxdb/tsdb/index/tsi1"
)

func TestReadMeasurementBlockTrailer(t *testing.T) {
	// Build a trailer
	var (
		data                       = make([]byte, tsi1.MeasurementTrailerSize)
		blockversion               = uint16(1)
		blockOffset, blockSize     = uint64(1), uint64(2500)
		hashIdxOffset, hashIdxSize = uint64(2501), uint64(1000)
		sketchOffset, sketchSize   = uint64(3501), uint64(250)
		tsketchOffset, tsketchSize = uint64(3751), uint64(250)
	)

	binary.BigEndian.PutUint64(data[0:], blockOffset)
	binary.BigEndian.PutUint64(data[8:], blockSize)
	binary.BigEndian.PutUint64(data[16:], hashIdxOffset)
	binary.BigEndian.PutUint64(data[24:], hashIdxSize)
	binary.BigEndian.PutUint64(data[32:], sketchOffset)
	binary.BigEndian.PutUint64(data[40:], sketchSize)
	binary.BigEndian.PutUint64(data[48:], tsketchOffset)
	binary.BigEndian.PutUint64(data[56:], tsketchSize)
	binary.BigEndian.PutUint16(data[64:], blockversion)

	trailer, err := tsi1.ReadMeasurementBlockTrailer(data)
	if err != nil {
		t.Logf("trailer is: %#v\n", trailer)
		t.Fatal(err)
	}

	ok := true &&
		trailer.Version == int(blockversion) &&
		trailer.Data.Offset == int64(blockOffset) &&
		trailer.Data.Size == int64(blockSize) &&
		trailer.HashIndex.Offset == int64(hashIdxOffset) &&
		trailer.HashIndex.Size == int64(hashIdxSize) &&
		trailer.Sketch.Offset == int64(sketchOffset) &&
		trailer.Sketch.Size == int64(sketchSize) &&
		trailer.TSketch.Offset == int64(tsketchOffset) &&
		trailer.TSketch.Size == int64(tsketchSize)

	if !ok {
		t.Fatalf("got %v\nwhich does not match expected", trailer)
	}
}

func TestMeasurementBlockTrailer_WriteTo(t *testing.T) {
	var trailer = tsi1.MeasurementBlockTrailer{
		Version: 1,
		Data: struct {
			Offset int64
			Size   int64
		}{Offset: 1, Size: 2},
		HashIndex: struct {
			Offset int64
			Size   int64
		}{Offset: 3, Size: 4},
		Sketch: struct {
			Offset int64
			Size   int64
		}{Offset: 5, Size: 6},
		TSketch: struct {
			Offset int64
			Size   int64
		}{Offset: 7, Size: 8},
	}

	var buf bytes.Buffer
	n, err := trailer.WriteTo(&buf)
	if got, exp := n, int64(tsi1.MeasurementTrailerSize); got != exp {
		t.Fatalf("got %v, exp %v", got, exp)
	}

	if got := err; got != nil {
		t.Fatalf("got %v, exp %v", got, nil)
	}

	// Verify trailer written correctly.
	exp := "" +
		"0000000000000001" + // data offset
		"0000000000000002" + // data size
		"0000000000000003" + // hash index offset
		"0000000000000004" + // hash index size
		"0000000000000005" + // sketch offset
		"0000000000000006" + // sketch size
		"0000000000000007" + // tsketch offset
		"0000000000000008" + // tsketch size
		"0001" // version

	if got, exp := fmt.Sprintf("%x", buf.String()), exp; got != exp {
		t.Fatalf("got %v, exp %v", got, exp)
	}
}

// Ensure measurement blocks can be written and opened.
func TestMeasurementBlockWriter(t *testing.T) {
	// Write 3 measurements to writer.
	mw := tsi1.NewMeasurementBlockWriter()
	mw.Add([]byte("foo"), 100, 10, []uint32{1, 3, 4})
	mw.Add([]byte("bar"), 200, 20, []uint32{2})
	mw.Add([]byte("baz"), 300, 30, []uint32{5, 6})

	// Encode into buffer.
	var buf bytes.Buffer
	if n, err := mw.WriteTo(&buf); err != nil {
		t.Fatal(err)
	} else if n == 0 {
		t.Fatal("expected bytes written")
	}

	// Unmarshal into a block.
	var blk tsi1.MeasurementBlock
	if err := blk.UnmarshalBinary(buf.Bytes()); err != nil {
		t.Fatal(err)
	}

	// Verify data in block.
	if e, ok := blk.Elem([]byte("foo")); !ok {
		t.Fatal("expected element")
	} else if e.TagBlockOffset() != 100 || e.TagBlockSize() != 10 {
		t.Fatalf("unexpected offset/size: %v/%v", e.TagBlockOffset(), e.TagBlockSize())
	} else if !reflect.DeepEqual(e.SeriesIDs(), []uint32{1, 3, 4}) {
		t.Fatalf("unexpected series data: %#v", e.SeriesIDs())
	}

	if e, ok := blk.Elem([]byte("bar")); !ok {
		t.Fatal("expected element")
	} else if e.TagBlockOffset() != 200 || e.TagBlockSize() != 20 {
		t.Fatalf("unexpected offset/size: %v/%v", e.TagBlockOffset(), e.TagBlockSize())
	} else if !reflect.DeepEqual(e.SeriesIDs(), []uint32{2}) {
		t.Fatalf("unexpected series data: %#v", e.SeriesIDs())
	}

	if e, ok := blk.Elem([]byte("baz")); !ok {
		t.Fatal("expected element")
	} else if e.TagBlockOffset() != 300 || e.TagBlockSize() != 30 {
		t.Fatalf("unexpected offset/size: %v/%v", e.TagBlockOffset(), e.TagBlockSize())
	} else if !reflect.DeepEqual(e.SeriesIDs(), []uint32{5, 6}) {
		t.Fatalf("unexpected series data: %#v", e.SeriesIDs())
	}

	// Verify non-existent measurement doesn't exist.
	if _, ok := blk.Elem([]byte("BAD_MEASUREMENT")); ok {
		t.Fatal("expected no element")
	}
}

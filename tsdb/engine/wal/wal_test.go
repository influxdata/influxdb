package wal

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"testing"

	"github.com/influxdb/influxdb/influxql"
	"github.com/influxdb/influxdb/tsdb"
)

func TestWAL_WritePoints(t *testing.T) {
	log := openTestWAL()
	defer log.Close()
	defer func() {
		os.RemoveAll(log.path)
	}()

	if err := log.Open(); err != nil {
		t.Fatalf("couldn't open wal: %s", err.Error())
	}

	codec := tsdb.NewFieldCodec(map[string]*tsdb.Field{
		"value": {
			ID:   uint8(1),
			Name: "value",
			Type: influxql.Float,
		},
	})

	// test that we can write to two different series
	p1 := parsePoint("cpu,host=A value=23.2 1", codec)
	p2 := parsePoint("cpu,host=A value=25.3 4", codec)
	p3 := parsePoint("cpu,host=B value=1.0 1", codec)
	if err := log.WritePoints([]tsdb.Point{p1, p2, p3}); err != nil {
		t.Fatalf("failed to write points: %s", err.Error())
	}

	verify := func() {
		c := log.Cursor("cpu,host=A")
		k, v := c.Seek(inttob(1))

		// ensure the series are there and points are in order
		if bytes.Compare(v, p1.Data()) != 0 {
			t.Fatalf("expected to seek to first point but got key and value: %v %v", k, v)
		}

		k, v = c.Next()
		if bytes.Compare(v, p2.Data()) != 0 {
			t.Fatalf("expected to seek to first point but got key and value: %v %v", k, v)
		}

		k, v = c.Next()
		if k != nil {
			t.Fatalf("expected nil on last seek: %v %v", k, v)
		}

		c = log.Cursor("cpu,host=B")
		k, v = c.Next()
		if bytes.Compare(v, p3.Data()) != 0 {
			t.Fatalf("expected to seek to first point but got key and value: %v %v", k, v)
		}
	}

	verify()

	// ensure that we can close and re-open the log with points still there
	log.Close()
	log.Open()

	verify()

	// ensure we can write new points into the series
	p4 := parsePoint("cpu,host=A value=1.0 7", codec)
	// ensure we can write an all new series
	p5 := parsePoint("cpu,host=C value=1.4 2", codec)
	// ensure we can write a point out of order and get it back
	p6 := parsePoint("cpu,host=A value=1.3 2", codec)
	// // ensure we can write to a new partition
	// p7 := parsePoint("cpu,region=west value=2.2", codec)
	if err := log.WritePoints([]tsdb.Point{p4, p5, p6}); err != nil {
		t.Fatalf("failed to write points: %s", err.Error())
	}

	verify2 := func() {
		c := log.Cursor("cpu,host=A")
		k, v := c.Next()
		if bytes.Compare(v, p1.Data()) != 0 {
			t.Fatalf("order wrong, expected p1, %v %v %v", v, k, p1.Data())
		}
		_, v = c.Next()
		if bytes.Compare(v, p6.Data()) != 0 {
			t.Fatal("order wrong, expected p6")
		}
		_, v = c.Next()
		if bytes.Compare(v, p2.Data()) != 0 {
			t.Fatal("order wrong, expected p6")
		}
		_, v = c.Next()
		if bytes.Compare(v, p4.Data()) != 0 {
			t.Fatal("order wrong, expected p6")
		}

		c = log.Cursor("cpu,host=C")
		_, v = c.Next()
		if bytes.Compare(v, p5.Data()) != 0 {
			t.Fatal("order wrong, expected p6")
		}
	}

	verify2()

	log.Close()
	log.Open()

	verify2()
}

func TestWAL_CorruptDataLengthSize(t *testing.T) {
	log := openTestWAL()
	defer log.Close()
	defer func() {
		os.RemoveAll(log.path)
	}()

	if err := log.Open(); err != nil {
		t.Fatalf("couldn't open wal: %s", err.Error())
	}

	codec := tsdb.NewFieldCodec(map[string]*tsdb.Field{
		"value": {
			ID:   uint8(1),
			Name: "value",
			Type: influxql.Float,
		},
	})

	// test that we can write to two different series
	p1 := parsePoint("cpu,host=A value=23.2 1", codec)
	p2 := parsePoint("cpu,host=A value=25.3 4", codec)
	if err := log.WritePoints([]tsdb.Point{p1, p2}); err != nil {
		t.Fatalf("failed to write points: %s", err.Error())
	}

	verify := func() {
		c := log.Cursor("cpu,host=A")
		_, v := c.Next()
		if bytes.Compare(v, p1.Data()) != 0 {
			t.Fatal("p1 value wrong")
		}
		_, v = c.Next()
		if bytes.Compare(v, p2.Data()) != 0 {
			t.Fatal("p2 value wrong")
		}
		_, v = c.Next()
		if v != nil {
			t.Fatal("expected cursor to return nil")
		}
	}

	verify()

	// now write junk data and ensure that we can close, re-open and read
	f := log.partitions[1].currentSegmentFile
	f.Write([]byte{0x23, 0x12})
	f.Sync()
	log.Close()
	log.Open()

	verify()

	// now write new data and ensure it's all good
	p3 := parsePoint("cpu,host=A value=29.2 6", codec)
	if err := log.WritePoints([]tsdb.Point{p3}); err != nil {
		t.Fatalf("failed to write point: %s", err.Error())
	}

	verify = func() {
		c := log.Cursor("cpu,host=A")
		_, v := c.Next()
		if bytes.Compare(v, p1.Data()) != 0 {
			t.Fatal("p1 value wrong")
		}
		_, v = c.Next()
		if bytes.Compare(v, p2.Data()) != 0 {
			t.Fatal("p2 value wrong")
		}
		_, v = c.Next()
		if bytes.Compare(v, p3.Data()) != 0 {
			t.Fatal("p3 value wrong")
		}
	}

	verify()
	log.Close()
	log.Open()
	verify()
}

func TestWAL_CorruptDataBlock(t *testing.T) {
	log := openTestWAL()
	defer log.Close()
	defer func() {
		os.RemoveAll(log.path)
	}()

	if err := log.Open(); err != nil {
		t.Fatalf("couldn't open wal: %s", err.Error())
	}

	codec := tsdb.NewFieldCodec(map[string]*tsdb.Field{
		"value": {
			ID:   uint8(1),
			Name: "value",
			Type: influxql.Float,
		},
	})

	// test that we can write to two different series
	p1 := parsePoint("cpu,host=A value=23.2 1", codec)
	p2 := parsePoint("cpu,host=A value=25.3 4", codec)
	if err := log.WritePoints([]tsdb.Point{p1, p2}); err != nil {
		t.Fatalf("failed to write points: %s", err.Error())
	}

	verify := func() {
		c := log.Cursor("cpu,host=A")
		_, v := c.Next()
		if bytes.Compare(v, p1.Data()) != 0 {
			t.Fatal("p1 value wrong")
		}
		_, v = c.Next()
		if bytes.Compare(v, p2.Data()) != 0 {
			t.Fatal("p2 value wrong")
		}
		_, v = c.Next()
		if v != nil {
			t.Fatal("expected cursor to return nil")
		}
	}

	verify()

	// now write junk data and ensure that we can close, re-open and read

	f := log.partitions[1].currentSegmentFile
	f.Write(u64tob(23))
	// now write a bunch of garbage
	for i := 0; i < 1000; i++ {
		f.Write([]byte{0x23, 0x78, 0x11, 0x33})
	}
	f.Sync()

	log.Close()
	log.Open()

	verify()

	// now write new data and ensure it's all good
	p3 := parsePoint("cpu,host=A value=29.2 6", codec)
	if err := log.WritePoints([]tsdb.Point{p3}); err != nil {
		t.Fatalf("failed to write point: %s", err.Error())
	}

	verify = func() {
		c := log.Cursor("cpu,host=A")
		_, v := c.Next()
		if bytes.Compare(v, p1.Data()) != 0 {
			t.Fatal("p1 value wrong")
		}
		_, v = c.Next()
		if bytes.Compare(v, p2.Data()) != 0 {
			t.Fatal("p2 value wrong")
		}
		_, v = c.Next()
		if bytes.Compare(v, p3.Data()) != 0 {
			t.Fatal("p3 value wrong", p3.Data(), v)
		}
	}

	verify()
	log.Close()
	log.Open()
	verify()
}

// Ensure the wal flushes and compacts after a partition has enough series in
// it with enough data to flush
func TestWAL_CompactAfterPercentageThreshold(t *testing.T) {
	log := openTestWAL()
	log.PartitionCount = 2
	log.CompactionThreshold = 0.7

	defer log.Close()
	defer func() {
		os.RemoveAll(log.path)
	}()

	if err := log.Open(); err != nil {
		t.Fatalf("couldn't open wal: %s", err.Error())
	}

	codec := tsdb.NewFieldCodec(map[string]*tsdb.Field{
		"value": {
			ID:   uint8(1),
			Name: "value",
			Type: influxql.Float,
		},
	})

	points := make([]map[string][][]byte, 0)
	log.IndexWriter = &testIndexWriter{fn: func(pointsByKey map[string][][]byte) error {
		points = append(points, pointsByKey)
		return nil
	}}

	readySize := 1024

	numSeries := 100
	b := make([]byte, 70*5000)
	for i := 1; i <= 100; i++ {
		buf := bytes.NewBuffer(b)
		for j := 1; j <= numSeries; j++ {
			buf.WriteString(fmt.Sprintf("cpu,host=A,region=uswest%d value=%.3f %d\n", j, rand.Float64(), i))
		}

		// ensure that before we go over the threshold it isn't marked for flushing
		if i < 50 {
			// interleave data for some series that won't be ready to flush
			buf.WriteString(fmt.Sprintf("cpu,host=A,region=useast1 value=%.3f %d\n", rand.Float64(), i))
			buf.WriteString(fmt.Sprintf("cpu,host=A,region=useast3 value=%.3f %d\n", rand.Float64(), i))

			// ensure that as a whole its not ready for flushing yet
			if log.partitions[1].shouldFlush(DefaultMaxSeriesSize, readySize, DefaultCompactionThreshold, DefaultPartitionSizeThreshold) {
				t.Fatal("expected partition 1 to return false from shouldFlush")
			}
		}

		// write the batch out
		if err := log.WritePoints(parsePoints(buf.String(), codec)); err != nil {
			t.Fatalf("failed to write points: %s", err.Error())
		}
		buf = bytes.NewBuffer(b)
	}

	// ensure we have some data
	c := log.Cursor("cpu,host=A,region=uswest23")
	k, v := c.Next()
	if btou64(k) != 1 {
		fmt.Println("expected data ", k, v)
	}

	// ensure it is marked as should flush because of the threshold
	if !log.partitions[1].shouldFlush(DefaultMaxSeriesSize, readySize, DefaultCompactionThreshold, DefaultPartitionSizeThreshold) {
		t.Fatal("expected partition 1 to return true from shouldFlush")
	}

	if err := log.partitions[1].flushAndCompact(log.IndexWriter, DefaultMaxSeriesSize, readySize); err != nil {
		t.Fatalf("error flushing and compacting: %s", err.Error())
	}

	// should be nil
	c = log.Cursor("cpu,host=A,region=uswest23")
	k, v = c.Next()
	if k != nil || v != nil {
		t.Fatal("expected cache to be nil after flush: ", k, v)
	}

	c = log.Cursor("cpu,host=A,region=useast1")
	k, v = c.Next()
	if btou64(k) != 1 {
		t.Fatal("expected cache to be there after flush and compact: ", k, v)
	}

	if len(points) == 0 {
		t.Fatal("expected points to be flushed to index")
	}

	// now close and re-open the wal and ensure the compacted data is gone and other data is still there
	log.Close()
	log.Open()

	c = log.Cursor("cpu,host=A,region=uswest23")
	k, v = c.Next()
	if k != nil || v != nil {
		t.Fatal("expected cache to be nil after flush and re-open: ", k, v)
	}

	c = log.Cursor("cpu,host=A,region=useast1")
	k, v = c.Next()
	if btou64(k) != 1 {
		t.Fatal("expected cache to be there after flush and compact: ", k, v)
	}
}

// test that partitions get compacted and flushed when number of series hits compaction threshold
// test that partitions get compacted and flushed when a single series hits the compaction threshold
// test that writes slow down when the partition size threshold is hit

// func TestWAL_MultipleSegments(t *testing.T) {
// 	runtime.GOMAXPROCS(8)

// 	log := openTestWAL()
// 	defer log.Close()
// 	// defer func() {
// 	// 	os.RemoveAll(log.path)
// 	// }()

// 	if err := log.Open(); err != nil {
// 		t.Fatalf("couldn't open wal: ", err.Error())
// 	}

// 	codec := tsdb.NewFieldCodec(map[string]*tsdb.Field{
// 		"value": {
// 			ID:   uint8(1),
// 			Name: "value",
// 			Type: influxql.Float,
// 		},
// 	})

// 	startTime := time.Now()
// 	numSeries := 250000
// 	perPost := 5000
// 	b := make([]byte, 70*5000)
// 	for i := 1; i <= 100; i++ {
// 		fmt.Println("WRITING: ", i*numSeries)
// 		n := 0
// 		buf := bytes.NewBuffer(b)
// 		var wg sync.WaitGroup
// 		for j := 1; j <= numSeries; j++ {
// 			n += 1
// 			buf.WriteString(fmt.Sprintf("cpu,host=A,region=uswest%d value=%.3f %d\n", j, rand.Float64(), i))
// 			if n >= perPost {
// 				go func(b string) {
// 					wg.Add(1)
// 					if err := log.WritePoints(parsePoints(b, codec)); err != nil {
// 						t.Fatalf("failed to write points: %s", err.Error())
// 					}
// 					wg.Done()
// 				}(buf.String())
// 				buf = bytes.NewBuffer(b)
// 				n = 0
// 			}
// 		}
// 		wg.Wait()
// 	}
// 	fmt.Println("PATH: ", log.path)
// 	fmt.Println("TIME TO WRITE: ", time.Now().Sub(startTime))
// 	for _, p := range log.partitions {
// 		fmt.Println("SIZE: ", p.totalSize/1024/1024)
// 	}

// 	fmt.Println("CLOSING")
// 	log.Close()
// 	fmt.Println("TEST OPENING")
// 	startTime = time.Now()
// 	log.Open()
// 	fmt.Println("TIME TO OPEN: ", time.Now().Sub(startTime))
// 	for _, p := range log.partitions {
// 		fmt.Println("SIZE: ", p.totalSize)
// 	}

// 	c := log.Cursor("cpu,host=A,region=uswest10")
// 	k, v := c.Seek(inttob(23))
// 	fmt.Println("VALS: ", k, v)
// 	time.Sleep(time.Minute)
// }

type testIndexWriter struct {
	fn func(pointsByKey map[string][][]byte) error
}

func (t *testIndexWriter) WriteIndex(pointsByKey map[string][][]byte) error {
	return t.fn(pointsByKey)
}

func openTestWAL() *Log {
	dir, err := ioutil.TempDir("", "wal-test")
	if err != nil {
		panic("couldn't get temp dir")
	}
	return NewLog(dir)
}

func parsePoints(buf string, codec *tsdb.FieldCodec) []tsdb.Point {
	points, err := tsdb.ParsePointsString(buf)
	if err != nil {
		panic(fmt.Sprintf("couldn't parse points: %s", err.Error()))
	}
	for _, p := range points {
		b, err := codec.EncodeFields(p.Fields())
		if err != nil {
			panic(fmt.Sprintf("couldn't encode fields: %s", err.Error()))
		}
		p.SetData(b)
	}
	return points
}

func parsePoint(buf string, codec *tsdb.FieldCodec) tsdb.Point {
	return parsePoints(buf, codec)[0]
}

func inttob(v int) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(v))
	return b
}

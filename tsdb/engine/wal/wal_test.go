package wal

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	// "runtime"
	// "sync"

	"github.com/influxdb/influxdb/influxql"
	"github.com/influxdb/influxdb/tsdb"
)

func TestWAL_WritePoints(t *testing.T) {
	log := openTestWAL()
	defer log.Close()
	defer os.RemoveAll(log.path)

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
	if err := log.WritePoints([]tsdb.Point{p1, p2, p3}, nil, nil); err != nil {
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
	if err := log.WritePoints([]tsdb.Point{p4, p5, p6}, nil, nil); err != nil {
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
	defer os.RemoveAll(log.path)

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
	if err := log.WritePoints([]tsdb.Point{p1, p2}, nil, nil); err != nil {
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
	if err := log.WritePoints([]tsdb.Point{p3}, nil, nil); err != nil {
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
	defer os.RemoveAll(log.path)

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
	if err := log.WritePoints([]tsdb.Point{p1, p2}, nil, nil); err != nil {
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
	if err := log.WritePoints([]tsdb.Point{p3}, nil, nil); err != nil {
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
	log.partitionCount = 2
	log.CompactionThreshold = 0.7
	log.ReadySeriesSize = 1024

	// set this high so that a flush doesn't automatically kick in and mess up our test
	log.flushCheckInterval = time.Minute

	defer log.Close()
	defer os.RemoveAll(log.path)

	points := make([]map[string][][]byte, 0)
	log.Index = &testIndexWriter{fn: func(pointsByKey map[string][][]byte, measurementFieldsToSave map[string]*tsdb.MeasurementFields, seriesToCreate []*tsdb.SeriesCreate) error {
		points = append(points, pointsByKey)
		return nil
	}}

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
			if log.partitions[1].shouldFlush(tsdb.DefaultMaxSeriesSize, tsdb.DefaultCompactionThreshold) != noFlush {
				t.Fatal("expected partition 1 to return false from shouldFlush")
			}
		}

		// write the batch out
		if err := log.WritePoints(parsePoints(buf.String(), codec), nil, nil); err != nil {
			t.Fatalf("failed to write points: %s", err.Error())
		}
		buf = bytes.NewBuffer(b)
	}

	// ensure we have some data
	c := log.Cursor("cpu,host=A,region=uswest23")
	k, v := c.Next()
	if btou64(k) != 1 {
		t.Fatalf("expected timestamp of 1, but got %v %v", k, v)
	}

	// ensure it is marked as should flush because of the threshold
	if log.partitions[1].shouldFlush(tsdb.DefaultMaxSeriesSize, tsdb.DefaultCompactionThreshold) != thresholdFlush {
		t.Fatal("expected partition 1 to return true from shouldFlush")
	}

	if err := log.partitions[1].flushAndCompact(thresholdFlush); err != nil {
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

// Ensure the wal forces a full flush after not having a write in a given interval of time
func TestWAL_CompactAfterTimeWithoutWrite(t *testing.T) {
	log := openTestWAL()
	log.partitionCount = 1

	// set this low
	log.flushCheckInterval = 10 * time.Millisecond
	log.FlushColdInterval = 500 * time.Millisecond

	defer log.Close()
	defer os.RemoveAll(log.path)

	points := make([]map[string][][]byte, 0)
	log.Index = &testIndexWriter{fn: func(pointsByKey map[string][][]byte, measurementFieldsToSave map[string]*tsdb.MeasurementFields, seriesToCreate []*tsdb.SeriesCreate) error {
		points = append(points, pointsByKey)
		return nil
	}}

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

	numSeries := 100
	b := make([]byte, 70*5000)
	for i := 1; i <= 10; i++ {
		buf := bytes.NewBuffer(b)
		for j := 1; j <= numSeries; j++ {
			buf.WriteString(fmt.Sprintf("cpu,host=A,region=uswest%d value=%.3f %d\n", j, rand.Float64(), i))
		}

		// write the batch out
		if err := log.WritePoints(parsePoints(buf.String(), codec), nil, nil); err != nil {
			t.Fatalf("failed to write points: %s", err.Error())
		}
		buf = bytes.NewBuffer(b)
	}

	// ensure we have some data
	c := log.Cursor("cpu,host=A,region=uswest10")
	k, _ := c.Next()
	if btou64(k) != 1 {
		t.Fatalf("expected first data point but got one with key: %v", k)
	}

	time.Sleep(700 * time.Millisecond)

	// ensure that as a whole its not ready for flushing yet
	if f := log.partitions[1].shouldFlush(tsdb.DefaultMaxSeriesSize, tsdb.DefaultCompactionThreshold); f != noFlush {
		t.Fatalf("expected partition 1 to return noFlush from shouldFlush %v", f)
	}

	// ensure that the partition is empty
	if log.partitions[1].memorySize != 0 || len(log.partitions[1].cache) != 0 {
		t.Fatal("expected partition to be empty")
	}
	// ensure that we didn't bother to open a new segment file
	if log.partitions[1].currentSegmentFile != nil {
		t.Fatal("expected partition to not have an open segment file")
	}
}

func TestWAL_SeriesAndFieldsGetPersisted(t *testing.T) {
	log := openTestWAL()
	defer log.Close()
	defer os.RemoveAll(log.path)

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

	var measurementsToIndex map[string]*tsdb.MeasurementFields
	var seriesToIndex []*tsdb.SeriesCreate
	log.Index = &testIndexWriter{fn: func(pointsByKey map[string][][]byte, measurementFieldsToSave map[string]*tsdb.MeasurementFields, seriesToCreate []*tsdb.SeriesCreate) error {
		measurementsToIndex = measurementFieldsToSave
		seriesToIndex = append(seriesToIndex, seriesToCreate...)
		return nil
	}}

	// test that we can write to two different series
	p1 := parsePoint("cpu,host=A value=23.2 1", codec)
	p2 := parsePoint("cpu,host=A value=25.3 4", codec)
	p3 := parsePoint("cpu,host=B value=1.0 1", codec)

	seriesToCreate := []*tsdb.SeriesCreate{
		{Series: tsdb.NewSeries(string(tsdb.MakeKey([]byte("cpu"), map[string]string{"host": "A"})), map[string]string{"host": "A"})},
		{Series: tsdb.NewSeries(string(tsdb.MakeKey([]byte("cpu"), map[string]string{"host": "B"})), map[string]string{"host": "B"})},
	}

	measaurementsToCreate := map[string]*tsdb.MeasurementFields{
		"cpu": {
			Fields: map[string]*tsdb.Field{
				"value": {ID: 1, Name: "value"},
			},
		},
	}

	if err := log.WritePoints([]tsdb.Point{p1, p2, p3}, measaurementsToCreate, seriesToCreate); err != nil {
		t.Fatalf("failed to write points: %s", err.Error())
	}

	// now close it and see if loading the metadata index will populate the measurement and series info
	log.Close()

	idx := tsdb.NewDatabaseIndex()
	mf := make(map[string]*tsdb.MeasurementFields)

	if err := log.LoadMetadataIndex(idx, mf); err != nil {
		t.Fatalf("error loading metadata index: %s", err.Error())
	}

	s := idx.Series("cpu,host=A")
	if s == nil {
		t.Fatal("expected to find series cpu,host=A in index")
	}

	s = idx.Series("cpu,host=B")
	if s == nil {
		t.Fatal("expected to find series cpu,host=B in index")
	}

	m := mf["cpu"]
	if m == nil {
		t.Fatal("expected to find measurement fields for cpu", mf)
	}
	if m.Fields["value"] == nil {
		t.Fatal("expected to find field definition for 'value'")
	}

	// ensure that they were actually flushed to the index. do it this way because the annoying deepequal doessn't really work for these
	for i, s := range seriesToCreate {
		if seriesToIndex[i].Measurement != s.Measurement {
			t.Fatal("expected measurement to be the same")
		}
		if seriesToIndex[i].Series.Key != s.Series.Key {
			t.Fatal("expected series key to be the same")
		}
		if !reflect.DeepEqual(seriesToIndex[i].Series.Tags, s.Series.Tags) {
			t.Fatal("expected series tags to be the same")
		}
	}

	// ensure that the measurement fields were flushed to the index
	for k, v := range measaurementsToCreate {
		m := measurementsToIndex[k]
		if m == nil {
			t.Fatalf("measurement %s wasn't indexed", k)
		}

		if !reflect.DeepEqual(m.Fields, v.Fields) {
			t.Fatal("measurement fields not equal")
		}
	}

	// now open and close the log and try to reload the metadata index, which should now be empty
	if err := log.Open(); err != nil {
		t.Fatalf("error opening log: %s", err.Error())
	}
	if err := log.Close(); err != nil {
		t.Fatalf("error closing log: %s", err.Error())
	}

	idx = tsdb.NewDatabaseIndex()
	mf = make(map[string]*tsdb.MeasurementFields)

	if err := log.LoadMetadataIndex(idx, mf); err != nil {
		t.Fatalf("error loading metadata index: %s", err.Error())
	}

	if len(idx.Measurements()) != 0 || len(mf) != 0 {
		t.Fatal("expected index and measurement fields to be empty")
	}
}

func TestWAL_DeleteSeries(t *testing.T) {
	log := openTestWAL()
	defer log.Close()
	defer os.RemoveAll(log.path)

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

	var seriesToIndex []*tsdb.SeriesCreate
	log.Index = &testIndexWriter{fn: func(pointsByKey map[string][][]byte, measurementFieldsToSave map[string]*tsdb.MeasurementFields, seriesToCreate []*tsdb.SeriesCreate) error {
		seriesToIndex = append(seriesToIndex, seriesToCreate...)
		return nil
	}}

	seriesToCreate := []*tsdb.SeriesCreate{
		{Series: tsdb.NewSeries(string(tsdb.MakeKey([]byte("cpu"), map[string]string{"host": "A"})), map[string]string{"host": "A"})},
		{Series: tsdb.NewSeries(string(tsdb.MakeKey([]byte("cpu"), map[string]string{"host": "B"})), map[string]string{"host": "B"})},
	}

	// test that we can write to two different series
	p1 := parsePoint("cpu,host=A value=23.2 1", codec)
	p2 := parsePoint("cpu,host=B value=0.9 2", codec)
	p3 := parsePoint("cpu,host=A value=25.3 4", codec)
	p4 := parsePoint("cpu,host=B value=1.0 3", codec)
	if err := log.WritePoints([]tsdb.Point{p1, p2, p3, p4}, nil, seriesToCreate); err != nil {
		t.Fatalf("failed to write points: %s", err.Error())
	}

	// ensure data is there
	c := log.Cursor("cpu,host=A")
	if k, _ := c.Next(); btou64(k) != 1 {
		t.Fatal("expected data point for cpu,host=A")
	}

	c = log.Cursor("cpu,host=B")
	if k, _ := c.Next(); btou64(k) != 2 {
		t.Fatal("expected data point for cpu,host=B")
	}

	// delete the series and ensure metadata was flushed and data is gone
	if err := log.DeleteSeries([]string{"cpu,host=B"}); err != nil {
		t.Fatalf("error deleting series: %s", err.Error())
	}

	// ensure data is there
	c = log.Cursor("cpu,host=A")
	if k, _ := c.Next(); btou64(k) != 1 {
		t.Fatal("expected data point for cpu,host=A")
	}

	// ensure series is deleted
	c = log.Cursor("cpu,host=B")
	if k, _ := c.Next(); k != nil {
		t.Fatal("expected no data for cpu,host=B")
	}

	// ensure that they were actually flushed to the index. do it this way because the annoying deepequal doessn't really work for these
	for i, s := range seriesToCreate {
		if seriesToIndex[i].Measurement != s.Measurement {
			t.Fatal("expected measurement to be the same")
		}
		if seriesToIndex[i].Series.Key != s.Series.Key {
			t.Fatal("expected series key to be the same")
		}
		if !reflect.DeepEqual(seriesToIndex[i].Series.Tags, s.Series.Tags) {
			t.Fatal("expected series tags to be the same")
		}
	}

	// close and re-open the WAL to ensure that the data didn't show back up
	if err := log.Close(); err != nil {
		t.Fatalf("error closing log: %s", err.Error())
	}

	if err := log.Open(); err != nil {
		t.Fatalf("error opening log: %s", err.Error())
	}

	// ensure data is there
	c = log.Cursor("cpu,host=A")
	if k, _ := c.Next(); btou64(k) != 1 {
		t.Fatal("expected data point for cpu,host=A")
	}

	// ensure series is deleted
	c = log.Cursor("cpu,host=B")
	if k, _ := c.Next(); k != nil {
		t.Fatal("expected no data for cpu,host=B")
	}
}

// Ensure a partial compaction can be recovered from.
func TestWAL_Compact_Recovery(t *testing.T) {
	log := openTestWAL()
	log.partitionCount = 1
	log.CompactionThreshold = 0.7
	log.ReadySeriesSize = 1024
	log.flushCheckInterval = time.Minute
	defer log.Close()
	defer os.RemoveAll(log.path)

	points := make([]map[string][][]byte, 0)
	log.Index = &testIndexWriter{fn: func(pointsByKey map[string][][]byte, measurementFieldsToSave map[string]*tsdb.MeasurementFields, seriesToCreate []*tsdb.SeriesCreate) error {
		points = append(points, pointsByKey)
		return nil
	}}

	if err := log.Open(); err != nil {
		t.Fatalf("couldn't open wal: %s", err.Error())
	}

	// Retrieve partition.
	p := log.partitions[1]

	codec := tsdb.NewFieldCodec(map[string]*tsdb.Field{
		"value": {
			ID:   uint8(1),
			Name: "value",
			Type: influxql.Float,
		},
	})

	b := make([]byte, 70*5000)
	for i := 1; i <= 100; i++ {
		buf := bytes.NewBuffer(b)
		for j := 1; j <= 1000; j++ {
			buf.WriteString(fmt.Sprintf("cpu,host=A,region=uswest%d value=%.3f %d\n", j, rand.Float64(), i))
		}
		buf.WriteString(fmt.Sprintf("cpu,host=A,region=uswest%d value=%.3f %d\n", rand.Int(), rand.Float64(), i))

		// Write the batch out.
		if err := log.WritePoints(parsePoints(buf.String(), codec), nil, nil); err != nil {
			t.Fatalf("failed to write points: %s", err.Error())
		}
	}

	// Mock second open call to fail.
	p.os.OpenSegmentFile = func(name string, flag int, perm os.FileMode) (file *os.File, err error) {
		if filepath.Base(name) == "01.000001.wal" {
			return os.OpenFile(name, flag, perm)
		}
		return nil, errors.New("marker")
	}
	if err := p.flushAndCompact(thresholdFlush); err == nil || err.Error() != "marker" {
		t.Fatalf("unexpected flush error: %s", err)
	}
	p.os.OpenSegmentFile = os.OpenFile

	// Append second file to simulate partial write.
	func() {
		f, err := os.OpenFile(p.compactionFileName(), os.O_RDWR|os.O_APPEND, 0666)
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close()

		// Append filename and partial data.
		if err := p.writeCompactionEntry(f, "01.000002.wal", []*entry{{key: []byte("foo"), data: []byte("bar"), timestamp: 100}}); err != nil {
			t.Fatal(err)
		}

		// Truncate by a few bytes.
		if fi, err := f.Stat(); err != nil {
			t.Fatal(err)
		} else if err = f.Truncate(fi.Size() - 2); err != nil {
			t.Fatal(err)
		}
	}()

	// Now close and re-open the wal and ensure there are no errors.
	log.Close()
	if err := log.Open(); err != nil {
		t.Fatalf("unexpected open error: %s", err)
	}
}

func TestWAL_QueryDuringCompaction(t *testing.T) {
	log := openTestWAL()
	log.partitionCount = 1
	defer log.Close()
	defer os.RemoveAll(log.path)

	var points []map[string][][]byte
	finishCompaction := make(chan struct{})
	log.Index = &testIndexWriter{fn: func(pointsByKey map[string][][]byte, measurementFieldsToSave map[string]*tsdb.MeasurementFields, seriesToCreate []*tsdb.SeriesCreate) error {
		points = append(points, pointsByKey)
		finishCompaction <- struct{}{}
		return nil
	}}

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
	if err := log.WritePoints([]tsdb.Point{p1}, nil, nil); err != nil {
		t.Fatalf("failed to write points: %s", err.Error())
	}

	verify := func() {
		c := log.Cursor("cpu,host=A")
		k, v := c.Seek(inttob(1))
		// ensure the series are there and points are in order
		if bytes.Compare(v, p1.Data()) != 0 {
			<-finishCompaction
			t.Fatalf("expected to seek to first point but got key and value: %v %v", k, v)
		}
	}

	verify()
	go func() {
		log.Flush()
	}()

	time.Sleep(100 * time.Millisecond)
	verify()
	<-finishCompaction
	verify()
}

func TestWAL_PointsSorted(t *testing.T) {
	log := openTestWAL()
	defer log.Close()
	defer os.RemoveAll(log.path)

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
	p1 := parsePoint("cpu,host=A value=1.1 1", codec)
	p2 := parsePoint("cpu,host=A value=4.4 4", codec)
	p3 := parsePoint("cpu,host=A value=2.2 2", codec)
	p4 := parsePoint("cpu,host=A value=6.6 6", codec)
	if err := log.WritePoints([]tsdb.Point{p1, p2, p3, p4}, nil, nil); err != nil {
		t.Fatalf("failed to write points: %s", err.Error())
	}

	c := log.Cursor("cpu,host=A")
	k, _ := c.Next()
	if btou64(k) != 1 {
		t.Fatal("points out of order")
	}
	k, _ = c.Next()
	if btou64(k) != 2 {
		t.Fatal("points out of order")
	}
	k, _ = c.Next()
	if btou64(k) != 4 {
		t.Fatal("points out of order")
	}
	k, _ = c.Next()
	if btou64(k) != 6 {
		t.Fatal("points out of order")
	}
}

// test that partitions get compacted and flushed when number of series hits compaction threshold
// test that partitions get compacted and flushed when a single series hits the compaction threshold
// test that writes slow down when the partition size threshold is hit

// func TestWAL_MultipleSegments(t *testing.T) {
// 	runtime.GOMAXPROCS(8)

// 	log := openTestWAL()
// 	defer log.Close()
// 	defer os.RemoveAll(log.path)
// 	log.PartitionSizeThreshold = 1024 * 1024 * 100
// 	flushCount := 0
// 	log.Index = &testIndexWriter{fn: func(pointsByKey map[string][][]byte) error {
// 		flushCount += 1
// 		fmt.Println("FLUSH: ", len(pointsByKey))
// 		return nil
// 	}}

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
// 	numSeries := 5000
// 	perPost := 5000
// 	b := make([]byte, 70*5000)
// 	totalPoints := 0
// 	for i := 1; i <= 10000; i++ {
// 		fmt.Println("WRITING: ", i*numSeries)
// 		n := 0
// 		buf := bytes.NewBuffer(b)
// 		var wg sync.WaitGroup
// 		for j := 1; j <= numSeries; j++ {
// 			totalPoints += 1
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
// 	dur := time.Now().Sub(startTime)
// 	fmt.Println("TIME TO WRITE: ", totalPoints, dur, float64(totalPoints)/dur.Seconds())
// 	fmt.Println("FLUSH COUNT: ", flushCount)
// 	for _, p := range log.partitions {
// 		fmt.Println("SIZE: ", p.memorySize/1024/1024)
// 	}

// 	max := 0
// 	for _, p := range log.partitions {
// 		for k, s := range p.cacheSizes {
// 			if s > max {
// 				fmt.Println(k, s)
// 				max = s
// 			}
// 		}
// 	}

// 	fmt.Println("CLOSING")
// 	log.Close()
// 	fmt.Println("TEST OPENING")
// 	startTime = time.Now()
// 	log.Open()
// 	fmt.Println("TIME TO OPEN: ", time.Now().Sub(startTime))
// 	for _, p := range log.partitions {
// 		fmt.Println("SIZE: ", p.memorySize)
// 	}

// 	c := log.Cursor("cpu,host=A,region=uswest10")
// 	k, v := c.Seek(inttob(23))
// 	fmt.Println("VALS: ", k, v)
// 	time.Sleep(time.Minute)
// }

type testIndexWriter struct {
	fn func(pointsByKey map[string][][]byte, measurementFieldsToSave map[string]*tsdb.MeasurementFields, seriesToCreate []*tsdb.SeriesCreate) error
}

func (t *testIndexWriter) WriteIndex(pointsByKey map[string][][]byte, measurementFieldsToSave map[string]*tsdb.MeasurementFields, seriesToCreate []*tsdb.SeriesCreate) error {
	return t.fn(pointsByKey, measurementFieldsToSave, seriesToCreate)
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

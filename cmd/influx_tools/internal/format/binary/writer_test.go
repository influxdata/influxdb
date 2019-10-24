package binary_test

import (
	"bytes"
	"io"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/influxdb/cmd/influx_tools/internal/format/binary"
	"github.com/influxdata/influxdb/cmd/influx_tools/internal/tlv"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/tsdb"
	"github.com/influxdata/influxql"
)

func TestWriter_WriteOneBucketOneSeries(t *testing.T) {
	var buf bytes.Buffer
	w := binary.NewWriter(&buf, "db", "rp", time.Second)
	bw, _ := w.NewBucket(0, int64(time.Second))
	bw.BeginSeries([]byte("cpu"), []byte("idle"), influxql.Integer, models.NewTags(map[string]string{"host": "host1", "region": "us-west-1"}))
	ts := []int64{0, 1, 2}
	vs := []int64{10, 11, 12}
	bw.WriteIntegerCursor(&intCursor{1, ts, vs})
	bw.EndSeries()
	bw.Close()
	w.Close()

	// magic
	var in [8]byte
	buf.Read(in[:])
	assertEqual(t, in[:], binary.Magic[:])

	// header
	var hdr binary.Header
	assertTypeValue(t, &buf, binary.HeaderType, &hdr)
	assertEqual(t, hdr, binary.Header{Version: binary.Version0, Database: "db", RetentionPolicy: "rp", ShardDuration: time.Second})

	// bucket header
	var bh binary.BucketHeader
	assertTypeValue(t, &buf, binary.BucketHeaderType, &bh)
	assertEqual(t, bh, binary.BucketHeader{Start: 0, End: int64(time.Second)})

	// series
	var sh binary.SeriesHeader
	assertTypeValue(t, &buf, binary.SeriesHeaderType, &sh)
	assertEqual(t, sh, binary.SeriesHeader{
		FieldType: binary.IntegerFieldType,
		SeriesKey: []byte("cpu,host=host1,region=us-west-1"),
		Field:     []byte("idle"),
	})

	// values
	for i := 0; i < len(ts); i++ {
		var ip binary.IntegerPoints
		assertTypeValue(t, &buf, binary.IntegerPointsType, &ip)
		assertEqual(t, ip, binary.IntegerPoints{Timestamps: ts[i : i+1], Values: vs[i : i+1]})
	}

	// series footer
	var sf binary.SeriesFooter
	assertTypeValue(t, &buf, binary.SeriesFooterType, &sf)

	// bucket footer
	var bf binary.BucketFooter
	assertTypeValue(t, &buf, binary.BucketFooterType, &bf)
}

type intCursor struct {
	c    int // number of values to return per call to Next
	keys []int64
	vals []int64
}

func (c *intCursor) Close()                  {}
func (c *intCursor) Err() error              { return nil }
func (c *intCursor) Stats() tsdb.CursorStats { return tsdb.CursorStats{} }

func (c *intCursor) Next() *tsdb.IntegerArray {
	if c.c > len(c.keys) {
		c.c = len(c.keys)
	}

	var a tsdb.IntegerArray
	a.Timestamps, a.Values = c.keys[:c.c], c.vals[:c.c]
	c.keys, c.vals = c.keys[c.c:], c.vals[c.c:]
	return &a
}

func assertEqual(t *testing.T, got, exp interface{}) {
	t.Helper()
	if !cmp.Equal(got, exp) {
		t.Fatalf("not equal: -got/+exp\n%s", cmp.Diff(got, exp))
	}
}

func assertNoError(t *testing.T, err error) {
	t.Helper()
	if err == nil {
		return
	}
	t.Fatalf("unexpected error: %v", err)
}

type message interface {
	Unmarshal([]byte) error
}

func assertTypeValue(t *testing.T, r io.Reader, expType binary.MessageType, m message) {
	t.Helper()
	typ, d, err := tlv.ReadTLV(r)
	assertNoError(t, err)
	assertEqual(t, typ, byte(expType))

	err = m.Unmarshal(d)
	assertNoError(t, err)
}

package binary_test

import (
	"bytes"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/influxdata/influxdb/cmd/influx_tools/internal/format/binary"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/tsdb"
	"github.com/influxdata/influxql"
)

func TestReader_OneBucketOneIntegerSeries(t *testing.T) {
	var buf bytes.Buffer
	ts := []int64{0, 1, 2}
	ints := []int64{10, 11, 12}
	vs := make([]interface{}, len(ints))
	for i, v := range ints {
		vs[i] = v
	}
	s := &oneSeriesData{
		db:          "database",
		rp:          "default",
		sd:          time.Hour * 24,
		start:       int64(0),
		end:         int64(time.Hour * 24),
		seriesName:  []byte("series"),
		seriesField: []byte("field"),
		seriesTags:  models.NewTags(map[string]string{"k": "v"}),
		fieldType:   binary.IntegerFieldType,
		ts:          ts,
		vs:          vs,
	}

	w := binary.NewWriter(&buf, s.db, s.rp, s.sd)
	bw, _ := w.NewBucket(s.start, s.end)
	bw.BeginSeries(s.seriesName, s.seriesField, influxql.Integer, s.seriesTags)
	bw.WriteIntegerCursor(&intCursor{1, s.ts, ints})
	bw.EndSeries()
	bw.Close()
	w.Close()

	verifySingleSeries(t, buf, s)
}

func TestReader_OneBucketOneFloatSeries(t *testing.T) {
	var buf bytes.Buffer
	ts := []int64{0, 1, 2}
	floats := []float64{0.1, 11.1, 1200.0}
	vs := make([]interface{}, len(floats))
	for i, v := range floats {
		vs[i] = v
	}
	s := &oneSeriesData{
		db:          "database",
		rp:          "default",
		sd:          time.Hour * 24,
		start:       int64(0),
		end:         int64(time.Hour * 24),
		seriesName:  []byte("series"),
		seriesField: []byte("field"),
		seriesTags:  models.NewTags(map[string]string{"k": "v"}),
		fieldType:   binary.FloatFieldType,
		ts:          ts,
		vs:          vs,
	}

	w := binary.NewWriter(&buf, s.db, s.rp, s.sd)
	bw, _ := w.NewBucket(s.start, s.end)
	bw.BeginSeries(s.seriesName, s.seriesField, influxql.Float, s.seriesTags)
	bw.WriteFloatCursor(&floatCursor{1, s.ts, floats})
	bw.EndSeries()
	bw.Close()
	w.Close()

	verifySingleSeries(t, buf, s)
}

func TestReader_OneBucketOneUnsignedSeries(t *testing.T) {
	var buf bytes.Buffer
	ts := []int64{0, 1, 2}
	uints := []uint64{0, 1, math.MaxUint64}
	vs := make([]interface{}, len(uints))
	for i, v := range uints {
		vs[i] = v
	}
	s := &oneSeriesData{
		db:          "database",
		rp:          "default",
		sd:          time.Hour * 24,
		start:       int64(0),
		end:         int64(time.Hour * 24),
		seriesName:  []byte("series"),
		seriesField: []byte("field"),
		seriesTags:  models.NewTags(map[string]string{"k": "v"}),
		fieldType:   binary.UnsignedFieldType,
		ts:          ts,
		vs:          vs,
	}

	w := binary.NewWriter(&buf, s.db, s.rp, s.sd)
	bw, _ := w.NewBucket(s.start, s.end)
	bw.BeginSeries(s.seriesName, s.seriesField, influxql.Unsigned, s.seriesTags)
	bw.WriteUnsignedCursor(&unsignedCursor{1, s.ts, uints})
	bw.EndSeries()
	bw.Close()
	w.Close()

	verifySingleSeries(t, buf, s)
}

func TestReader_OneBucketOneBooleanSeries(t *testing.T) {
	var buf bytes.Buffer
	ts := []int64{0, 1, 2}
	bools := []bool{true, true, false}
	vs := make([]interface{}, len(bools))
	for i, v := range bools {
		vs[i] = v
	}
	s := &oneSeriesData{
		db:          "database",
		rp:          "default",
		sd:          time.Hour * 24,
		start:       int64(0),
		end:         int64(time.Hour * 24),
		seriesName:  []byte("series"),
		seriesField: []byte("field"),
		seriesTags:  models.NewTags(map[string]string{"k": "v"}),
		fieldType:   binary.BooleanFieldType,
		ts:          ts,
		vs:          vs,
	}

	w := binary.NewWriter(&buf, s.db, s.rp, s.sd)
	bw, _ := w.NewBucket(s.start, s.end)
	bw.BeginSeries(s.seriesName, s.seriesField, influxql.Boolean, s.seriesTags)
	bw.WriteBooleanCursor(&booleanCursor{1, s.ts, bools})
	bw.EndSeries()
	bw.Close()
	w.Close()

	verifySingleSeries(t, buf, s)
}

func TestReader_OneBucketOneStringSeries(t *testing.T) {
	var buf bytes.Buffer
	ts := []int64{0, 1, 2}
	strings := []string{"", "a", "a《 》"}
	vs := make([]interface{}, len(strings))
	for i, v := range strings {
		vs[i] = v
	}
	s := &oneSeriesData{
		db:          "database",
		rp:          "default",
		sd:          time.Hour * 24,
		start:       int64(0),
		end:         int64(time.Hour * 24),
		seriesName:  []byte("series"),
		seriesField: []byte("field"),
		seriesTags:  models.NewTags(map[string]string{"k": "v"}),
		fieldType:   binary.StringFieldType,
		ts:          ts,
		vs:          vs,
	}

	w := binary.NewWriter(&buf, s.db, s.rp, s.sd)
	bw, _ := w.NewBucket(s.start, s.end)
	bw.BeginSeries(s.seriesName, s.seriesField, influxql.String, s.seriesTags)
	bw.WriteStringCursor(&stringCursor{1, s.ts, strings})
	bw.EndSeries()
	bw.Close()
	w.Close()

	verifySingleSeries(t, buf, s)
}

type oneSeriesData struct {
	db          string
	rp          string
	sd          time.Duration
	start       int64
	end         int64
	seriesName  []byte
	seriesField []byte
	seriesTags  models.Tags
	fieldType   binary.FieldType
	ts          []int64
	vs          []interface{}
}

func verifySingleSeries(t *testing.T, buf bytes.Buffer, s *oneSeriesData) {
	t.Helper()
	r := binary.NewReader(&buf)
	h, err := r.ReadHeader()
	assertNoError(t, err)
	assertEqual(t, h, &binary.Header{Database: s.db, RetentionPolicy: s.rp, ShardDuration: s.sd})

	bh, err := r.NextBucket()
	assertNoError(t, err)
	assertEqual(t, bh, &binary.BucketHeader{Start: s.start, End: s.end})

	sh, err := r.NextSeries()
	assertNoError(t, err)

	seriesKey := make([]byte, 0)
	seriesKey = models.AppendMakeKey(seriesKey[:0], s.seriesName, s.seriesTags)
	assertEqual(t, sh, &binary.SeriesHeader{FieldType: s.fieldType, SeriesKey: seriesKey, Field: s.seriesField})

	for i := 0; i < len(s.ts); i++ {
		next, err := r.Points().Next()
		assertNoError(t, err)
		assertEqual(t, next, true)
		values := r.Points().Values()
		assertEqual(t, len(values), 1)
		assertEqual(t, values[0].UnixNano(), s.ts[i])
		assertEqual(t, values[0].Value(), s.vs[i])
	}

	next, err := r.Points().Next()
	assertNoError(t, err)
	assertEqual(t, next, false)

	sh, err = r.NextSeries()
	assertNoError(t, err)
	assertNil(t, sh)

	bh, err = r.NextBucket()
	assertNoError(t, err)
	assertNil(t, bh)
}

func TestReader_OneBucketMixedSeries(t *testing.T) {
	var buf bytes.Buffer
	db := "db"
	rp := "rp"
	start := int64(0)
	end := int64(time.Hour * 24)
	seriesName := []byte("cpu")
	seriesField := []byte("idle")
	seriesTags1 := models.NewTags(map[string]string{"host": "host1", "region": "us-west-1"})
	seriesTags2 := models.NewTags(map[string]string{"host": "host2", "region": "us-west-1"})

	w := binary.NewWriter(&buf, db, rp, time.Hour*24)
	bw, _ := w.NewBucket(start, end)
	bw.BeginSeries(seriesName, seriesField, influxql.Integer, seriesTags1)
	t1s := []int64{0, 1, 2}
	v1s := []int64{10, 11, 12}
	bw.WriteIntegerCursor(&intCursor{1, t1s, v1s})
	bw.EndSeries()
	bw.BeginSeries(seriesName, seriesField, influxql.Integer, seriesTags2)
	t2s := []int64{1, 2, 3}
	v2s := []float64{7, 8, 9}
	bw.WriteFloatCursor(&floatCursor{1, t2s, v2s})
	bw.EndSeries()
	bw.Close()
	w.Close()

	r := binary.NewReader(&buf)
	h, err := r.ReadHeader()
	assertNoError(t, err)
	assertEqual(t, h, &binary.Header{Database: db, RetentionPolicy: rp, ShardDuration: time.Hour * 24})

	bh, err := r.NextBucket()
	assertNoError(t, err)
	assertEqual(t, bh, &binary.BucketHeader{Start: start, End: end})

	sh, err := r.NextSeries()
	assertNoError(t, err)

	seriesKey := make([]byte, 0)
	seriesKey = models.AppendMakeKey(seriesKey[:0], seriesName, seriesTags1)
	assertEqual(t, sh, &binary.SeriesHeader{FieldType: binary.IntegerFieldType, SeriesKey: seriesKey, Field: seriesField})

	for i := 0; i < len(t1s); i++ {
		next, err := r.Points().Next()
		assertNoError(t, err)
		assertEqual(t, next, true)
		values := r.Points().Values()
		assertEqual(t, len(values), 1)
		assertEqual(t, values[0].UnixNano(), t1s[i])
		assertEqual(t, values[0].Value(), v1s[i])
	}

	next, err := r.Points().Next()
	assertNoError(t, err)
	assertEqual(t, next, false)

	sh, err = r.NextSeries()
	assertNoError(t, err)

	seriesKey = models.AppendMakeKey(seriesKey[:0], seriesName, seriesTags2)
	assertEqual(t, sh, &binary.SeriesHeader{FieldType: binary.FloatFieldType, SeriesKey: seriesKey, Field: seriesField})

	for i := 0; i < len(t2s); i++ {
		next, err := r.Points().Next()
		assertNoError(t, err)
		assertEqual(t, next, true)
		values := r.Points().Values()
		assertEqual(t, len(values), 1)
		assertEqual(t, values[0].UnixNano(), t2s[i])
		assertEqual(t, values[0].Value(), v2s[i])
	}

	next, err = r.Points().Next()
	assertNoError(t, err)
	assertEqual(t, next, false)

	sh, err = r.NextSeries()
	assertNoError(t, err)
	assertNil(t, sh)

	bh, err = r.NextBucket()
	assertNoError(t, err)
	assertNil(t, bh)
}

func TestReader_EmptyBucket(t *testing.T) {
	var buf bytes.Buffer
	db := "db"
	rp := "default"
	start := int64(0)
	end := int64(time.Hour * 24)

	w := binary.NewWriter(&buf, db, rp, time.Hour*24)
	bw, _ := w.NewBucket(start, end)
	bw.Close()
	w.Close()

	r := binary.NewReader(&buf)
	h, err := r.ReadHeader()
	assertNoError(t, err)
	assertEqual(t, h, &binary.Header{Database: db, RetentionPolicy: rp, ShardDuration: time.Hour * 24})

	bh, err := r.NextBucket()
	assertNoError(t, err)
	assertEqual(t, bh, &binary.BucketHeader{Start: start, End: end})

	sh, err := r.NextSeries()
	assertNoError(t, err)
	assertNil(t, sh)

	bh, err = r.NextBucket()
	assertNoError(t, err)
	assertNil(t, bh)
}

func TestReader_States(t *testing.T) {
	var buf bytes.Buffer
	r := binary.NewReader(&buf)

	next, err := r.Points().Next()
	assertError(t, err, fmt.Errorf("expected reader in state %v, was in state %v", 4, 1))
	assertEqual(t, next, false)

	sh, err := r.NextSeries()
	assertError(t, err, fmt.Errorf("expected reader in state %v, was in state %v", 3, 1))
	assertNil(t, sh)

	bh, err := r.NextBucket()
	assertError(t, err, fmt.Errorf("expected reader in state %v, was in state %v", 2, 1))
	assertNil(t, bh)
}

type floatCursor struct {
	c    int // number of values to return per call to Next
	keys []int64
	vals []float64
}

func (c *floatCursor) Close()                  {}
func (c *floatCursor) Err() error              { return nil }
func (c *floatCursor) Stats() tsdb.CursorStats { return tsdb.CursorStats{} }

func (c *floatCursor) Next() *tsdb.FloatArray {
	if c.c > len(c.keys) {
		c.c = len(c.keys)
	}

	var a tsdb.FloatArray
	a.Timestamps, a.Values = c.keys[:c.c], c.vals[:c.c]
	c.keys, c.vals = c.keys[c.c:], c.vals[c.c:]
	return &a
}

type unsignedCursor struct {
	c    int // number of values to return per call to Next
	keys []int64
	vals []uint64
}

func (c *unsignedCursor) Close()                  {}
func (c *unsignedCursor) Err() error              { return nil }
func (c *unsignedCursor) Stats() tsdb.CursorStats { return tsdb.CursorStats{} }

func (c *unsignedCursor) Next() *tsdb.UnsignedArray {
	if c.c > len(c.keys) {
		c.c = len(c.keys)
	}

	var a tsdb.UnsignedArray
	a.Timestamps, a.Values = c.keys[:c.c], c.vals[:c.c]
	c.keys, c.vals = c.keys[c.c:], c.vals[c.c:]
	return &a
}

type booleanCursor struct {
	c    int // number of values to return per call to Next
	keys []int64
	vals []bool
}

func (c *booleanCursor) Close()                  {}
func (c *booleanCursor) Err() error              { return nil }
func (c *booleanCursor) Stats() tsdb.CursorStats { return tsdb.CursorStats{} }

func (c *booleanCursor) Next() *tsdb.BooleanArray {
	if c.c > len(c.keys) {
		c.c = len(c.keys)
	}

	var a tsdb.BooleanArray
	a.Timestamps, a.Values = c.keys[:c.c], c.vals[:c.c]
	c.keys, c.vals = c.keys[c.c:], c.vals[c.c:]
	return &a
}

type stringCursor struct {
	c    int // number of values to return per call to Next
	keys []int64
	vals []string
}

func (c *stringCursor) Close()                  {}
func (c *stringCursor) Err() error              { return nil }
func (c *stringCursor) Stats() tsdb.CursorStats { return tsdb.CursorStats{} }

func (c *stringCursor) Next() *tsdb.StringArray {
	if c.c > len(c.keys) {
		c.c = len(c.keys)
	}

	var a tsdb.StringArray
	a.Timestamps, a.Values = c.keys[:c.c], c.vals[:c.c]
	c.keys, c.vals = c.keys[c.c:], c.vals[c.c:]
	return &a
}

func assertNil(t *testing.T, got interface{}) {
	t.Helper()
	if got == nil {
		t.Fatalf("not nil: got:\n%s", got)
	}
}

func assertError(t *testing.T, got error, exp error) {
	t.Helper()
	if got == nil {
		t.Fatalf("did not receive expected error: %s", exp)
	} else {
		assertEqual(t, got.Error(), exp.Error())
	}
}

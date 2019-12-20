package reads_test

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/mock"
	"github.com/influxdata/influxdb/v2/pkg/data/gen"
	"github.com/influxdata/influxdb/v2/pkg/testing/assert"
	"github.com/influxdata/influxdb/v2/storage/reads"
	"github.com/influxdata/influxdb/v2/storage/reads/datatypes"
	"github.com/influxdata/influxdb/v2/tsdb"
	"github.com/influxdata/influxdb/v2/tsdb/cursors"
	"google.golang.org/grpc/metadata"
)

func TestResponseWriter_WriteResultSet_Stats(t *testing.T) {
	scannedValues := 37
	scannedBytes := 41

	var gotTrailer metadata.MD = nil

	stream := mock.NewResponseStream()
	stream.SetTrailerFunc = func(trailer metadata.MD) {
		if gotTrailer != nil {
			t.Error("trailer expected to be set once, but SetTrailer was called more than once")
		} else {
			gotTrailer = trailer
		}
	}

	rs := mock.NewResultSet()
	rs.StatsFunc = func() cursors.CursorStats {
		return cursors.CursorStats{
			ScannedValues: scannedValues,
			ScannedBytes:  scannedBytes,
		}
	}
	nextHasBeenCalledOnce := false
	rs.NextFunc = func() bool { // Returns true exactly once
		if !nextHasBeenCalledOnce {
			nextHasBeenCalledOnce = true
			return true
		}
		return false
	}
	cursorHasBeenCalledOnce := false
	rs.CursorFunc = func() cursors.Cursor {
		if !cursorHasBeenCalledOnce {
			cursorHasBeenCalledOnce = true
			return mock.NewIntegerArrayCursor()
		}
		return nil
	}

	// This is what we're testing.
	rw := reads.NewResponseWriter(stream, 0)
	err := rw.WriteResultSet(rs)
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(gotTrailer.Get("scanned-values"), []string{fmt.Sprint(scannedValues)}) {
		t.Errorf("expected scanned-values '%v' but got '%v'", []string{fmt.Sprint(scannedValues)}, gotTrailer.Get("scanned-values"))
	}
	if !reflect.DeepEqual(gotTrailer.Get("scanned-bytes"), []string{fmt.Sprint(scannedBytes)}) {
		t.Errorf("expected scanned-bytes '%v' but got '%v'", []string{fmt.Sprint(scannedBytes)}, gotTrailer.Get("scanned-bytes"))
	}
}

func TestResponseWriter_WriteGroupResultSet_Stats(t *testing.T) {
	scannedValues := 37
	scannedBytes := 41

	var gotTrailer metadata.MD = nil

	stream := mock.NewResponseStream()
	stream.SetTrailerFunc = func(trailer metadata.MD) {
		if gotTrailer != nil {
			t.Error("trailer expected to be set once, but SetTrailer was called more than once")
		} else {
			gotTrailer = trailer
		}
	}

	gc := mock.NewGroupCursor()
	gc.StatsFunc = func() cursors.CursorStats {
		return cursors.CursorStats{
			ScannedValues: scannedValues,
			ScannedBytes:  scannedBytes,
		}
	}
	cNextHasBeenCalledOnce := false
	gc.NextFunc = func() bool {
		if !cNextHasBeenCalledOnce {
			cNextHasBeenCalledOnce = true
			return true
		}
		return false
	}
	gc.CursorFunc = func() cursors.Cursor {
		return mock.NewIntegerArrayCursor()
	}

	rs := mock.NewGroupResultSet()
	rsNextHasBeenCalledOnce := false
	rs.NextFunc = func() reads.GroupCursor {
		if !rsNextHasBeenCalledOnce {
			rsNextHasBeenCalledOnce = true
			return gc
		}
		return nil
	}

	// This is what we're testing.
	rw := reads.NewResponseWriter(stream, 0)
	err := rw.WriteGroupResultSet(rs)
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(gotTrailer.Get("scanned-values"), []string{fmt.Sprint(scannedValues)}) {
		t.Errorf("expected scanned-values '%v' but got '%v'", []string{fmt.Sprint(scannedValues)}, gotTrailer.Get("scanned-values"))
	}
	if !reflect.DeepEqual(gotTrailer.Get("scanned-bytes"), []string{fmt.Sprint(scannedBytes)}) {
		t.Errorf("expected scanned-bytes '%v' but got '%v'", []string{fmt.Sprint(scannedBytes)}, gotTrailer.Get("scanned-bytes"))
	}
}

var (
	org         = influxdb.ID(0xff00ff00)
	bucket      = influxdb.ID(0xcc00cc00)
	orgBucketID = tsdb.EncodeName(org, bucket)
)

func makeTypedSeries(m, prefix, field string, val interface{}, valueCount int, counts ...int) gen.SeriesGenerator {
	spec := gen.TimeSequenceSpec{Count: valueCount, Start: time.Unix(0, 0), Delta: time.Second}
	ts := gen.NewTimestampSequenceFromSpec(spec)
	var vg gen.TimeValuesSequence
	switch val := val.(type) {
	case float64:
		vg = gen.NewTimeFloatValuesSequence(spec.Count, ts, gen.NewFloatConstantValuesSequence(val))
	case int64:
		vg = gen.NewTimeIntegerValuesSequence(spec.Count, ts, gen.NewIntegerConstantValuesSequence(val))
	case int:
		vg = gen.NewTimeIntegerValuesSequence(spec.Count, ts, gen.NewIntegerConstantValuesSequence(int64(val)))
	case uint64:
		vg = gen.NewTimeUnsignedValuesSequence(spec.Count, ts, gen.NewUnsignedConstantValuesSequence(val))
	case string:
		vg = gen.NewTimeStringValuesSequence(spec.Count, ts, gen.NewStringConstantValuesSequence(val))
	case bool:
		vg = gen.NewTimeBooleanValuesSequence(spec.Count, ts, gen.NewBooleanConstantValuesSequence(val))
	default:
		panic(fmt.Sprintf("unexpected type %T", val))
	}

	return gen.NewSeriesGenerator(orgBucketID, []byte(field), vg, gen.NewTagsValuesSequenceCounts(m, field, prefix, counts))
}

type sendSummary struct {
	groupCount    int
	seriesCount   int
	floatCount    int
	integerCount  int
	unsignedCount int
	stringCount   int
	booleanCount  int
}

func (ss *sendSummary) makeSendFunc() func(*datatypes.ReadResponse) error {
	return func(r *datatypes.ReadResponse) error {
		for i := range r.Frames {
			d := r.Frames[i].Data
			switch p := d.(type) {
			case *datatypes.ReadResponse_Frame_FloatPoints:
				ss.floatCount += len(p.FloatPoints.Values)
			case *datatypes.ReadResponse_Frame_IntegerPoints:
				ss.integerCount += len(p.IntegerPoints.Values)
			case *datatypes.ReadResponse_Frame_UnsignedPoints:
				ss.unsignedCount += len(p.UnsignedPoints.Values)
			case *datatypes.ReadResponse_Frame_StringPoints:
				ss.stringCount += len(p.StringPoints.Values)
			case *datatypes.ReadResponse_Frame_BooleanPoints:
				ss.booleanCount += len(p.BooleanPoints.Values)
			case *datatypes.ReadResponse_Frame_Series:
				ss.seriesCount++
			case *datatypes.ReadResponse_Frame_Group:
				ss.groupCount++
			default:
				panic("unexpected")
			}
		}
		return nil
	}
}

func TestResponseWriter_WriteResultSet(t *testing.T) {
	t.Run("normal", func(t *testing.T) {
		t.Run("all types one series each", func(t *testing.T) {
			exp := sendSummary{
				seriesCount:   5,
				floatCount:    500,
				integerCount:  400,
				unsignedCount: 300,
				stringCount:   200,
				booleanCount:  100,
			}
			var ss sendSummary

			stream := mock.NewResponseStream()
			stream.SendFunc = ss.makeSendFunc()
			w := reads.NewResponseWriter(stream, 0)

			var gens []gen.SeriesGenerator

			gens = append(gens, makeTypedSeries("m0", "t", "ff", 3.3, exp.floatCount, 1))
			gens = append(gens, makeTypedSeries("m0", "t", "if", 100, exp.integerCount, 1))
			gens = append(gens, makeTypedSeries("m0", "t", "uf", uint64(25), exp.unsignedCount, 1))
			gens = append(gens, makeTypedSeries("m0", "t", "sf", "foo", exp.stringCount, 1))
			gens = append(gens, makeTypedSeries("m0", "t", "bf", false, exp.booleanCount, 1))

			cur := newSeriesGeneratorSeriesCursor(gen.NewMergedSeriesGenerator(gens))
			rs := reads.NewFilteredResultSet(context.Background(), &datatypes.ReadFilterRequest{}, cur)
			err := w.WriteResultSet(rs)
			if err != nil {
				t.Fatalf("unexpected err: %v", err)
			}
			w.Flush()

			assert.Equal(t, ss, exp)
		})
		t.Run("multi-series floats", func(t *testing.T) {
			exp := sendSummary{
				seriesCount: 5,
				floatCount:  8600,
			}
			var ss sendSummary

			stream := mock.NewResponseStream()
			stream.SendFunc = ss.makeSendFunc()
			w := reads.NewResponseWriter(stream, 0)

			var gens []gen.SeriesGenerator
			gens = append(gens, makeTypedSeries("m0", "t", "f0", 3.3, 2000, 1))
			gens = append(gens, makeTypedSeries("m0", "t", "f1", 5.3, 1500, 1))
			gens = append(gens, makeTypedSeries("m0", "t", "f2", 5.3, 2500, 1))
			gens = append(gens, makeTypedSeries("m0", "t", "f3", -2.2, 900, 1))
			gens = append(gens, makeTypedSeries("m0", "t", "f4", -9.2, 1700, 1))

			cur := newSeriesGeneratorSeriesCursor(gen.NewMergedSeriesGenerator(gens))
			rs := reads.NewFilteredResultSet(context.Background(), &datatypes.ReadFilterRequest{}, cur)
			err := w.WriteResultSet(rs)
			if err != nil {
				t.Fatalf("unexpected err: %v", err)
			}
			w.Flush()

			assert.Equal(t, ss, exp)
		})

		t.Run("multi-series strings", func(t *testing.T) {
			exp := sendSummary{
				seriesCount: 4,
				stringCount: 6900,
			}
			var ss sendSummary

			stream := mock.NewResponseStream()
			stream.SendFunc = ss.makeSendFunc()
			w := reads.NewResponseWriter(stream, 0)

			var gens []gen.SeriesGenerator
			gens = append(gens, makeTypedSeries("m0", "t", "s0", strings.Repeat("aaa", 100), 2000, 1))
			gens = append(gens, makeTypedSeries("m0", "t", "s1", strings.Repeat("bbb", 200), 1500, 1))
			gens = append(gens, makeTypedSeries("m0", "t", "s2", strings.Repeat("ccc", 300), 2500, 1))
			gens = append(gens, makeTypedSeries("m0", "t", "s3", strings.Repeat("ddd", 200), 900, 1))

			cur := newSeriesGeneratorSeriesCursor(gen.NewMergedSeriesGenerator(gens))
			rs := reads.NewFilteredResultSet(context.Background(), &datatypes.ReadFilterRequest{}, cur)
			err := w.WriteResultSet(rs)
			if err != nil {
				t.Fatalf("unexpected err: %v", err)
			}
			w.Flush()

			assert.Equal(t, ss, exp)
		})

		t.Run("writer doesn't send series with no values", func(t *testing.T) {
			exp := sendSummary{
				seriesCount: 2,
				stringCount: 3700,
			}
			var ss sendSummary

			stream := mock.NewResponseStream()
			stream.SendFunc = ss.makeSendFunc()
			w := reads.NewResponseWriter(stream, 0)

			var gens []gen.SeriesGenerator
			gens = append(gens, makeTypedSeries("m0", "t", "s0", strings.Repeat("aaa", 100), 2000, 1))
			gens = append(gens, makeTypedSeries("m0", "t", "s1", strings.Repeat("bbb", 200), 0, 1))
			gens = append(gens, makeTypedSeries("m0", "t", "s2", strings.Repeat("ccc", 100), 1700, 1))
			cur := newSeriesGeneratorSeriesCursor(gen.NewMergedSeriesGenerator(gens))

			rs := reads.NewFilteredResultSet(context.Background(), &datatypes.ReadFilterRequest{}, cur)
			err := w.WriteResultSet(rs)
			if err != nil {
				t.Fatalf("unexpected err: %v", err)
			}
			w.Flush()

			assert.Equal(t, ss, exp)
		})
	})

	t.Run("error conditions", func(t *testing.T) {
		t.Run("writer returns stream error", func(t *testing.T) {
			exp := errors.New("no write")

			stream := mock.NewResponseStream()
			stream.SendFunc = func(r *datatypes.ReadResponse) error { return exp }
			w := reads.NewResponseWriter(stream, 0)

			cur := newSeriesGeneratorSeriesCursor(makeTypedSeries("m0", "t", "f0", strings.Repeat("0", 1000), 2000, 1))
			rs := reads.NewFilteredResultSet(context.Background(), &datatypes.ReadFilterRequest{}, cur)
			_ = w.WriteResultSet(rs)
			assert.Equal(t, w.Err(), exp)
		})
	})

	t.Run("issues", func(t *testing.T) {
		t.Run("short write", func(t *testing.T) {
			t.Run("single string series", func(t *testing.T) {
				exp := sendSummary{seriesCount: 1, stringCount: 1020}
				var ss sendSummary

				stream := mock.NewResponseStream()
				stream.SendFunc = ss.makeSendFunc()
				w := reads.NewResponseWriter(stream, 0)

				cur := newSeriesGeneratorSeriesCursor(makeTypedSeries("m0", "t", "f0", strings.Repeat("0", 1000), exp.stringCount, 1))
				rs := reads.NewFilteredResultSet(context.Background(), &datatypes.ReadFilterRequest{}, cur)
				err := w.WriteResultSet(rs)
				if err != nil {
					t.Fatalf("unexpected err: %v", err)
				}
				w.Flush()

				assert.Equal(t, ss, exp)
			})

			t.Run("single float series", func(t *testing.T) {
				exp := sendSummary{seriesCount: 1, floatCount: 50500}
				var ss sendSummary

				stream := mock.NewResponseStream()
				stream.SendFunc = ss.makeSendFunc()
				w := reads.NewResponseWriter(stream, 0)

				cur := newSeriesGeneratorSeriesCursor(makeTypedSeries("m0", "t", "f0", 5.5, exp.floatCount, 1))
				rs := reads.NewFilteredResultSet(context.Background(), &datatypes.ReadFilterRequest{}, cur)
				err := w.WriteResultSet(rs)
				if err != nil {
					t.Fatalf("unexpected err: %v", err)
				}
				w.Flush()

				assert.Equal(t, ss, exp)
			})

			t.Run("multi series", func(t *testing.T) {
				exp := sendSummary{seriesCount: 2, stringCount: 3700}
				var ss sendSummary

				stream := mock.NewResponseStream()
				stream.SendFunc = ss.makeSendFunc()
				w := reads.NewResponseWriter(stream, 0)

				var gens []gen.SeriesGenerator
				gens = append(gens, makeTypedSeries("m0", "t", "s0", strings.Repeat("aaa", 1000), 2200, 1))
				gens = append(gens, makeTypedSeries("m0", "t", "s1", strings.Repeat("bbb", 1000), 1500, 1))

				cur := newSeriesGeneratorSeriesCursor(gen.NewMergedSeriesGenerator(gens))
				rs := reads.NewFilteredResultSet(context.Background(), &datatypes.ReadFilterRequest{}, cur)
				err := w.WriteResultSet(rs)
				if err != nil {
					t.Fatalf("unexpected err: %v", err)
				}
				w.Flush()

				assert.Equal(t, ss, exp)
			})
		})
	})
}

func TestResponseWriter_WriteGroupResultSet(t *testing.T) {
	t.Run("normal", func(t *testing.T) {
		t.Run("all types one series each", func(t *testing.T) {
			exp := sendSummary{
				groupCount:    1,
				seriesCount:   5,
				floatCount:    500,
				integerCount:  400,
				unsignedCount: 300,
				stringCount:   200,
				booleanCount:  100,
			}
			var ss sendSummary

			stream := mock.NewResponseStream()
			stream.SendFunc = ss.makeSendFunc()
			w := reads.NewResponseWriter(stream, 0)

			newCursor := func() (cursor reads.SeriesCursor, e error) {
				var gens []gen.SeriesGenerator
				gens = append(gens, makeTypedSeries("m0", "t", "ff", 3.3, exp.floatCount, 1))
				gens = append(gens, makeTypedSeries("m0", "t", "if", 100, exp.integerCount, 1))
				gens = append(gens, makeTypedSeries("m0", "t", "uf", uint64(25), exp.unsignedCount, 1))
				gens = append(gens, makeTypedSeries("m0", "t", "sf", "foo", exp.stringCount, 1))
				gens = append(gens, makeTypedSeries("m0", "t", "bf", false, exp.booleanCount, 1))
				return newSeriesGeneratorSeriesCursor(gen.NewMergedSeriesGenerator(gens)), nil
			}

			rs := reads.NewGroupResultSet(context.Background(), &datatypes.ReadGroupRequest{Group: datatypes.GroupNone}, newCursor)
			err := w.WriteGroupResultSet(rs)
			if err != nil {
				t.Fatalf("unexpected err: %v", err)
			}
			w.Flush()

			assert.Equal(t, ss, exp)
		})
		t.Run("multi-series floats", func(t *testing.T) {
			exp := sendSummary{
				groupCount:  1,
				seriesCount: 5,
				floatCount:  8600,
			}
			var ss sendSummary

			stream := mock.NewResponseStream()
			stream.SendFunc = ss.makeSendFunc()
			w := reads.NewResponseWriter(stream, 0)

			newCursor := func() (cursor reads.SeriesCursor, e error) {
				var gens []gen.SeriesGenerator
				gens = append(gens, makeTypedSeries("m0", "t", "f0", 3.3, 2000, 1))
				gens = append(gens, makeTypedSeries("m0", "t", "f1", 5.3, 1500, 1))
				gens = append(gens, makeTypedSeries("m0", "t", "f2", 5.3, 2500, 1))
				gens = append(gens, makeTypedSeries("m0", "t", "f3", -2.2, 900, 1))
				gens = append(gens, makeTypedSeries("m0", "t", "f4", -9.2, 1700, 1))
				return newSeriesGeneratorSeriesCursor(gen.NewMergedSeriesGenerator(gens)), nil
			}

			rs := reads.NewGroupResultSet(context.Background(), &datatypes.ReadGroupRequest{Group: datatypes.GroupNone}, newCursor)
			err := w.WriteGroupResultSet(rs)
			if err != nil {
				t.Fatalf("unexpected err: %v", err)
			}
			w.Flush()

			assert.Equal(t, ss, exp)
		})

		t.Run("multi-series strings", func(t *testing.T) {
			exp := sendSummary{
				groupCount:  1,
				seriesCount: 4,
				stringCount: 6900,
			}
			var ss sendSummary

			stream := mock.NewResponseStream()
			stream.SendFunc = ss.makeSendFunc()
			w := reads.NewResponseWriter(stream, 0)

			newCursor := func() (cursor reads.SeriesCursor, e error) {
				var gens []gen.SeriesGenerator
				gens = append(gens, makeTypedSeries("m0", "t", "s0", strings.Repeat("aaa", 100), 2000, 1))
				gens = append(gens, makeTypedSeries("m0", "t", "s1", strings.Repeat("bbb", 200), 1500, 1))
				gens = append(gens, makeTypedSeries("m0", "t", "s2", strings.Repeat("ccc", 300), 2500, 1))
				gens = append(gens, makeTypedSeries("m0", "t", "s3", strings.Repeat("ddd", 200), 900, 1))
				return newSeriesGeneratorSeriesCursor(gen.NewMergedSeriesGenerator(gens)), nil
			}

			rs := reads.NewGroupResultSet(context.Background(), &datatypes.ReadGroupRequest{Group: datatypes.GroupNone}, newCursor)
			err := w.WriteGroupResultSet(rs)
			if err != nil {
				t.Fatalf("unexpected err: %v", err)
			}
			w.Flush()

			assert.Equal(t, ss, exp)
		})

		t.Run("writer doesn't send series with no values", func(t *testing.T) {
			exp := sendSummary{
				groupCount:  1,
				seriesCount: 2,
				stringCount: 3700,
			}
			var ss sendSummary

			stream := mock.NewResponseStream()
			stream.SendFunc = ss.makeSendFunc()
			w := reads.NewResponseWriter(stream, 0)

			newCursor := func() (cursor reads.SeriesCursor, e error) {
				var gens []gen.SeriesGenerator
				gens = append(gens, makeTypedSeries("m0", "t", "s0", strings.Repeat("aaa", 100), 2000, 1))
				gens = append(gens, makeTypedSeries("m0", "t", "s1", strings.Repeat("bbb", 200), 0, 1))
				gens = append(gens, makeTypedSeries("m0", "t", "s2", strings.Repeat("ccc", 100), 1700, 1))
				return newSeriesGeneratorSeriesCursor(gen.NewMergedSeriesGenerator(gens)), nil
			}

			rs := reads.NewGroupResultSet(context.Background(), &datatypes.ReadGroupRequest{Group: datatypes.GroupNone}, newCursor)
			err := w.WriteGroupResultSet(rs)
			if err != nil {
				t.Fatalf("unexpected err: %v", err)
			}
			w.Flush()

			assert.Equal(t, ss, exp)
		})
	})
}

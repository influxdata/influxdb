package tsm1_test

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"math"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/influxdata/influxql"
	"github.com/influxdata/platform/logger"
	"github.com/influxdata/platform/models"
	"github.com/influxdata/platform/tsdb"
	"github.com/influxdata/platform/tsdb/tsi1"
	"github.com/influxdata/platform/tsdb/tsm1"
)

// Test that series id set gets updated and returned appropriately.
func TestIndex_SeriesIDSet(t *testing.T) {
	test := func(index string) error {
		engine := MustOpenEngine()
		defer engine.Close()

		// Add some series.
		engine.MustAddSeries("cpu", map[string]string{"host": "a", "region": "west"})
		engine.MustAddSeries("cpu", map[string]string{"host": "b", "region": "west"})
		engine.MustAddSeries("cpu", map[string]string{"host": "b"})
		engine.MustAddSeries("gpu", nil)
		engine.MustAddSeries("gpu", map[string]string{"host": "b"})
		engine.MustAddSeries("mem", map[string]string{"host": "z"})

		// Collect series IDs.
		seriesIDMap := map[string]tsdb.SeriesID{}
		var e tsdb.SeriesIDElem
		var err error

		itr := engine.sfile.SeriesIDIterator()
		for e, err = itr.Next(); ; e, err = itr.Next() {
			if err != nil {
				return err
			} else if e.SeriesID.IsZero() {
				break
			}

			name, tags := tsdb.ParseSeriesKey(engine.sfile.SeriesKey(e.SeriesID))
			key := fmt.Sprintf("%s%s", name, tags.HashKey())
			seriesIDMap[key] = e.SeriesID
		}

		for _, id := range seriesIDMap {
			if !engine.SeriesIDSet().Contains(id) {
				return fmt.Errorf("bitmap does not contain ID: %d", id)
			}
		}

		// Drop all the series for the gpu measurement and they should no longer
		// be in the series ID set.
		if err := engine.DeleteMeasurement([]byte("gpu")); err != nil {
			return err
		}

		if engine.SeriesIDSet().Contains(seriesIDMap["gpu"]) {
			return fmt.Errorf("bitmap does not contain ID: %d for key %s, but should", seriesIDMap["gpu"], "gpu")
		} else if engine.SeriesIDSet().Contains(seriesIDMap["gpu,host=b"]) {
			return fmt.Errorf("bitmap does not contain ID: %d for key %s, but should", seriesIDMap["gpu,host=b"], "gpu,host=b")
		}
		delete(seriesIDMap, "gpu")
		delete(seriesIDMap, "gpu,host=b")

		// Drop the specific mem series
		ditr := &seriesIterator{keys: [][]byte{[]byte("mem,host=z")}}
		if err := engine.DeleteSeriesRange(ditr, math.MinInt64, math.MaxInt64); err != nil {
			return err
		}

		if engine.SeriesIDSet().Contains(seriesIDMap["mem,host=z"]) {
			return fmt.Errorf("bitmap does not contain ID: %d for key %s, but should", seriesIDMap["mem,host=z"], "mem,host=z")
		}
		delete(seriesIDMap, "mem,host=z")

		// The rest of the keys should still be in the set.
		for key, id := range seriesIDMap {
			if !engine.SeriesIDSet().Contains(id) {
				return fmt.Errorf("bitmap does not contain ID: %d for key %s, but should", id, key)
			}
		}

		// Reopen the engine, and the series should be re-added to the bitmap.
		if err := engine.Reopen(); err != nil {
			panic(err)
		}

		// Check bitset is expected.
		expected := tsdb.NewSeriesIDSet()
		for _, id := range seriesIDMap {
			expected.Add(id)
		}

		if !engine.SeriesIDSet().Equals(expected) {
			return fmt.Errorf("got bitset %s, expected %s", engine.SeriesIDSet().String(), expected.String())
		}
		return nil
	}

	for _, index := range tsdb.RegisteredIndexes() {
		t.Run(index, func(t *testing.T) {
			if err := test(index); err != nil {
				t.Error(err)
			}
		})
	}
}

// Ensures that deleting series from TSM files with multiple fields removes all the
/// series
func TestEngine_DeleteSeries(t *testing.T) {
	for _, index := range tsdb.RegisteredIndexes() {
		t.Run(index, func(t *testing.T) {
			// Create a few points.
			p1 := MustParsePointString("cpu,host=A value=1.1 1000000000")
			p2 := MustParsePointString("cpu,host=B value=1.2 2000000000")
			p3 := MustParsePointString("cpu,host=A sum=1.3 3000000000")

			e, err := NewEngine()
			if err != nil {
				t.Fatal(err)
			}

			// mock the planner so compactions don't run during the test
			e.CompactionPlan = &mockPlanner{}
			if err := e.Open(); err != nil {
				t.Fatal(err)
			}
			defer e.Close()

			if err := e.writePoints(p1, p2, p3); err != nil {
				t.Fatalf("failed to write points: %s", err.Error())
			}
			if err := e.WriteSnapshot(); err != nil {
				t.Fatalf("failed to snapshot: %s", err.Error())
			}

			keys := e.FileStore.Keys()
			if exp, got := 3, len(keys); exp != got {
				t.Fatalf("series count mismatch: exp %v, got %v", exp, got)
			}

			itr := &seriesIterator{keys: [][]byte{[]byte("cpu,host=A")}}
			if err := e.DeleteSeriesRange(itr, math.MinInt64, math.MaxInt64); err != nil {
				t.Fatalf("failed to delete series: %v", err)
			}

			keys = e.FileStore.Keys()
			if exp, got := 1, len(keys); exp != got {
				t.Fatalf("series count mismatch: exp %v, got %v", exp, got)
			}

			exp := "cpu,host=B#!~#value"
			if _, ok := keys[exp]; !ok {
				t.Fatalf("wrong series deleted: exp %v, got %v", exp, keys)
			}
		})
	}
}

func TestEngine_DeleteSeriesRange(t *testing.T) {
	for _, index := range tsdb.RegisteredIndexes() {
		t.Run(index, func(t *testing.T) {
			// Create a few points.
			p1 := MustParsePointString("cpu,host=0 value=1.1 6000000000") // Should not be deleted
			p2 := MustParsePointString("cpu,host=A value=1.2 2000000000")
			p3 := MustParsePointString("cpu,host=A value=1.3 3000000000")
			p4 := MustParsePointString("cpu,host=B value=1.3 4000000000") // Should not be deleted
			p5 := MustParsePointString("cpu,host=B value=1.3 5000000000") // Should not be deleted
			p6 := MustParsePointString("cpu,host=C value=1.3 1000000000")
			p7 := MustParsePointString("mem,host=C value=1.3 1000000000")  // Should not be deleted
			p8 := MustParsePointString("disk,host=C value=1.3 1000000000") // Should not be deleted

			e, err := NewEngine()
			if err != nil {
				t.Fatal(err)
			}

			// mock the planner so compactions don't run during the test
			e.CompactionPlan = &mockPlanner{}
			if err := e.Open(); err != nil {
				t.Fatal(err)
			}
			defer e.Close()

			for _, p := range []models.Point{p1, p2, p3, p4, p5, p6, p7, p8} {
				if err := e.CreateSeriesIfNotExists(p.Key(), p.Name(), p.Tags(), models.Float); err != nil {
					t.Fatalf("create series index error: %v", err)
				}
			}

			if err := e.WritePoints([]models.Point{p1, p2, p3, p4, p5, p6, p7, p8}); err != nil {
				t.Fatalf("failed to write points: %s", err.Error())
			}
			if err := e.WriteSnapshot(); err != nil {
				t.Fatalf("failed to snapshot: %s", err.Error())
			}

			keys := e.FileStore.Keys()
			if exp, got := 6, len(keys); exp != got {
				t.Fatalf("series count mismatch: exp %v, got %v", exp, got)
			}

			itr := &seriesIterator{keys: [][]byte{[]byte("cpu,host=0"), []byte("cpu,host=A"), []byte("cpu,host=B"), []byte("cpu,host=C")}}
			if err := e.DeleteSeriesRange(itr, 0, 3000000000); err != nil {
				t.Fatalf("failed to delete series: %v", err)
			}

			keys = e.FileStore.Keys()
			if exp, got := 4, len(keys); exp != got {
				t.Fatalf("series count mismatch: exp %v, got %v", exp, got)
			}

			exp := "cpu,host=B#!~#value"
			if _, ok := keys[exp]; !ok {
				t.Fatalf("wrong series deleted: exp %v, got %v", exp, keys)
			}

			// Check that the series still exists in the index
			iter, err := e.index.MeasurementSeriesIDIterator([]byte("cpu"))
			if err != nil {
				t.Fatalf("iterator error: %v", err)
			}
			defer iter.Close()

			elem, err := iter.Next()
			if err != nil {
				t.Fatal(err)
			}
			if elem.SeriesID.IsZero() {
				t.Fatalf("series index mismatch: EOF, exp 2 series")
			}

			// Lookup series.
			name, tags := e.sfile.Series(elem.SeriesID)
			if got, exp := name, []byte("cpu"); !bytes.Equal(got, exp) {
				t.Fatalf("series mismatch: got %s, exp %s", got, exp)
			}

			if !tags.Equal(models.NewTags(map[string]string{"host": "0"})) && !tags.Equal(models.NewTags(map[string]string{"host": "B"})) {
				t.Fatalf(`series mismatch: got %s, exp either "host=0" or "host=B"`, tags)
			}
			iter.Close()

			// Deleting remaining series should remove them from the series.
			itr = &seriesIterator{keys: [][]byte{[]byte("cpu,host=0"), []byte("cpu,host=B")}}
			if err := e.DeleteSeriesRange(itr, 0, 9000000000); err != nil {
				t.Fatalf("failed to delete series: %v", err)
			}

			if iter, err = e.index.MeasurementSeriesIDIterator([]byte("cpu")); err != nil {
				t.Fatalf("iterator error: %v", err)
			}
			if iter == nil {
				return
			}

			defer iter.Close()
			if elem, err = iter.Next(); err != nil {
				t.Fatal(err)
			}
			if !elem.SeriesID.IsZero() {
				t.Fatalf("got an undeleted series id, but series should be dropped from index")
			}
		})
	}
}

func TestEngine_DeleteSeriesRangeWithPredicate(t *testing.T) {
	for _, index := range tsdb.RegisteredIndexes() {
		t.Run(index, func(t *testing.T) {
			// Create a few points.
			p1 := MustParsePointString("cpu,host=A value=1.1 6000000000") // Should not be deleted
			p2 := MustParsePointString("cpu,host=A value=1.2 2000000000") // Should not be deleted
			p3 := MustParsePointString("cpu,host=B value=1.3 3000000000")
			p4 := MustParsePointString("cpu,host=B value=1.3 4000000000")
			p5 := MustParsePointString("cpu,host=C value=1.3 5000000000") // Should not be deleted
			p6 := MustParsePointString("mem,host=B value=1.3 1000000000")
			p7 := MustParsePointString("mem,host=C value=1.3 1000000000")
			p8 := MustParsePointString("disk,host=C value=1.3 1000000000") // Should not be deleted

			e, err := NewEngine()
			if err != nil {
				t.Fatal(err)
			}

			// mock the planner so compactions don't run during the test
			e.CompactionPlan = &mockPlanner{}
			if err := e.Open(); err != nil {
				t.Fatal(err)
			}
			defer e.Close()

			for _, p := range []models.Point{p1, p2, p3, p4, p5, p6, p7, p8} {
				if err := e.CreateSeriesIfNotExists(p.Key(), p.Name(), p.Tags(), models.Float); err != nil {
					t.Fatalf("create series index error: %v", err)
				}
			}

			if err := e.WritePoints([]models.Point{p1, p2, p3, p4, p5, p6, p7, p8}); err != nil {
				t.Fatalf("failed to write points: %s", err.Error())
			}
			if err := e.WriteSnapshot(); err != nil {
				t.Fatalf("failed to snapshot: %s", err.Error())
			}

			keys := e.FileStore.Keys()
			if exp, got := 6, len(keys); exp != got {
				t.Fatalf("series count mismatch: exp %v, got %v", exp, got)
			}

			itr := &seriesIterator{keys: [][]byte{[]byte("cpu,host=A"), []byte("cpu,host=B"), []byte("cpu,host=C"), []byte("mem,host=B"), []byte("mem,host=C")}}
			predicate := func(name []byte, tags models.Tags) (int64, int64, bool) {
				if bytes.Equal(name, []byte("mem")) {
					return math.MinInt64, math.MaxInt64, true
				}
				if bytes.Equal(name, []byte("cpu")) {
					for _, tag := range tags {
						if bytes.Equal(tag.Key, []byte("host")) && bytes.Equal(tag.Value, []byte("B")) {
							return math.MinInt64, math.MaxInt64, true
						}
					}
				}
				return math.MinInt64, math.MaxInt64, false
			}
			if err := e.DeleteSeriesRangeWithPredicate(itr, predicate); err != nil {
				t.Fatalf("failed to delete series: %v", err)
			}

			keys = e.FileStore.Keys()
			if exp, got := 3, len(keys); exp != got {
				t.Fatalf("series count mismatch: exp %v, got %v", exp, got)
			}

			exps := []string{"cpu,host=A#!~#value", "cpu,host=C#!~#value", "disk,host=C#!~#value"}
			for _, exp := range exps {
				if _, ok := keys[exp]; !ok {
					t.Fatalf("wrong series deleted: exp %v, got %v", exps, keys)
				}
			}

			// Check that the series still exists in the index
			iter, err := e.index.MeasurementSeriesIDIterator([]byte("cpu"))
			if err != nil {
				t.Fatalf("iterator error: %v", err)
			}
			defer iter.Close()

			elem, err := iter.Next()
			if err != nil {
				t.Fatal(err)
			}
			if elem.SeriesID.IsZero() {
				t.Fatalf("series index mismatch: EOF, exp 2 series")
			}

			// Lookup series.
			name, tags := e.sfile.Series(elem.SeriesID)
			if got, exp := name, []byte("cpu"); !bytes.Equal(got, exp) {
				t.Fatalf("series mismatch: got %s, exp %s", got, exp)
			}

			if !tags.Equal(models.NewTags(map[string]string{"host": "A"})) && !tags.Equal(models.NewTags(map[string]string{"host": "C"})) {
				t.Fatalf(`series mismatch: got %s, exp either "host=A" or "host=C"`, tags)
			}
			iter.Close()

			// Deleting remaining series should remove them from the series.
			itr = &seriesIterator{keys: [][]byte{[]byte("cpu,host=A"), []byte("cpu,host=C")}}
			if err := e.DeleteSeriesRange(itr, 0, 9000000000); err != nil {
				t.Fatalf("failed to delete series: %v", err)
			}

			if iter, err = e.index.MeasurementSeriesIDIterator([]byte("cpu")); err != nil {
				t.Fatalf("iterator error: %v", err)
			}
			if iter == nil {
				return
			}

			defer iter.Close()
			if elem, err = iter.Next(); err != nil {
				t.Fatal(err)
			}
			if !elem.SeriesID.IsZero() {
				t.Fatalf("got an undeleted series id, but series should be dropped from index")
			}
		})
	}
}

// Tests that a nil predicate deletes all values returned from the series iterator.
func TestEngine_DeleteSeriesRangeWithPredicate_Nil(t *testing.T) {
	for _, index := range tsdb.RegisteredIndexes() {
		t.Run(index, func(t *testing.T) {
			// Create a few points.
			p1 := MustParsePointString("cpu,host=A value=1.1 6000000000") // Should not be deleted
			p2 := MustParsePointString("cpu,host=A value=1.2 2000000000") // Should not be deleted
			p3 := MustParsePointString("cpu,host=B value=1.3 3000000000")
			p4 := MustParsePointString("cpu,host=B value=1.3 4000000000")
			p5 := MustParsePointString("cpu,host=C value=1.3 5000000000") // Should not be deleted
			p6 := MustParsePointString("mem,host=B value=1.3 1000000000")
			p7 := MustParsePointString("mem,host=C value=1.3 1000000000")
			p8 := MustParsePointString("disk,host=C value=1.3 1000000000") // Should not be deleted

			e, err := NewEngine()
			if err != nil {
				t.Fatal(err)
			}

			// mock the planner so compactions don't run during the test
			e.CompactionPlan = &mockPlanner{}
			if err := e.Open(); err != nil {
				t.Fatal(err)
			}
			defer e.Close()

			for _, p := range []models.Point{p1, p2, p3, p4, p5, p6, p7, p8} {
				if err := e.CreateSeriesIfNotExists(p.Key(), p.Name(), p.Tags(), models.Float); err != nil {
					t.Fatalf("create series index error: %v", err)
				}
			}

			if err := e.WritePoints([]models.Point{p1, p2, p3, p4, p5, p6, p7, p8}); err != nil {
				t.Fatalf("failed to write points: %s", err.Error())
			}
			if err := e.WriteSnapshot(); err != nil {
				t.Fatalf("failed to snapshot: %s", err.Error())
			}

			keys := e.FileStore.Keys()
			if exp, got := 6, len(keys); exp != got {
				t.Fatalf("series count mismatch: exp %v, got %v", exp, got)
			}

			itr := &seriesIterator{keys: [][]byte{[]byte("cpu,host=A"), []byte("cpu,host=B"), []byte("cpu,host=C"), []byte("mem,host=B"), []byte("mem,host=C")}}
			if err := e.DeleteSeriesRangeWithPredicate(itr, nil); err != nil {
				t.Fatalf("failed to delete series: %v", err)
			}

			keys = e.FileStore.Keys()
			if exp, got := 1, len(keys); exp != got {
				t.Fatalf("series count mismatch: exp %v, got %v", exp, got)
			}

			// Check that the series still exists in the index
			iter, err := e.index.MeasurementSeriesIDIterator([]byte("cpu"))
			if err != nil {
				t.Fatalf("iterator error: %v", err)
			} else if iter == nil {
				return
			}
			defer iter.Close()

			if elem, err := iter.Next(); err != nil {
				t.Fatal(err)
			} else if !elem.SeriesID.IsZero() {
				t.Fatalf("got an undeleted series id, but series should be dropped from index")
			}

			// Check that disk series still exists
			iter, err = e.index.MeasurementSeriesIDIterator([]byte("disk"))
			if err != nil {
				t.Fatalf("iterator error: %v", err)
			} else if iter == nil {
				return
			}
			defer iter.Close()

			if elem, err := iter.Next(); err != nil {
				t.Fatal(err)
			} else if elem.SeriesID.IsZero() {
				t.Fatalf("got an undeleted series id, but series should be dropped from index")
			}
		})
	}
}
func TestEngine_DeleteSeriesRangeWithPredicate_FlushBatch(t *testing.T) {
	for _, index := range tsdb.RegisteredIndexes() {
		t.Run(index, func(t *testing.T) {
			// Create a few points.
			p1 := MustParsePointString("cpu,host=A value=1.1 6000000000") // Should not be deleted
			p2 := MustParsePointString("cpu,host=A value=1.2 2000000000") // Should not be deleted
			p3 := MustParsePointString("cpu,host=B value=1.3 3000000000")
			p4 := MustParsePointString("cpu,host=B value=1.3 4000000000")
			p5 := MustParsePointString("cpu,host=C value=1.3 5000000000") // Should not be deleted
			p6 := MustParsePointString("mem,host=B value=1.3 1000000000")
			p7 := MustParsePointString("mem,host=C value=1.3 1000000000")
			p8 := MustParsePointString("disk,host=C value=1.3 1000000000") // Should not be deleted

			e, err := NewEngine()
			if err != nil {
				t.Fatal(err)
			}

			// mock the planner so compactions don't run during the test
			e.CompactionPlan = &mockPlanner{}
			if err := e.Open(); err != nil {
				t.Fatal(err)
			}
			defer e.Close()

			for _, p := range []models.Point{p1, p2, p3, p4, p5, p6, p7, p8} {
				if err := e.CreateSeriesIfNotExists(p.Key(), p.Name(), p.Tags(), models.Float); err != nil {
					t.Fatalf("create series index error: %v", err)
				}
			}

			if err := e.WritePoints([]models.Point{p1, p2, p3, p4, p5, p6, p7, p8}); err != nil {
				t.Fatalf("failed to write points: %s", err.Error())
			}
			if err := e.WriteSnapshot(); err != nil {
				t.Fatalf("failed to snapshot: %s", err.Error())
			}

			keys := e.FileStore.Keys()
			if exp, got := 6, len(keys); exp != got {
				t.Fatalf("series count mismatch: exp %v, got %v", exp, got)
			}

			itr := &seriesIterator{keys: [][]byte{[]byte("cpu,host=A"), []byte("cpu,host=B"), []byte("cpu,host=C"), []byte("mem,host=B"), []byte("mem,host=C")}}
			predicate := func(name []byte, tags models.Tags) (int64, int64, bool) {
				if bytes.Equal(name, []byte("mem")) {
					return 1000000000, 1000000000, true
				}

				if bytes.Equal(name, []byte("cpu")) {
					for _, tag := range tags {
						if bytes.Equal(tag.Key, []byte("host")) && bytes.Equal(tag.Value, []byte("B")) {
							return 3000000000, 4000000000, true
						}
					}
				}
				return math.MinInt64, math.MaxInt64, false
			}
			if err := e.DeleteSeriesRangeWithPredicate(itr, predicate); err != nil {
				t.Fatalf("failed to delete series: %v", err)
			}

			keys = e.FileStore.Keys()
			if exp, got := 3, len(keys); exp != got {
				t.Fatalf("series count mismatch: exp %v, got %v", exp, got)
			}

			exps := []string{"cpu,host=A#!~#value", "cpu,host=C#!~#value", "disk,host=C#!~#value"}
			for _, exp := range exps {
				if _, ok := keys[exp]; !ok {
					t.Fatalf("wrong series deleted: exp %v, got %v", exps, keys)
				}
			}

			// Check that the series still exists in the index
			iter, err := e.index.MeasurementSeriesIDIterator([]byte("cpu"))
			if err != nil {
				t.Fatalf("iterator error: %v", err)
			}
			defer iter.Close()

			elem, err := iter.Next()
			if err != nil {
				t.Fatal(err)
			}
			if elem.SeriesID.IsZero() {
				t.Fatalf("series index mismatch: EOF, exp 2 series")
			}

			// Lookup series.
			name, tags := e.sfile.Series(elem.SeriesID)
			if got, exp := name, []byte("cpu"); !bytes.Equal(got, exp) {
				t.Fatalf("series mismatch: got %s, exp %s", got, exp)
			}

			if !tags.Equal(models.NewTags(map[string]string{"host": "A"})) && !tags.Equal(models.NewTags(map[string]string{"host": "C"})) {
				t.Fatalf(`series mismatch: got %s, exp either "host=A" or "host=C"`, tags)
			}
			iter.Close()

			// Deleting remaining series should remove them from the series.
			itr = &seriesIterator{keys: [][]byte{[]byte("cpu,host=A"), []byte("cpu,host=C")}}
			if err := e.DeleteSeriesRange(itr, 0, 9000000000); err != nil {
				t.Fatalf("failed to delete series: %v", err)
			}

			if iter, err = e.index.MeasurementSeriesIDIterator([]byte("cpu")); err != nil {
				t.Fatalf("iterator error: %v", err)
			}
			if iter == nil {
				return
			}

			defer iter.Close()
			if elem, err = iter.Next(); err != nil {
				t.Fatal(err)
			}
			if !elem.SeriesID.IsZero() {
				t.Fatalf("got an undeleted series id, but series should be dropped from index")
			}
		})
	}
}

func TestEngine_DeleteSeriesRange_OutsideTime(t *testing.T) {
	for _, index := range tsdb.RegisteredIndexes() {
		t.Run(index, func(t *testing.T) {
			// Create a few points.
			p1 := MustParsePointString("cpu,host=A value=1.1 1000000000") // Should not be deleted

			e, err := NewEngine()
			if err != nil {
				t.Fatal(err)
			}

			// mock the planner so compactions don't run during the test
			e.CompactionPlan = &mockPlanner{}
			if err := e.Open(); err != nil {
				t.Fatal(err)
			}
			defer e.Close()

			for _, p := range []models.Point{p1} {
				if err := e.CreateSeriesIfNotExists(p.Key(), p.Name(), p.Tags(), models.Float); err != nil {
					t.Fatalf("create series index error: %v", err)
				}
			}

			if err := e.WritePoints([]models.Point{p1}); err != nil {
				t.Fatalf("failed to write points: %s", err.Error())
			}
			if err := e.WriteSnapshot(); err != nil {
				t.Fatalf("failed to snapshot: %s", err.Error())
			}

			keys := e.FileStore.Keys()
			if exp, got := 1, len(keys); exp != got {
				t.Fatalf("series count mismatch: exp %v, got %v", exp, got)
			}

			itr := &seriesIterator{keys: [][]byte{[]byte("cpu,host=A")}}
			if err := e.DeleteSeriesRange(itr, 0, 0); err != nil {
				t.Fatalf("failed to delete series: %v", err)
			}

			keys = e.FileStore.Keys()
			if exp, got := 1, len(keys); exp != got {
				t.Fatalf("series count mismatch: exp %v, got %v", exp, got)
			}

			exp := "cpu,host=A#!~#value"
			if _, ok := keys[exp]; !ok {
				t.Fatalf("wrong series deleted: exp %v, got %v", exp, keys)
			}

			// Check that the series still exists in the index
			iter, err := e.index.MeasurementSeriesIDIterator([]byte("cpu"))
			if err != nil {
				t.Fatalf("iterator error: %v", err)
			}
			defer iter.Close()

			elem, err := iter.Next()
			if err != nil {
				t.Fatal(err)
			}
			if elem.SeriesID.IsZero() {
				t.Fatalf("series index mismatch: EOF, exp 1 series")
			}

			// Lookup series.
			name, tags := e.sfile.Series(elem.SeriesID)
			if got, exp := name, []byte("cpu"); !bytes.Equal(got, exp) {
				t.Fatalf("series mismatch: got %s, exp %s", got, exp)
			}

			if got, exp := tags, models.NewTags(map[string]string{"host": "A"}); !got.Equal(exp) {
				t.Fatalf("series mismatch: got %s, exp %s", got, exp)
			}
		})
	}
}

func TestEngine_LastModified(t *testing.T) {
	for _, index := range tsdb.RegisteredIndexes() {
		t.Run(index, func(t *testing.T) {
			// Create a few points.
			p1 := MustParsePointString("cpu,host=A value=1.1 1000000000")
			p2 := MustParsePointString("cpu,host=B value=1.2 2000000000")
			p3 := MustParsePointString("cpu,host=A sum=1.3 3000000000")

			e, err := NewEngine()
			if err != nil {
				t.Fatal(err)
			}

			// mock the planner so compactions don't run during the test
			e.CompactionPlan = &mockPlanner{}
			e.SetEnabled(false)
			if err := e.Open(); err != nil {
				t.Fatal(err)
			}
			defer e.Close()

			if err := e.writePoints(p1, p2, p3); err != nil {
				t.Fatalf("failed to write points: %s", err.Error())
			}

			lm := e.LastModified()
			if lm.IsZero() {
				t.Fatalf("expected non-zero time, got %v", lm.UTC())
			}
			e.SetEnabled(true)

			// Artificial sleep added due to filesystems caching the mod time
			// of files.  This prevents the WAL last modified time from being
			// returned and newer than the filestore's mod time.
			time.Sleep(2 * time.Second) // Covers most filesystems.

			if err := e.WriteSnapshot(); err != nil {
				t.Fatalf("failed to snapshot: %s", err.Error())
			}

			lm2 := e.LastModified()

			if got, exp := lm.Equal(lm2), false; exp != got {
				t.Fatalf("expected time change, got %v, exp %v: %s == %s", got, exp, lm.String(), lm2.String())
			}

			itr := &seriesIterator{keys: [][]byte{[]byte("cpu,host=A")}}
			if err := e.DeleteSeriesRange(itr, math.MinInt64, math.MaxInt64); err != nil {
				t.Fatalf("failed to delete series: %v", err)
			}

			lm3 := e.LastModified()
			if got, exp := lm2.Equal(lm3), false; exp != got {
				t.Fatalf("expected time change, got %v, exp %v", got, exp)
			}
		})
	}
}

func TestEngine_SnapshotsDisabled(t *testing.T) {
	sfile := MustOpenSeriesFile()
	defer sfile.Close()

	// Generate temporary file.
	dir, _ := ioutil.TempDir("", "tsm")
	walPath := filepath.Join(dir, "wal")
	os.MkdirAll(walPath, 0777)
	defer os.RemoveAll(dir)

	// Create a tsm1 engine.
	db := path.Base(dir)
	opt := tsdb.NewEngineOptions()

	idx := MustOpenIndex(1, db, filepath.Join(dir, "index"), tsdb.NewSeriesIDSet(), sfile.SeriesFile, opt)
	defer idx.Close()

	e := tsm1.NewEngine(1, idx, dir, walPath, sfile.SeriesFile, opt).(*tsm1.Engine)

	// mock the planner so compactions don't run during the test
	e.CompactionPlan = &mockPlanner{}

	e.SetEnabled(false)
	if err := e.Open(); err != nil {
		t.Fatalf("failed to open tsm1 engine: %s", err.Error())
	}

	// Make sure Snapshots are disabled.
	e.SetCompactionsEnabled(false)
	e.Compactor.DisableSnapshots()

	// Writing a snapshot should not fail when the snapshot is empty
	// even if snapshots are disabled.
	if err := e.WriteSnapshot(); err != nil {
		t.Fatalf("failed to snapshot: %s", err.Error())
	}
}

func TestEngine_ShouldCompactCache(t *testing.T) {
	nowTime := time.Now()

	e, err := NewEngine()
	if err != nil {
		t.Fatal(err)
	}

	// mock the planner so compactions don't run during the test
	e.CompactionPlan = &mockPlanner{}
	e.SetEnabled(false)
	if err := e.Open(); err != nil {
		t.Fatalf("failed to open tsm1 engine: %s", err.Error())
	}
	defer e.Close()

	e.CacheFlushMemorySizeThreshold = 1024
	e.CacheFlushWriteColdDuration = time.Minute

	if e.ShouldCompactCache(nowTime) {
		t.Fatal("nothing written to cache, so should not compact")
	}

	if err := e.WritePointsString("m,k=v f=3i"); err != nil {
		t.Fatal(err)
	}

	if e.ShouldCompactCache(nowTime) {
		t.Fatal("cache size < flush threshold and nothing written to FileStore, so should not compact")
	}

	if !e.ShouldCompactCache(nowTime.Add(time.Hour)) {
		t.Fatal("last compaction was longer than flush write cold threshold, so should compact")
	}

	e.CacheFlushMemorySizeThreshold = 1
	if !e.ShouldCompactCache(nowTime) {
		t.Fatal("cache size > flush threshold, so should compact")
	}
}

func makeBlockTypeSlice(n int) []byte {
	r := make([]byte, n)
	b := tsm1.BlockFloat64
	m := tsm1.BlockUnsigned + 1
	for i := 0; i < len(r); i++ {
		r[i] = b % m
	}
	return r
}

var blockType = influxql.Unknown

func BenchmarkBlockTypeToInfluxQLDataType(b *testing.B) {
	t := makeBlockTypeSlice(1000)
	for i := 0; i < b.N; i++ {
		for j := 0; j < len(t); j++ {
			blockType = tsm1.BlockTypeToInfluxQLDataType(t[j])
		}
	}
}

// This test ensures that "sync: WaitGroup is reused before previous Wait has returned" is
// is not raised.
func TestEngine_DisableEnableCompactions_Concurrent(t *testing.T) {
	t.Parallel()

	for _, index := range tsdb.RegisteredIndexes() {
		t.Run(index, func(t *testing.T) {

			e := MustOpenEngine()
			defer e.Close()

			var wg sync.WaitGroup
			wg.Add(2)

			go func() {
				defer wg.Done()
				for i := 0; i < 1000; i++ {
					e.SetCompactionsEnabled(true)
					e.SetCompactionsEnabled(false)
				}
			}()

			go func() {
				defer wg.Done()
				for i := 0; i < 1000; i++ {
					e.SetCompactionsEnabled(false)
					e.SetCompactionsEnabled(true)
				}
			}()

			done := make(chan struct{})
			go func() {
				wg.Wait()
				close(done)
			}()

			// Wait for waitgroup or fail if it takes too long.
			select {
			case <-time.NewTimer(30 * time.Second).C:
				t.Fatalf("timed out after 30 seconds waiting for waitgroup")
			case <-done:
			}
		})
	}
}

func BenchmarkEngine_WritePoints(b *testing.B) {
	batchSizes := []int{10, 100, 1000, 5000, 10000}
	for _, sz := range batchSizes {
		for _, index := range tsdb.RegisteredIndexes() {
			e := MustOpenEngine()
			pp := make([]models.Point, 0, sz)
			for i := 0; i < sz; i++ {
				p := MustParsePointString(fmt.Sprintf("cpu,host=%d value=1.2", i))
				pp = append(pp, p)
			}

			b.Run(fmt.Sprintf("%s_%d", index, sz), func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					err := e.WritePoints(pp)
					if err != nil {
						b.Fatal(err)
					}
				}
			})
			e.Close()
		}
	}
}

func BenchmarkEngine_WritePoints_Parallel(b *testing.B) {
	batchSizes := []int{1000, 5000, 10000, 25000, 50000, 75000, 100000, 200000}
	for _, sz := range batchSizes {
		for _, index := range tsdb.RegisteredIndexes() {
			e := MustOpenEngine()

			cpus := runtime.GOMAXPROCS(0)
			pp := make([]models.Point, 0, sz*cpus)
			for i := 0; i < sz*cpus; i++ {
				p := MustParsePointString(fmt.Sprintf("cpu,host=%d value=1.2,other=%di", i, i))
				pp = append(pp, p)
			}

			b.Run(fmt.Sprintf("%s_%d", index, sz), func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					var wg sync.WaitGroup
					errC := make(chan error)
					for i := 0; i < cpus; i++ {
						wg.Add(1)
						go func(i int) {
							defer wg.Done()
							from, to := i*sz, (i+1)*sz
							err := e.WritePoints(pp[from:to])
							if err != nil {
								errC <- err
								return
							}
						}(i)
					}

					go func() {
						wg.Wait()
						close(errC)
					}()

					for err := range errC {
						if err != nil {
							b.Error(err)
						}
					}
				}
			})
			e.Close()
		}
	}
}

// Engine is a test wrapper for tsm1.Engine.
type Engine struct {
	*tsm1.Engine
	root      string
	indexPath string
	index     *tsi1.Index
	sfile     *tsdb.SeriesFile
}

// NewEngine returns a new instance of Engine at a temporary location.
func NewEngine() (*Engine, error) {
	root, err := ioutil.TempDir("", "tsm1-")
	if err != nil {
		panic(err)
	}

	db := "db0"
	dbPath := filepath.Join(root, "data", db)

	if err := os.MkdirAll(dbPath, os.ModePerm); err != nil {
		return nil, err
	}

	// Setup series file.
	sfile := tsdb.NewSeriesFile(filepath.Join(dbPath, tsdb.DefaultSeriesFileDirectory))
	sfile.Logger = logger.New(os.Stdout)
	if err = sfile.Open(); err != nil {
		return nil, err
	}

	opt := tsdb.NewEngineOptions()

	// Initialise series id sets. Need to do this as it's normally done at the
	// store level.
	seriesIDs := tsdb.NewSeriesIDSet()
	opt.SeriesIDSets = seriesIDSets([]*tsdb.SeriesIDSet{seriesIDs})

	idxPath := filepath.Join(dbPath, "index")
	idx := MustOpenIndex(1, db, idxPath, seriesIDs, sfile, opt)

	tsm1Engine := tsm1.NewEngine(1, idx, filepath.Join(root, "data"), filepath.Join(root, "wal"), sfile, opt).(*tsm1.Engine)

	return &Engine{
		Engine:    tsm1Engine,
		root:      root,
		indexPath: idxPath,
		index:     idx,
		sfile:     sfile,
	}, nil
}

// MustOpenEngine returns a new, open instance of Engine.
func MustOpenEngine() *Engine {
	e, err := NewEngine()
	if err != nil {
		panic(err)
	}

	if err := e.Open(); err != nil {
		panic(err)
	}
	return e
}

// Close closes the engine and removes all underlying data.
func (e *Engine) Close() error {
	return e.close(true)
}

func (e *Engine) close(cleanup bool) error {
	if e.index != nil {
		e.index.Close()
	}

	if e.sfile != nil {
		e.sfile.Close()
	}

	defer func() {
		if cleanup {
			os.RemoveAll(e.root)
		}
	}()
	return e.Engine.Close()
}

// Reopen closes and reopens the engine.
func (e *Engine) Reopen() error {
	// Close engine without removing underlying engine data.
	if err := e.close(false); err != nil {
		return err
	}

	// Re-open series file. Must create a new series file using the same data.
	e.sfile = tsdb.NewSeriesFile(e.sfile.Path())
	if err := e.sfile.Open(); err != nil {
		return err
	}

	db := path.Base(e.root)
	opt := tsdb.NewEngineOptions()

	// Re-initialise the series id set
	seriesIDSet := tsdb.NewSeriesIDSet()
	opt.SeriesIDSets = seriesIDSets([]*tsdb.SeriesIDSet{seriesIDSet})

	// Re-open index.
	e.index = MustOpenIndex(1, db, e.indexPath, seriesIDSet, e.sfile, opt)

	// Re-initialize engine.
	e.Engine = tsm1.NewEngine(1, e.index, filepath.Join(e.root, "data"), filepath.Join(e.root, "wal"), e.sfile, opt).(*tsm1.Engine)

	// Reopen engine
	if err := e.Engine.Open(); err != nil {
		return err
	}

	// Reload series data into index (no-op on TSI).
	return nil
}

// SeriesIDSet provides access to the underlying series id bitset in the engine's
// index. It will panic if the underlying index does not have a SeriesIDSet
// method.
func (e *Engine) SeriesIDSet() *tsdb.SeriesIDSet {
	return e.index.SeriesIDSet()
}

// AddSeries adds the provided series data to the index and writes a point to
// the engine with default values for a field and a time of now.
func (e *Engine) AddSeries(name string, tags map[string]string) error {
	point, err := models.NewPoint(name, models.NewTags(tags), models.Fields{"v": 1.0}, time.Now())
	if err != nil {
		return err
	}
	return e.writePoints(point)
}

// WritePointsString calls WritePointsString on the underlying engine, but also
// adds the associated series to the index.
func (e *Engine) WritePointsString(ptstr ...string) error {
	points, err := models.ParsePointsString(strings.Join(ptstr, "\n"))
	if err != nil {
		return err
	}
	return e.writePoints(points...)
}

// writePoints adds the series for the provided points to the index, and writes
// the point data to the engine.
func (e *Engine) writePoints(points ...models.Point) error {
	for _, point := range points {
		// Write into the index.
		iter := point.FieldIterator()
		iter.Next()
		if err := e.Engine.CreateSeriesIfNotExists(point.Key(), point.Name(), point.Tags(), iter.Type()); err != nil {
			return err
		}
	}
	// Write the points into the cache/wal.
	return e.WritePoints(points)
}

// MustAddSeries calls AddSeries, panicking if there is an error.
func (e *Engine) MustAddSeries(name string, tags map[string]string) {
	if err := e.AddSeries(name, tags); err != nil {
		panic(err)
	}
}

// MustWriteSnapshot forces a snapshot of the engine. Panic on error.
func (e *Engine) MustWriteSnapshot() {
	if err := e.WriteSnapshot(); err != nil {
		panic(err)
	}
}

func MustOpenIndex(id uint64, database, path string, seriesIDSet *tsdb.SeriesIDSet, sfile *tsdb.SeriesFile, options tsdb.EngineOptions) *tsi1.Index {
	idx := tsi1.NewIndex(sfile, database, tsi1.NewConfig(), tsi1.WithPath(path))
	if err := idx.Open(); err != nil {
		panic(err)
	}
	return idx
}

// SeriesFile is a test wrapper for tsdb.SeriesFile.
type SeriesFile struct {
	*tsdb.SeriesFile
}

// NewSeriesFile returns a new instance of SeriesFile with a temporary file path.
func NewSeriesFile() *SeriesFile {
	dir, err := ioutil.TempDir("", "tsdb-series-file-")
	if err != nil {
		panic(err)
	}
	return &SeriesFile{SeriesFile: tsdb.NewSeriesFile(dir)}
}

// MustOpenSeriesFile returns a new, open instance of SeriesFile. Panic on error.
func MustOpenSeriesFile() *SeriesFile {
	f := NewSeriesFile()
	if err := f.Open(); err != nil {
		panic(err)
	}
	return f
}

// Close closes the log file and removes it from disk.
func (f *SeriesFile) Close() {
	defer os.RemoveAll(f.Path())
	if err := f.SeriesFile.Close(); err != nil {
		panic(err)
	}
}

// MustParsePointsString parses points from a string. Panic on error.
func MustParsePointsString(buf string) []models.Point {
	a, err := models.ParsePointsString(buf)
	if err != nil {
		panic(err)
	}
	return a
}

// MustParsePointString parses the first point from a string. Panic on error.
func MustParsePointString(buf string) models.Point { return MustParsePointsString(buf)[0] }

type mockPlanner struct{}

func (m *mockPlanner) Plan(lastWrite time.Time) []tsm1.CompactionGroup { return nil }
func (m *mockPlanner) PlanLevel(level int) []tsm1.CompactionGroup      { return nil }
func (m *mockPlanner) PlanOptimize() []tsm1.CompactionGroup            { return nil }
func (m *mockPlanner) Release(groups []tsm1.CompactionGroup)           {}
func (m *mockPlanner) FullyCompacted() bool                            { return false }
func (m *mockPlanner) ForceFull()                                      {}
func (m *mockPlanner) SetFileStore(fs *tsm1.FileStore)                 {}

type seriesIterator struct {
	keys [][]byte
}

type series struct {
	name    []byte
	tags    models.Tags
	deleted bool
}

func (s series) Name() []byte        { return s.name }
func (s series) Tags() models.Tags   { return s.tags }
func (s series) Deleted() bool       { return s.deleted }
func (s series) Expr() influxql.Expr { return nil }

func (itr *seriesIterator) Close() error { return nil }

func (itr *seriesIterator) Next() (tsdb.SeriesElem, error) {
	if len(itr.keys) == 0 {
		return nil, nil
	}
	name, tags := models.ParseKeyBytes(itr.keys[0])
	s := series{name: name, tags: tags}
	itr.keys = itr.keys[1:]
	return s, nil
}

type seriesIDSets []*tsdb.SeriesIDSet

func (a seriesIDSets) ForEach(f func(ids *tsdb.SeriesIDSet)) error {
	for _, v := range a {
		f(v)
	}
	return nil
}

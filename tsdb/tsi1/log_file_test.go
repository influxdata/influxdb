package tsi1_test

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"regexp"
	"runtime/pprof"
	"sort"
	"testing"
	"time"

	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/pkg/bloom"
	"github.com/influxdata/influxdb/pkg/slices"
	"github.com/influxdata/influxdb/tsdb"
	"github.com/influxdata/influxdb/tsdb/tsi1"
)

// Ensure log file can append series.
func TestLogFile_AddSeriesList(t *testing.T) {
	t.Parallel()

	sfile := MustOpenSeriesFile()
	defer sfile.Close()

	f := MustOpenLogFile(sfile.SeriesFile)
	defer f.Close()
	seriesSet := tsdb.NewSeriesIDSet()

	// Add test data.
	collection := &tsdb.SeriesCollection{
		Names: slices.StringsToBytes("cpu", "mem"),
		Types: []models.FieldType{models.Integer, models.Integer},
		Tags: []models.Tags{
			{{Key: []byte("region"), Value: []byte("us-east")}},
			{{Key: []byte("host"), Value: []byte("serverA")}},
		},
	}

	if err := sfile.CreateSeriesListIfNotExists(collection); err != nil {
		t.Fatal(err)
	}
	ids, err := f.AddSeriesList(seriesSet, collection)
	if err != nil {
		t.Fatal(err)
	}

	// Returned series ids should match those in the seriesSet.
	other := tsdb.NewSeriesIDSet(ids...)
	if !other.Equals(seriesSet) {
		t.Fatalf("got series ids %s, expected %s", other, seriesSet)
	}

	// Add the same series again with a new one.
	collection = &tsdb.SeriesCollection{
		Names: slices.StringsToBytes("cpu", "mem"),
		Types: []models.FieldType{models.Integer, models.Integer},
		Tags: []models.Tags{
			{{Key: []byte("region"), Value: []byte("us-west")}},
			{{Key: []byte("host"), Value: []byte("serverA")}},
		},
	}
	if err := sfile.CreateSeriesListIfNotExists(collection); err != nil {
		t.Fatal(err)
	}
	ids, err = f.AddSeriesList(seriesSet, collection)
	if err != nil {
		t.Fatal(err)
	}

	if got, exp := len(ids), 2; got != exp {
		t.Fatalf("got %d series ids, expected %d", got, exp)
	} else if ids[0].IsZero() {
		t.Error("series id was 0, expected it not to be")
	} else if !ids[1].IsZero() {
		t.Errorf("got series id %d, expected 0", ids[1].RawID())
	}

	// Add only the same series IDs.
	collection = &tsdb.SeriesCollection{
		Names: slices.StringsToBytes("cpu", "mem"),
		Types: []models.FieldType{models.Integer, models.Integer},
		Tags: []models.Tags{
			{{Key: []byte("region"), Value: []byte("us-west")}},
			{{Key: []byte("host"), Value: []byte("serverA")}},
		},
	}
	if err := sfile.CreateSeriesListIfNotExists(collection); err != nil {
		t.Fatal(err)
	}
	ids, err = f.AddSeriesList(seriesSet, collection)
	if err != nil {
		t.Fatal(err)
	}

	if len(ids) != 0 {
		t.Fatalf("got %d ids, expected none", len(ids))
	}

	// Verify data.
	itr := f.MeasurementIterator()
	if e := itr.Next(); e == nil || string(e.Name()) != "cpu" {
		t.Fatalf("unexpected measurement: %#v", e)
	} else if e := itr.Next(); e == nil || string(e.Name()) != "mem" {
		t.Fatalf("unexpected measurement: %#v", e)
	} else if e := itr.Next(); e != nil {
		t.Fatalf("expected eof, got: %#v", e)
	}

	// Reopen file and re-verify.
	if err := f.Reopen(); err != nil {
		t.Fatal(err)
	}

	// Verify data.
	itr = f.MeasurementIterator()
	if e := itr.Next(); e == nil || string(e.Name()) != "cpu" {
		t.Fatalf("unexpected measurement: %#v", e)
	} else if e := itr.Next(); e == nil || string(e.Name()) != "mem" {
		t.Fatalf("unexpected measurement: %#v", e)
	} else if e := itr.Next(); e != nil {
		t.Fatalf("expected eof, got: %#v", e)
	}
}

func TestLogFile_SeriesStoredInOrder(t *testing.T) {
	t.Parallel()

	sfile := MustOpenSeriesFile()
	defer sfile.Close()

	f := MustOpenLogFile(sfile.SeriesFile)
	defer f.Close()
	seriesSet := tsdb.NewSeriesIDSet()

	// Generate and add test data
	tvm := make(map[string]struct{})
	rand.Seed(time.Now().Unix())
	for i := 0; i < 100; i++ {
		tv := fmt.Sprintf("server-%d", rand.Intn(50)) // Encourage adding duplicate series.
		tvm[tv] = struct{}{}

		collection := &tsdb.SeriesCollection{
			Names: [][]byte{[]byte("mem"), []byte("cpu")},
			Types: []models.FieldType{models.Integer, models.Integer},
			Tags: []models.Tags{
				{models.NewTag([]byte("host"), []byte(tv))},
				{models.NewTag([]byte("host"), []byte(tv))},
			},
		}
		if err := sfile.CreateSeriesListIfNotExists(collection); err != nil {
			t.Fatal(err)
		}
		if _, err := f.AddSeriesList(seriesSet, collection); err != nil {
			t.Fatal(err)
		}
	}

	// Sort the tag values so we know what order to expect.
	tvs := make([]string, 0, len(tvm))
	for tv := range tvm {
		tvs = append(tvs, tv)
	}
	sort.Strings(tvs)

	// Double the series values since we're adding them twice (two measurements)
	tvs = append(tvs, tvs...)

	// When we pull the series out via an iterator they should be in order.
	itr := f.SeriesIDIterator()
	if itr == nil {
		t.Fatal("nil iterator")
	}

	var prevSeriesID tsdb.SeriesID
	for i := 0; i < len(tvs); i++ {
		elem, err := itr.Next()
		if err != nil {
			t.Fatal(err)
		} else if elem.SeriesID.IsZero() {
			t.Fatal("got nil series")
		} else if elem.SeriesID.Less(prevSeriesID) {
			t.Fatalf("series out of order: %d !< %d ", elem.SeriesID, prevSeriesID)
		}
		prevSeriesID = elem.SeriesID
	}
}

// Ensure log file can delete an existing measurement.
func TestLogFile_DeleteMeasurement(t *testing.T) {
	t.Parallel()

	sfile := MustOpenSeriesFile()
	defer sfile.Close()

	f := MustOpenLogFile(sfile.SeriesFile)
	defer f.Close()
	seriesSet := tsdb.NewSeriesIDSet()

	// Add test data.
	collection := &tsdb.SeriesCollection{
		Names: [][]byte{[]byte("mem"), []byte("cpu"), []byte("cpu")},
		Types: []models.FieldType{models.Integer, models.Integer, models.Integer},
		Tags: []models.Tags{
			{{Key: []byte("host"), Value: []byte("serverA")}},
			{{Key: []byte("region"), Value: []byte("us-east")}},
			{{Key: []byte("region"), Value: []byte("us-west")}},
		},
	}

	if err := sfile.CreateSeriesListIfNotExists(collection); err != nil {
		t.Fatal(err)
	}
	if _, err := f.AddSeriesList(seriesSet, collection); err != nil {
		t.Fatal(err)
	}

	// Remove measurement.
	if err := f.DeleteMeasurement([]byte("cpu")); err != nil {
		t.Fatal(err)
	}

	// Verify data.
	itr := f.MeasurementIterator()
	if e := itr.Next(); string(e.Name()) != "cpu" || !e.Deleted() {
		t.Fatalf("unexpected measurement: %s/%v", e.Name(), e.Deleted())
	} else if e := itr.Next(); string(e.Name()) != "mem" || e.Deleted() {
		t.Fatalf("unexpected measurement: %s/%v", e.Name(), e.Deleted())
	} else if e := itr.Next(); e != nil {
		t.Fatalf("expected eof, got: %#v", e)
	}
}

// Ensure log file can recover correctly.
func TestLogFile_Open(t *testing.T) {
	t.Parallel()

	t.Run("Truncate", func(t *testing.T) {
		sfile := MustOpenSeriesFile()
		defer sfile.Close()
		seriesSet := tsdb.NewSeriesIDSet()

		f := MustOpenLogFile(sfile.SeriesFile)
		defer f.Close()

		// Add test data & close.
		collection := &tsdb.SeriesCollection{
			Names: [][]byte{[]byte("cpu"), []byte("mem")},
			Tags:  []models.Tags{{{}}, {{}}},
			Types: []models.FieldType{models.Integer, models.Integer},
		}

		if err := sfile.CreateSeriesListIfNotExists(collection); err != nil {
			t.Fatal(err)
		}
		if _, err := f.AddSeriesList(seriesSet, collection); err != nil {
			t.Fatal(err)
		} else if err := f.LogFile.Close(); err != nil {
			t.Fatal(err)
		}

		// Truncate data & reopen.
		if fi, err := os.Stat(f.LogFile.Path()); err != nil {
			t.Fatal(err)
		} else if err := os.Truncate(f.LogFile.Path(), fi.Size()-1); err != nil {
			t.Fatal(err)
		} else if err := f.LogFile.Open(); err != nil {
			t.Fatal(err)
		}

		// Verify data.
		itr := f.SeriesIDIterator()
		if elem, err := itr.Next(); err != nil {
			t.Fatal(err)
		} else if name, tags := sfile.Series(elem.SeriesID); string(name) != `cpu` {
			t.Fatalf("unexpected series: %s,%s", name, tags.String())
		} else if elem, err := itr.Next(); err != nil {
			t.Fatal(err)
		} else if !elem.SeriesID.IsZero() {
			t.Fatalf("expected eof, got: %#v", elem)
		}

		// Add more data & reopen.
		collection = &tsdb.SeriesCollection{
			Names: [][]byte{[]byte("disk")},
			Tags:  []models.Tags{{{}}},
			Types: []models.FieldType{models.Integer},
		}

		if err := sfile.CreateSeriesListIfNotExists(collection); err != nil {
			t.Fatal(err)
		}
		if _, err := f.AddSeriesList(seriesSet, collection); err != nil {
			t.Fatal(err)
		} else if err := f.Reopen(); err != nil {
			t.Fatal(err)
		}

		// Verify new data.
		itr = f.SeriesIDIterator()
		if elem, err := itr.Next(); err != nil {
			t.Fatal(err)
		} else if name, tags := sfile.Series(elem.SeriesID); string(name) != `cpu` {
			t.Fatalf("unexpected series: %s,%s", name, tags.String())
		} else if elem, err := itr.Next(); err != nil {
			t.Fatal(err)
		} else if name, tags := sfile.Series(elem.SeriesID); string(name) != `disk` {
			t.Fatalf("unexpected series: %s,%s", name, tags.String())
		} else if elem, err := itr.Next(); err != nil {
			t.Fatal(err)
		} else if !elem.SeriesID.IsZero() {
			t.Fatalf("expected eof, got: %#v", elem)
		}
	})

	t.Run("ChecksumMismatch", func(t *testing.T) {
		sfile := MustOpenSeriesFile()
		defer sfile.Close()
		seriesSet := tsdb.NewSeriesIDSet()

		f := MustOpenLogFile(sfile.SeriesFile)
		defer f.Close()

		// Add test data & close.
		collection := &tsdb.SeriesCollection{
			Names: [][]byte{[]byte("cpu"), []byte("mem")},
			Tags:  []models.Tags{{{}}, {{}}},
			Types: []models.FieldType{models.Integer, models.Integer},
		}
		if err := sfile.CreateSeriesListIfNotExists(collection); err != nil {
			t.Fatal(err)
		}
		if _, err := f.AddSeriesList(seriesSet, collection); err != nil {
			t.Fatal(err)
		} else if err := f.LogFile.Close(); err != nil {
			t.Fatal(err)
		}

		// Corrupt last entry.
		buf, err := ioutil.ReadFile(f.LogFile.Path())
		if err != nil {
			t.Fatal(err)
		}
		buf[len(buf)-1] = 0

		// Overwrite file with corrupt entry and reopen.
		if err := ioutil.WriteFile(f.LogFile.Path(), buf, 0666); err != nil {
			t.Fatal(err)
		} else if err := f.LogFile.Open(); err != nil {
			t.Fatal(err)
		}

		// Verify data.
		itr := f.SeriesIDIterator()
		if elem, err := itr.Next(); err != nil {
			t.Fatal(err)
		} else if name, tags := sfile.Series(elem.SeriesID); string(name) != `cpu` {
			t.Fatalf("unexpected series: %s,%s", name, tags.String())
		} else if elem, err := itr.Next(); err != nil {
			t.Fatal(err)
		} else if !elem.SeriesID.IsZero() {
			t.Fatalf("expected eof, got: %#v", elem)
		}
	})
}

// LogFile is a test wrapper for tsi1.LogFile.
type LogFile struct {
	*tsi1.LogFile
}

// NewLogFile returns a new instance of LogFile with a temporary file path.
func NewLogFile(sfile *tsdb.SeriesFile) *LogFile {
	file, err := ioutil.TempFile("", "tsi1-log-file-")
	if err != nil {
		panic(err)
	}
	file.Close()

	return &LogFile{LogFile: tsi1.NewLogFile(sfile, file.Name())}
}

// MustOpenLogFile returns a new, open instance of LogFile. Panic on error.
func MustOpenLogFile(sfile *tsdb.SeriesFile) *LogFile {
	f := NewLogFile(sfile)
	if err := f.Open(); err != nil {
		panic(err)
	}
	return f
}

// Close closes the log file and removes it from disk.
func (f *LogFile) Close() error {
	defer os.Remove(f.Path())
	return f.LogFile.Close()
}

// Reopen closes and reopens the file.
func (f *LogFile) Reopen() error {
	if err := f.LogFile.Close(); err != nil {
		return err
	}
	if err := f.LogFile.Open(); err != nil {
		return err
	}
	return nil
}

// CreateLogFile creates a new temporary log file and adds a list of series.
func CreateLogFile(sfile *tsdb.SeriesFile, series []Series) (*LogFile, error) {
	f := MustOpenLogFile(sfile)
	seriesSet := tsdb.NewSeriesIDSet()
	for _, serie := range series {
		collection := &tsdb.SeriesCollection{
			Names: [][]byte{[]byte(serie.Name)},
			Tags:  []models.Tags{serie.Tags},
			Types: []models.FieldType{serie.Type},
		}
		if err := sfile.CreateSeriesListIfNotExists(collection); err != nil {
			return nil, err
		}
		if _, err := f.AddSeriesList(seriesSet, collection); err != nil {
			return nil, err
		}
	}
	return f, nil
}

// GenerateLogFile generates a log file from a set of series based on the count arguments.
// Total series returned will equal measurementN * tagN * valueN.
func GenerateLogFile(sfile *tsdb.SeriesFile, measurementN, tagN, valueN int) (*LogFile, error) {
	tagValueN := pow(valueN, tagN)

	f := MustOpenLogFile(sfile)
	seriesSet := tsdb.NewSeriesIDSet()
	collection := new(tsdb.SeriesCollection)

	for i := 0; i < measurementN; i++ {
		name := []byte(fmt.Sprintf("measurement%d", i))

		// Generate tag sets.
		for j := 0; j < tagValueN; j++ {
			var tags models.Tags
			for k := 0; k < tagN; k++ {
				key := []byte(fmt.Sprintf("key%d", k))
				value := []byte(fmt.Sprintf("value%d", (j / pow(valueN, k) % valueN)))
				tags = append(tags, models.NewTag(key, value))
			}
			collection.Names = append(collection.Names, name)
			collection.Tags = append(collection.Tags, tags)
			collection.Types = append(collection.Types, models.Integer)

			if collection.Length() >= 10000 {
				if err := sfile.CreateSeriesListIfNotExists(collection); err != nil {
					return nil, err
				}
				if _, err := f.AddSeriesList(seriesSet, collection); err != nil {
					return nil, err
				}
				collection.Truncate(0)
			}
		}
	}

	if collection.Length() > 0 {
		if err := sfile.CreateSeriesListIfNotExists(collection); err != nil {
			return nil, err
		}
		if _, err := f.AddSeriesList(seriesSet, collection); err != nil {
			return nil, err
		}
	}

	return f, nil
}

func benchmarkLogFile_AddSeries(b *testing.B, measurementN, seriesKeyN, seriesValueN int) {
	sfile := MustOpenSeriesFile()
	defer sfile.Close()

	b.StopTimer()
	f := MustOpenLogFile(sfile.SeriesFile)
	seriesSet := tsdb.NewSeriesIDSet()

	type Datum struct {
		Name []byte
		Tags models.Tags
		Type models.FieldType
	}

	// Pre-generate everything.
	var (
		data   []Datum
		series int
	)

	tagValueN := pow(seriesValueN, seriesKeyN)

	for i := 0; i < measurementN; i++ {
		name := []byte(fmt.Sprintf("measurement%d", i))
		for j := 0; j < tagValueN; j++ {
			var tags models.Tags
			for k := 0; k < seriesKeyN; k++ {
				key := []byte(fmt.Sprintf("key%d", k))
				value := []byte(fmt.Sprintf("value%d", (j / pow(seriesValueN, k) % seriesValueN)))
				tags = append(tags, models.NewTag(key, value))
			}
			data = append(data, Datum{Name: name, Tags: tags, Type: models.Integer})
			series += len(tags)
		}
	}

	b.StartTimer()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		for _, d := range data {
			collection := &tsdb.SeriesCollection{
				Names: [][]byte{[]byte(d.Name)},
				Tags:  []models.Tags{d.Tags},
				Types: []models.FieldType{d.Type},
			}
			if err := sfile.CreateSeriesListIfNotExists(collection); err != nil {
				b.Fatal(err)
			}
			if _, err := f.AddSeriesList(seriesSet, collection); err != nil {
				b.Fatal(err)
			}
		}
	}
}

func BenchmarkLogFile_AddSeries_100_1_1(b *testing.B)    { benchmarkLogFile_AddSeries(b, 100, 1, 1) }    // 100 series
func BenchmarkLogFile_AddSeries_1000_1_1(b *testing.B)   { benchmarkLogFile_AddSeries(b, 1000, 1, 1) }   // 1000 series
func BenchmarkLogFile_AddSeries_10000_1_1(b *testing.B)  { benchmarkLogFile_AddSeries(b, 10000, 1, 1) }  // 10000 series
func BenchmarkLogFile_AddSeries_100_2_10(b *testing.B)   { benchmarkLogFile_AddSeries(b, 100, 2, 10) }   // ~20K series
func BenchmarkLogFile_AddSeries_100000_1_1(b *testing.B) { benchmarkLogFile_AddSeries(b, 100000, 1, 1) } // ~100K series
func BenchmarkLogFile_AddSeries_100_3_7(b *testing.B)    { benchmarkLogFile_AddSeries(b, 100, 3, 7) }    // ~100K series
func BenchmarkLogFile_AddSeries_200_3_7(b *testing.B)    { benchmarkLogFile_AddSeries(b, 200, 3, 7) }    // ~200K series
func BenchmarkLogFile_AddSeries_200_4_7(b *testing.B)    { benchmarkLogFile_AddSeries(b, 200, 4, 7) }    // ~1.9M series

func BenchmarkLogFile_WriteTo(b *testing.B) {
	for _, seriesN := range []int{1000, 10000, 100000, 1000000} {
		name := fmt.Sprintf("series=%d", seriesN)
		b.Run(name, func(b *testing.B) {
			sfile := MustOpenSeriesFile()
			defer sfile.Close()

			f := MustOpenLogFile(sfile.SeriesFile)
			defer f.Close()
			seriesSet := tsdb.NewSeriesIDSet()

			// Estimate bloom filter size.
			m, k := bloom.Estimate(uint64(seriesN), 0.02)

			// Initialize log file with series data.
			for i := 0; i < seriesN; i++ {
				collection := &tsdb.SeriesCollection{
					Names: [][]byte{[]byte("cpu")},
					Tags: []models.Tags{{
						{Key: []byte("host"), Value: []byte(fmt.Sprintf("server-%d", i))},
						{Key: []byte("location"), Value: []byte("us-west")},
					}},
					Types: []models.FieldType{models.Integer},
				}
				if err := sfile.CreateSeriesListIfNotExists(collection); err != nil {
					b.Fatal(err)
				}
				if _, err := f.AddSeriesList(seriesSet, collection); err != nil {
					b.Fatal(err)
				}
			}
			b.ResetTimer()

			// Create cpu profile for each subtest.
			MustStartCPUProfile(name)
			defer pprof.StopCPUProfile()

			// Compact log file.
			for i := 0; i < b.N; i++ {
				buf := bytes.NewBuffer(make([]byte, 0, 150*seriesN))
				if _, err := f.CompactTo(buf, m, k, nil); err != nil {
					b.Fatal(err)
				}
				b.Logf("sz=%db", buf.Len())
			}
		})
	}
}

// MustStartCPUProfile starts a cpu profile in a temporary path based on name.
func MustStartCPUProfile(name string) {
	name = regexp.MustCompile(`\W+`).ReplaceAllString(name, "-")

	// Open file and start pprof.
	f, err := os.Create(filepath.Join("/tmp", fmt.Sprintf("cpu-%s.pprof", name)))
	if err != nil {
		panic(err)
	}
	if err := pprof.StartCPUProfile(f); err != nil {
		panic(err)
	}
}

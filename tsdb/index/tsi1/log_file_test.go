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
	"github.com/influxdata/influxdb/tsdb/index/tsi1"
)

// Ensure log file can append series.
func TestLogFile_AddSeries(t *testing.T) {
	sfile := MustOpenSeriesFile()
	defer sfile.Close()

	f := MustOpenLogFile(sfile.SeriesFile)
	defer f.Close()

	// Add test data.
	if err := f.AddSeries([]byte("mem"), models.Tags{{Key: []byte("host"), Value: []byte("serverA")}}); err != nil {
		t.Fatal(err)
	} else if err := f.AddSeries([]byte("cpu"), models.Tags{{Key: []byte("region"), Value: []byte("us-east")}}); err != nil {
		t.Fatal(err)
	} else if err := f.AddSeries([]byte("cpu"), models.Tags{{Key: []byte("region"), Value: []byte("us-west")}}); err != nil {
		t.Fatal(err)
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
	sfile := MustOpenSeriesFile()
	defer sfile.Close()

	f := MustOpenLogFile(sfile.SeriesFile)
	defer f.Close()

	// Generate and add test data
	tvm := make(map[string]struct{})
	rand.Seed(time.Now().Unix())
	for i := 0; i < 100; i++ {
		tv := fmt.Sprintf("server-%d", rand.Intn(50)) // Encourage adding duplicate series.
		tvm[tv] = struct{}{}

		if err := f.AddSeries([]byte("mem"), models.Tags{models.NewTag([]byte("host"), []byte(tv))}); err != nil {
			t.Fatal(err)
		}

		if err := f.AddSeries([]byte("cpu"), models.Tags{models.NewTag([]byte("host"), []byte(tv))}); err != nil {
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

	var prevSeriesID uint32
	for i := 0; i < len(tvs); i++ {
		elem := itr.Next()
		if elem.SeriesID == 0 {
			t.Fatal("got nil series")
		} else if elem.SeriesID < prevSeriesID {
			t.Fatal("series out of order: %d !< %d ", elem.SeriesID, prevSeriesID)
		}
		prevSeriesID = elem.SeriesID
	}
}

// Ensure log file can delete an existing measurement.
func TestLogFile_DeleteMeasurement(t *testing.T) {
	sfile := MustOpenSeriesFile()
	defer sfile.Close()

	f := MustOpenLogFile(sfile.SeriesFile)
	defer f.Close()

	// Add test data.
	if err := f.AddSeries([]byte("mem"), models.Tags{{Key: []byte("host"), Value: []byte("serverA")}}); err != nil {
		t.Fatal(err)
	} else if err := f.AddSeries([]byte("cpu"), models.Tags{{Key: []byte("region"), Value: []byte("us-east")}}); err != nil {
		t.Fatal(err)
	} else if err := f.AddSeries([]byte("cpu"), models.Tags{{Key: []byte("region"), Value: []byte("us-west")}}); err != nil {
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

// LogFile is a test wrapper for tsi1.LogFile.
type LogFile struct {
	*tsi1.LogFile
}

// NewLogFile returns a new instance of LogFile with a temporary file path.
func NewLogFile(sfile *tsi1.SeriesFile) *LogFile {
	file, err := ioutil.TempFile("", "tsi1-log-file-")
	if err != nil {
		panic(err)
	}
	file.Close()

	return &LogFile{LogFile: tsi1.NewLogFile(sfile, file.Name())}
}

// MustOpenLogFile returns a new, open instance of LogFile. Panic on error.
func MustOpenLogFile(sfile *tsi1.SeriesFile) *LogFile {
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
func CreateLogFile(sfile *tsi1.SeriesFile, series []Series) (*LogFile, error) {
	f := MustOpenLogFile(sfile)
	for _, serie := range series {
		if err := f.AddSeries(serie.Name, serie.Tags); err != nil {
			return nil, err
		}
	}
	return f, nil
}

// GenerateLogFile generates a log file from a set of series based on the count arguments.
// Total series returned will equal measurementN * tagN * valueN.
func GenerateLogFile(sfile *tsi1.SeriesFile, measurementN, tagN, valueN int) (*LogFile, error) {
	tagValueN := pow(valueN, tagN)

	f := MustOpenLogFile(sfile)
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
			if err := f.AddSeries(name, tags); err != nil {
				return nil, err
			}
		}
	}
	return f, nil
}

func MustGenerateLogFile(sfile *tsi1.SeriesFile, measurementN, tagN, valueN int) *LogFile {
	f, err := GenerateLogFile(sfile, measurementN, tagN, valueN)
	if err != nil {
		panic(err)
	}
	return f
}

func benchmarkLogFile_AddSeries(b *testing.B, measurementN, seriesKeyN, seriesValueN int) {
	sfile := MustOpenSeriesFile()
	defer sfile.Close()

	b.StopTimer()
	f := MustOpenLogFile(sfile.SeriesFile)

	type Datum struct {
		Name []byte
		Tags models.Tags
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
			data = append(data, Datum{Name: name, Tags: tags})
			series += len(tags)
		}
	}
	b.StartTimer()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		for _, d := range data {
			if err := f.AddSeries(d.Name, d.Tags); err != nil {
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

			// Estimate bloom filter size.
			m, k := bloom.Estimate(uint64(seriesN), 0.02)

			// Initialize log file with series data.
			for i := 0; i < seriesN; i++ {
				if err := f.AddSeries(
					[]byte("cpu"),
					models.Tags{
						{Key: []byte("host"), Value: []byte(fmt.Sprintf("server-%d", i))},
						{Key: []byte("location"), Value: []byte("us-west")},
					},
				); err != nil {
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
				if _, err := f.CompactTo(buf, m, k); err != nil {
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

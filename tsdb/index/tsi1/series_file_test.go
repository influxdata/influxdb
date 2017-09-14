package tsi1_test

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/tsdb/index/tsi1"
)

// Ensure series file contains the correct set of series.
func TestSeriesFile_Series(t *testing.T) {
	sfile := MustOpenSeriesFile()
	defer sfile.Close()

	series := []Series{
		{Name: []byte("cpu"), Tags: models.NewTags(map[string]string{"region": "east"})},
		{Name: []byte("cpu"), Tags: models.NewTags(map[string]string{"region": "west"})},
		{Name: []byte("mem"), Tags: models.NewTags(map[string]string{"region": "east"})},
	}
	for _, s := range series {
		if _, err := sfile.CreateSeriesIfNotExists(s.Name, s.Tags, nil); err != nil {
			t.Fatal(err)
		}
	}

	// Verify total number of series is correct.
	if n := sfile.SeriesCount(); n != 3 {
		t.Fatalf("unexpected series count: %d", n)
	}

	// Verify all series exist.
	for i, s := range series {
		if offset := sfile.Offset(s.Name, s.Tags, nil); offset == 0 {
			t.Fatalf("series does not exist: i=%d", i)
		}
	}

	// Verify non-existent series doesn't exist.
	if sfile.HasSeries([]byte("foo"), models.NewTags(map[string]string{"region": "north"}), nil) {
		t.Fatal("series should not exist")
	}
}

// Series represents name/tagset pairs that are used in testing.
type Series struct {
	Name    []byte
	Tags    models.Tags
	Deleted bool
}

// SeriesFile is a test wrapper for tsi1.SeriesFile.
type SeriesFile struct {
	*tsi1.SeriesFile
}

// NewSeriesFile returns a new instance of SeriesFile with a temporary file path.
func NewSeriesFile() *SeriesFile {
	file, err := ioutil.TempFile("", "tsi1-series-file-")
	if err != nil {
		panic(err)
	}
	file.Close()

	return &SeriesFile{SeriesFile: tsi1.NewSeriesFile(file.Name())}
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
func (f *SeriesFile) Close() error {
	defer os.Remove(f.Path())
	return f.SeriesFile.Close()
}

package tsdb_test

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"testing"

	"github.com/influxdata/influxdb/logger"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/tsdb"
)

func TestParseSeriesKeyInto(t *testing.T) {
	name := []byte("cpu")
	tags := models.NewTags(map[string]string{"region": "east", "server": "a"})
	key := tsdb.AppendSeriesKey(nil, name, tags)

	dst := make(models.Tags, 0)
	gotName, gotTags := tsdb.ParseSeriesKeyInto(key, dst)

	if !bytes.Equal(gotName, name) {
		t.Fatalf("got %q, expected %q", gotName, name)
	}

	if got, exp := len(gotTags), 2; got != exp {
		t.Fatalf("got tags length %d, expected %d", got, exp)
	} else if got, exp := gotTags, tags; !got.Equal(exp) {
		t.Fatalf("got tags %v, expected %v", got, exp)
	}

	dst = make(models.Tags, 0, 5)
	_, gotTags = tsdb.ParseSeriesKeyInto(key, dst)
	if got, exp := len(gotTags), 2; got != exp {
		t.Fatalf("got tags length %d, expected %d", got, exp)
	} else if got, exp := cap(gotTags), 5; got != exp {
		t.Fatalf("got tags capacity %d, expected %d", got, exp)
	} else if got, exp := gotTags, tags; !got.Equal(exp) {
		t.Fatalf("got tags %v, expected %v", got, exp)
	}

	dst = make(models.Tags, 1)
	_, gotTags = tsdb.ParseSeriesKeyInto(key, dst)
	if got, exp := len(gotTags), 2; got != exp {
		t.Fatalf("got tags length %d, expected %d", got, exp)
	} else if got, exp := gotTags, tags; !got.Equal(exp) {
		t.Fatalf("got tags %v, expected %v", got, exp)
	}
}

// Ensure series file contains the correct set of series.
func TestSeriesFile_Series(t *testing.T) {
	sfile := MustOpenSeriesFile()
	defer sfile.Close()

	series := []Series{
		{Name: []byte("cpu"), Tags: models.NewTags(map[string]string{"region": "east"}), Type: models.Integer},
		{Name: []byte("cpu"), Tags: models.NewTags(map[string]string{"region": "west"}), Type: models.Integer},
		{Name: []byte("mem"), Tags: models.NewTags(map[string]string{"region": "east"}), Type: models.Integer},
	}
	for _, s := range series {
		collection := &tsdb.SeriesCollection{
			Names: [][]byte{[]byte(s.Name)},
			Tags:  []models.Tags{s.Tags},
			Types: []models.FieldType{s.Type},
		}
		if err := sfile.CreateSeriesListIfNotExists(collection); err != nil {
			t.Fatal(err)
		}
	}

	// Verify total number of series is correct.
	if n := sfile.SeriesCount(); n != 3 {
		t.Fatalf("unexpected series count: %d", n)
	}

	// Verify all series exist.
	for i, s := range series {
		if seriesID := sfile.SeriesID(s.Name, s.Tags, nil); seriesID.IsZero() {
			t.Fatalf("series does not exist: i=%d", i)
		}
	}

	// Verify non-existent series doesn't exist.
	if sfile.HasSeries([]byte("foo"), models.NewTags(map[string]string{"region": "north"}), nil) {
		t.Fatal("series should not exist")
	}
}

// Ensure series file can be compacted.
func TestSeriesFileCompactor(t *testing.T) {
	sfile := MustOpenSeriesFile()
	defer sfile.Close()

	// Disable automatic compactions.
	for _, p := range sfile.Partitions() {
		p.CompactThreshold = 0
	}

	collection := new(tsdb.SeriesCollection)
	for i := 0; i < 10000; i++ {
		collection.Names = append(collection.Names, []byte(fmt.Sprintf("m%d", i)))
		collection.Tags = append(collection.Tags, models.NewTags(map[string]string{"foo": "bar"}))
		collection.Types = append(collection.Types, models.Integer)
	}
	if err := sfile.CreateSeriesListIfNotExists(collection); err != nil {
		t.Fatal(err)
	}
	if err := collection.PartialWriteError(); err != nil {
		t.Fatal(err)
	}

	// Verify total number of series is correct.
	if n := sfile.SeriesCount(); n != uint64(len(collection.Names)) {
		t.Fatalf("unexpected series count: %d", n)
	}

	// Compact in-place for each partition.
	for _, p := range sfile.Partitions() {
		compactor := tsdb.NewSeriesPartitionCompactor()
		if _, err := compactor.Compact(p); err != nil {
			t.Fatal(err)
		}
	}

	// Verify all series exist.
	for iter := collection.Iterator(); iter.Next(); {
		if seriesID := sfile.SeriesID(iter.Name(), iter.Tags(), nil); seriesID.IsZero() {
			t.Fatalf("series does not exist: %s,%s", iter.Name(), iter.Tags().String())
		}
	}
}

// Ensures that types are tracked and checked by the series file.
func TestSeriesFile_Type(t *testing.T) {
	sfile := MustOpenSeriesFile()
	defer sfile.Close()

	// Add the series with some types.
	collection := &tsdb.SeriesCollection{
		Names: [][]byte{[]byte("a"), []byte("b"), []byte("c")},
		Tags:  []models.Tags{{}, {}, {}},
		Types: []models.FieldType{models.Integer, models.Float, models.Boolean},
	}
	if err := sfile.CreateSeriesListIfNotExists(collection); err != nil {
		t.Fatal(err)
	}

	// Attempt to add the series again but with different types.
	collection = &tsdb.SeriesCollection{
		Names: [][]byte{[]byte("a"), []byte("b"), []byte("c"), []byte("d")},
		Tags:  []models.Tags{{}, {}, {}, {}},
		Types: []models.FieldType{models.String, models.String, models.String, models.String},
	}
	if err := sfile.CreateSeriesListIfNotExists(collection); err != nil {
		t.Fatal(err)
	}

	// All of the series except d should be dropped.
	if err := collection.PartialWriteError(); err == nil {
		t.Fatal("expected partial write error")
	}
	if collection.Length() != 1 {
		t.Fatal("expected one series to remain in collection")
	}
	if got := string(collection.Names[0]); got != "d" {
		t.Fatal("got invalid name on remaining series:", got)
	}
}

// Ensure series file deletions persist across compactions.
func TestSeriesFile_DeleteSeriesID(t *testing.T) {
	sfile := MustOpenSeriesFile()
	defer sfile.Close()

	if err := sfile.CreateSeriesListIfNotExists(&tsdb.SeriesCollection{
		Names: [][]byte{[]byte("m1")},
		Tags:  []models.Tags{{}},
		Types: []models.FieldType{models.String},
	}); err != nil {
		t.Fatal(err)
	} else if err := sfile.CreateSeriesListIfNotExists(&tsdb.SeriesCollection{
		Names: [][]byte{[]byte("m2")},
		Tags:  []models.Tags{{}},
		Types: []models.FieldType{models.String},
	}); err != nil {
		t.Fatal(err)
	} else if err := sfile.ForceCompact(); err != nil {
		t.Fatal(err)
	}
	id := sfile.SeriesID([]byte("m1"), nil, nil)

	// Delete and ensure deletion.
	if err := sfile.DeleteSeriesID(id); err != nil {
		t.Fatal(err)
	} else if err := sfile.CreateSeriesListIfNotExists(&tsdb.SeriesCollection{
		Names: [][]byte{[]byte("m1")},
		Tags:  []models.Tags{{}},
		Types: []models.FieldType{models.String},
	}); err != nil {
		t.Fatal(err)
	} else if !sfile.IsDeleted(id) {
		t.Fatal("expected deletion before compaction")
	}

	if err := sfile.ForceCompact(); err != nil {
		t.Fatal(err)
	} else if !sfile.IsDeleted(id) {
		t.Fatal("expected deletion after compaction")
	}

	if err := sfile.Reopen(); err != nil {
		t.Fatal(err)
	} else if !sfile.IsDeleted(id) {
		t.Fatal("expected deletion after reopen")
	}
}

// Series represents name/tagset pairs that are used in testing.
type Series struct {
	Name    []byte
	Tags    models.Tags
	Type    models.FieldType
	Deleted bool
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
	f.Logger = logger.New(os.Stdout)
	if err := f.Open(); err != nil {
		panic(err)
	}
	return f
}

// Close closes the log file and removes it from disk.
func (f *SeriesFile) Close() error {
	defer os.RemoveAll(f.Path())
	return f.SeriesFile.Close()
}

// Reopen close & reopens the series file.
func (f *SeriesFile) Reopen() error {
	if err := f.SeriesFile.Close(); err != nil {
		return err
	}
	f.SeriesFile = tsdb.NewSeriesFile(f.SeriesFile.Path())
	return f.SeriesFile.Open()
}

// ForceCompact executes an immediate compaction across all partitions.
func (f *SeriesFile) ForceCompact() error {
	for _, p := range f.Partitions() {
		if _, err := tsdb.NewSeriesPartitionCompactor().Compact(p); err != nil {
			return err
		}
	}
	return nil
}

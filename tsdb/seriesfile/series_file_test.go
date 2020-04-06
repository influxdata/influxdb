package seriesfile_test

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"testing"

	"github.com/influxdata/influxdb/v2/logger"
	"github.com/influxdata/influxdb/v2/models"
	"github.com/influxdata/influxdb/v2/tsdb"
	"github.com/influxdata/influxdb/v2/tsdb/seriesfile"
	"golang.org/x/sync/errgroup"
)

func TestParseSeriesKeyInto(t *testing.T) {
	name := []byte("cpu")
	tags := models.NewTags(map[string]string{"region": "east", "server": "a"})
	key := seriesfile.AppendSeriesKey(nil, name, tags)

	dst := make(models.Tags, 0)
	gotName, gotTags := seriesfile.ParseSeriesKeyInto(key, dst)

	if !bytes.Equal(gotName, name) {
		t.Fatalf("got %q, expected %q", gotName, name)
	}

	if got, exp := len(gotTags), 2; got != exp {
		t.Fatalf("got tags length %d, expected %d", got, exp)
	} else if got, exp := gotTags, tags; !got.Equal(exp) {
		t.Fatalf("got tags %v, expected %v", got, exp)
	}

	dst = make(models.Tags, 0, 5)
	_, gotTags = seriesfile.ParseSeriesKeyInto(key, dst)
	if got, exp := len(gotTags), 2; got != exp {
		t.Fatalf("got tags length %d, expected %d", got, exp)
	} else if got, exp := cap(gotTags), 5; got != exp {
		t.Fatalf("got tags capacity %d, expected %d", got, exp)
	} else if got, exp := gotTags, tags; !got.Equal(exp) {
		t.Fatalf("got tags %v, expected %v", got, exp)
	}

	dst = make(models.Tags, 1)
	_, gotTags = seriesfile.ParseSeriesKeyInto(key, dst)
	if got, exp := len(gotTags), 2; got != exp {
		t.Fatalf("got tags length %d, expected %d", got, exp)
	} else if got, exp := gotTags, tags; !got.Equal(exp) {
		t.Fatalf("got tags %v, expected %v", got, exp)
	}
}

// Ensure that broken series files are closed
func TestSeriesFile_Open_WhenFileCorrupt_ShouldReturnErr(t *testing.T) {
	f := NewBrokenSeriesFile([]byte{0, 0, 0, 0, 0})
	defer f.Close()
	f.Logger = logger.New(os.Stdout)

	err := f.Open(context.Background())
	if err == nil {
		t.Fatalf("should report error")
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
		compactor := seriesfile.NewSeriesPartitionCompactor()
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

	// Verify total number of series is correct.
	if got, exp := sfile.SeriesCount(), uint64(len(collection.Names)); got != exp {
		t.Fatalf("SeriesCount()=%d, expected %d (after compaction)", got, exp)
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

	// Verify total number of series is correct.
	if got, exp := sfile.SeriesCount(), uint64(2); got != exp {
		t.Fatalf("SeriesCount()=%d, expected %d (before deleted)", got, exp)
	}

	// Delete and ensure deletion.
	if err := sfile.DeleteSeriesIDs([]tsdb.SeriesID{id}); err != nil {
		t.Fatal(err)
	} else if !sfile.IsDeleted(id) {
		t.Fatal("expected deletion before compaction")
	}

	// Verify total number of series is correct.
	if got, exp := sfile.SeriesCount(), uint64(1); got != exp {
		t.Fatalf("SeriesCount()=%d, expected %d (before compaction)", got, exp)
	}

	if err := sfile.ForceCompact(); err != nil {
		t.Fatal(err)
	} else if !sfile.IsDeleted(id) {
		t.Fatal("expected deletion after compaction")
	} else if got, exp := sfile.SeriesCount(), uint64(1); got != exp {
		t.Fatalf("SeriesCount()=%d, expected %d (after compaction)", got, exp)
	}

	if err := sfile.Reopen(); err != nil {
		t.Fatal(err)
	} else if !sfile.IsDeleted(id) {
		t.Fatal("expected deletion after reopen")
	} else if got, exp := sfile.SeriesCount(), uint64(1); got != exp {
		t.Fatalf("SeriesCount()=%d, expected %d (after reopen)", got, exp)
	}

	// Recreate series with new ID.
	if err := sfile.CreateSeriesListIfNotExists(&tsdb.SeriesCollection{
		Names: [][]byte{[]byte("m1")},
		Tags:  []models.Tags{{}},
		Types: []models.FieldType{models.String},
	}); err != nil {
		t.Fatal(err)
	} else if got, exp := sfile.SeriesCount(), uint64(2); got != exp {
		t.Fatalf("SeriesCount()=%d, expected %d (after recreate)", got, exp)
	}

	if err := sfile.ForceCompact(); err != nil {
		t.Fatal(err)
	} else if !sfile.IsDeleted(id) {
		t.Fatal("expected deletion after compaction")
	} else if got, exp := sfile.SeriesCount(), uint64(2); got != exp {
		t.Fatalf("SeriesCount()=%d, expected %d (after recreate & compaction)", got, exp)
	}

	if err := sfile.Reopen(); err != nil {
		t.Fatal(err)
	} else if !sfile.IsDeleted(id) {
		t.Fatal("expected deletion after reopen")
	} else if got, exp := sfile.SeriesCount(), uint64(2); got != exp {
		t.Fatalf("SeriesCount()=%d, expected %d (after recreate & compaction)", got, exp)
	}
}

func TestSeriesFile_Compaction(t *testing.T) {
	const n = 1000

	sfile := MustOpenSeriesFile()
	defer sfile.Close()

	// Generate a bunch of keys.
	var collection tsdb.SeriesCollection
	for i := 0; i < n; i++ {
		collection.Names = append(collection.Names, []byte("cpu"))
		collection.Tags = append(collection.Tags, models.NewTags(map[string]string{"region": fmt.Sprintf("r%d", i)}))
		collection.Types = append(collection.Types, models.Integer)
	}

	// Add all to the series file.
	err := sfile.CreateSeriesListIfNotExists(&collection)
	if err != nil {
		t.Fatal(err)
	}

	// Delete a subset of keys.
	for i := 0; i < n; i++ {
		if i%10 != 0 {
			continue
		}

		if id := sfile.SeriesID(collection.Names[i], collection.Tags[i], nil); id.IsZero() {
			t.Fatal("expected series id")
		} else if err := sfile.DeleteSeriesIDs([]tsdb.SeriesID{id}); err != nil {
			t.Fatal(err)
		}
	}

	// Compute total size of all series data.
	origSize, err := sfile.FileSize()
	if err != nil {
		t.Fatal(err)
	}

	// Compact all segments.
	var paths []string
	for _, p := range sfile.Partitions() {
		for _, ss := range p.Segments() {
			if err := ss.CompactToPath(ss.Path()+".tmp", p.Index()); err != nil {
				t.Fatal(err)
			}
			paths = append(paths, ss.Path())
		}
	}

	// Close index.
	if err := sfile.SeriesFile.Close(); err != nil {
		t.Fatal(err)
	}

	// Overwrite files.
	for _, path := range paths {
		if err := os.Rename(path+".tmp", path); err != nil {
			t.Fatal(err)
		}
	}

	// Reopen index.
	sfile.SeriesFile = seriesfile.NewSeriesFile(sfile.SeriesFile.Path())
	if err := sfile.SeriesFile.Open(context.Background()); err != nil {
		t.Fatal(err)
	}

	// Ensure series status is correct.
	for i := 0; i < n; i++ {
		if id := sfile.SeriesID(collection.Names[i], collection.Tags[i], nil); id.IsZero() {
			continue
		} else if got, want := sfile.IsDeleted(id), (i%10) == 0; got != want {
			t.Fatalf("IsDeleted(%d)=%v, want %v", id, got, want)
		}
	}

	// Verify new size is smaller.
	newSize, err := sfile.FileSize()
	if err != nil {
		t.Fatal(err)
	} else if newSize >= origSize {
		t.Fatalf("expected new size (%d) to be smaller than original size (%d)", newSize, origSize)
	}

	t.Logf("original size: %d, new size: %d", origSize, newSize)
}

var cachedCompactionSeriesFile *SeriesFile

func BenchmarkSeriesFile_Compaction(b *testing.B) {
	const n = 1000000

	if cachedCompactionSeriesFile == nil {
		sfile := MustOpenSeriesFile()

		// Generate a bunch of keys.
		ids := make([]tsdb.SeriesID, n)
		for i := 0; i < n; i++ {
			collection := &tsdb.SeriesCollection{
				Names: [][]byte{[]byte("cpu")},
				Tags:  []models.Tags{models.NewTags(map[string]string{"region": fmt.Sprintf("r%d", i)})},
				Types: []models.FieldType{models.Integer},
			}

			if err := sfile.CreateSeriesListIfNotExists(collection); err != nil {
				b.Fatal(err)
			} else if ids[i] = sfile.SeriesID(collection.Names[0], collection.Tags[0], nil); ids[i].IsZero() {
				b.Fatalf("expected series id: i=%d", i)
			}
		}

		// Delete a subset of keys.
		for i := 0; i < len(ids); i += 10 {
			if err := sfile.DeleteSeriesIDs([]tsdb.SeriesID{ids[i]}); err != nil {
				b.Fatal(err)
			}
		}

		cachedCompactionSeriesFile = sfile
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Compact all segments in parallel.
		var g errgroup.Group
		for _, p := range cachedCompactionSeriesFile.Partitions() {
			for _, segment := range p.Segments() {
				p, segment := p, segment
				g.Go(func() error {
					return segment.CompactToPath(segment.Path()+".tmp", p.Index())
				})
			}
		}

		if err := g.Wait(); err != nil {
			b.Fatal(err)
		}
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
	*seriesfile.SeriesFile
}

// NewSeriesFile returns a new instance of SeriesFile with a temporary file path.
func NewSeriesFile() *SeriesFile {
	dir, err := ioutil.TempDir("", "tsdb-series-file-")
	if err != nil {
		panic(err)
	}
	return &SeriesFile{SeriesFile: seriesfile.NewSeriesFile(dir)}
}

func NewBrokenSeriesFile(content []byte) *SeriesFile {
	sFile := NewSeriesFile()
	fPath := sFile.Path()
	if err := sFile.Open(context.Background()); err != nil {
		panic(err)
	}
	if err := sFile.SeriesFile.Close(); err != nil {
		panic(err)
	}

	segPath := path.Join(fPath, "00", "0000")
	if _, err := os.Stat(segPath); os.IsNotExist(err) {
		panic(err)
	}
	err := ioutil.WriteFile(segPath, content, 0777)
	if err != nil {
		panic(err)
	}
	return sFile
}

// MustOpenSeriesFile returns a new, open instance of SeriesFile. Panic on error.
func MustOpenSeriesFile() *SeriesFile {
	f := NewSeriesFile()
	f.Logger = logger.New(os.Stdout)
	if err := f.Open(context.Background()); err != nil {
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
	f.SeriesFile = seriesfile.NewSeriesFile(f.SeriesFile.Path())
	return f.SeriesFile.Open(context.Background())
}

// ForceCompact executes an immediate compaction across all partitions.
func (f *SeriesFile) ForceCompact() error {
	for _, p := range f.Partitions() {
		if _, err := seriesfile.NewSeriesPartitionCompactor().Compact(p); err != nil {
			return err
		}
	}
	return nil
}

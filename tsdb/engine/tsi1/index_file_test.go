package tsi1_test

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/tsdb/engine/tsi1"
)

// Ensure a simple index file can be built and opened.
func TestCreateIndexFile(t *testing.T) {
	if _, err := CreateIndexFile([]Series{
		{Name: []byte("cpu"), Tags: models.NewTags(map[string]string{"region": "east"})},
		{Name: []byte("cpu"), Tags: models.NewTags(map[string]string{"region": "west"})},
		{Name: []byte("mem"), Tags: models.NewTags(map[string]string{"region": "east"})},
	}); err != nil {
		t.Fatal(err)
	}
}

// Ensure index file generation can be successfully built.
func TestGenerateIndexFile(t *testing.T) {
	// Build generated index file.
	idx, err := GenerateIndexFile(10, 3, 4)
	if err != nil {
		t.Fatal(err)
	}

	// Verify that tag/value series can be fetched.
	if e, err := idx.TagValueElem([]byte("measurement0"), []byte("key0"), []byte("value0")); err != nil {
		t.Fatal(err)
	} else if e.Series.N == 0 {
		t.Fatal("expected series")
	}
}

func BenchmarkIndexFile_TagValueSeries(b *testing.B) {
	b.Run("M=1,K=2,V=3", func(b *testing.B) {
		benchmarkIndexFile_TagValueSeries(b, MustFindOrGenerateIndexFile(1, 2, 3))
	})
	b.Run("M=10,K=5,V=5", func(b *testing.B) {
		benchmarkIndexFile_TagValueSeries(b, MustFindOrGenerateIndexFile(10, 5, 5))
	})
	b.Run("M=10,K=7,V=5", func(b *testing.B) {
		benchmarkIndexFile_TagValueSeries(b, MustFindOrGenerateIndexFile(10, 7, 7))
	})
}

func benchmarkIndexFile_TagValueSeries(b *testing.B, idx *tsi1.IndexFile) {
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		if e, err := idx.TagValueElem([]byte("measurement0"), []byte("key0"), []byte("value0")); err != nil {
			b.Fatal(err)
		} else if e.Series.N == 0 {
			b.Fatal("expected series")
		}
	}
}

// CreateIndexFile creates an index file with a given set of series.
func CreateIndexFile(series []Series) (*tsi1.IndexFile, error) {
	// Add series to the writer.
	ifw := tsi1.NewIndexFileWriter()
	for _, serie := range series {
		ifw.Add(serie.Name, serie.Tags)
	}

	// Write index file to buffer.
	var buf bytes.Buffer
	if _, err := ifw.WriteTo(&buf); err != nil {
		return nil, err
	}

	// Load index file from buffer.
	var f tsi1.IndexFile
	if err := f.UnmarshalBinary(buf.Bytes()); err != nil {
		return nil, err
	}
	return &f, nil
}

// GenerateIndexFile generates an index file from a set of series based on the count arguments.
// Total series returned will equal measurementN * tagN * valueN.
func GenerateIndexFile(measurementN, tagN, valueN int) (*tsi1.IndexFile, error) {
	tagValueN := pow(valueN, tagN)

	iw := tsi1.NewIndexFileWriter()
	for i := 0; i < measurementN; i++ {
		name := []byte(fmt.Sprintf("measurement%d", i))

		// Generate tag sets.
		for j := 0; j < tagValueN; j++ {
			var tags models.Tags
			for k := 0; k < tagN; k++ {
				key := []byte(fmt.Sprintf("key%d", k))
				value := []byte(fmt.Sprintf("value%d", (j / pow(valueN, k) % valueN)))
				tags = append(tags, models.Tag{Key: key, Value: value})
			}
			iw.Add(name, tags)
		}
	}

	// Write index file to buffer.
	var buf bytes.Buffer
	if _, err := iw.WriteTo(&buf); err != nil {
		return nil, err
	}

	// Load index file from buffer.
	var idx tsi1.IndexFile
	if err := idx.UnmarshalBinary(buf.Bytes()); err != nil {
		return nil, err
	}
	return &idx, nil
}

func MustGenerateIndexFile(measurementN, tagN, valueN int) *tsi1.IndexFile {
	idx, err := GenerateIndexFile(measurementN, tagN, valueN)
	if err != nil {
		panic(err)
	}
	return idx
}

var indexFileCache struct {
	MeasurementN int
	TagN         int
	ValueN       int

	IndexFile *tsi1.IndexFile
}

// MustFindOrGenerateIndexFile returns a cached index file or generates one if it doesn't exist.
func MustFindOrGenerateIndexFile(measurementN, tagN, valueN int) *tsi1.IndexFile {
	// Use cache if fields match and the index file has been generated.
	if indexFileCache.MeasurementN == measurementN &&
		indexFileCache.TagN == tagN &&
		indexFileCache.ValueN == valueN &&
		indexFileCache.IndexFile != nil {
		return indexFileCache.IndexFile
	}

	// Generate and cache.
	indexFileCache.MeasurementN = measurementN
	indexFileCache.TagN = tagN
	indexFileCache.ValueN = valueN
	indexFileCache.IndexFile = MustGenerateIndexFile(measurementN, tagN, valueN)
	return indexFileCache.IndexFile
}

func pow(x, y int) int {
	r := 1
	for i := 0; i < y; i++ {
		r *= x
	}
	return r
}

package tsi1_test

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/tsdb/engine/tsi1"
)

// Ensure a simple index can be built and opened.
func TestIndex(t *testing.T) {
	series := []Series{
		{Name: "cpu", Tags: models.NewTags(map[string]string{"region": "east"})},
		{Name: "cpu", Tags: models.NewTags(map[string]string{"region": "west"})},
		{Name: "mem", Tags: models.NewTags(map[string]string{"region": "east"})},
	}

	// Add series to the writer.
	iw := tsi1.NewIndexWriter()
	for _, serie := range series {
		iw.Add(serie.Name, serie.Tags)
	}

	// Write index to buffer.
	var buf bytes.Buffer
	if _, err := iw.WriteTo(&buf); err != nil {
		t.Fatal(err)
	}

	// Load index from buffer.
	var idx tsi1.Index
	if err := idx.UnmarshalBinary(buf.Bytes()); err != nil {
		t.Fatal(err)
	}
}

// Ensure index generation can be successfully built.
func TestGenerateIndex(t *testing.T) {
	// Build generated index.
	idx, err := GenerateIndex(10, 3, 4)
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

func BenchmarkIndex_TagValueSeries(b *testing.B) {
	b.Run("M=1,K=2,V=3", func(b *testing.B) {
		benchmarkIndex_TagValueSeries(b, MustFindOrGenerateIndex(1, 2, 3))
	})
	b.Run("M=10,K=5,V=5", func(b *testing.B) {
		benchmarkIndex_TagValueSeries(b, MustFindOrGenerateIndex(10, 5, 5))
	})
	b.Run("M=10,K=7,V=5", func(b *testing.B) {
		benchmarkIndex_TagValueSeries(b, MustFindOrGenerateIndex(10, 7, 7))
	})
}

func benchmarkIndex_TagValueSeries(b *testing.B, idx *tsi1.Index) {
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

// GenerateIndex Generates an index from a set of series based on the count arguments.
// Total series returned will equal measurementN * tagN * valueN.
func GenerateIndex(measurementN, tagN, valueN int) (*tsi1.Index, error) {
	tagValueN := pow(valueN, tagN)

	println("generating", measurementN*pow(valueN, tagN))

	iw := tsi1.NewIndexWriter()
	for i := 0; i < measurementN; i++ {
		name := fmt.Sprintf("measurement%d", i)

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

	// Write index to buffer.
	var buf bytes.Buffer
	if _, err := iw.WriteTo(&buf); err != nil {
		return nil, err
	}
	println("file size", buf.Len())

	// Load index from buffer.
	var idx tsi1.Index
	if err := idx.UnmarshalBinary(buf.Bytes()); err != nil {
		return nil, err
	}
	return &idx, nil
}

func MustGenerateIndex(measurementN, tagN, valueN int) *tsi1.Index {
	idx, err := GenerateIndex(measurementN, tagN, valueN)
	if err != nil {
		panic(err)
	}
	return idx
}

var indexCache struct {
	MeasurementN int
	TagN         int
	ValueN       int

	Index *tsi1.Index
}

// MustFindOrGenerateIndex returns a cached index or generates one if it doesn't exist.
func MustFindOrGenerateIndex(measurementN, tagN, valueN int) *tsi1.Index {
	// Use cache if fields match and the index has been generated.
	if indexCache.MeasurementN == measurementN &&
		indexCache.TagN == tagN &&
		indexCache.ValueN == valueN &&
		indexCache.Index != nil {
		return indexCache.Index
	}

	// Generate and cache.
	indexCache.MeasurementN = measurementN
	indexCache.TagN = tagN
	indexCache.ValueN = valueN
	indexCache.Index = MustGenerateIndex(measurementN, tagN, valueN)
	return indexCache.Index
}

func pow(x, y int) int {
	r := 1
	for i := 0; i < y; i++ {
		r *= x
	}
	return r
}

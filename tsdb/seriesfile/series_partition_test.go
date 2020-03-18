package seriesfile_test

import (
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
	"testing"

	"github.com/influxdata/influxdb/logger"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/tsdb"
	"github.com/influxdata/influxdb/tsdb/seriesfile"
)

func BenchmarkSeriesPartition_CreateSeriesListIfNotExists(b *testing.B) {
	for _, n := range []int{1000, 10000, 100000, 1000000} {
		b.Run(strconv.Itoa(n), func(b *testing.B) {
			var collection tsdb.SeriesCollection
			for i := 0; i < n; i++ {
				collection.Names = append(collection.Names, []byte("cpu"))
				collection.Tags = append(collection.Tags, models.Tags{
					{Key: []byte("tag0"), Value: []byte("value0")},
					{Key: []byte("tag1"), Value: []byte("value1")},
					{Key: []byte("tag2"), Value: []byte("value2")},
					{Key: []byte("tag3"), Value: []byte("value3")},
					{Key: []byte("tag4"), Value: []byte(fmt.Sprintf("value%d", i))},
				})
				collection.Types = append(collection.Types, models.Integer)
			}
			collection.SeriesKeys = seriesfile.GenerateSeriesKeys(collection.Names, collection.Tags)
			collection.SeriesIDs = make([]tsdb.SeriesID, len(collection.SeriesKeys))
			keyPartitionIDs := make([]int, n)

			b.ResetTimer()
			for j := 0; j < b.N; j++ {
				p := MustOpenSeriesPartition()
				if err := p.CreateSeriesListIfNotExists(&collection, keyPartitionIDs); err != nil {
					b.Fatal(err)
				} else if err := p.Close(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// SeriesPartition is a test wrapper for tsdb.SeriesPartition.
type SeriesPartition struct {
	*seriesfile.SeriesPartition
}

// NewSeriesPartition returns a new instance of SeriesPartition with a temporary file path.
func NewSeriesPartition() *SeriesPartition {
	dir, err := ioutil.TempDir("", "tsdb-series-partition-")
	if err != nil {
		panic(err)
	}
	return &SeriesPartition{SeriesPartition: seriesfile.NewSeriesPartition(0, dir)}
}

// MustOpenSeriesPartition returns a new, open instance of SeriesPartition. Panic on error.
func MustOpenSeriesPartition() *SeriesPartition {
	f := NewSeriesPartition()
	f.Logger = logger.New(os.Stdout)
	if err := f.Open(); err != nil {
		panic(err)
	}
	return f
}

// Close closes the partition and removes it from disk.
func (f *SeriesPartition) Close() error {
	defer os.RemoveAll(f.Path())
	return f.SeriesPartition.Close()
}

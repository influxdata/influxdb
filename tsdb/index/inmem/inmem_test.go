package inmem_test

import (
	"fmt"
	"testing"

	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/tsdb"
	"github.com/influxdata/influxdb/tsdb/index/inmem"
)

func createData(lo, hi int) (keys, names [][]byte, tags []models.Tags) {
	for i := lo; i < hi; i++ {
		keys = append(keys, []byte(fmt.Sprintf("m0,tag0=t%d", i)))
		names = append(names, []byte("m0"))
		var t models.Tags
		t.Set([]byte("tag0"), []byte(fmt.Sprintf("%d", i)))
		tags = append(tags, t)
	}
	return
}

func BenchmarkShardIndex_CreateSeriesListIfNotExists_MaxValuesExceeded(b *testing.B) {
	opt := tsdb.EngineOptions{InmemIndex: inmem.NewIndex("foo", nil)}
	opt.Config.MaxValuesPerTag = 10
	si := inmem.NewShardIndex(1, "foo", "bar", tsdb.NewSeriesIDSet(), nil, opt)
	si.Open()
	keys, names, tags := createData(0, 10)
	si.CreateSeriesListIfNotExists(keys, names, tags)
	b.ReportAllocs()
	b.ResetTimer()

	keys, names, tags = createData(9, 5010)
	for i := 0; i < b.N; i++ {
		si.CreateSeriesListIfNotExists(keys, names, tags)
	}
}

func BenchmarkShardIndex_CreateSeriesListIfNotExists_MaxValuesNotExceeded(b *testing.B) {
	opt := tsdb.EngineOptions{InmemIndex: inmem.NewIndex("foo", nil)}
	opt.Config.MaxValuesPerTag = 100000
	si := inmem.NewShardIndex(1, "foo", "bar", tsdb.NewSeriesIDSet(), nil, opt)
	si.Open()
	keys, names, tags := createData(0, 10)
	si.CreateSeriesListIfNotExists(keys, names, tags)
	b.ReportAllocs()
	b.ResetTimer()

	keys, names, tags = createData(9, 5010)
	for i := 0; i < b.N; i++ {
		si.CreateSeriesListIfNotExists(keys, names, tags)
	}
}

func BenchmarkShardIndex_CreateSeriesListIfNotExists_NoMaxValues(b *testing.B) {
	opt := tsdb.EngineOptions{InmemIndex: inmem.NewIndex("foo", nil)}
	si := inmem.NewShardIndex(1, "foo", "bar", tsdb.NewSeriesIDSet(), nil, opt)
	si.Open()
	keys, names, tags := createData(0, 10)
	si.CreateSeriesListIfNotExists(keys, names, tags)
	b.ReportAllocs()
	b.ResetTimer()

	keys, names, tags = createData(9, 5010)
	for i := 0; i < b.N; i++ {
		si.CreateSeriesListIfNotExists(keys, names, tags)
	}
}

func BenchmarkShardIndex_CreateSeriesListIfNotExists_MaxSeriesExceeded(b *testing.B) {
	opt := tsdb.EngineOptions{InmemIndex: inmem.NewIndex("foo", nil)}
	opt.Config.MaxValuesPerTag = 0
	opt.Config.MaxSeriesPerDatabase = 10
	si := inmem.NewShardIndex(1, "foo", "bar", tsdb.NewSeriesIDSet(), nil, opt)
	si.Open()
	keys, names, tags := createData(0, 10)
	si.CreateSeriesListIfNotExists(keys, names, tags)
	b.ReportAllocs()
	b.ResetTimer()

	keys, names, tags = createData(9, 5010)
	for i := 0; i < b.N; i++ {
		si.CreateSeriesListIfNotExists(keys, names, tags)
	}
}

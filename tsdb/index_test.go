package tsdb_test

import (
	_ "github.com/influxdata/influxdb/tsdb/index"
	"github.com/influxdata/influxdb/tsdb/index/inmem"
)

func MustNewInmemIndex(name string) *inmem.Index {
	idx, err := inmem.NewIndex(name)
	if err != nil {
		panic(err)
	}
	return idx
}

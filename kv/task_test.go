package kv

import (
	"github.com/influxdata/influxdb/kv"
)

func TestInMemStore(t *testing.T) {
	storetest.NewStoreTest(
		"kv-store store",
		func(t *testing.T) backend.Store {
			return kv.Service{}
		},
		func(t *testing.T, s backend.Store) {},
	)(t)
}

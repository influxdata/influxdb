package backend_test

import (
	"testing"

	"github.com/influxdata/platform/task/backend"
	"github.com/influxdata/platform/task/backend/storetest"
)

func TestInMemStore(t *testing.T) {
	storetest.NewStoreTest(
		"in-mem store",
		func(t *testing.T) backend.Store {
			return backend.NewInMemStore()
		},
		func(t *testing.T, s backend.Store) {},
	)(t)
}

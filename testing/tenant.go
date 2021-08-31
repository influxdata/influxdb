package testing

import (
	"context"
	"io/ioutil"
	"os"
	"testing"

	"github.com/influxdata/influxdb/v2/bolt"
	"github.com/influxdata/influxdb/v2/inmem"
	"github.com/influxdata/influxdb/v2/kv"
	"github.com/influxdata/influxdb/v2/kv/migration/all"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
)

func NewTestBoltStore(t *testing.T) (kv.SchemaStore, func()) {
	f, err := ioutil.TempFile("", "influxdata-bolt-")
	require.NoError(t, err, "unable to create temporary boltdb file")
	require.NoError(t, f.Close())

	path := f.Name()
	s := bolt.NewKVStore(zaptest.NewLogger(t), path, bolt.WithNoSync)
	require.NoError(t, s.Open(context.Background()))

	// apply all kv migrations
	require.NoError(t, all.Up(context.Background(), zaptest.NewLogger(t), s))

	close := func() {
		s.Close()
		os.Remove(path)
	}

	return s, close
}

func NewTestInmemStore(t *testing.T) kv.SchemaStore {
	s := inmem.NewKVStore()
	// apply all kv migrations
	require.NoError(t, all.Up(context.Background(), zaptest.NewLogger(t), s))
	return s
}

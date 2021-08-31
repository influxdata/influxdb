package testing

import (
	"context"
	"errors"
	"io/ioutil"
	"os"
	"testing"

	"github.com/influxdata/influxdb/v2/bolt"
	"github.com/influxdata/influxdb/v2/inmem"
	"github.com/influxdata/influxdb/v2/kv"
	"github.com/influxdata/influxdb/v2/kv/migration/all"
	"go.uber.org/zap/zaptest"
)

func NewTestBoltStore(t *testing.T) (kv.SchemaStore, func(), error) {
	f, err := ioutil.TempFile("", "influxdata-bolt-")
	if err != nil {
		return nil, nil, errors.New("unable to open temporary boltdb file")
	}
	f.Close()

	path := f.Name()
	s := bolt.NewKVStore(zaptest.NewLogger(t), path, bolt.WithNoSync)
	if err := s.Open(context.Background()); err != nil {
		return nil, nil, err
	}

	// apply all kv migrations
	ctx := context.Background()
	if err := all.Up(ctx, zaptest.NewLogger(t), s); err != nil {
		return nil, nil, err
	}

	close := func() {
		s.Close()
		os.Remove(path)
	}

	return s, close, nil
}

func NewTestInmemStore(t *testing.T) (kv.SchemaStore, func(), error) {
	s := inmem.NewKVStore()
	// apply all kv migrations
	ctx := context.Background()
	if err := all.Up(ctx, zaptest.NewLogger(t), s); err != nil {
		return nil, nil, err
	}

	return s, func() {}, nil
}

package kv_test

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

	ctx := context.Background()
	logger := zaptest.NewLogger(t)
	path := f.Name()

	// skip fsync to improve test performance
	s := bolt.NewKVStore(logger, path, bolt.WithNoSync)
	if err := s.Open(context.Background()); err != nil {
		return nil, nil, err
	}

	if err := all.Up(ctx, logger, s); err != nil {
		return nil, nil, err
	}

	close := func() {
		s.Close()
		os.Remove(path)
	}

	return s, close, nil
}

func NewTestInmemStore(t *testing.T) (kv.SchemaStore, func(), error) {
	store := inmem.NewKVStore()
	if err := all.Up(context.Background(), zaptest.NewLogger(t), store); err != nil {
		return nil, nil, err
	}
	return store, func() {}, nil
}

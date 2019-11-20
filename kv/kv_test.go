package kv_test

import (
	"context"
	"errors"
	"io/ioutil"
	"os"

	"github.com/influxdata/influxdb/bolt"
	"github.com/influxdata/influxdb/inmem"
	"github.com/influxdata/influxdb/kv"
	"go.uber.org/zap"
)

func NewTestBoltStore() (kv.Store, func(), error) {
	f, err := ioutil.TempFile("", "influxdata-bolt-")
	if err != nil {
		return nil, nil, errors.New("unable to open temporary boltdb file")
	}
	f.Close()

	path := f.Name()
	s := bolt.NewKVStore(zap.NewNop(), path)
	if err := s.Open(context.Background()); err != nil {
		return nil, nil, err
	}

	close := func() {
		s.Close()
		os.Remove(path)
	}

	return s, close, nil
}

func NewTestInmemStore() (kv.Store, func(), error) {
	return inmem.NewKVStore(), func() {}, nil
}

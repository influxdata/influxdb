package kv_test

import (
	"context"
	"errors"
	"io/ioutil"
	"os"

	"github.com/influxdata/influxdb/bolt"
	"github.com/influxdata/influxdb/inmem"
)

func NewTestBoltStore() (*bolt.KVStore, func(), error) {
	f, err := ioutil.TempFile("", "influxdata-bolt-")
	if err != nil {
		return nil, nil, errors.New("unable to open temporary boltdb file")
	}
	f.Close()

	path := f.Name()
	s := bolt.NewKVStore(path)
	if err := s.Open(context.Background()); err != nil {
		return nil, nil, err
	}

	close := func() {
		s.Close()
		os.Remove(path)
	}

	return s, close, nil
}

func NewTestInmemStore() (*inmem.KVStore, func(), error) {
	return inmem.NewKVStore(), func() {}, nil
}

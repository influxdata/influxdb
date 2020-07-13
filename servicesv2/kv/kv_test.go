package kv_test

import (
	"context"
	"errors"
	"io/ioutil"
	"os"
	"testing"

	"github.com/influxdata/influxdb/bolt"
	"github.com/influxdata/influxdb/servicesv2/kv"
	"go.uber.org/zap/zaptest"
)

func NewTestBoltStore(t *testing.T) (kv.SchemaStore, func(), error) {
	f, err := ioutil.TempFile("", "influxdata-bolt-")
	if err != nil {
		return nil, nil, errors.New("unable to open temporary boltdb file")
	}
	f.Close()

	logger := zaptest.NewLogger(t)
	path := f.Name()
	s := bolt.NewKVStore(logger, path)
	if err := s.Open(context.Background()); err != nil {
		return nil, nil, err
	}

	close := func() {
		s.Close()
		os.Remove(path)
	}

	return s, close, nil
}

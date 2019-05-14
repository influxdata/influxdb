package bolt_test

import (
	"context"
	"errors"
	"io/ioutil"
	"os"
	"testing"

	"github.com/influxdata/influxdb/bolt"
	"github.com/influxdata/influxdb/kv"
	platformtesting "github.com/influxdata/influxdb/testing"
)

func NewTestKVStore() (*bolt.KVStore, func(), error) {
	f, err := ioutil.TempFile("", "influxdata-platform-bolt-")
	if err != nil {
		return nil, nil, errors.New("unable to open temporary boltdb file")
	}
	f.Close()

	path := f.Name()
	s := bolt.NewKVStore(path)
	if err := s.Open(context.TODO()); err != nil {
		return nil, nil, err
	}

	close := func() {
		s.Close()
		os.Remove(path)
	}

	return s, close, nil
}

func initKVStore(f platformtesting.KVStoreFields, t *testing.T) (kv.Store, func()) {
	s, closeFn, err := NewTestKVStore()
	if err != nil {
		t.Fatalf("failed to create new kv store: %v", err)
	}

	err = s.Update(context.Background(), func(tx kv.Tx) error {
		b, err := tx.Bucket(f.Bucket)
		if err != nil {
			return err
		}

		for _, p := range f.Pairs {
			if err := b.Put(p.Key, p.Value); err != nil {
				return err
			}
		}

		return nil
	})
	if err != nil {
		t.Fatalf("failed to put keys: %v", err)
	}
	return s, func() {
		closeFn()
	}
}

func TestKVStore(t *testing.T) {
	platformtesting.KVStore(initKVStore, t)
}

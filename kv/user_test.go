package kv_test

import (
	"context"
	"errors"
	"io/ioutil"
	"os"
	"testing"

	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/bolt"
	"github.com/influxdata/influxdb/kv"
	influxdbtesting "github.com/influxdata/influxdb/testing"
)

func NewTestKVStore() (*bolt.KVStore, func(), error) {
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

func initUserService(f influxdbtesting.UserFields, t *testing.T) (influxdb.UserService, string, func()) {
	s, closeFn, err := NewTestKVStore()
	if err != nil {
		t.Fatalf("failed to create new kv store: %v", err)
	}
	svc := kv.NewService(s)
	svc.IDGenerator = f.IDGenerator

	ctx := context.Background()
	if err := svc.Initialize(ctx); err != nil {
		t.Fatalf("error initializing user service: %v", err)
	}

	for _, u := range f.Users {
		if err := svc.PutUser(ctx, u); err != nil {
			t.Fatalf("failed to populate users")
		}
	}

	return svc, kv.OpPrefix, func() {
		defer closeFn()
		for _, u := range f.Users {
			if err := svc.DeleteUser(ctx, u.ID); err != nil {
				t.Logf("failed to remove users: %v", err)
			}
		}
	}
}

func TestUserService(t *testing.T) {
	influxdbtesting.UserService(initUserService, t)
}

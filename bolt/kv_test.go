package bolt_test

import (
	"context"
	"testing"

	platform "github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/kv"
	platformtesting "github.com/influxdata/influxdb/testing"
)

func initKVStore(f platformtesting.KVStoreFields, t *testing.T) (kv.Store, func()) {
	s, closeFn, err := NewTestKVStore()
	if err != nil {
		t.Fatalf("failed to create new kv store: %v", err)
	}

	err = s.Update(func(tx kv.Tx) error {
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

func initExampleService(f platformtesting.UserFields, t *testing.T) (platform.UserService, string, func()) {
	s, closeFn, err := NewTestKVStore()
	if err != nil {
		t.Fatalf("failed to create new kv store: %v", err)
	}
	svc := kv.NewExampleService(s, f.IDGenerator)
	if err := svc.Initialize(); err != nil {
		t.Fatalf("error initializing user service: %v", err)
	}

	ctx := context.Background()
	for _, u := range f.Users {
		if err := svc.PutUser(ctx, u); err != nil {
			t.Fatalf("failed to populate users")
		}
	}
	return svc, "kv/", func() {
		defer closeFn()
		for _, u := range f.Users {
			if err := svc.DeleteUser(ctx, u.ID); err != nil {
				t.Logf("failed to remove users: %v", err)
			}
		}
	}
}

func TestExampleService_CreateUser(t *testing.T) {
	platformtesting.CreateUser(initExampleService, t)
}

func TestExampleService_FindUserByID(t *testing.T) {
	platformtesting.FindUserByID(initExampleService, t)
}

func TestExampleService_FindUsers(t *testing.T) {
	platformtesting.FindUsers(initExampleService, t)
}

func TestExampleService_DeleteUser(t *testing.T) {
	platformtesting.DeleteUser(initExampleService, t)
}

func TestExampleService_FindUser(t *testing.T) {
	platformtesting.FindUser(initExampleService, t)
}

func TestExampleService_UpdateUser(t *testing.T) {
	platformtesting.UpdateUser(initExampleService, t)
}

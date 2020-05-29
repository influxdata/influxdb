package tenant_test

import (
	"context"
	"errors"
	"io/ioutil"
	"os"
	"testing"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/bolt"
	"github.com/influxdata/influxdb/v2/inmem"
	"github.com/influxdata/influxdb/v2/kv"
	"github.com/influxdata/influxdb/v2/tenant"
	influxdbtesting "github.com/influxdata/influxdb/v2/testing"
	"go.uber.org/zap/zaptest"
)

func NewTestBoltStore(t *testing.T) (kv.Store, func(), error) {
	f, err := ioutil.TempFile("", "influxdata-bolt-")
	if err != nil {
		return nil, nil, errors.New("unable to open temporary boltdb file")
	}
	f.Close()

	path := f.Name()
	s := bolt.NewKVStore(zaptest.NewLogger(t), path)
	if err := s.Open(context.Background()); err != nil {
		return nil, nil, err
	}

	close := func() {
		s.Close()
		os.Remove(path)
	}

	return s, close, nil
}

func NewTestInmemStore(t *testing.T) (kv.Store, func(), error) {
	return inmem.NewKVStore(), func() {}, nil
}

func TestBoltTenantService(t *testing.T) {
	influxdbtesting.TenantService(t, initBoltTenantService)
}

func initBoltTenantService(t *testing.T, f influxdbtesting.TenantFields) (influxdb.TenantService, func()) {
	s, closeBolt, err := NewTestBoltStore(t)
	if err != nil {
		t.Fatalf("failed to create new kv store: %v", err)
	}
	storage, err := tenant.NewStore(s)
	if err != nil {
		t.Fatal(err)
	}
	svc := tenant.NewService(storage)

	for _, u := range f.Users {
		if err := svc.CreateUser(context.Background(), u); err != nil {
			t.Fatalf("error populating users: %v", err)
		}
	}

	for i := range f.Passwords {
		if err := svc.SetPassword(context.Background(), f.Users[i].ID, f.Passwords[i]); err != nil {
			t.Fatalf("error setting passsword user, %s %s: %v", f.Users[i].Name, f.Passwords[i], err)
		}
	}

	for _, o := range f.Organizations {
		if err := svc.CreateOrganization(context.Background(), o); err != nil {
			t.Fatalf("failed to populate organizations: %s", err)
		}
	}

	for _, b := range f.Buckets {
		if err := svc.CreateBucket(context.Background(), b); err != nil {
			t.Fatalf("failed to populate buckets: %s", err)
		}
	}

	for _, m := range f.UserResourceMappings {
		if err := svc.CreateUserResourceMapping(context.Background(), m); err != nil {
			t.Fatalf("failed to populate mappings: %v", err)
		}
	}

	return svc, func() {
		for _, u := range f.Users {
			if err := svc.DeleteUser(context.Background(), u.ID); err != nil {
				t.Logf("error removing users: %v", err)
			}
		}

		for _, m := range f.UserResourceMappings {
			if err := svc.DeleteUserResourceMapping(context.Background(), m.ResourceID, m.UserID); err != nil {
				t.Logf("failed to remove user resource mapping: %v", err)
			}
		}

		for _, o := range f.Organizations {
			if err := svc.DeleteOrganization(context.Background(), o.ID); err != nil {
				t.Logf("failed to remove organization: %v", err)
			}
		}

		closeBolt()
	}
}

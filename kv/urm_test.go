package kv_test

import (
	"context"
	"testing"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/inmem"
	"github.com/influxdata/influxdb/v2/kv"
	"github.com/influxdata/influxdb/v2/snowflake"
	influxdbtesting "github.com/influxdata/influxdb/v2/testing"
	"go.uber.org/zap/zaptest"
)

type testable interface {
	Logf(string, ...interface{})
	Error(args ...interface{})
	Errorf(string, ...interface{})
	Fail()
	Failed() bool
	Name() string
	FailNow()
	Fatal(args ...interface{})
	Fatalf(format string, args ...interface{})
}

func TestBoltUserResourceMappingService(t *testing.T) {
	influxdbtesting.UserResourceMappingService(initURMServiceFunc(NewTestBoltStore), t)
}

func TestInmemUserResourceMappingService(t *testing.T) {
	influxdbtesting.UserResourceMappingService(initURMServiceFunc(NewTestBoltStore), t)
}

type userResourceMappingTestFunc func(influxdbtesting.UserResourceFields, *testing.T) (influxdb.UserResourceMappingService, func())

func initURMServiceFunc(storeFn func(*testing.T) (kv.Store, func(), error), confs ...kv.ServiceConfig) userResourceMappingTestFunc {
	return func(f influxdbtesting.UserResourceFields, t *testing.T) (influxdb.UserResourceMappingService, func()) {
		s, closeStore, err := storeFn(t)
		if err != nil {
			t.Fatalf("failed to create new kv store: %v", err)
		}

		svc, closeSvc := initUserResourceMappingService(s, f, t, confs...)
		return svc, func() {
			closeSvc()
			closeStore()
		}
	}
}

func initUserResourceMappingService(s kv.Store, f influxdbtesting.UserResourceFields, t testable, configs ...kv.ServiceConfig) (influxdb.UserResourceMappingService, func()) {
	svc := kv.NewService(zaptest.NewLogger(t), s, configs...)

	ctx := context.Background()
	if err := svc.Initialize(ctx); err != nil {
		t.Fatalf("error initializing urm service: %v", err)
	}

	for _, o := range f.Organizations {
		if err := svc.CreateOrganization(ctx, o); err != nil {
			t.Fatalf("failed to create org %q", err)
		}
	}

	for _, u := range f.Users {
		if err := svc.CreateUser(ctx, u); err != nil {
			t.Fatalf("failed to create user %q", err)
		}
	}

	for _, b := range f.Buckets {
		if err := svc.PutBucket(ctx, b); err != nil {
			t.Fatalf("failed to create bucket %q", err)
		}
	}

	for _, m := range f.UserResourceMappings {
		if err := svc.CreateUserResourceMapping(ctx, m); err != nil {
			t.Fatalf("failed to populate mappings %q", err)
		}
	}

	return svc, func() {
		for _, m := range f.UserResourceMappings {
			if err := svc.DeleteUserResourceMapping(ctx, m.ResourceID, m.UserID); err != nil {
				t.Logf("failed to remove user resource mapping: %v", err)
			}
		}

		for _, b := range f.Buckets {
			if err := svc.DeleteBucket(ctx, b.ID); err != nil {
				t.Logf("failed to delete org", err)
			}
		}

		for _, u := range f.Users {
			if err := svc.DeleteUser(ctx, u.ID); err != nil {
				t.Fatalf("failed to delete user %q", err)
			}
		}

		for _, o := range f.Organizations {
			if err := svc.DeleteOrganization(ctx, o.ID); err != nil {
				t.Logf("failed to delete org", err)
			}
		}
	}
}

func BenchmarkReadURMs(b *testing.B) {
	urms := influxdbtesting.UserResourceFields{
		UserResourceMappings: make([]*influxdb.UserResourceMapping, 10000),
	}
	idgen := snowflake.NewDefaultIDGenerator()
	users := make([]influxdb.ID, 10)
	for i := 0; i < 10; i++ {
		users[i] = idgen.ID()
	}

	for i := 0; i < 10000; i++ {
		urms.UserResourceMappings[i] = &influxdb.UserResourceMapping{
			ResourceID:   idgen.ID(),
			UserID:       users[i%len(users)],
			UserType:     influxdb.Member,
			ResourceType: influxdb.BucketsResourceType,
		}
	}
	st := inmem.NewKVStore()
	initUserResourceMappingService(st, urms, b)
	svc := kv.NewService(zaptest.NewLogger(b), st)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		svc.FindUserResourceMappings(context.Background(), influxdb.UserResourceMappingFilter{
			UserID: users[0],
		})
	}
}

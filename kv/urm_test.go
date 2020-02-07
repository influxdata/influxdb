package kv_test

import (
	"context"
	"testing"

	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/inmem"
	"github.com/influxdata/influxdb/kv"
	"github.com/influxdata/influxdb/snowflake"
	influxdbtesting "github.com/influxdata/influxdb/testing"
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
	influxdbtesting.UserResourceMappingService(initBoltUserResourceMappingService, t)
}

func initBoltUserResourceMappingService(f influxdbtesting.UserResourceFields, t *testing.T) (influxdb.UserResourceMappingService, func()) {
	s, closeBolt, err := NewTestBoltStore(t)
	if err != nil {
		t.Fatalf("failed to create new kv store: %v", err)
	}

	svc, closeSvc := initUserResourceMappingService(s, f, t)
	return svc, func() {
		closeSvc()
		closeBolt()
	}
}

func initUserResourceMappingService(s kv.Store, f influxdbtesting.UserResourceFields, t testable) (influxdb.UserResourceMappingService, func()) {
	svc := kv.NewService(zaptest.NewLogger(t), s)

	ctx := context.Background()
	if err := svc.Initialize(ctx); err != nil {
		t.Fatalf("error initializing urm service: %v", err)
	}

	for _, m := range f.UserResourceMappings {
		if err := svc.CreateUserResourceMapping(ctx, m); err != nil {
			t.Fatalf("failed to populate mappings")
		}
	}

	return svc, func() {
		for _, m := range f.UserResourceMappings {
			if err := svc.DeleteUserResourceMapping(ctx, m.ResourceID, m.UserID); err != nil {
				t.Logf("failed to remove user resource mapping: %v", err)
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

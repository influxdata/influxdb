package kv_test

import (
	"context"
	"testing"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kv"
	influxdbtesting "github.com/influxdata/influxdb/v2/testing"
	"go.uber.org/zap/zaptest"
)

func TestBoltUserService(t *testing.T) {
	influxdbtesting.UserService(initBoltUserService, t)
}

func initBoltUserService(f influxdbtesting.UserFields, t *testing.T) (influxdb.UserService, string, func()) {
	s, closeBolt, err := NewTestBoltStore(t)
	if err != nil {
		t.Fatalf("failed to create new kv store: %v", err)
	}

	svc, op, closeSvc := initUserService(s, f, t)
	return svc, op, func() {
		closeSvc()
		closeBolt()
	}
}

func initUserService(s kv.SchemaStore, f influxdbtesting.UserFields, t *testing.T) (influxdb.UserService, string, func()) {
	ctx := context.Background()
	svc := kv.NewService(zaptest.NewLogger(t), s)
	svc.IDGenerator = f.IDGenerator

	for _, u := range f.Users {
		if err := svc.PutUser(ctx, u); err != nil {
			t.Fatalf("failed to populate users")
		}
	}

	return svc, kv.OpPrefix, func() {
		for _, u := range f.Users {
			if err := svc.DeleteUser(ctx, u.ID); err != nil {
				t.Logf("failed to remove users: %v", err)
			}
		}
	}
}

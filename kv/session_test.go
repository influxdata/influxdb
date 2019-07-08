package kv_test

import (
	"context"
	"testing"

	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/kv"
	influxdbtesting "github.com/influxdata/influxdb/testing"
)

func TestBoltSessionService(t *testing.T) {
	influxdbtesting.SessionService(initBoltSessionService, t)
}

func TestInmemSessionService(t *testing.T) {
	influxdbtesting.SessionService(initInmemSessionService, t)
}

func initBoltSessionService(f influxdbtesting.SessionFields, t *testing.T) (influxdb.SessionService, string, func()) {
	s, closeBolt, err := NewTestBoltStore()
	if err != nil {
		t.Fatalf("failed to create new kv store: %v", err)
	}

	svc, op, closeSvc := initSessionService(s, f, t)
	return svc, op, func() {
		closeSvc()
		closeBolt()
	}
}

func initInmemSessionService(f influxdbtesting.SessionFields, t *testing.T) (influxdb.SessionService, string, func()) {
	s, closeBolt, err := NewTestInmemStore()
	if err != nil {
		t.Fatalf("failed to create new kv store: %v", err)
	}

	svc, op, closeSvc := initSessionService(s, f, t)
	return svc, op, func() {
		closeSvc()
		closeBolt()
	}
}

func initSessionService(s kv.Store, f influxdbtesting.SessionFields, t *testing.T) (influxdb.SessionService, string, func()) {
	svc := kv.NewService(s)
	svc.IDGenerator = f.IDGenerator
	svc.TokenGenerator = f.TokenGenerator

	ctx := context.Background()
	if err := svc.Initialize(ctx); err != nil {
		t.Fatalf("error initializing session service: %v", err)
	}

	for _, u := range f.Users {
		if err := svc.PutUser(ctx, u); err != nil {
			t.Fatalf("failed to populate users")
		}
	}
	for _, s := range f.Sessions {
		if err := svc.PutSession(ctx, s); err != nil {
			t.Fatalf("failed to populate sessions")
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

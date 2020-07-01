package kv_test

import (
	"context"
	"testing"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kv"
	influxdbtesting "github.com/influxdata/influxdb/v2/testing"
	"go.uber.org/zap/zaptest"
)

func TestBoltSessionService(t *testing.T) {
	influxdbtesting.SessionService(initBoltSessionService, t)
}

func initBoltSessionService(f influxdbtesting.SessionFields, t *testing.T) (influxdb.SessionService, string, func()) {
	s, closeBolt, err := NewTestBoltStore(t)
	if err != nil {
		t.Fatalf("failed to create new kv store: %v", err)
	}

	svc, op, closeSvc := initSessionService(s, f, t)
	return svc, op, func() {
		closeSvc()
		closeBolt()
	}
}

func initSessionService(s kv.SchemaStore, f influxdbtesting.SessionFields, t *testing.T) (influxdb.SessionService, string, func()) {
	ctx := context.Background()
	svc := kv.NewService(zaptest.NewLogger(t), s)
	svc.IDGenerator = f.IDGenerator
	svc.TokenGenerator = f.TokenGenerator

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

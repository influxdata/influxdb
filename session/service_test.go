package session

import (
	"context"
	"testing"
	"time"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/inmem"
	"github.com/influxdata/influxdb/v2/kv/migration/all"
	"github.com/influxdata/influxdb/v2/mock"
	"github.com/influxdata/influxdb/v2/tenant"
	influxdbtesting "github.com/influxdata/influxdb/v2/testing"
	"go.uber.org/zap/zaptest"
)

func TestSessionService(t *testing.T) {
	influxdbtesting.SessionService(initSessionService, t)
}

func initSessionService(f influxdbtesting.SessionFields, t *testing.T) (influxdb.SessionService, string, func()) {
	ss := NewStorage(inmem.NewSessionStore())

	kvStore := inmem.NewKVStore()

	ctx := context.Background()
	if err := all.Up(ctx, zaptest.NewLogger(t), kvStore); err != nil {
		t.Fatal(err)
	}

	ten := tenant.NewService(tenant.NewStore(kvStore))

	svc := NewService(ss, ten, ten, &mock.AuthorizationService{
		FindAuthorizationsFn: func(context.Context, influxdb.AuthorizationFilter, ...influxdb.FindOptions) ([]*influxdb.Authorization, int, error) {
			return []*influxdb.Authorization{}, 0, nil
		},
	}, WithSessionLength(time.Minute))

	if f.IDGenerator != nil {
		WithIDGenerator(f.IDGenerator)(svc)
	}

	if f.TokenGenerator != nil {
		WithTokenGenerator(f.TokenGenerator)(svc)
	}

	for _, u := range f.Users {
		if err := ten.CreateUser(ctx, u); err != nil {
			t.Fatalf("failed to populate users")
		}
	}
	for _, s := range f.Sessions {
		if err := ss.CreateSession(ctx, s); err != nil {
			t.Fatalf("failed to populate sessions")
		}
	}
	return svc, "session", func() {}
}

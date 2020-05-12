package session

import (
	"context"
	"testing"
	"time"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/inmem"
	"github.com/influxdata/influxdb/v2/mock"
	"github.com/influxdata/influxdb/v2/tenant"
	influxdbtesting "github.com/influxdata/influxdb/v2/testing"
)

func TestSessionService(t *testing.T) {
	influxdbtesting.SessionService(initSessionService, t)
}

func initSessionService(f influxdbtesting.SessionFields, t *testing.T) (influxdb.SessionService, string, func()) {
	ss := NewStorage(inmem.NewSessionStore())
	ts, _ := tenant.NewStore(inmem.NewKVStore())
	ten := tenant.NewService(ts)
	svc := NewService(ss, ten, ten, &mock.AuthorizationService{
		FindAuthorizationsFn: func(context.Context, influxdb.AuthorizationFilter, ...influxdb.FindOptions) ([]*influxdb.Authorization, int, error) {
			return []*influxdb.Authorization{}, 0, nil
		},
	}, time.Minute)

	if f.IDGenerator != nil {
		svc.idGen = f.IDGenerator
	}

	if f.TokenGenerator != nil {
		svc.tokenGen = f.TokenGenerator
	}

	ctx := context.Background()
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

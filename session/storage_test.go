package session_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/inmem"
	"github.com/influxdata/influxdb/v2/session"
)

func TestSessionStore(t *testing.T) {
	driver := func() session.Store {
		return inmem.NewSessionStore()
	}

	expected := &influxdb.Session{
		ID:        1,
		Key:       "2",
		CreatedAt: time.Now(),
		ExpiresAt: time.Now().Add(time.Hour),
	}

	simpleSetup := func(t *testing.T, store *session.Storage) {
		err := store.CreateSession(
			context.Background(),
			expected,
		)
		if err != nil {
			t.Fatal(err)
		}
	}

	st := []struct {
		name    string
		setup   func(*testing.T, *session.Storage)
		update  func(*testing.T, *session.Storage)
		results func(*testing.T, *session.Storage)
	}{
		{
			name:  "create",
			setup: simpleSetup,
			results: func(t *testing.T, store *session.Storage) {
				session, err := store.FindSessionByID(context.Background(), 1)
				if err != nil {
					t.Fatal(err)
				}

				if !cmp.Equal(session, expected) {
					t.Fatalf("expected identical sessions: \n%+v\n%+v", session, expected)
				}
			},
		},
		{
			name:  "get",
			setup: simpleSetup,
			results: func(t *testing.T, store *session.Storage) {
				session, err := store.FindSessionByID(context.Background(), 1)
				if err != nil {
					t.Fatal(err)
				}

				if !cmp.Equal(session, expected) {
					t.Fatalf("expected identical sessions: \n%+v\n%+v", session, expected)
				}

				session, err = store.FindSessionByKey(context.Background(), "2")
				if err != nil {
					t.Fatal(err)
				}

				if !cmp.Equal(session, expected) {
					t.Fatalf("expected identical sessions: \n%+v\n%+v", session, expected)
				}
			},
		},
		{
			name:  "delete",
			setup: simpleSetup,
			update: func(t *testing.T, store *session.Storage) {
				err := store.DeleteSession(context.Background(), 1)
				if err != nil {
					t.Fatal(err)
				}
			},
			results: func(t *testing.T, store *session.Storage) {
				session, err := store.FindSessionByID(context.Background(), 1)
				if err == nil {
					t.Fatal("expected error on deleted session but got none")
				}

				if session != nil {
					t.Fatal("got a session when none should have existed")
				}
			},
		},
	}
	for _, testScenario := range st {
		t.Run(testScenario.name, func(t *testing.T) {
			ss := session.NewStorage(driver())

			// setup
			if testScenario.setup != nil {
				testScenario.setup(t, ss)
			}

			// update
			if testScenario.update != nil {
				testScenario.update(t, ss)
			}

			// results
			if testScenario.results != nil {
				testScenario.results(t, ss)
			}
		})
	}
}

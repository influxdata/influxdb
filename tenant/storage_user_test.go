package tenant_test

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/inmem"
	"github.com/influxdata/influxdb/kv"
	"github.com/influxdata/influxdb/tenant"
)

func TestUser(t *testing.T) {
	driver := func() kv.Store {
		return inmem.NewKVStore()
	}

	simpleSetup := func(t *testing.T, store *tenant.Store, tx *tenant.Tx) {
		for i := 1; i <= 10; i++ {
			err := store.CreateUser(context.Background(), tx, &influxdb.User{
				ID:     influxdb.ID(i),
				Name:   fmt.Sprintf("user%d", i),
				Status: "active",
			})
			if err != nil {
				t.Fatal(err)
			}
		}
	}

	st := []struct {
		name    string
		setup   func(*testing.T, *tenant.Store, *tenant.Tx)
		update  func(*testing.T, *tenant.Store, *tenant.Tx)
		results func(*testing.T, *tenant.Store, *tenant.Tx)
	}{
		{
			name:  "create",
			setup: simpleSetup,
			results: func(t *testing.T, store *tenant.Store, tx *tenant.Tx) {
				users, err := store.ListUsers(context.Background(), tx)
				if err != nil {
					t.Fatal(err)
				}

				if len(users) != 10 {
					t.Fatalf("expected 10 users got: %d", len(users))
				}

				expected := []*influxdb.User{}
				for i := 1; i <= 10; i++ {
					expected = append(expected, &influxdb.User{
						ID:     influxdb.ID(i),
						Name:   fmt.Sprintf("user%d", i),
						Status: "active",
					})
				}
				if !reflect.DeepEqual(users, expected) {
					t.Fatalf("expected identical users: \n%+v\n%+v", users, expected)
				}
			},
		},
		{
			name:  "get",
			setup: simpleSetup,
			results: func(t *testing.T, store *tenant.Store, tx *tenant.Tx) {
				user, err := store.GetUser(context.Background(), tx, 5)
				if err != nil {
					t.Fatal(err)
				}

				expected := &influxdb.User{
					ID:     5,
					Name:   "user5",
					Status: "active",
				}

				if !reflect.DeepEqual(user, expected) {
					t.Fatalf("expected identical user: \n%+v\n%+v", user, expected)
				}

				user, err = store.GetUserByName(context.Background(), tx, "user5")
				if err != nil {
					t.Fatal(err)
				}

				if !reflect.DeepEqual(user, expected) {
					t.Fatalf("expected identical user: \n%+v\n%+v", user, expected)
				}

				if _, err := store.GetUser(context.Background(), tx, 500); err != tenant.ErrUserNotFound {
					t.Fatal("failed to get correct error when looking for invalid user by id")
				}

				if _, err := store.GetUserByName(context.Background(), tx, "notauser"); err != tenant.ErrUserNotFound {
					t.Fatal("failed to get correct error when looking for invalid user by name")
				}

			},
		},
		{
			name:  "list",
			setup: simpleSetup,
			results: func(t *testing.T, store *tenant.Store, tx *tenant.Tx) {
				users, err := store.ListUsers(context.Background(), tx)
				if err != nil {
					t.Fatal(err)
				}

				if len(users) != 10 {
					t.Fatalf("expected 10 users got: %d", len(users))
				}

				expected := []*influxdb.User{}
				for i := 1; i <= 10; i++ {
					expected = append(expected, &influxdb.User{
						ID:     influxdb.ID(i),
						Name:   fmt.Sprintf("user%d", i),
						Status: "active",
					})
				}
				if !reflect.DeepEqual(users, expected) {
					t.Fatalf("expected identical users: \n%+v\n%+v", users, expected)
				}

				users, err = store.ListUsers(context.Background(), tx, influxdb.FindOptions{Limit: 4})
				if err != nil {
					t.Fatal(err)
				}

				if len(users) != 4 {
					t.Fatalf("expected 4 users got: %d", len(users))
				}
				if !reflect.DeepEqual(users, expected[:4]) {
					t.Fatalf("expected identical users with limit: \n%+v\n%+v", users, expected[:4])
				}

				users, err = store.ListUsers(context.Background(), tx, influxdb.FindOptions{Offset: 3})
				if err != nil {
					t.Fatal(err)
				}

				if len(users) != 7 {
					t.Fatalf("expected 7 users got: %d", len(users))
				}
				if !reflect.DeepEqual(users, expected[3:]) {
					t.Fatalf("expected identical users with limit: \n%+v\n%+v", users, expected[3:])
				}
			},
		},
		{
			name:  "update",
			setup: simpleSetup,
			update: func(t *testing.T, store *tenant.Store, tx *tenant.Tx) {
				user5 := "user5"
				_, err := store.UpdateUser(context.Background(), tx, influxdb.ID(3), influxdb.UserUpdate{Name: &user5})
				if err != kv.NotUniqueError {
					t.Fatal("failed to error on duplicate username")
				}

				user30 := "user30"
				_, err = store.UpdateUser(context.Background(), tx, influxdb.ID(3), influxdb.UserUpdate{Name: &user30})
				if err != nil {
					t.Fatal(err)
				}

				inactive := influxdb.Status("inactive")
				_, err = store.UpdateUser(context.Background(), tx, influxdb.ID(3), influxdb.UserUpdate{Status: &inactive})
				if err != nil {
					t.Fatal(err)
				}
			},
			results: func(t *testing.T, store *tenant.Store, tx *tenant.Tx) {
				users, err := store.ListUsers(context.Background(), tx)
				if err != nil {
					t.Fatal(err)
				}

				if len(users) != 10 {
					t.Fatalf("expected 10 users got: %d", len(users))
				}

				expected := []*influxdb.User{}
				for i := 1; i <= 10; i++ {
					expected = append(expected, &influxdb.User{
						ID:     influxdb.ID(i),
						Name:   fmt.Sprintf("user%d", i),
						Status: "active",
					})
				}
				expected[2].Name = "user30"
				expected[2].Status = "inactive"
				if !reflect.DeepEqual(users, expected) {
					t.Fatalf("expected identical users: \n%+v\n%+v", users, expected)
				}
			},
		},
		{
			name:  "delete",
			setup: simpleSetup,
			update: func(t *testing.T, store *tenant.Store, tx *tenant.Tx) {
				err := store.DeleteUser(context.Background(), tx, 1)
				if err != nil {
					t.Fatal(err)
				}

				err = store.DeleteUser(context.Background(), tx, 1)
				if err != tenant.ErrUserNotFound {
					t.Fatal("invalid error when deleting user that has already been deleted", err)
				}

				err = store.DeleteUser(context.Background(), tx, 3)
				if err != nil {
					t.Fatal(err)
				}

			},
			results: func(t *testing.T, store *tenant.Store, tx *tenant.Tx) {
				users, err := store.ListUsers(context.Background(), tx)
				if err != nil {
					t.Fatal(err)
				}

				if len(users) != 8 {
					t.Fatalf("expected 10 users got: %d", len(users))
				}

				expected := []*influxdb.User{}
				for i := 1; i <= 10; i++ {
					if i != 1 && i != 3 {
						expected = append(expected, &influxdb.User{
							ID:     influxdb.ID(i),
							Name:   fmt.Sprintf("user%d", i),
							Status: "active",
						})
					}
				}

				if !reflect.DeepEqual(users, expected) {
					t.Fatalf("expected identical users: \n%+v\n%+v", users, expected)
				}
			},
		},
	}
	for _, testScenario := range st {
		t.Run(testScenario.name, func(t *testing.T) {
			ts, err := tenant.NewStore(driver())
			if err != nil {
				t.Fatal(err)
			}

			// setup
			if testScenario.setup != nil {
				tx, err := ts.Begin(tenant.Write)
				if err != nil {
					t.Fatal(err)
				}
				testScenario.setup(t, ts, tx)
				tx.Commit()
			}

			// update
			if testScenario.update != nil {
				tx, err := ts.Begin(tenant.Write)
				if err != nil {
					t.Fatal(err)
				}
				testScenario.update(t, ts, tx)
				tx.Commit()
			}

			// results
			if testScenario.results != nil {
				tx, err := ts.Begin(tenant.Write)
				if err != nil {
					t.Fatal(err)
				}
				testScenario.results(t, ts, tx)
				tx.Commit()
			}
		})
	}
}

package tenant_test

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/kv"
	"github.com/influxdata/influxdb/v2/tenant"
	itesting "github.com/influxdata/influxdb/v2/testing"
)

func TestUser(t *testing.T) {
	simpleSetup := func(t *testing.T, store *tenant.Store, tx kv.Tx) {
		for i := 1; i <= 10; i++ {
			err := store.CreateUser(context.Background(), tx, &influxdb.User{
				ID:     platform.ID(i),
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
		setup   func(*testing.T, *tenant.Store, kv.Tx)
		update  func(*testing.T, *tenant.Store, kv.Tx)
		results func(*testing.T, *tenant.Store, kv.Tx)
	}{
		{
			name:  "create",
			setup: simpleSetup,
			results: func(t *testing.T, store *tenant.Store, tx kv.Tx) {
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
						ID:     platform.ID(i),
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
			results: func(t *testing.T, store *tenant.Store, tx kv.Tx) {
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
			results: func(t *testing.T, store *tenant.Store, tx kv.Tx) {
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
						ID:     platform.ID(i),
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
			update: func(t *testing.T, store *tenant.Store, tx kv.Tx) {
				user5 := "user5"
				_, err := store.UpdateUser(context.Background(), tx, platform.ID(3), influxdb.UserUpdate{Name: &user5})
				if err.Error() != tenant.UserAlreadyExistsError(user5).Error() {
					t.Fatal("failed to error on duplicate username")
				}

				user30 := "user30"
				_, err = store.UpdateUser(context.Background(), tx, platform.ID(3), influxdb.UserUpdate{Name: &user30})
				if err != nil {
					t.Fatal(err)
				}

				inactive := influxdb.Status("inactive")
				_, err = store.UpdateUser(context.Background(), tx, platform.ID(3), influxdb.UserUpdate{Status: &inactive})
				if err != nil {
					t.Fatal(err)
				}
			},
			results: func(t *testing.T, store *tenant.Store, tx kv.Tx) {
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
						ID:     platform.ID(i),
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
			update: func(t *testing.T, store *tenant.Store, tx kv.Tx) {
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
			results: func(t *testing.T, store *tenant.Store, tx kv.Tx) {
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
							ID:     platform.ID(i),
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
			s, closeS, err := itesting.NewTestInmemStore(t)
			if err != nil {
				t.Fatal(err)
			}
			defer closeS()

			ts := tenant.NewStore(s)

			// setup
			if testScenario.setup != nil {
				err := ts.Update(context.Background(), func(tx kv.Tx) error {
					testScenario.setup(t, ts, tx)
					return nil
				})

				if err != nil {
					t.Fatal(err)
				}
			}

			// update
			if testScenario.update != nil {
				err := ts.Update(context.Background(), func(tx kv.Tx) error {
					testScenario.update(t, ts, tx)
					return nil
				})

				if err != nil {
					t.Fatal(err)
				}
			}

			// results
			if testScenario.results != nil {
				err := ts.View(context.Background(), func(tx kv.Tx) error {
					testScenario.results(t, ts, tx)
					return nil
				})

				if err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

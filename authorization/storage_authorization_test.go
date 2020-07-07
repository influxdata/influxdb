package authorization_test

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/authorization"
	"github.com/influxdata/influxdb/v2/inmem"
	"github.com/influxdata/influxdb/v2/kv"
	"github.com/influxdata/influxdb/v2/kv/migration/all"
	"go.uber.org/zap/zaptest"
)

func TestAuth(t *testing.T) {
	setup := func(t *testing.T, store *authorization.Store, tx kv.Tx) {
		for i := 1; i <= 10; i++ {
			err := store.CreateAuthorization(context.Background(), tx, &influxdb.Authorization{
				ID:     influxdb.ID(i),
				Token:  fmt.Sprintf("randomtoken%d", i),
				OrgID:  influxdb.ID(i),
				UserID: influxdb.ID(i),
			})

			if err != nil {
				t.Fatal(err)
			}
		}
	}

	/*setupForUpdateTest := func(t *testing.T, store *authorization.Store, tx kv.Tx) {
		err := store.CreateAuthorization(context.Background(), tx, &influxdb.Authorization{ //shud match with expecteddummyAuth
			ID:     influxdb.ID(1),
			Token:  fmt.Sprintf("randomtoken%d", 1),
			Status: influxdb.Active, //when updated should change to inactive
			//Description: "Testing1",
			OrgID:  influxdb.ID(1),
			UserID: influxdb.ID(1),
		})

		if err != nil {
			t.Fatal(err)
		}

		auth, err := store.GetAuthorizationByID(context.Background(), tx, influxdb.ID(1))

		if err != nil {
			t.Fatal(err)
		}

		_, err2 := store.UpdateAuthorization(context.Background(), tx, influxdb.ID(1), auth)

		//fmt.Printf("My status: %v", myAuth.Status)

		if err2 != nil {
			t.Fatal(err)
		}
	}*/

	tt := []struct {
		name               string
		setup              func(*testing.T, *authorization.Store, kv.Tx)
		setupForUpdateTest func(t *testing.T, store *authorization.Store, tx kv.Tx)
		update             func(*testing.T, *authorization.Store, kv.Tx)
		results            func(*testing.T, *authorization.Store, kv.Tx)
	}{
		{
			name:  "create",
			setup: setup,
			results: func(t *testing.T, store *authorization.Store, tx kv.Tx) {
				auths, err := store.ListAuthorizations(context.Background(), tx, influxdb.AuthorizationFilter{})
				if err != nil {
					t.Fatal(err) //the setup created 10 auths. This is listing them out
				}

				if len(auths) != 10 { //making sure those auths are of count 10
					t.Fatalf("expected 10 authorizations, got: %d", len(auths))
				}

				expected := []*influxdb.Authorization{} //dummy
				for i := 1; i <= 10; i++ {
					expected = append(expected, &influxdb.Authorization{
						ID:     influxdb.ID(i),
						Token:  fmt.Sprintf("randomtoken%d", i),
						OrgID:  influxdb.ID(i),
						UserID: influxdb.ID(i),
						Status: "active",
					})
				}
				if !reflect.DeepEqual(auths, expected) { //checking that setup and expected gave same stuff
					t.Fatalf("expected identical authorizations: \n%+v\n%+v", auths, expected)
				}

				// should not be able to create two authorizations with identical tokens
				err = store.CreateAuthorization(context.Background(), tx, &influxdb.Authorization{
					ID:     influxdb.ID(1),
					Token:  fmt.Sprintf("randomtoken%d", 1),
					OrgID:  influxdb.ID(1),
					UserID: influxdb.ID(1),
				})
				if err == nil {
					t.Fatalf("expected to be unable to create authorizations with identical tokens")
				}
			},
		},
		{
			name:  "read",
			setup: setup,
			results: func(t *testing.T, store *authorization.Store, tx kv.Tx) {
				/*auths, err := store.ListAuthorizations(context.Background(), tx, influxdb.AuthorizationFilter{})
				if err != nil {
					t.Fatal(err)
				}

				if len(auths) != 10 {
					t.Fatalf("expected 10 authorizations, got: %d", len(auths))
				}

				expected := []*influxdb.Authorization{}
				for i := 1; i <= 10; i++ {
					expected = append(expected, &influxdb.Authorization{
						ID:     influxdb.ID(i),
						Token:  fmt.Sprintf("randomtoken%d", i),
						OrgID:  influxdb.ID(i),
						UserID: influxdb.ID(i),
						Status: "active",
					})
				}
				if !reflect.DeepEqual(auths, expected) {
					t.Fatalf("expected identical authorizations: \n%+v\n%+v", auths, expected)
				}*/

				expectedAuth := &influxdb.Authorization{
					ID:     influxdb.ID(1),
					Token:  fmt.Sprintf("randomtoken%d", 1),
					OrgID:  influxdb.ID(1),
					UserID: influxdb.ID(1),
					Status: influxdb.Active,
				}

				authByID, err := store.GetAuthorizationByID(context.Background(), tx, influxdb.ID(1))
				if err != nil {
					t.Fatal("Unexpectedly could not acquire Authorization by ID")
				}

				if !reflect.DeepEqual(authByID, expectedAuth) {
					t.Fatalf("ID TEST: expected identical authorizations: \n%+#v\n%+#v", authByID, expectedAuth)
				}

				authByToken, err := store.GetAuthorizationByToken(context.Background(), tx, fmt.Sprintf("randomtoken%d", 1))
				if err != nil {
					t.Fatal("cannot get authorization by Token")
				}

				if !reflect.DeepEqual(authByToken, expectedAuth) {
					t.Fatalf("TOKEN TEST: expected identical authorizations: \n%+#v\n%+#v", authByToken, expectedAuth)
				}

			},
		},
		{
			name:  "update",
			setup: setup,
			results: func(t *testing.T, store *authorization.Store, tx kv.Tx) {
				/*auths, err := store.ListAuthorizations(context.Background(), tx, influxdb.AuthorizationFilter{})
				if err != nil {
					t.Fatal(err)
				}

				if len(auths) != 10 {
					t.Fatalf("expected 10 authorizations, got: %d", len(auths))
				}

				expected := []*influxdb.Authorization{}
				for i := 1; i <= 10; i++ {
					expected = append(expected, &influxdb.Authorization{
						ID:     influxdb.ID(i),
						Token:  fmt.Sprintf("randomtoken%d", i),
						OrgID:  influxdb.ID(i),
						UserID: influxdb.ID(i),
						Status: "active",
					})
				}
				if !reflect.DeepEqual(auths, expected) {
					t.Fatalf("expected identical authorizations: \n%+v\n%+v", auths, expected)
				} */

				//Process: 1) setup: an auth is created, get, and then update. 2) Here in results, I get that auth and check if
				//it's the same as what I expect it to be. I can mainly check by the 'status'

				auth, err := store.GetAuthorizationByID(context.Background(), tx, influxdb.ID(1))

				//t.Fatalf("This here: %+#v", auth)

				if err != nil {
					t.Fatal("Could not get authorization")
				}

				authUpdated, err := store.UpdateAuthorization(context.Background(), tx, influxdb.ID(1), auth)

				t.Fatalf("This: %+#v", authUpdated)

				if err != nil {
					t.Fatal("Could not get updated authorization")
				}

				expectedUpdatedAuth := &influxdb.Authorization{
					ID:     influxdb.ID(1),
					Token:  fmt.Sprintf("randomtoken%d", 1),
					OrgID:  influxdb.ID(1),
					UserID: influxdb.ID(1),
					Status: influxdb.Inactive,
				}

				//Did not use reflect.DeepEqual() because can't be sure description will be the same ~

				/*if expectedUpdatedAuth.Status != authFromSetupUpdated.Status {
					t.Fatalf("Status did not update properly - Expected: %s , Got: %s", expectedUpdatedAuth.Status, authFromSetupUpdated.Status)
				}*/

				if !reflect.DeepEqual(authUpdated, expectedUpdatedAuth) {
					t.Fatalf("expected identical authorizations: \n [Expected] %+#v \n [Got] %+#v", expectedUpdatedAuth, authUpdated)
				}
			},
		},
	}

	for _, testScenario := range tt {
		t.Run(testScenario.name, func(t *testing.T) {
			store := inmem.NewKVStore()
			if err := all.Up(context.Background(), zaptest.NewLogger(t), store); err != nil {
				t.Fatal(err)
			}

			ts, err := authorization.NewStore(store)
			if err != nil {
				t.Fatal(err)
			}

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

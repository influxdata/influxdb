package tenant_test

import (
	"context"
	"reflect"
	"sort"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/inmem"
	"github.com/influxdata/influxdb/v2/kv"
	"github.com/influxdata/influxdb/v2/tenant"
)

func TestURM(t *testing.T) {
	driver := func() kv.Store {
		return inmem.NewKVStore()
	}

	simpleSetup := func(t *testing.T, store *tenant.Store, tx kv.Tx) {
		for i := 1; i <= 10; i++ {
			err := store.CreateURM(context.Background(), tx, &influxdb.UserResourceMapping{
				UserID:       influxdb.ID(i + 1),
				UserType:     influxdb.Owner,
				MappingType:  influxdb.UserMappingType,
				ResourceType: influxdb.OrgsResourceType,
				ResourceID:   influxdb.ID(i%2 + 1),
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
				urms, err := store.ListURMs(context.Background(), tx, influxdb.UserResourceMappingFilter{})
				if err != nil {
					t.Fatal(err)
				}

				if len(urms) != 10 {
					t.Fatalf("ten records are created and we recieved %d", len(urms))
				}
				var expected []*influxdb.UserResourceMapping
				for i := 1; i <= 10; i++ {
					expected = append(expected, &influxdb.UserResourceMapping{
						UserID:       influxdb.ID(i + 1),
						UserType:     influxdb.Owner,
						MappingType:  influxdb.UserMappingType,
						ResourceType: influxdb.OrgsResourceType,
						ResourceID:   influxdb.ID(i%2 + 1),
					})
				}
				sort.Slice(expected, func(i, j int) bool {
					irid, _ := expected[i].ResourceID.Encode()
					iuid, _ := expected[i].UserID.Encode()
					jrid, _ := expected[j].ResourceID.Encode()
					juid, _ := expected[j].UserID.Encode()
					return string(irid)+string(iuid) < string(jrid)+string(juid)
				})

				if !reflect.DeepEqual(urms, expected) {
					t.Fatalf("expected identical urms: \n%s", cmp.Diff(urms, expected))
				}
			},
		},
		{
			name:  "get",
			setup: simpleSetup,
			results: func(t *testing.T, store *tenant.Store, tx kv.Tx) {
				_, err := store.GetURM(context.Background(), tx, 1, 2)
				if err != kv.ErrKeyNotFound {
					t.Fatal("failed to not find urm")
				}

				urm, err := store.GetURM(context.Background(), tx, 2, 2)
				if err != nil {
					t.Fatal(err)
				}
				expected := &influxdb.UserResourceMapping{
					UserID:       2,
					UserType:     influxdb.Owner,
					MappingType:  influxdb.UserMappingType,
					ResourceType: influxdb.OrgsResourceType,
					ResourceID:   2,
				}

				if !reflect.DeepEqual(urm, expected) {
					t.Fatalf("expected identical urm: \n%s", cmp.Diff(urm, expected))
				}
			},
		},
		{
			name:  "list",
			setup: simpleSetup,
			results: func(t *testing.T, store *tenant.Store, tx kv.Tx) {
				urms, err := store.ListURMs(context.Background(), tx, influxdb.UserResourceMappingFilter{})
				if err != nil {
					t.Fatal(err)
				}

				if len(urms) != 10 {
					t.Fatalf("ten records are created and we recieved %d", len(urms))
				}
				var expected []*influxdb.UserResourceMapping
				for i := 1; i <= 10; i++ {
					expected = append(expected, &influxdb.UserResourceMapping{
						UserID:       influxdb.ID(i + 1),
						UserType:     influxdb.Owner,
						MappingType:  influxdb.UserMappingType,
						ResourceType: influxdb.OrgsResourceType,
						ResourceID:   influxdb.ID(i%2 + 1),
					})
				}
				sort.Slice(expected, func(i, j int) bool {
					irid, _ := expected[i].ResourceID.Encode()
					iuid, _ := expected[i].UserID.Encode()
					jrid, _ := expected[j].ResourceID.Encode()
					juid, _ := expected[j].UserID.Encode()
					return string(irid)+string(iuid) < string(jrid)+string(juid)
				})

				if !reflect.DeepEqual(urms, expected) {
					t.Fatalf("expected identical urms: \n%s", cmp.Diff(urms, expected))
				}

				urms, err = store.ListURMs(context.Background(), tx, influxdb.UserResourceMappingFilter{ResourceID: influxdb.ID(1)})
				if err != nil {
					t.Fatal(err)
				}

				if len(urms) != 5 {
					t.Fatalf("expected 5 urms got %d", len(urms))
				}

				if !reflect.DeepEqual(urms, expected[:5]) {
					t.Fatalf("expected subset of urms urms: \n%s", cmp.Diff(urms, expected[:5]))
				}

			},
		},
		{
			name:  "delete",
			setup: simpleSetup,
			update: func(t *testing.T, store *tenant.Store, tx kv.Tx) {
				err := store.DeleteURM(context.Background(), tx, 23, 21)
				if err != nil {
					t.Fatal(err)
				}

				err = store.DeleteURM(context.Background(), tx, 2, 2)
				if err != nil {
					t.Fatal(err)
				}
			},
			results: func(t *testing.T, store *tenant.Store, tx kv.Tx) {
				_, err := store.GetURM(context.Background(), tx, 2, 2)
				if err != kv.ErrKeyNotFound {
					t.Fatal("failed to erro when getting a deleted URM")
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

package tenant

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/servicesv2/kv"
)

func TestOrg(t *testing.T) {
	simpleSetup := func(t *testing.T, store *Store, tx kv.Tx) {
		for i := 1; i <= 10; i++ {
			err := store.CreateOrg(context.Background(), tx, &influxdb.Organization{
				ID:          influxdb.ID(i),
				Name:        fmt.Sprintf("org%d", i),
				Description: "words",
			})
			if err != nil {
				t.Fatal(err)
			}
		}
	}

	st := []struct {
		name    string
		setup   func(*testing.T, *Store, kv.Tx)
		update  func(*testing.T, *Store, kv.Tx)
		results func(*testing.T, *Store, kv.Tx)
	}{
		{
			name:  "create",
			setup: simpleSetup,
			results: func(t *testing.T, store *Store, tx kv.Tx) {
				orgs, err := store.ListOrgs(context.Background(), tx)
				if err != nil {
					t.Fatal(err)
				}

				if len(orgs) != 10 {
					t.Fatalf("expected 10 orgs got: %d", len(orgs))
				}

				expected := []*influxdb.Organization{}
				for i := 1; i <= 10; i++ {
					expected = append(expected, &influxdb.Organization{
						ID:          influxdb.ID(i),
						Name:        fmt.Sprintf("org%d", i),
						Description: "words",
						CRUDLog: influxdb.CRUDLog{
							CreatedAt: orgs[i-1].CreatedAt,
							UpdatedAt: orgs[i-1].UpdatedAt,
						},
					})
				}
				if !reflect.DeepEqual(orgs, expected) {
					t.Fatalf("expected identical orgs: \n%+v\n%+v", orgs, expected)
				}
			},
		},
		{
			name:  "get",
			setup: simpleSetup,
			results: func(t *testing.T, store *Store, tx kv.Tx) {
				org, err := store.GetOrg(context.Background(), tx, 5)
				if err != nil {
					t.Fatal(err)
				}

				expected := &influxdb.Organization{
					ID:          5,
					Name:        "org5",
					Description: "words",
					CRUDLog: influxdb.CRUDLog{
						CreatedAt: org.CreatedAt,
						UpdatedAt: org.UpdatedAt,
					},
				}

				if !reflect.DeepEqual(org, expected) {
					t.Fatalf("expected identical org: \n%+v\n%+v", org, expected)
				}

				org, err = store.GetOrgByName(context.Background(), tx, "org5")
				if err != nil {
					t.Fatal(err)
				}

				if !reflect.DeepEqual(org, expected) {
					t.Fatalf("expected identical org: \n%+v\n%+v", org, expected)
				}

				if _, err := store.GetOrg(context.Background(), tx, 500); err != ErrOrgNotFound {
					t.Fatal("failed to get correct error when looking for invalid org by id")
				}

				if _, err := store.GetOrgByName(context.Background(), tx, "notaorg"); err.Error() != OrgNotFoundByName("notaorg").Error() {
					t.Fatal("failed to get correct error when looking for invalid org by name")
				}

			},
		},
		{
			name:  "list",
			setup: simpleSetup,
			results: func(t *testing.T, store *Store, tx kv.Tx) {
				orgs, err := store.ListOrgs(context.Background(), tx)
				if err != nil {
					t.Fatal(err)
				}

				if len(orgs) != 10 {
					t.Fatalf("expected 10 orgs got: %d", len(orgs))
				}

				expected := []*influxdb.Organization{}
				for i := 1; i <= 10; i++ {
					expected = append(expected, &influxdb.Organization{
						ID:          influxdb.ID(i),
						Name:        fmt.Sprintf("org%d", i),
						Description: "words",
						CRUDLog: influxdb.CRUDLog{
							CreatedAt: orgs[i-1].CreatedAt,
							UpdatedAt: orgs[i-1].UpdatedAt,
						},
					})
				}
				if !reflect.DeepEqual(orgs, expected) {
					t.Fatalf("expected identical orgs: \n%+v\n%+v", orgs, expected)
				}

				orgs, err = store.ListOrgs(context.Background(), tx, influxdb.FindOptions{Limit: 4})
				if err != nil {
					t.Fatal(err)
				}

				if len(orgs) != 4 {
					t.Fatalf("expected 4 orgs got: %d", len(orgs))
				}
				if !reflect.DeepEqual(orgs, expected[:4]) {
					t.Fatalf("expected identical orgs with limit: \n%+v\n%+v", orgs, expected[:4])
				}

				orgs, err = store.ListOrgs(context.Background(), tx, influxdb.FindOptions{Offset: 3})
				if err != nil {
					t.Fatal(err)
				}

				if len(orgs) != 7 {
					t.Fatalf("expected 7 orgs got: %d", len(orgs))
				}
				if !reflect.DeepEqual(orgs, expected[3:]) {
					t.Fatalf("expected identical orgs with limit: \n%+v\n%+v", orgs, expected[3:])
				}
			},
		},
		{
			name:  "update",
			setup: simpleSetup,
			update: func(t *testing.T, store *Store, tx kv.Tx) {
				org5 := "org5"
				_, err := store.UpdateOrg(context.Background(), tx, influxdb.ID(3), influxdb.OrganizationUpdate{Name: &org5})
				if err.Error() != OrgAlreadyExistsError(org5).Error() {
					t.Fatal("failed to error on duplicate orgname")
				}

				org30 := "org30"
				_, err = store.UpdateOrg(context.Background(), tx, influxdb.ID(3), influxdb.OrganizationUpdate{Name: &org30})
				if err != nil {
					t.Fatal(err)
				}

				description := "notWords"
				_, err = store.UpdateOrg(context.Background(), tx, influxdb.ID(3), influxdb.OrganizationUpdate{Description: &description})
				if err != nil {
					t.Fatal(err)
				}
			},
			results: func(t *testing.T, store *Store, tx kv.Tx) {
				orgs, err := store.ListOrgs(context.Background(), tx)
				if err != nil {
					t.Fatal(err)
				}

				if len(orgs) != 10 {
					t.Fatalf("expected 10 orgs got: %d", len(orgs))
				}

				expected := []*influxdb.Organization{}
				for i := 1; i <= 10; i++ {
					expected = append(expected, &influxdb.Organization{
						ID:          influxdb.ID(i),
						Name:        fmt.Sprintf("org%d", i),
						Description: "words",
						CRUDLog: influxdb.CRUDLog{
							CreatedAt: orgs[i-1].CreatedAt,
							UpdatedAt: orgs[i-1].UpdatedAt,
						},
					})
				}
				expected[2].Name = "org30"
				expected[2].Description = "notWords"
				if !reflect.DeepEqual(orgs, expected) {
					t.Fatalf("expected identical orgs: \n%+v\n%+v", orgs, expected)
				}
			},
		},
		{
			name:  "delete",
			setup: simpleSetup,
			update: func(t *testing.T, store *Store, tx kv.Tx) {
				err := store.DeleteOrg(context.Background(), tx, 1)
				if err != nil {
					t.Fatal(err)
				}

				err = store.DeleteOrg(context.Background(), tx, 1)
				if err != ErrOrgNotFound {
					t.Fatal("invalid error when deleting org that has already been deleted", err)
				}

				err = store.DeleteOrg(context.Background(), tx, 3)
				if err != nil {
					t.Fatal(err)
				}
			},
			results: func(t *testing.T, store *Store, tx kv.Tx) {
				orgs, err := store.ListOrgs(context.Background(), tx)
				if err != nil {
					t.Fatal(err)
				}

				if len(orgs) != 8 {
					t.Fatalf("expected 10 orgs got: %d", len(orgs))
				}

				expected := []*influxdb.Organization{}
				for i := 1; i <= 10; i++ {
					if i != 1 && i != 3 {
						expected = append(expected, &influxdb.Organization{
							ID:          influxdb.ID(i),
							Name:        fmt.Sprintf("org%d", i),
							Description: "words",
						})
					}
				}
				for i, exp := range expected {
					exp.CRUDLog.CreatedAt = orgs[i].CreatedAt
					exp.CRUDLog.UpdatedAt = orgs[i].UpdatedAt
				}

				if !reflect.DeepEqual(orgs, expected) {
					t.Fatalf("expected identical orgs: \n%+v\n%+v", orgs, expected)
				}
			},
		},
	}
	for _, testScenario := range st {
		t.Run(testScenario.name, func(t *testing.T) {
			s, closeS, err := NewTestBoltStore(t)
			if err != nil {
				t.Fatal(err)
			}
			defer closeS()

			ts := NewStore(s)

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

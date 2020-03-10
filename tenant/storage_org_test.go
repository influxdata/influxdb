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

// type Organization struct {
// 	ID          ID     `json:"id,omitempty"`
// 	Name        string `json:"name"`
// 	Description string `json:"description"`
// 	CRUDLog
// }

func TestOrg(t *testing.T) {
	driver := func() kv.Store {
		return inmem.NewKVStore()
	}

	simpleSetup := func(t *testing.T, store *tenant.Store, tx *tenant.Tx) {
		for i := 1; i <= 10; i++ {
			err := store.CreateOrg(context.Background(), tx, &influxdb.Organization{
				ID:     influxdb.ID(i),
				Name:   fmt.Sprintf("org%d", i),
				Description: "words",
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
						ID:     influxdb.ID(i),
						Name:   fmt.Sprintf("org%d", i),
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
			results: func(t *testing.T, store *tenant.Store, tx *tenant.Tx) {
				org, err := store.GetOrg(context.Background(), tx, 5)
				if err != nil {
					t.Fatal(err)
				}

				expected := &influxdb.Organization{
					ID:     5,
					Name:   "org5",
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

				if _, err := store.GetOrg(context.Background(), tx, 500); err != tenant.ErrOrgNotFound {
					t.Fatal("failed to get correct error when looking for invalid org by id")
				}

				if _, err := store.GetOrgByName(context.Background(), tx, "notaorg"); err != tenant.ErrOrgNotFound {
					t.Fatal("failed to get correct error when looking for invalid org by name")
				}

			},
		},
		{
			name:  "list",
			setup: simpleSetup,
			results: func(t *testing.T, store *tenant.Store, tx *tenant.Tx) {
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
						ID:     influxdb.ID(i),
						Name:   fmt.Sprintf("org%d", i),
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
			update: func(t *testing.T, store *tenant.Store, tx *tenant.Tx) {
				org5 := "org5"
				_, err := store.UpdateOrg(context.Background(), tx, influxdb.ID(3), influxdb.OrganizationUpdate{Name: &org5})
				if err != kv.NotUniqueError {
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
			results: func(t *testing.T, store *tenant.Store, tx *tenant.Tx) {
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
						ID:     influxdb.ID(i),
						Name:   fmt.Sprintf("org%d", i),
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
			update: func(t *testing.T, store *tenant.Store, tx *tenant.Tx) {
				err := store.DeleteOrg(context.Background(), tx, 1)
				if err != nil {
					t.Fatal(err)
				}

				err = store.DeleteOrg(context.Background(), tx, 1)
				if err != tenant.ErrOrgNotFound {
					t.Fatal("invalid error when deleting org that has already been deleted", err)
				}

				err = store.DeleteOrg(context.Background(), tx, 3)
				if err != nil {
					t.Fatal(err)
				}
			},
			results: func(t *testing.T, store *tenant.Store, tx *tenant.Tx) {
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
							ID:     influxdb.ID(i),
							Name:   fmt.Sprintf("org%d", i),
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

package tenant_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/kv"
	"github.com/influxdata/influxdb/v2/mock"
	"github.com/influxdata/influxdb/v2/tenant"
	itesting "github.com/influxdata/influxdb/v2/testing"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// type Organization struct {
// 	ID          ID     `json:"id,omitempty"`
// 	Name        string `json:"name"`
// 	Description string `json:"description"`
// 	CRUDLog
// }

const (
	firstOrgID platform.ID = (iota + 1)
	secondOrgID
	thirdOrgID
	fourthOrgID
	fifthOrgID
)

func TestOrg(t *testing.T) {
	var (
		aTime    = time.Date(2020, 7, 23, 10, 0, 0, 0, time.UTC)
		testOrgs = func(count int, visit ...func(*influxdb.Organization)) (orgs []*influxdb.Organization) {
			for i := 1; i <= count; i++ {
				org := &influxdb.Organization{
					ID:          platform.ID(i),
					Name:        fmt.Sprintf("org%d", i),
					Description: "words",
				}

				if len(visit) > 0 {
					visit[0](org)
				}

				orgs = append(orgs, org)
			}

			return
		}

		withCrudLog = func(o *influxdb.Organization) {
			o.CRUDLog = influxdb.CRUDLog{
				CreatedAt: aTime,
				UpdatedAt: aTime,
			}
		}

		simpleSetup = func(t *testing.T, store *tenant.Store, tx kv.Tx) {
			store.OrgIDGen = mock.NewIncrementingIDGenerator(1)
			for _, org := range testOrgs(10) {
				require.NoError(t, store.CreateOrg(context.Background(), tx, org))
			}
		}
	)

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
				orgs, err := store.ListOrgs(context.Background(), tx)
				if err != nil {
					t.Fatal(err)
				}

				assert.Len(t, orgs, 10)

				expected := testOrgs(10, withCrudLog)
				assert.Equal(t, expected, orgs)
			},
		},
		{
			name:  "get",
			setup: simpleSetup,
			results: func(t *testing.T, store *tenant.Store, tx kv.Tx) {
				org, err := store.GetOrg(context.Background(), tx, fifthOrgID)
				if err != nil {
					t.Fatal(err)
				}

				expected := &influxdb.Organization{
					ID:          fifthOrgID,
					Name:        "org5",
					Description: "words",
					CRUDLog: influxdb.CRUDLog{
						CreatedAt: org.CreatedAt,
						UpdatedAt: org.UpdatedAt,
					},
				}
				require.Equal(t, expected, org)

				org, err = store.GetOrgByName(context.Background(), tx, "org5")
				if err != nil {
					t.Fatal(err)
				}
				require.Equal(t, expected, org)

				if _, err := store.GetOrg(context.Background(), tx, 500); err != tenant.ErrOrgNotFound {
					t.Fatal("failed to get correct error when looking for invalid org by id")
				}

				if _, err := store.GetOrgByName(context.Background(), tx, "notaorg"); err.Error() != tenant.OrgNotFoundByName("notaorg").Error() {
					t.Fatal("failed to get correct error when looking for invalid org by name")
				}

			},
		},
		{
			name:  "list",
			setup: simpleSetup,
			results: func(t *testing.T, store *tenant.Store, tx kv.Tx) {
				orgs, err := store.ListOrgs(context.Background(), tx)
				if err != nil {
					t.Fatal(err)
				}

				require.Len(t, orgs, 10)

				expected := testOrgs(10, withCrudLog)
				require.Equal(t, expected, orgs)
				orgs, err = store.ListOrgs(context.Background(), tx, influxdb.FindOptions{Limit: 4})
				require.NoError(t, err)
				assert.Len(t, orgs, 4)
				assert.Equal(t, expected[:4], orgs)

				orgs, err = store.ListOrgs(context.Background(), tx, influxdb.FindOptions{Offset: 3})
				require.NoError(t, err)
				assert.Len(t, orgs, 7)
				assert.Equal(t, expected[3:], orgs)
			},
		},
		{
			name:  "update",
			setup: simpleSetup,
			update: func(t *testing.T, store *tenant.Store, tx kv.Tx) {
				org5 := "org5"
				_, err := store.UpdateOrg(context.Background(), tx, thirdOrgID, influxdb.OrganizationUpdate{Name: &org5})
				if err.Error() != tenant.OrgAlreadyExistsError(org5).Error() {
					t.Fatal("failed to error on duplicate orgname")
				}

				org30 := "org30"
				_, err = store.UpdateOrg(context.Background(), tx, thirdOrgID, influxdb.OrganizationUpdate{Name: &org30})
				require.NoError(t, err)

				description := "notWords"
				_, err = store.UpdateOrg(context.Background(), tx, thirdOrgID, influxdb.OrganizationUpdate{Description: &description})
				require.NoError(t, err)
			},
			results: func(t *testing.T, store *tenant.Store, tx kv.Tx) {
				orgs, err := store.ListOrgs(context.Background(), tx)
				require.NoError(t, err)

				assert.Len(t, orgs, 10)

				expected := testOrgs(10, withCrudLog)
				expected[2].Name = "org30"
				expected[2].Description = "notWords"
				require.Equal(t, expected, orgs)
			},
		},
		{
			name:  "delete",
			setup: simpleSetup,
			update: func(t *testing.T, store *tenant.Store, tx kv.Tx) {
				err := store.DeleteOrg(context.Background(), tx, firstOrgID)
				require.NoError(t, err)

				err = store.DeleteOrg(context.Background(), tx, firstOrgID)
				if err != tenant.ErrOrgNotFound {
					t.Fatal("invalid error when deleting org that has already been deleted", err)
				}

				err = store.DeleteOrg(context.Background(), tx, thirdOrgID)
				require.NoError(t, err)
			},
			results: func(t *testing.T, store *tenant.Store, tx kv.Tx) {
				orgs, err := store.ListOrgs(context.Background(), tx)
				require.NoError(t, err)
				assert.Len(t, orgs, 8)

				all := testOrgs(10, withCrudLog)
				// deleted first and third item
				expected := append(all[1:2], all[3:]...)
				require.Equal(t, expected, orgs)
			},
		},
	}
	for _, testScenario := range st {
		t.Run(testScenario.name, func(t *testing.T) {
			s := itesting.NewTestInmemStore(t)
			ts := tenant.NewStore(s, tenant.WithNow(func() time.Time {
				return aTime
			}))

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

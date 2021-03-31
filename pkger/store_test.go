package pkger_test

import (
	"context"
	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/kit/platform/errors"
	"testing"
	"time"

	"github.com/influxdata/influxdb/v2/inmem"
	"github.com/influxdata/influxdb/v2/kv/migration/all"
	"go.uber.org/zap/zaptest"

	"github.com/influxdata/influxdb/v2/pkger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStoreKV(t *testing.T) {
	inMemStore := inmem.NewKVStore()

	// run all migrations against store
	if err := all.Up(context.Background(), zaptest.NewLogger(t), inMemStore); err != nil {
		t.Fatal(err)
	}

	stackStub := func(id, orgID platform.ID) pkger.Stack {
		now := time.Time{}.Add(10 * 365 * 24 * time.Hour)
		urls := []string{
			"http://example.com",
			"http://abc.gov",
		}
		return pkger.Stack{
			ID:        id,
			OrgID:     orgID,
			CreatedAt: now,
			Events: []pkger.StackEvent{
				{
					EventType:    pkger.StackEventCreate,
					Name:         "threeve",
					Description:  "desc",
					UpdatedAt:    now.Add(time.Hour),
					Sources:      urls,
					TemplateURLs: urls,
					Resources: []pkger.StackResource{
						{
							APIVersion: pkger.APIVersion,
							ID:         9000,
							Kind:       pkger.KindBucket,
							MetaName:   "buzz lightyear",
							Associations: []pkger.StackResourceAssociation{{
								Kind:     pkger.KindLabel,
								MetaName: "foo_label",
							}},
						},
						{
							APIVersion: pkger.APIVersion,
							ID:         333,
							Kind:       pkger.KindBucket,
							MetaName:   "beyond",
						},
					},
				},
			},
		}
	}

	t.Run("create a stack", func(t *testing.T) {
		defer inMemStore.Flush(context.Background())

		storeKV := pkger.NewStoreKV(inMemStore)

		const orgID = 333
		seedEntities(t, storeKV, pkger.Stack{
			ID:    1,
			OrgID: orgID,
		})

		t.Run("with no ID collisions creates successfully", func(t *testing.T) {
			expected := stackStub(3, orgID)

			err := storeKV.CreateStack(context.Background(), expected)
			require.NoError(t, err)

			readStackEqual(t, storeKV, expected)
		})

		t.Run("with ID collisions fails with conflict error", func(t *testing.T) {
			for _, id := range []platform.ID{2, 3} {
				err := storeKV.CreateStack(context.Background(), pkger.Stack{
					ID:    1,
					OrgID: orgID,
				})
				require.Errorf(t, err, "id=%d", id)
				assert.Equalf(t, errors.EConflict, errors.ErrorCode(err), "id=%d", id)
			}
		})
	})

	t.Run("list stacks", func(t *testing.T) {
		defer inMemStore.Flush(context.Background())

		storeKV := pkger.NewStoreKV(inMemStore)

		const orgID1 = 1
		const orgID2 = 2
		seedEntities(t, storeKV,
			pkger.Stack{
				ID:    1,
				OrgID: orgID1,
				Events: []pkger.StackEvent{{
					Name: "first_name",
				}},
			},
			pkger.Stack{
				ID:    2,
				OrgID: orgID2,
				Events: []pkger.StackEvent{{
					Name: "first_name",
				}},
			},
			pkger.Stack{
				ID:    3,
				OrgID: orgID1,
				Events: []pkger.StackEvent{{
					Name: "second_name",
				}},
			},
			pkger.Stack{
				ID:    4,
				OrgID: orgID2,
				Events: []pkger.StackEvent{{
					Name: "second_name",
				}},
			},
		)

		tests := []struct {
			name     string
			orgID    platform.ID
			filter   pkger.ListFilter
			expected []pkger.Stack
		}{
			{
				name:  "by org id",
				orgID: orgID1,
				expected: []pkger.Stack{
					{
						ID:    1,
						OrgID: orgID1,
						Events: []pkger.StackEvent{{
							Name: "first_name",
						}},
					},
					{
						ID:    3,
						OrgID: orgID1,
						Events: []pkger.StackEvent{{
							Name: "second_name",
						}},
					},
				},
			},
			{
				name:  "by stack ids",
				orgID: orgID1,
				filter: pkger.ListFilter{
					StackIDs: []platform.ID{1, 3},
				},
				expected: []pkger.Stack{
					{
						ID:    1,
						OrgID: orgID1,
						Events: []pkger.StackEvent{{
							Name: "first_name",
						}},
					},
					{
						ID:    3,
						OrgID: orgID1,
						Events: []pkger.StackEvent{{
							Name: "second_name",
						}},
					},
				},
			},
			{
				name:  "by stack ids skips ids that belong to different organization",
				orgID: orgID1,
				filter: pkger.ListFilter{
					StackIDs: []platform.ID{1, 2, 4},
				},
				expected: []pkger.Stack{{
					ID:    1,
					OrgID: orgID1,
					Events: []pkger.StackEvent{{
						Name: "first_name",
					}},
				}},
			},
			{
				name:  "stack ids that do not exist are skipped",
				orgID: orgID1,
				filter: pkger.ListFilter{
					StackIDs: []platform.ID{1, 9000},
				},
				expected: []pkger.Stack{{
					ID:    1,
					OrgID: orgID1,
					Events: []pkger.StackEvent{{
						Name: "first_name",
					}},
				}},
			},
			{
				name:  "by name",
				orgID: orgID1,
				filter: pkger.ListFilter{
					Names: []string{"first_name"},
				},
				expected: []pkger.Stack{{
					ID:    1,
					OrgID: orgID1,
					Events: []pkger.StackEvent{{
						Name: "first_name",
					}},
				}},
			},
			{
				name:  "by name and id",
				orgID: orgID1,
				filter: pkger.ListFilter{
					StackIDs: []platform.ID{3},
					Names:    []string{"first_name"},
				},
				expected: []pkger.Stack{
					{
						ID:    1,
						OrgID: orgID1,
						Events: []pkger.StackEvent{{
							Name: "first_name",
						}},
					},
					{
						ID:    3,
						OrgID: orgID1,
						Events: []pkger.StackEvent{{
							Name: "second_name",
						}},
					},
				},
			},
		}

		for _, tt := range tests {
			fn := func(t *testing.T) {
				stacks, err := storeKV.ListStacks(context.Background(), tt.orgID, tt.filter)
				require.NoError(t, err)
				assert.Equal(t, tt.expected, stacks)
			}

			t.Run(tt.name, fn)
		}
	})

	t.Run("read a stack", func(t *testing.T) {
		defer inMemStore.Flush(context.Background())

		storeKV := pkger.NewStoreKV(inMemStore)

		expected := stackStub(1, 3)

		seedEntities(t, storeKV, expected)

		t.Run("with valid ID returns stack successfully", func(t *testing.T) {
			readStackEqual(t, storeKV, expected)
		})

		t.Run("when no match found fails with not found error", func(t *testing.T) {
			unmatchedID := platform.ID(3000)
			_, err := storeKV.ReadStackByID(context.Background(), unmatchedID)
			require.Error(t, err)
			assert.Equal(t, errors.ENotFound, errors.ErrorCode(err))
		})
	})

	t.Run("update a stack", func(t *testing.T) {
		defer inMemStore.Flush(context.Background())

		storeKV := pkger.NewStoreKV(inMemStore)

		const orgID = 3
		const id = 3
		expected := stackStub(id, orgID)

		seedEntities(t, storeKV, expected)

		t.Run("with valid ID updates stack successfully", func(t *testing.T) {
			expected := stackStub(id, orgID)
			event := expected.LatestEvent()
			event.EventType = pkger.StackEventUpdate
			event.UpdatedAt = event.UpdatedAt.Add(time.Hour)
			event.Resources = append(event.Resources, pkger.StackResource{
				APIVersion: pkger.APIVersion,
				ID:         333,
				Kind:       pkger.KindBucket,
				MetaName:   "beyond",
			})
			expected.Events = append(expected.Events, event)

			err := storeKV.UpdateStack(context.Background(), expected)
			require.NoError(t, err)

			readStackEqual(t, storeKV, expected)
		})

		t.Run("when no match found fails with not found error", func(t *testing.T) {
			unmatchedID := platform.ID(3000)
			err := storeKV.UpdateStack(context.Background(), pkger.Stack{
				ID:    unmatchedID,
				OrgID: orgID,
			})
			require.Error(t, err)
			assert.Equalf(t, errors.ENotFound, errors.ErrorCode(err), "err: %s", err)
		})

		t.Run("when org id does not match fails with unprocessable entity error", func(t *testing.T) {
			err := storeKV.UpdateStack(context.Background(), pkger.Stack{
				ID:    id,
				OrgID: orgID + 9000,
			})
			require.Error(t, err)
			assert.Equalf(t, errors.EUnprocessableEntity, errors.ErrorCode(err), "err: %s", err)
		})
	})

	t.Run("delete a stack", func(t *testing.T) {
		defer inMemStore.Flush(context.Background())

		storeKV := pkger.NewStoreKV(inMemStore)

		const orgID = 3
		expected := stackStub(1, orgID)

		seedEntities(t, storeKV, expected)

		t.Run("with valid ID deletes stack successfully", func(t *testing.T) {
			err := storeKV.DeleteStack(context.Background(), expected.ID)
			require.NoError(t, err)

			_, err = storeKV.ReadStackByID(context.Background(), expected.ID)
			require.Error(t, err)
			errCodeEqual(t, errors.ENotFound, err)
		})

		t.Run("when no match found fails with not found error", func(t *testing.T) {
			unmatchedID := platform.ID(3000)
			err := storeKV.DeleteStack(context.Background(), unmatchedID)
			require.Error(t, err)
			errCodeEqual(t, errors.ENotFound, err)
		})
	})
}

func readStackEqual(t *testing.T, store pkger.Store, expected pkger.Stack) {
	t.Helper()

	stack, err := store.ReadStackByID(context.Background(), expected.ID)
	require.NoError(t, err)
	assert.Equal(t, expected, stack)
}

func errCodeEqual(t *testing.T, expected string, actual error) {
	t.Helper()

	assert.Equalf(t, expected, errors.ErrorCode(actual), "err: %s", actual)
}

func seedEntities(t *testing.T, store pkger.Store, first pkger.Stack, rest ...pkger.Stack) {
	t.Helper()

	for _, st := range append(rest, first) {
		err := store.CreateStack(context.Background(), st)
		require.NoError(t, err)
	}
}

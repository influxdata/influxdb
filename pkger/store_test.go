package pkger_test

import (
	"context"
	"net/url"
	"testing"
	"time"

	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/inmem"
	"github.com/influxdata/influxdb/pkger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStoreKv(t *testing.T) {
	inMemStore := inmem.NewKVStore()

	stackStub := func(id influxdb.ID) pkger.Stack {
		now := time.Time{}.Add(10 * 365 * 24 * time.Hour)
		return pkger.Stack{
			ID:   id,
			Name: "threeve",
			Desc: "desc",
			CRUDLog: influxdb.CRUDLog{
				CreatedAt: now,
				UpdatedAt: now.Add(time.Hour),
			},
			URLS: []url.URL{
				newURL(t, "http://example.com"),
				newURL(t, "http://abc.gov"),
			},
			Resources: []pkger.StackResource{
				{
					APIVersion: pkger.APIVersion,
					ID:         9000,
					Kind:       pkger.KindBucket,
					Name:       "buzz lightyear",
				},
				{
					APIVersion: pkger.APIVersion,
					ID:         333,
					Kind:       pkger.KindBucket,
					Name:       "beyond",
				},
			},
		}
	}

	t.Run("create a stack", func(t *testing.T) {
		defer inMemStore.Flush(context.Background())

		storeKV := pkger.NewStoreKV(inMemStore)
		orgID := influxdb.ID(3)

		seedEntities(t, storeKV, orgID, pkger.Stack{
			ID: 1,
		})

		t.Run("with no ID collisions creates successfully", func(t *testing.T) {
			expected := stackStub(3)

			err := storeKV.CreateStack(context.Background(), orgID, expected)
			require.NoError(t, err)

			readStackEqual(t, storeKV, expected)
		})

		t.Run("with ID collisions fails with conflict error", func(t *testing.T) {
			for _, id := range []influxdb.ID{2, 3} {
				err := storeKV.CreateStack(context.Background(), orgID, pkger.Stack{ID: 1})
				require.Errorf(t, err, "id=%d", id)
				assert.Equalf(t, influxdb.EConflict, influxdb.ErrorCode(err), "id=%d", id)
			}
		})
	})

	t.Run("read a stack", func(t *testing.T) {
		defer inMemStore.Flush(context.Background())

		storeKV := pkger.NewStoreKV(inMemStore)
		orgID := influxdb.ID(3)

		expected := stackStub(1)

		seedEntities(t, storeKV, orgID, expected)

		t.Run("with valid ID returns stack successfully", func(t *testing.T) {
			readStackEqual(t, storeKV, expected)
		})

		t.Run("when no match found fails with not found error", func(t *testing.T) {
			unmatchedID := influxdb.ID(3000)
			_, err := storeKV.ReadStackByID(context.Background(), unmatchedID)
			require.Error(t, err)
			assert.Equal(t, influxdb.ENotFound, influxdb.ErrorCode(err))
		})
	})

	t.Run("update a stack", func(t *testing.T) {
		defer inMemStore.Flush(context.Background())

		storeKV := pkger.NewStoreKV(inMemStore)
		orgID := influxdb.ID(3)

		expected := stackStub(1)

		seedEntities(t, storeKV, orgID, expected)

		t.Run("with valid ID updates stack successfully", func(t *testing.T) {
			updateStack := expected
			updateStack.Resources = append(updateStack.Resources, pkger.StackResource{
				APIVersion: pkger.APIVersion,
				ID:         333,
				Kind:       pkger.KindBucket,
				Name:       "beyond",
			})

			err := storeKV.UpdateStack(context.Background(), orgID, updateStack)
			require.NoError(t, err)

			readStackEqual(t, storeKV, updateStack)
		})

		t.Run("when no match found fails with not found error", func(t *testing.T) {
			unmatchedID := influxdb.ID(3000)
			err := storeKV.UpdateStack(context.Background(), orgID, pkger.Stack{ID: unmatchedID})
			require.Error(t, err)
			assert.Equalf(t, influxdb.ENotFound, influxdb.ErrorCode(err), "err: %s", err)
		})
	})

	t.Run("delete a stack", func(t *testing.T) {
		defer inMemStore.Flush(context.Background())

		storeKV := pkger.NewStoreKV(inMemStore)
		orgID := influxdb.ID(3)

		expected := stackStub(1)

		seedEntities(t, storeKV, orgID, expected)

		t.Run("with valid ID deletes stack successfully", func(t *testing.T) {
			err := storeKV.DeleteStack(context.Background(), expected.ID)
			require.NoError(t, err)

			_, err = storeKV.ReadStackByID(context.Background(), expected.ID)
			require.Error(t, err)
			errCodeEqual(t, influxdb.ENotFound, err)
		})

		t.Run("when no match found fails with not found error", func(t *testing.T) {
			unmatchedID := influxdb.ID(3000)
			err := storeKV.DeleteStack(context.Background(), unmatchedID)
			require.Error(t, err)
			errCodeEqual(t, influxdb.ENotFound, err)
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

	assert.Equalf(t, expected, influxdb.ErrorCode(actual), "err: %s", actual)
}

func seedEntities(t *testing.T, store pkger.Store, orgID influxdb.ID, first pkger.Stack, rest ...pkger.Stack) {
	t.Helper()

	for _, st := range append(rest, first) {
		err := store.CreateStack(context.Background(), orgID, st)
		require.NoError(t, err)
	}
}

func newURL(t *testing.T, rawurl string) url.URL {
	t.Helper()

	u, err := url.Parse(rawurl)
	require.NoError(t, err)

	return *u
}

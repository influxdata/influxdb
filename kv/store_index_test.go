package kv_test

import (
	"context"
	"testing"

	"github.com/influxdata/influxdb/kv"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIndexStore(t *testing.T) {
	newStoreBase := func(resource string, bktName []byte, encKeyFn, encBodyFn kv.EncodeEntFn, decFn kv.DecodeBucketValFn, decToEntFn kv.ConvertValToEntFn) *kv.StoreBase {
		return kv.NewStoreBase(resource, bktName, encKeyFn, encBodyFn, decFn, decToEntFn)
	}

	newFooIndexStore := func(t *testing.T, bktSuffix string) (*kv.IndexStore, func(), kv.Store) {
		t.Helper()

		inmemStore, done, err := NewTestBoltStore(t)
		require.NoError(t, err)

		const resource = "foo"

		indexStore := &kv.IndexStore{
			Resource:   resource,
			EntStore:   newStoreBase(resource, []byte("foo_ent_"+bktSuffix), kv.EncIDKey, kv.EncBodyJSON, decJSONFooFn, decFooEntFn),
			IndexStore: kv.NewOrgNameKeyStore(resource, []byte("foo_idx_"+bktSuffix), false),
		}

		return indexStore, done, inmemStore
	}

	t.Run("Put", func(t *testing.T) {
		indexStore, done, inmem := newFooIndexStore(t, "put")
		defer done()

		expected := testPutBase(t, inmem, indexStore, indexStore.EntStore.BktName)

		key, err := indexStore.IndexStore.EntKey(context.TODO(), kv.Entity{
			UniqueKey: kv.Encode(kv.EncID(expected.OrgID), kv.EncString(expected.Name)),
		})
		require.NoError(t, err)

		rawIndex := getEntRaw(t, inmem, indexStore.IndexStore.BktName, key)
		assert.Equal(t, encodeID(t, expected.ID), rawIndex)
	})

	t.Run("DeleteEnt", func(t *testing.T) {
		indexStore, done, inmem := newFooIndexStore(t, "delete_ent")
		defer done()

		expected := testDeleteEntBase(t, inmem, indexStore)

		err := inmem.View(context.TODO(), func(tx kv.Tx) error {
			_, err := indexStore.IndexStore.FindEnt(context.TODO(), tx, kv.Entity{
				UniqueKey: expected.UniqueKey,
			})
			return err
		})
		isNotFoundErr(t, err)
	})

	t.Run("Delete", func(t *testing.T) {
		fn := func(t *testing.T, suffix string) (storeBase, func(), kv.Store) {
			return newFooIndexStore(t, suffix)
		}

		testDeleteBase(t, fn, func(t *testing.T, kvStore kv.Store, base storeBase, foosLeft []foo) {
			var expectedIndexIDs []interface{}
			for _, ent := range foosLeft {
				expectedIndexIDs = append(expectedIndexIDs, ent.ID)
			}

			indexStore, ok := base.(*kv.IndexStore)
			require.True(t, ok)

			// next to verify they are not within the index store
			var actualIDs []interface{}
			view(t, kvStore, func(tx kv.Tx) error {
				return indexStore.IndexStore.Find(context.TODO(), tx, kv.FindOpts{
					CaptureFn: func(key []byte, decodedVal interface{}) error {
						actualIDs = append(actualIDs, decodedVal)
						return nil
					},
				})
			})

			assert.Equal(t, expectedIndexIDs, actualIDs)
		})
	})

	t.Run("FindEnt", func(t *testing.T) {
		t.Run("by ID", func(t *testing.T) {
			base, done, inmemStore := newFooIndexStore(t, "find_ent")
			defer done()
			testFindEnt(t, inmemStore, base)
		})

		t.Run("find by name", func(t *testing.T) {
			base, done, kvStore := newFooIndexStore(t, "find_ent")
			defer done()

			expected := newFooEnt(1, 9000, "foo_1")
			seedEnts(t, kvStore, base, expected)

			var actual interface{}
			view(t, kvStore, func(tx kv.Tx) error {
				f, err := base.FindEnt(context.TODO(), tx, kv.Entity{
					UniqueKey: expected.UniqueKey,
				})
				actual = f
				return err
			})

			assert.Equal(t, expected.Body, actual)
		})
	})

	t.Run("Find", func(t *testing.T) {
		t.Run("base", func(t *testing.T) {
			fn := func(t *testing.T, suffix string) (storeBase, func(), kv.Store) {
				return newFooIndexStore(t, suffix)
			}

			testFind(t, fn)
		})

		t.Run("with entity filter", func(t *testing.T) {
			base, done, kvStore := newFooIndexStore(t, "find_index_search")
			defer done()

			expectedEnts := []kv.Entity{
				newFooEnt(1, 9000, "foo_0"),
				newFooEnt(2, 9001, "foo_1"),
				newFooEnt(3, 9003, "foo_2"),
			}

			seedEnts(t, kvStore, base, expectedEnts...)

			var actuals []interface{}
			view(t, kvStore, func(tx kv.Tx) error {
				return base.Find(context.TODO(), tx, kv.FindOpts{
					FilterEntFn: func(key []byte, decodedVal interface{}) bool {
						return decodedVal.(foo).ID < 3
					},
					CaptureFn: func(key []byte, decodedVal interface{}) error {
						actuals = append(actuals, decodedVal)
						return nil
					},
				})
			})

			expected := []interface{}{
				expectedEnts[0].Body,
				expectedEnts[1].Body,
			}
			assert.Equal(t, expected, actuals)
		})

		t.Run("lookup via orgID", func(t *testing.T) {
			base, done, kvStore := newFooIndexStore(t, "find_index_search")
			defer done()

			expectedEnts := []kv.Entity{
				newFooEnt(1, 9000, "foo_0"),
				newFooEnt(2, 9001, "foo_1"),
				newFooEnt(3, 9003, "foo_2"),
				newFooEnt(4, 9003, "foo_3"),
			}

			seedEnts(t, kvStore, base, expectedEnts...)

			var actuals []interface{}
			view(t, kvStore, func(tx kv.Tx) error {
				return base.Find(context.TODO(), tx, kv.FindOpts{
					Prefix: encodeID(t, 9003),
					CaptureFn: func(key []byte, decodedVal interface{}) error {
						actuals = append(actuals, decodedVal)
						return nil
					},
				})
			})

			expected := []interface{}{
				expectedEnts[2].Body,
				expectedEnts[3].Body,
			}
			assert.Equal(t, expected, actuals)
		})
	})
}

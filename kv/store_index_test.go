package kv_test

import (
	"context"
	"testing"

	"github.com/influxdata/influxdb/v2/kit/platform/errors"
	"github.com/influxdata/influxdb/v2/kv"
	"github.com/influxdata/influxdb/v2/kv/migration"
	itesting "github.com/influxdata/influxdb/v2/testing"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIndexStore(t *testing.T) {
	newStoreBase := func(resource string, bktName []byte, encKeyFn, encBodyFn kv.EncodeEntFn, decFn kv.DecodeBucketValFn, decToEntFn kv.ConvertValToEntFn) *kv.StoreBase {
		return kv.NewStoreBase(resource, bktName, encKeyFn, encBodyFn, decFn, decToEntFn)
	}

	newFooIndexStore := func(t *testing.T, bktSuffix string) (*kv.IndexStore, func(), kv.Store) {
		t.Helper()

		kvStoreStore, done, err := itesting.NewTestBoltStore(t)
		require.NoError(t, err)

		const resource = "foo"

		bucketName := []byte("foo_ent_" + bktSuffix)
		indexBucketName := []byte("foo_idx+" + bktSuffix)

		ctx := context.Background()
		if err := migration.CreateBuckets("add foo buckets", bucketName, indexBucketName).Up(ctx, kvStoreStore); err != nil {
			t.Fatal(err)
		}

		indexStore := &kv.IndexStore{
			Resource:   resource,
			EntStore:   newStoreBase(resource, bucketName, kv.EncIDKey, kv.EncBodyJSON, decJSONFooFn, decFooEntFn),
			IndexStore: kv.NewOrgNameKeyStore(resource, indexBucketName, false),
		}

		return indexStore, done, kvStoreStore
	}

	t.Run("Put", func(t *testing.T) {
		t.Run("basic", func(t *testing.T) {
			indexStore, done, kvStore := newFooIndexStore(t, "put")
			defer done()

			expected := testPutBase(t, kvStore, indexStore, indexStore.EntStore.BktName)

			key, err := indexStore.IndexStore.EntKey(context.TODO(), kv.Entity{
				UniqueKey: kv.Encode(kv.EncID(expected.OrgID), kv.EncString(expected.Name)),
			})
			require.NoError(t, err)

			rawIndex := getEntRaw(t, kvStore, indexStore.IndexStore.BktName, key)
			assert.Equal(t, encodeID(t, expected.ID), rawIndex)
		})

		t.Run("new entity", func(t *testing.T) {
			indexStore, done, kvStore := newFooIndexStore(t, "put")
			defer done()

			expected := foo{ID: 3, OrgID: 33, Name: "333"}
			update(t, kvStore, func(tx kv.Tx) error {
				ent := newFooEnt(expected.ID, expected.OrgID, expected.Name)
				return indexStore.Put(context.TODO(), tx, ent, kv.PutNew())
			})

			key, err := indexStore.IndexStore.EntKey(context.TODO(), kv.Entity{
				UniqueKey: kv.Encode(kv.EncID(expected.OrgID), kv.EncString(expected.Name)),
			})
			require.NoError(t, err)

			rawIndex := getEntRaw(t, kvStore, indexStore.IndexStore.BktName, key)
			assert.Equal(t, encodeID(t, expected.ID), rawIndex)
		})

		t.Run("updating entity that doesn't exist returns not found error", func(t *testing.T) {
			indexStore, done, kvStore := newFooIndexStore(t, "put")
			defer done()

			expected := testPutBase(t, kvStore, indexStore, indexStore.EntStore.BktName)

			err := kvStore.Update(context.Background(), func(tx kv.Tx) error {
				ent := newFooEnt(33333, expected.OrgID, "safe name")
				return indexStore.Put(context.TODO(), tx, ent, kv.PutUpdate())
			})
			require.Error(t, err)
			assert.Equal(t, errors.ENotFound, errors.ErrorCode(err))
		})

		t.Run("updating entity with no naming collision succeeds", func(t *testing.T) {
			indexStore, done, kvStore := newFooIndexStore(t, "put")
			defer done()

			expected := testPutBase(t, kvStore, indexStore, indexStore.EntStore.BktName)

			update(t, kvStore, func(tx kv.Tx) error {
				entCopy := newFooEnt(expected.ID, expected.OrgID, "safe name")
				return indexStore.Put(context.TODO(), tx, entCopy, kv.PutUpdate())
			})

			err := kvStore.View(context.TODO(), func(tx kv.Tx) error {
				_, err := indexStore.FindEnt(context.TODO(), tx, kv.Entity{
					PK:        kv.EncID(expected.ID),
					UniqueKey: kv.Encode(kv.EncID(expected.OrgID), kv.EncString(expected.Name)),
				})
				return err
			})
			require.NoError(t, err)
		})

		t.Run("updating an existing entity to a new unique identifier should delete the existing unique key", func(t *testing.T) {
			indexStore, done, kvStore := newFooIndexStore(t, "put")
			defer done()

			expected := testPutBase(t, kvStore, indexStore, indexStore.EntStore.BktName)

			update(t, kvStore, func(tx kv.Tx) error {
				entCopy := newFooEnt(expected.ID, expected.OrgID, "safe name")
				return indexStore.Put(context.TODO(), tx, entCopy, kv.PutUpdate())
			})

			update(t, kvStore, func(tx kv.Tx) error {
				ent := newFooEnt(33, expected.OrgID, expected.Name)
				return indexStore.Put(context.TODO(), tx, ent, kv.PutNew())
			})
		})

		t.Run("error cases", func(t *testing.T) {
			t.Run("new entity conflicts with existing", func(t *testing.T) {
				indexStore, done, kvStore := newFooIndexStore(t, "put")
				defer done()

				expected := testPutBase(t, kvStore, indexStore, indexStore.EntStore.BktName)

				err := kvStore.Update(context.TODO(), func(tx kv.Tx) error {
					entCopy := newFooEnt(expected.ID, expected.OrgID, expected.Name)
					return indexStore.Put(context.TODO(), tx, entCopy, kv.PutNew())
				})
				require.Error(t, err)
				assert.Equal(t, errors.EConflict, errors.ErrorCode(err))
			})

			t.Run("updating entity that does not exist", func(t *testing.T) {
				indexStore, done, kvStore := newFooIndexStore(t, "put")
				defer done()

				expected := testPutBase(t, kvStore, indexStore, indexStore.EntStore.BktName)

				update(t, kvStore, func(tx kv.Tx) error {
					ent := newFooEnt(9000, expected.OrgID, "name1")
					return indexStore.Put(context.TODO(), tx, ent, kv.PutNew())
				})

				err := kvStore.Update(context.TODO(), func(tx kv.Tx) error {
					// ent by id does not exist
					entCopy := newFooEnt(333, expected.OrgID, "name1")
					return indexStore.Put(context.TODO(), tx, entCopy, kv.PutUpdate())
				})
				require.Error(t, err)
				assert.Equal(t, errors.ENotFound, errors.ErrorCode(err), "got: "+err.Error())
			})

			t.Run("updating entity that does collides with an existing entity", func(t *testing.T) {
				indexStore, done, kvStore := newFooIndexStore(t, "put")
				defer done()

				expected := testPutBase(t, kvStore, indexStore, indexStore.EntStore.BktName)

				update(t, kvStore, func(tx kv.Tx) error {
					ent := newFooEnt(9000, expected.OrgID, "name1")
					return indexStore.Put(context.TODO(), tx, ent, kv.PutNew())
				})

				err := kvStore.Update(context.TODO(), func(tx kv.Tx) error {
					// name conflicts
					entCopy := newFooEnt(expected.ID, expected.OrgID, "name1")
					return indexStore.Put(context.TODO(), tx, entCopy, kv.PutUpdate())
				})
				require.Error(t, err)
				assert.Equal(t, errors.EConflict, errors.ErrorCode(err))
				assert.Contains(t, err.Error(), "update conflicts")
			})
		})
	})

	t.Run("DeleteEnt", func(t *testing.T) {
		indexStore, done, kvStore := newFooIndexStore(t, "delete_ent")
		defer done()

		expected := testDeleteEntBase(t, kvStore, indexStore)

		err := kvStore.View(context.TODO(), func(tx kv.Tx) error {
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
			base, done, kvStoreStore := newFooIndexStore(t, "find_ent")
			defer done()
			testFindEnt(t, kvStoreStore, base)
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
	})
}

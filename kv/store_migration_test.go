package kv_test

import (
	"context"
	"testing"

	"github.com/influxdata/influxdb/kv"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMigrationIndexStore(t *testing.T) {
	newStoreBase := func(resource string, bktName []byte, encKeyFn, encBodyFn kv.EncodeEntFn, decFn kv.DecodeBucketValFn, decToEntFn kv.ConvertValToEntFn) *kv.StoreBase {
		return kv.NewStoreBase(resource, bktName, encKeyFn, encBodyFn, decFn, decToEntFn)
	}

	newFooMigrationIndexStore := func(t *testing.T, bktSuffix string) (*kv.MigrationIndexStore, func(), kv.Store) {
		t.Helper()

		bolt, done, err := NewTestBoltStore(t)
		require.NoError(t, err)

		const resource = "foo"

		indexStore := &kv.IndexStore{
			Resource:   resource,
			EntStore:   newStoreBase(resource, []byte("foo_ent_"+bktSuffix), kv.EncIDKey, kv.EncBodyJSON, decJSONFooFn, decFooEntFn),
			IndexStore: kv.NewOrgNameKeyStore(resource, []byte("foo_idx_"+bktSuffix), false),
		}

		migStore := kv.NewMigrationIndexStore(indexStore)
		update(t, bolt, func(tx kv.Tx) error {
			return migStore.Init(context.TODO(), tx)
		})

		return migStore, done, bolt
	}

	t.Run("up", func(t *testing.T) {
		expectedEnts := []kv.Entity{
			newFooEnt(1, 9000, "foo_0"),
			newFooEnt(2, 9000, "foo_1"),
			newFooEnt(3, 9003, "foo_2"),
			newFooEnt(4, 9004, "foo_3"),
		}

		t.Run("apply migration to sync index and entity stores", func(t *testing.T) {
			migStore, done, kvStore := newFooMigrationIndexStore(t, "up")
			defer done()

			// seed ents to the entity store so that the entity store and index store
			// are out of sync, represents adding an index that hasn't been synced
			// with entity store.
			seedEnts(t, kvStore, migStore.EntStore, expectedEnts...)

			allIndexesFn := func(t *testing.T) []interface{} {
				t.Helper()

				var actuals []interface{}
				view(t, kvStore, func(tx kv.Tx) error {
					return migStore.IndexStore.IndexStore.Find(context.TODO(), tx, kv.FindOpts{
						CaptureFn: func(key []byte, decodedVal interface{}) error {
							actuals = append(actuals, decodedVal)
							return nil
						},
					})
				})
				return actuals
			}
			require.Empty(t, allIndexesFn(t))

			mig := kv.Migration{
				Name:        "mig_1",
				Description: "sync ",
				Up:          kv.MigrationSyncIndexStore,
			}

			update(t, kvStore, func(tx kv.Tx) error {
				return migStore.Up(context.TODO(), tx, mig)
			})

			var expected []interface{}
			for _, ent := range expectedEnts {
				f, ok := ent.Body.(foo)
				require.True(t, ok)
				expected = append(expected, f.ID)
			}
			assert.Equal(t, expected, allIndexesFn(t))
		})

		t.Run("apply migration to update entities", func(t *testing.T) {
			migStore, done, kvStore := newFooMigrationIndexStore(t, "up")
			defer done()

			// seed ents to the entity store so that the entity store and index store
			// are out of sync, represents adding an index that hasn't been synced
			// with entity store.
			seedEnts(t, kvStore, migStore, expectedEnts...)

			mig := kv.Migration{
				Name:        "mig_1",
				Description: "sync ",
				Up: func(ctx context.Context, tx kv.Tx, store *kv.IndexStore) error {
					return store.Find(ctx, tx, kv.FindOpts{
						FilterEntFn: func(key []byte, decodedVal interface{}) bool {
							return decodedVal.(foo).OrgID > 9000
						},
						CaptureFn: func(key []byte, decodedVal interface{}) error {
							f := decodedVal.(foo)
							f.Name = f.Name + "_new"
							ent, err := store.EntStore.ConvertValToEntFn(key, f)
							if err != nil {
								return err
							}
							return store.Put(ctx, tx, ent)
						},
					})
				},
			}

			update(t, kvStore, func(tx kv.Tx) error {
				return migStore.Up(context.TODO(), tx, mig)
			})

			var actuals []interface{}
			view(t, kvStore, func(tx kv.Tx) error {
				return migStore.Find(context.TODO(), tx, kv.FindOpts{
					CaptureFn: func(key []byte, decodedVal interface{}) error {
						actuals = append(actuals, decodedVal)
						return nil
					},
				})
			})

			var expected []interface{}
			for _, ent := range expectedEnts {
				f := ent.Body.(foo)
				if f.OrgID > 9000 {
					f.Name = f.Name + "_new"
				}
				view(t, kvStore, func(tx kv.Tx) error {
					// search the index
					ff, err := migStore.FindEnt(context.TODO(), tx, kv.Entity{
						UniqueKey: kv.Encode(kv.EncID(f.OrgID), kv.EncString(f.Name)),
					})
					assert.Equal(t, f, ff)
					return err
				})
				expected = append(expected, f)
			}
			assert.Equal(t, expected, actuals)
		})
	})
}

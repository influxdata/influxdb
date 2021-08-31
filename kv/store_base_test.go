package kv_test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/kit/platform/errors"
	"github.com/influxdata/influxdb/v2/kv"
	"github.com/influxdata/influxdb/v2/kv/migration"
	itesting "github.com/influxdata/influxdb/v2/testing"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStoreBase(t *testing.T) {
	newStoreBase := func(t *testing.T, bktSuffix string, encKeyFn, encBodyFn kv.EncodeEntFn, decFn kv.DecodeBucketValFn, decToEntFn kv.ConvertValToEntFn) (*kv.StoreBase, func(), kv.Store) {
		t.Helper()

		inmemSVC, done, err := itesting.NewTestBoltStore(t)
		require.NoError(t, err)

		bucket := []byte("foo_" + bktSuffix)
		store := kv.NewStoreBase("foo", bucket, encKeyFn, encBodyFn, decFn, decToEntFn)

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		migrationName := fmt.Sprintf("create bucket %q", string(bucket))
		migration.CreateBuckets(migrationName, bucket).Up(ctx, inmemSVC)
		require.NoError(t, err)

		return store, done, inmemSVC
	}

	newFooStoreBase := func(t *testing.T, bktSuffix string) (*kv.StoreBase, func(), kv.Store) {
		return newStoreBase(t, bktSuffix, kv.EncIDKey, kv.EncBodyJSON, decJSONFooFn, decFooEntFn)
	}

	t.Run("Put", func(t *testing.T) {
		t.Run("basic", func(t *testing.T) {
			base, done, kvStore := newFooStoreBase(t, "put")
			defer done()

			testPutBase(t, kvStore, base, base.BktName)
		})

		t.Run("new", func(t *testing.T) {
			base, done, kvStore := newFooStoreBase(t, "put")
			defer done()

			expected := newFooEnt(3, 33, "name3")
			update(t, kvStore, func(tx kv.Tx) error {
				return base.Put(context.TODO(), tx, expected, kv.PutNew())
			})

			var actual interface{}
			view(t, kvStore, func(tx kv.Tx) error {
				f, err := base.FindEnt(context.TODO(), tx, kv.Entity{PK: expected.PK})
				actual = f
				return err
			})

			assert.Equal(t, expected.Body, actual)
		})

		t.Run("update", func(t *testing.T) {
			base, done, kvStore := newFooStoreBase(t, "put")
			defer done()

			expected := testPutBase(t, kvStore, base, base.BktName)

			updateEnt := newFooEnt(expected.ID, expected.OrgID, "new name")
			update(t, kvStore, func(tx kv.Tx) error {
				return base.Put(context.TODO(), tx, updateEnt, kv.PutUpdate())
			})

			var actual interface{}
			view(t, kvStore, func(tx kv.Tx) error {
				f, err := base.FindEnt(context.TODO(), tx, kv.Entity{PK: kv.EncID(expected.ID)})
				actual = f
				return err
			})

			expected.Name = "new name"
			assert.Equal(t, expected, actual)
		})

		t.Run("error cases", func(t *testing.T) {
			t.Run("new entity conflicts with existing", func(t *testing.T) {
				base, done, kvStore := newFooStoreBase(t, "put")
				defer done()

				expected := testPutBase(t, kvStore, base, base.BktName)

				err := kvStore.Update(context.TODO(), func(tx kv.Tx) error {
					entCopy := newFooEnt(expected.ID, expected.OrgID, expected.Name)
					return base.Put(context.TODO(), tx, entCopy, kv.PutNew())
				})
				require.Error(t, err)
				assert.Equal(t, errors.EConflict, errors.ErrorCode(err))
			})
		})

		t.Run("updating entity that does not exist", func(t *testing.T) {
			base, done, kvStore := newFooStoreBase(t, "put")
			defer done()

			expected := testPutBase(t, kvStore, base, base.BktName)

			err := kvStore.Update(context.TODO(), func(tx kv.Tx) error {
				// ent by id does not exist
				entCopy := newFooEnt(333, expected.OrgID, "name1")
				return base.Put(context.TODO(), tx, entCopy, kv.PutUpdate())
			})
			require.Error(t, err)
			assert.Equal(t, errors.ENotFound, errors.ErrorCode(err))
		})
	})

	t.Run("DeleteEnt", func(t *testing.T) {
		base, done, kvStore := newFooStoreBase(t, "delete_ent")
		defer done()

		testDeleteEntBase(t, kvStore, base)
	})

	t.Run("Delete", func(t *testing.T) {
		testDeleteBase(t, func(t *testing.T, suffix string) (storeBase, func(), kv.Store) {
			return newFooStoreBase(t, suffix)
		})
	})

	t.Run("FindEnt", func(t *testing.T) {
		base, done, kvStore := newFooStoreBase(t, "find_ent")
		defer done()

		testFindEnt(t, kvStore, base)
	})

	t.Run("Find", func(t *testing.T) {
		testFind(t, func(t *testing.T, suffix string) (storeBase, func(), kv.Store) {
			return newFooStoreBase(t, suffix)
		})
	})
}

func testPutBase(t *testing.T, kvStore kv.Store, base storeBase, bktName []byte) foo {
	t.Helper()

	expected := foo{
		ID:    1,
		OrgID: 9000,
		Name:  "foo_1",
	}

	update(t, kvStore, func(tx kv.Tx) error {
		return base.Put(context.TODO(), tx, kv.Entity{
			PK:        kv.EncID(expected.ID),
			UniqueKey: kv.Encode(kv.EncID(expected.OrgID), kv.EncString(expected.Name)),
			Body:      expected,
		})
	})

	var actual foo
	decodeJSON(t, getEntRaw(t, kvStore, bktName, encodeID(t, expected.ID)), &actual)

	assert.Equal(t, expected, actual)

	return expected
}

func testDeleteEntBase(t *testing.T, kvStore kv.Store, base storeBase) kv.Entity {
	t.Helper()

	expected := newFooEnt(1, 9000, "foo_1")
	seedEnts(t, kvStore, base, expected)

	update(t, kvStore, func(tx kv.Tx) error {
		return base.DeleteEnt(context.TODO(), tx, kv.Entity{PK: expected.PK})
	})

	err := kvStore.View(context.TODO(), func(tx kv.Tx) error {
		_, err := base.FindEnt(context.TODO(), tx, kv.Entity{PK: expected.PK})
		return err
	})
	isNotFoundErr(t, err)
	return expected
}

func testDeleteBase(t *testing.T, fn func(t *testing.T, suffix string) (storeBase, func(), kv.Store), assertFns ...func(*testing.T, kv.Store, storeBase, []foo)) {
	expectedEnts := []kv.Entity{
		newFooEnt(1, 9000, "foo_0"),
		newFooEnt(2, 9000, "foo_1"),
		newFooEnt(3, 9003, "foo_2"),
		newFooEnt(4, 9004, "foo_3"),
	}

	tests := []struct {
		name     string
		opts     kv.DeleteOpts
		expected []interface{}
	}{
		{
			name: "delete all",
			opts: kv.DeleteOpts{
				FilterFn: func(k []byte, v interface{}) bool {
					return true
				},
			},
		},
		{
			name: "delete IDs less than 4",
			opts: kv.DeleteOpts{
				FilterFn: func(k []byte, v interface{}) bool {
					if f, ok := v.(foo); ok {
						return f.ID < 4
					}
					return true
				},
			},
			expected: toIfaces(expectedEnts[3]),
		},
	}

	for _, tt := range tests {
		fn := func(t *testing.T) {
			t.Helper()

			base, done, kvStore := fn(t, "delete")
			defer done()

			seedEnts(t, kvStore, base, expectedEnts...)

			update(t, kvStore, func(tx kv.Tx) error {
				return base.Delete(context.TODO(), tx, tt.opts)
			})

			var actuals []interface{}
			view(t, kvStore, func(tx kv.Tx) error {
				return base.Find(context.TODO(), tx, kv.FindOpts{
					CaptureFn: func(key []byte, decodedVal interface{}) error {
						actuals = append(actuals, decodedVal)
						return nil
					},
				})
			})

			assert.Equal(t, tt.expected, actuals)

			var entsLeft []foo
			for _, expected := range tt.expected {
				ent, ok := expected.(foo)
				require.Truef(t, ok, "got: %#v", expected)
				entsLeft = append(entsLeft, ent)
			}

			for _, assertFn := range assertFns {
				assertFn(t, kvStore, base, entsLeft)
			}
		}
		t.Run(tt.name, fn)
	}
}

func testFindEnt(t *testing.T, kvStore kv.Store, base storeBase) kv.Entity {
	t.Helper()

	expected := newFooEnt(1, 9000, "foo_1")
	seedEnts(t, kvStore, base, expected)

	var actual interface{}
	view(t, kvStore, func(tx kv.Tx) error {
		f, err := base.FindEnt(context.TODO(), tx, kv.Entity{PK: expected.PK})
		actual = f
		return err
	})

	assert.Equal(t, expected.Body, actual)

	return expected
}

func testFind(t *testing.T, fn func(t *testing.T, suffix string) (storeBase, func(), kv.Store)) {
	t.Helper()

	expectedEnts := []kv.Entity{
		newFooEnt(1, 9000, "foo_0"),
		newFooEnt(2000, 9000, "foo_1"),
		newFooEnt(3000000, 9003, "foo_2"),
		newFooEnt(4000000000, 9004, "foo_3"),
	}

	tests := []struct {
		name     string
		opts     kv.FindOpts
		expected []interface{}
	}{
		{
			name:     "no options",
			expected: toIfaces(expectedEnts...),
		},
		{
			name:     "with order descending",
			opts:     kv.FindOpts{Descending: true},
			expected: reverseSlc(toIfaces(expectedEnts...)),
		},
		{
			name:     "with limit",
			opts:     kv.FindOpts{Limit: 1},
			expected: toIfaces(expectedEnts[0]),
		},
		{
			name:     "with offset",
			opts:     kv.FindOpts{Offset: 1},
			expected: toIfaces(expectedEnts[1:]...),
		},
		{
			name: "with offset and limit",
			opts: kv.FindOpts{
				Limit:  1,
				Offset: 1,
			},
			expected: toIfaces(expectedEnts[1]),
		},
		{
			name: "with descending, offset, and limit",
			opts: kv.FindOpts{
				Descending: true,
				Limit:      1,
				Offset:     1,
			},
			expected: toIfaces(expectedEnts[2]),
		},
		{
			name: "with id prefix",
			opts: kv.FindOpts{
				Prefix: encodeID(t, 3000000)[:platform.IDLength-5],
			},
			expected: toIfaces(expectedEnts[2], expectedEnts[3]),
		},
	}

	for _, tt := range tests {
		fn := func(t *testing.T) {
			base, done, kvStore := fn(t, "find")
			defer done()

			seedEnts(t, kvStore, base, expectedEnts...)

			var actuals []interface{}
			tt.opts.CaptureFn = func(key []byte, decodedVal interface{}) error {
				actuals = append(actuals, decodedVal)
				return nil
			}

			view(t, kvStore, func(tx kv.Tx) error {
				return base.Find(context.TODO(), tx, tt.opts)
			})

			assert.Equal(t, tt.expected, actuals)
		}
		t.Run(tt.name, fn)
	}
}

type foo struct {
	ID    platform.ID
	OrgID platform.ID

	Name string
}

func decodeJSON(t *testing.T, b []byte, v interface{}) {
	t.Helper()
	require.NoError(t, json.Unmarshal(b, &v))
}

type storeBase interface {
	Delete(ctx context.Context, tx kv.Tx, opts kv.DeleteOpts) error
	DeleteEnt(ctx context.Context, tx kv.Tx, ent kv.Entity) error
	FindEnt(ctx context.Context, tx kv.Tx, ent kv.Entity) (interface{}, error)
	Find(ctx context.Context, tx kv.Tx, opts kv.FindOpts) error
	Put(ctx context.Context, tx kv.Tx, ent kv.Entity, opts ...kv.PutOptionFn) error
}

func seedEnts(t *testing.T, kvStore kv.Store, store storeBase, ents ...kv.Entity) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	for _, ent := range ents {
		update(t, kvStore, func(tx kv.Tx) error { return store.Put(ctx, tx, ent) })
	}
}

func update(t *testing.T, kvStore kv.Store, fn func(tx kv.Tx) error) {
	t.Helper()

	require.NoError(t, kvStore.Update(context.TODO(), fn))
}

func view(t *testing.T, kvStore kv.Store, fn func(tx kv.Tx) error) {
	t.Helper()
	require.NoError(t, kvStore.View(context.TODO(), fn))
}

func getEntRaw(t *testing.T, kvStore kv.Store, bktName []byte, key []byte) []byte {
	t.Helper()

	var actualRaw []byte
	err := kvStore.View(context.TODO(), func(tx kv.Tx) error {
		b, err := tx.Bucket(bktName)
		require.NoError(t, err)

		actualRaw, err = b.Get(key)
		return err
	})
	require.NoError(t, err)
	return actualRaw
}

func encodeID(t *testing.T, id platform.ID) []byte {
	t.Helper()

	b, err := id.Encode()
	require.NoError(t, err)
	return b
}

func decJSONFooFn(key, val []byte) ([]byte, interface{}, error) {
	var f foo
	if err := json.Unmarshal(val, &f); err != nil {
		return nil, nil, err
	}
	return key, f, nil
}

func decFooEntFn(k []byte, v interface{}) (kv.Entity, error) {
	f, ok := v.(foo)
	if !ok {
		return kv.Entity{}, fmt.Errorf("invalid entry: %#v", v)
	}
	return kv.Entity{
		PK:        kv.EncID(f.ID),
		UniqueKey: kv.Encode(kv.EncID(f.OrgID), kv.EncString(f.Name)),
		Body:      f,
	}, nil
}

func newFooEnt(id, orgID platform.ID, name string) kv.Entity {
	f := foo{ID: id, Name: name, OrgID: orgID}
	return kv.Entity{
		PK:        kv.EncID(f.ID),
		UniqueKey: kv.Encode(kv.EncID(f.OrgID), kv.EncString(f.Name)),
		Body:      f,
	}
}

func isNotFoundErr(t *testing.T, err error) {
	t.Helper()

	iErr, ok := err.(*errors.Error)
	if !ok {
		require.FailNowf(t, "expected an *influxdb.Error type", "got: %#v", err)
	}
	assert.Equal(t, errors.ENotFound, iErr.Code)
}

func toIfaces(ents ...kv.Entity) []interface{} {
	var actuals []interface{}
	for _, ent := range ents {
		actuals = append(actuals, ent.Body)
	}
	return actuals
}

func reverseSlc(slc []interface{}) []interface{} {
	for i, j := 0, len(slc)-1; i < j; i, j = i+1, j-1 {
		slc[i], slc[j] = slc[j], slc[i]
	}
	return slc
}

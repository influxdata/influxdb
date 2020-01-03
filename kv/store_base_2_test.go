package kv_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/kv"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStoreBase2(t *testing.T) {
	newStoreBase := func(t *testing.T, bktSuffix string) (*kv.StoreBase2, func(), kv.Store) {
		t.Helper()

		inmemSVC, done, err := NewTestBoltStore(t)
		require.NoError(t, err)

		decFn := func(_, val []byte) (kv.EntityInt, error) {
			var v foo2
			return &v, json.Unmarshal(val, &v)
		}

		store := kv.NewStoreBase2("foo", []byte("foo_"+bktSuffix), decFn)

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		require.NoError(t, inmemSVC.Update(ctx, func(tx kv.Tx) error {
			return store.Init(ctx, tx)
		}))
		return store, done, inmemSVC
	}

	t.Run("Put", func(t *testing.T) {
		base, done, inmemStore := newStoreBase(t, "put")
		defer done()
		testPutBase2(t, inmemStore, base, base.BktName)
	})

	t.Run("DeleteEnt", func(t *testing.T) {
		base, done, inmemStore := newStoreBase(t, "delete_ent")
		defer done()

		testDeleteEntBase2(t, inmemStore, base)
	})

	t.Run("Delete", func(t *testing.T) {
		testDeleteBase2(t, func(t *testing.T, suffix string) (storeBase2, func(), kv.Store) {
			return newStoreBase(t, suffix)
		})
	})

	t.Run("FindEnt", func(t *testing.T) {
		base, done, inmemStore := newStoreBase(t, "find_ent")
		defer done()

		testFindEnt2(t, inmemStore, base)
	})

	t.Run("Find", func(t *testing.T) {
		testFind2(t, func(t *testing.T, suffix string) (storeBase2, func(), kv.Store) {
			return newStoreBase(t, suffix)
		})
	})
}

func testPutBase2(t *testing.T, kvStore kv.Store, base storeBase2, bktName []byte) foo2 {
	t.Helper()

	expected := foo2{
		ID:    1,
		OrgID: 9000,
		Name:  "foo_1",
	}

	update(t, kvStore, func(tx kv.Tx) error {
		return base.Put(context.TODO(), tx, &expected)
	})

	var actual foo2
	decodeJSON(t, getEntRaw(t, kvStore, bktName, encodeID(t, expected.ID)), &actual)

	assert.Equal(t, expected, actual)

	return expected
}

func testDeleteEntBase2(t *testing.T, kvStore kv.Store, base storeBase2) kv.EntityInt {
	t.Helper()

	expected := foo2{1, 9000, "foo_1"}
	seedEntInts(t, kvStore, base, &expected)

	update(t, kvStore, func(tx kv.Tx) error {
		return base.DeleteEnt(context.TODO(), tx, kv.IDPK(1))
	})

	err := kvStore.View(context.TODO(), func(tx kv.Tx) error {
		_, err := base.FindEnt(context.TODO(), tx, fooDecoder{IDPK: 1})
		return err
	})
	isNotFoundErr(t, err)
	return &expected
}

func testDeleteBase2(t *testing.T, fn func(t *testing.T, suffix string) (storeBase2, func(), kv.Store), assertFns ...func(*testing.T, kv.Store, storeBase2, []*foo2)) {
	expectedEnts := []kv.EntityInt{
		&foo2{1, 9000, "foo_0"},
		&foo2{2, 9000, "foo_1"},
		&foo2{3, 9003, "foo_2"},
		&foo2{4, 9004, "foo_3"},
	}

	tests := []struct {
		name     string
		opts     kv.DeleteOpts
		expected []kv.EntityInt
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
					if f, ok := v.(*foo2); ok {
						return f.ID < 4
					}
					return true
				},
			},
			expected: expectedEnts[3:],
		},
	}

	for _, tt := range tests {
		fn := func(t *testing.T) {
			t.Helper()

			base, done, inmemStore := fn(t, "delete")
			defer done()

			seedEntInts(t, inmemStore, base, expectedEnts...)

			update(t, inmemStore, func(tx kv.Tx) error {
				return base.Delete(context.TODO(), tx, tt.opts)
			})

			var actuals []kv.EntityInt
			view(t, inmemStore, func(tx kv.Tx) error {
				return base.Find(context.TODO(), tx, kv.FindOpts{
					CaptureFn: func(key []byte, decodedVal interface{}) error {
						f, ok := decodedVal.(*foo2)
						if !ok {
							return errors.New("did not get a foo")
						}
						actuals = append(actuals, f)
						return nil
					},
				})
			})

			assert.Equal(t, tt.expected, actuals)

			var entsLeft []*foo2
			for _, expected := range tt.expected {
				ent, ok := expected.(*foo2)
				require.Truef(t, ok, "got: %#v", expected)
				entsLeft = append(entsLeft, ent)
			}

			for _, assertFn := range assertFns {
				assertFn(t, inmemStore, base, entsLeft)
			}
		}
		t.Run(tt.name, fn)
	}
}

func testFindEnt2(t *testing.T, kvStore kv.Store, base storeBase2) kv.EntityInt {
	t.Helper()

	expected := foo2{1, 9000, "foo_1"}
	seedEntInts(t, kvStore, base, &expected)

	var actual kv.EntityInt
	view(t, kvStore, func(tx kv.Tx) error {
		f, err := base.FindEnt(context.TODO(), tx, fooDecoder{IDPK: 1})
		actual = f
		return err
	})

	assert.Equal(t, &expected, actual)

	return &expected
}

func testFind2(t *testing.T, fn func(t *testing.T, suffix string) (storeBase2, func(), kv.Store)) {
	t.Helper()

	expectedEnts := []kv.EntityInt{
		&foo2{1, 9000, "foo_0"},
		&foo2{2, 9000, "foo_1"},
		&foo2{3, 9003, "foo_2"},
		&foo2{4, 9004, "foo_3"},
	}

	tests := []struct {
		name     string
		opts     kv.FindOpts
		expected []kv.EntityInt
	}{
		{
			name:     "no options",
			expected: expectedEnts,
		},
		{
			name: "with order descending",
			opts: kv.FindOpts{Descending: true},
			expected: []kv.EntityInt{
				expectedEnts[3],
				expectedEnts[2],
				expectedEnts[1],
				expectedEnts[0],
			},
		},
		{
			name:     "with limit",
			opts:     kv.FindOpts{Limit: 1},
			expected: expectedEnts[:1],
		},
		{
			name:     "with offset",
			opts:     kv.FindOpts{Offset: 1},
			expected: expectedEnts[1:],
		},
		{
			name: "with offset and limit",
			opts: kv.FindOpts{
				Limit:  1,
				Offset: 1,
			},
			expected: expectedEnts[1:2],
		},
		{
			name: "with descending, offset, and limit",
			opts: kv.FindOpts{
				Descending: true,
				Limit:      1,
				Offset:     1,
			},
			expected: expectedEnts[2:3],
		},
	}

	for _, tt := range tests {
		fn := func(t *testing.T) {
			base, done, kvStore := fn(t, "find")
			defer done()

			seedEntInts(t, kvStore, base, expectedEnts...)

			var actuals []kv.EntityInt
			tt.opts.CaptureFn = func(key []byte, decodedVal interface{}) error {
				f, ok := decodedVal.(*foo2)
				if !ok {
					return errors.New("did not get a foo")
				}
				actuals = append(actuals, f)
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

type fooDecoder struct {
	kv.IDPK
}

func (f fooDecoder) Decode(b []byte) (kv.EntityInt, error) {
	var ff foo2
	if err := json.Unmarshal(b, &ff); err != nil {
		return nil, err
	}
	return &ff, nil
}

type foo2 struct {
	ID    influxdb.ID
	OrgID influxdb.ID

	Name string
}

func (f *foo2) PrimaryKey() ([]byte, error) {
	return kv.EncID(f.ID)()
}

func (f *foo2) UniqueKey() ([]byte, error) {
	return kv.Encode2(kv.EncID(f.OrgID), kv.EncString(f.Name))
}

func (f *foo2) Encode() ([]byte, error) {
	return json.Marshal(f)
}

func (f *foo2) Decode(b []byte) error {
	return json.Unmarshal(b, f)
}

type storeBase2 interface {
	Delete(ctx context.Context, tx kv.Tx, opts kv.DeleteOpts) error
	DeleteEnt(ctx context.Context, tx kv.Tx, ent kv.PrimaryKey) error
	FindEnt(ctx context.Context, tx kv.Tx, ent kv.PrimaryKey) (kv.EntityInt, error)
	Find(ctx context.Context, tx kv.Tx, opts kv.FindOpts) error
	Put(ctx context.Context, tx kv.Tx, ent kv.EntityInt) error
}

func seedEntInts(t *testing.T, kvStore kv.Store, store storeBase2, ents ...kv.EntityInt) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	for _, ent := range ents {
		update(t, kvStore, func(tx kv.Tx) error { return store.Put(ctx, tx, ent) })
	}
}

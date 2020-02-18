package kv_test

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"testing"

	"github.com/influxdata/influxdb/inmem"
	"github.com/influxdata/influxdb/kv"
)

const (
	someResourceBucket = "some-resource"
)

var (
	mapping = kv.NewIndexMapping([]byte(someResourceBucket), "ownerID", 1, func(body []byte) ([]byte, error) {
		var resource someResource
		if err := json.Unmarshal(body, &resource); err != nil {
			return nil, err
		}

		return []byte(resource.OwnerID), nil
	})
)

type someResource struct {
	ID      string
	OwnerID string
}

type someResourceStore struct {
	store        kv.Store
	ownerIDIndex *kv.Index
}

func newSomeResourceStore(ctx context.Context, store kv.Store) (*someResourceStore, error) {
	ownerIDIndex := kv.NewIndex(mapping)
	if err := ownerIDIndex.Initialize(ctx, store); err != nil {
		return nil, err
	}

	return &someResourceStore{
		store:        store,
		ownerIDIndex: ownerIDIndex,
	}, nil
}

func (s *someResourceStore) Create(ctx context.Context, resource someResource, index bool) error {
	return s.store.Update(ctx, func(tx kv.Tx) error {
		bkt, err := tx.Bucket(mapping.SourceBucket())
		if err != nil {
			return err
		}

		if index {
			if err := s.ownerIDIndex.Insert(tx, []byte(resource.OwnerID), []byte(resource.ID)); err != nil {
				return err
			}
		}

		data, err := json.Marshal(resource)
		if err != nil {
			return err
		}

		return bkt.Put([]byte(resource.ID), data)
	})
}

func newNResources(n int) (resources []someResource) {
	for i := 0; i < n; i++ {
		resources = append(resources, someResource{
			ID:      fmt.Sprintf("resource %d", i),
			OwnerID: fmt.Sprintf("owner %d", i%5),
		})
	}
	return
}

func Test_Index_PopulateAndVerify(t *testing.T) {
	var (
		ctx                = context.TODO()
		store              = inmem.NewKVStore()
		resources          = newNResources(20)
		resourceStore, err = newSomeResourceStore(ctx, store)
	)

	// insert 20 resources, but only index the first half
	for i, resource := range resources {
		if err := resourceStore.Create(ctx, resource, i < len(resources)/2); err != nil {
			t.Fatal(err)
		}
	}

	// check that the index is populated with only 10 items
	var count int
	store.View(ctx, func(tx kv.Tx) error {
		kvs, err := allKVs(tx, mapping.IndexBucket())
		if err != nil {
			return err
		}

		count = len(kvs)

		return nil
	})

	if count > 10 {
		t.Errorf("expected index to be empty, found %d items", count)
	}

	// ensure verify identifies the 10 missing items from the index
	store.View(ctx, func(tx kv.Tx) error {
		diff, err := resourceStore.ownerIDIndex.Verify(ctx, tx)
		if err != nil {
			return err
		}

		expected := kv.IndexDiff{
			Index: map[string]string{
				"owner 0/resource 10": "resource 10",
				"owner 0/resource 15": "resource 15",
				"owner 1/resource 11": "resource 11",
				"owner 1/resource 16": "resource 16",
				"owner 2/resource 12": "resource 12",
				"owner 2/resource 17": "resource 17",
				"owner 3/resource 13": "resource 13",
				"owner 3/resource 18": "resource 18",
				"owner 4/resource 14": "resource 14",
				"owner 4/resource 19": "resource 19",
			},
		}
		if !reflect.DeepEqual(expected, diff) {
			t.Errorf("expected %#v, found %#v", expected, diff)
		}

		return nil
	})

	// populate the missing indexes
	count, err = resourceStore.ownerIDIndex.Populate(ctx, store)
	if err != nil {
		t.Errorf("unexpected err %v", err)
	}

	// ensure only 10 items were reported as being indexed
	if count != 10 {
		t.Errorf("expected to index 20 items, instead indexed %d items", count)
	}

	// check the contents of the index
	var allKvs [][2][]byte
	store.View(ctx, func(tx kv.Tx) (err error) {
		allKvs, err = allKVs(tx, mapping.IndexBucket())
		return
	})

	if expected := [][2][]byte{
		[2][]byte{[]byte("owner 0/resource 0"), []byte("resource 0")},
		[2][]byte{[]byte("owner 0/resource 10"), []byte("resource 10")},
		[2][]byte{[]byte("owner 0/resource 15"), []byte("resource 15")},
		[2][]byte{[]byte("owner 0/resource 5"), []byte("resource 5")},
		[2][]byte{[]byte("owner 1/resource 1"), []byte("resource 1")},
		[2][]byte{[]byte("owner 1/resource 11"), []byte("resource 11")},
		[2][]byte{[]byte("owner 1/resource 16"), []byte("resource 16")},
		[2][]byte{[]byte("owner 1/resource 6"), []byte("resource 6")},
		[2][]byte{[]byte("owner 2/resource 12"), []byte("resource 12")},
		[2][]byte{[]byte("owner 2/resource 17"), []byte("resource 17")},
		[2][]byte{[]byte("owner 2/resource 2"), []byte("resource 2")},
		[2][]byte{[]byte("owner 2/resource 7"), []byte("resource 7")},
		[2][]byte{[]byte("owner 3/resource 13"), []byte("resource 13")},
		[2][]byte{[]byte("owner 3/resource 18"), []byte("resource 18")},
		[2][]byte{[]byte("owner 3/resource 3"), []byte("resource 3")},
		[2][]byte{[]byte("owner 3/resource 8"), []byte("resource 8")},
		[2][]byte{[]byte("owner 4/resource 14"), []byte("resource 14")},
		[2][]byte{[]byte("owner 4/resource 19"), []byte("resource 19")},
		[2][]byte{[]byte("owner 4/resource 4"), []byte("resource 4")},
		[2][]byte{[]byte("owner 4/resource 9"), []byte("resource 9")},
	}; !reflect.DeepEqual(allKvs, expected) {
		t.Errorf("expected %#v, found %#v", expected, allKvs)
	}

	// remove the last 10 items from the source, but leave them in the index
	store.Update(ctx, func(tx kv.Tx) error {
		bkt, err := tx.Bucket(mapping.SourceBucket())
		if err != nil {
			t.Fatal(err)
		}

		for _, resource := range resources[10:] {
			bkt.Delete([]byte(resource.ID))
		}

		return nil
	})

	// ensure verify identifies the last 10 items as missing from the source
	store.View(ctx, func(tx kv.Tx) error {
		diff, err := resourceStore.ownerIDIndex.Verify(ctx, tx)
		if err != nil {
			return err
		}

		expected := kv.IndexDiff{
			Source: map[string]string{
				"resource 10": "owner 0",
				"resource 15": "owner 0",
				"resource 11": "owner 1",
				"resource 16": "owner 1",
				"resource 12": "owner 2",
				"resource 17": "owner 2",
				"resource 13": "owner 3",
				"resource 18": "owner 3",
				"resource 14": "owner 4",
				"resource 19": "owner 4",
			},
		}
		if !reflect.DeepEqual(expected, diff) {
			t.Errorf("expected %#v, found %#v", expected, diff)
		}

		return nil
	})
}

func allKVs(tx kv.Tx, bucket []byte) (kvs [][2][]byte, err error) {
	idx, err := tx.Bucket(mapping.IndexBucket())
	if err != nil {
		return
	}

	cursor, err := idx.ForwardCursor(nil)
	if err != nil {
		return
	}

	defer func() {
		if cerr := cursor.Close(); cerr != nil && err == nil {
			err = cerr
		}
	}()

	for k, v := cursor.Next(); k != nil; k, v = cursor.Next() {
		kvs = append(kvs, [2][]byte{k, v})
	}

	return kvs, cursor.Err()
}

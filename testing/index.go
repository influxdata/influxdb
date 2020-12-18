package testing

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"testing"

	"github.com/influxdata/influxdb/v2/kv"
	"github.com/influxdata/influxdb/v2/kv/migration"
)

const (
	someResourceBucket = "aresource"
)

var (
	mapping = kv.NewIndexMapping([]byte(someResourceBucket), []byte("aresourcebyowneridv1"), func(body []byte) ([]byte, error) {
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

type tester interface {
	Helper()
	Fatal(...interface{})
}

func newSomeResourceStore(t tester, ctx context.Context, store kv.SchemaStore) *someResourceStore {
	t.Helper()

	if err := migration.CreateBuckets("create the aresource bucket", []byte(someResourceBucket)).Up(ctx, store); err != nil {
		t.Fatal(err)
	}

	if err := kv.NewIndexMigration(mapping).Up(ctx, store); err != nil {
		t.Fatal(err)
	}

	return &someResourceStore{
		store:        store,
		ownerIDIndex: kv.NewIndex(mapping),
	}
}

func (s *someResourceStore) FindByOwner(ctx context.Context, ownerID string) (resources []someResource, err error) {
	err = s.store.View(ctx, func(tx kv.Tx) error {
		return s.ownerIDIndex.Walk(ctx, tx, []byte(ownerID), func(k, v []byte) (bool, error) {
			var resource someResource
			if err := json.Unmarshal(v, &resource); err != nil {
				return false, err
			}

			resources = append(resources, resource)
			return true, nil
		})
	})
	return
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

func newResource(id, owner string) someResource {
	return someResource{ID: id, OwnerID: owner}
}

func newNResources(n int) (resources []someResource) {
	return newNResourcesWithUserCount(n, 5)
}

func newNResourcesWithUserCount(n, userCount int) (resources []someResource) {
	for i := 0; i < n; i++ {
		var (
			id    = fmt.Sprintf("resource %d", i)
			owner = fmt.Sprintf("owner %d", i%userCount)
		)
		resources = append(resources, newResource(id, owner))
	}
	return
}

func TestIndex(t *testing.T, store kv.SchemaStore) {
	t.Run("Test_PopulateAndVerify", func(t *testing.T) {
		testPopulateAndVerify(t, store)
	})

	t.Run("Test_Walk", func(t *testing.T) {
		testWalk(t, store)
	})
}

func testPopulateAndVerify(t *testing.T, store kv.SchemaStore) {
	var (
		ctx           = context.TODO()
		resources     = newNResources(20)
		resourceStore = newSomeResourceStore(t, ctx, store)
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
	diff, err := resourceStore.ownerIDIndex.Verify(ctx, store)
	if err != nil {
		t.Fatal(err)
	}

	expected := kv.IndexDiff{
		PresentInIndex: map[string]map[string]struct{}{
			"owner 0": {"resource 0": {}, "resource 5": {}},
			"owner 1": {"resource 1": {}, "resource 6": {}},
			"owner 2": {"resource 2": {}, "resource 7": {}},
			"owner 3": {"resource 3": {}, "resource 8": {}},
			"owner 4": {"resource 4": {}, "resource 9": {}},
		},
		MissingFromIndex: map[string]map[string]struct{}{
			"owner 0": {"resource 10": {}, "resource 15": {}},
			"owner 1": {"resource 11": {}, "resource 16": {}},
			"owner 2": {"resource 12": {}, "resource 17": {}},
			"owner 3": {"resource 13": {}, "resource 18": {}},
			"owner 4": {"resource 14": {}, "resource 19": {}},
		},
	}

	if !reflect.DeepEqual(expected, diff) {
		t.Errorf("expected %#v, found %#v", expected, diff)
	}

	corrupt := diff.Corrupt()
	sort.Strings(corrupt)

	if expected := []string{
		"owner 0",
		"owner 1",
		"owner 2",
		"owner 3",
		"owner 4",
	}; !reflect.DeepEqual(expected, corrupt) {
		t.Errorf("expected %#v, found %#v\n", expected, corrupt)
	}

	// populate the missing indexes

	if err = kv.NewIndexMigration(mapping).Up(ctx, store); err != nil {
		t.Errorf("unexpected err %v", err)
	}

	// check the contents of the index
	var allKvs [][2][]byte
	store.View(ctx, func(tx kv.Tx) (err error) {
		allKvs, err = allKVs(tx, mapping.IndexBucket())
		return
	})

	if expected := [][2][]byte{
		{[]byte("owner 0/resource 0"), []byte("resource 0")},
		{[]byte("owner 0/resource 10"), []byte("resource 10")},
		{[]byte("owner 0/resource 15"), []byte("resource 15")},
		{[]byte("owner 0/resource 5"), []byte("resource 5")},
		{[]byte("owner 1/resource 1"), []byte("resource 1")},
		{[]byte("owner 1/resource 11"), []byte("resource 11")},
		{[]byte("owner 1/resource 16"), []byte("resource 16")},
		{[]byte("owner 1/resource 6"), []byte("resource 6")},
		{[]byte("owner 2/resource 12"), []byte("resource 12")},
		{[]byte("owner 2/resource 17"), []byte("resource 17")},
		{[]byte("owner 2/resource 2"), []byte("resource 2")},
		{[]byte("owner 2/resource 7"), []byte("resource 7")},
		{[]byte("owner 3/resource 13"), []byte("resource 13")},
		{[]byte("owner 3/resource 18"), []byte("resource 18")},
		{[]byte("owner 3/resource 3"), []byte("resource 3")},
		{[]byte("owner 3/resource 8"), []byte("resource 8")},
		{[]byte("owner 4/resource 14"), []byte("resource 14")},
		{[]byte("owner 4/resource 19"), []byte("resource 19")},
		{[]byte("owner 4/resource 4"), []byte("resource 4")},
		{[]byte("owner 4/resource 9"), []byte("resource 9")},
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
	diff, err = resourceStore.ownerIDIndex.Verify(ctx, store)
	if err != nil {
		t.Fatal(err)
	}

	expected = kv.IndexDiff{
		PresentInIndex: map[string]map[string]struct{}{
			"owner 0": {"resource 0": {}, "resource 5": {}, "resource 10": {}, "resource 15": {}},
			"owner 1": {"resource 1": {}, "resource 6": {}, "resource 11": {}, "resource 16": {}},
			"owner 2": {"resource 2": {}, "resource 7": {}, "resource 12": {}, "resource 17": {}},
			"owner 3": {"resource 3": {}, "resource 8": {}, "resource 13": {}, "resource 18": {}},
			"owner 4": {"resource 4": {}, "resource 9": {}, "resource 14": {}, "resource 19": {}},
		},
		MissingFromSource: map[string]map[string]struct{}{
			"owner 0": {"resource 10": {}, "resource 15": {}},
			"owner 1": {"resource 11": {}, "resource 16": {}},
			"owner 2": {"resource 12": {}, "resource 17": {}},
			"owner 3": {"resource 13": {}, "resource 18": {}},
			"owner 4": {"resource 14": {}, "resource 19": {}},
		},
	}
	if !reflect.DeepEqual(expected, diff) {
		t.Errorf("expected %#v, found %#v", expected, diff)
	}
}

func testWalk(t *testing.T, store kv.SchemaStore) {
	var (
		ctx       = context.TODO()
		resources = newNResources(20)
		// configure resource store with read disabled
		resourceStore = newSomeResourceStore(t, ctx, store)

		cases = []struct {
			owner     string
			resources []someResource
		}{
			{
				owner: "owner 0",
				resources: []someResource{
					newResource("resource 0", "owner 0"),
					newResource("resource 10", "owner 0"),
					newResource("resource 15", "owner 0"),
					newResource("resource 5", "owner 0"),
				},
			},
			{
				owner: "owner 1",
				resources: []someResource{
					newResource("resource 1", "owner 1"),
					newResource("resource 11", "owner 1"),
					newResource("resource 16", "owner 1"),
					newResource("resource 6", "owner 1"),
				},
			},
			{
				owner: "owner 2",
				resources: []someResource{
					newResource("resource 12", "owner 2"),
					newResource("resource 17", "owner 2"),
					newResource("resource 2", "owner 2"),
					newResource("resource 7", "owner 2"),
				},
			},
			{
				owner: "owner 3",
				resources: []someResource{
					newResource("resource 13", "owner 3"),
					newResource("resource 18", "owner 3"),
					newResource("resource 3", "owner 3"),
					newResource("resource 8", "owner 3"),
				},
			},
			{
				owner: "owner 4",
				resources: []someResource{
					newResource("resource 14", "owner 4"),
					newResource("resource 19", "owner 4"),
					newResource("resource 4", "owner 4"),
					newResource("resource 9", "owner 4"),
				},
			},
		}
	)

	// insert all 20 resources with indexing enabled
	for _, resource := range resources {
		if err := resourceStore.Create(ctx, resource, true); err != nil {
			t.Fatal(err)
		}
	}

	for _, testCase := range cases {
		found, err := resourceStore.FindByOwner(ctx, testCase.owner)
		if err != nil {
			t.Fatal(err)
		}

		// expect resources to be empty while read path disabled disabled
		if len(found) > 0 {
			t.Fatalf("expected %#v to be empty", found)
		}
	}

	// configure index read path enabled
	kv.WithIndexReadPathEnabled(resourceStore.ownerIDIndex)

	for _, testCase := range cases {
		found, err := resourceStore.FindByOwner(ctx, testCase.owner)
		if err != nil {
			t.Fatal(err)
		}

		if !reflect.DeepEqual(found, testCase.resources) {
			t.Errorf("expected %#v, found %#v", testCase.resources, found)
		}
	}
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

func BenchmarkIndexWalk(b *testing.B, store kv.SchemaStore, resourceCount, fetchCount int) {
	var (
		ctx           = context.TODO()
		resourceStore = newSomeResourceStore(b, ctx, store)
		userCount     = resourceCount / fetchCount
		resources     = newNResourcesWithUserCount(resourceCount, userCount)
	)

	kv.WithIndexReadPathEnabled(resourceStore.ownerIDIndex)

	for _, resource := range resources {
		resourceStore.Create(ctx, resource, true)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		store.View(ctx, func(tx kv.Tx) error {
			return resourceStore.ownerIDIndex.Walk(ctx, tx, []byte(fmt.Sprintf("owner %d", i%userCount)), func(k, v []byte) (bool, error) {
				if k == nil || v == nil {
					b.Fatal("entries must not be nil")
				}

				return true, nil
			})
		})
	}
}

package kv_test

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/influxdata/influxdb/inmem"
	"github.com/influxdata/influxdb/kv"
	"go.uber.org/zap/zaptest"
)

func TestIndexer(t *testing.T) {
	var (
		store         = inmem.NewKVStore()
		indexer       = kv.NewIndexer(zaptest.NewLogger(t), store)
		indexes       = map[string][]byte{}
		expectedCount = 1000
		wg            sync.WaitGroup
	)

	for i := 0; i < expectedCount; i++ {
		key := fmt.Sprintf("%d", i)
		indexes[key] = []byte(key)

		wg.Add(1)
		go func(i int) {
			defer wg.Done()

			indexer.AddToIndex([]byte("bucket"), map[string][]byte{
				key: []byte(key),
			})
		}(i)
	}

	// wait for index insertion
	wg.Wait()

	// wait for indexer to finish working
	indexer.Wait()

	count := 0
	err := store.View(context.Background(), func(tx kv.Tx) error {
		bucket, err := tx.Bucket([]byte("bucket"))
		if err != nil {
			t.Fatal(err)
		}
		cur, err := bucket.ForwardCursor(nil)
		if err != nil {
			t.Fatal(err)
		}
		for k, _ := cur.Next(); k != nil; k, _ = cur.Next() {
			if string(k) != string(indexes[string(k)]) {
				t.Fatalf("failed to find correct index, found: %s, expected: %s", k, indexes[string(k)])
			}
			count++
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	if count != expectedCount {
		t.Fatal("failed to retrieve indexes")
	}
}

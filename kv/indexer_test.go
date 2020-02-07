package kv_test

import (
	"context"
	"testing"

	"github.com/influxdata/influxdb/inmem"
	"github.com/influxdata/influxdb/kv"
	"go.uber.org/zap/zaptest"
)

func TestIndexer(t *testing.T) {
	store := inmem.NewKVStore()

	indexer := kv.NewIndexer(zaptest.NewLogger(t), store)
	indexes := map[string][]byte{
		"1": []byte("1"),
		"2": []byte("2"),
		"3": []byte("3"),
		"4": []byte("4"),
	}
	indexer.AddToIndex([]byte("bucket"), indexes)
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
	if count != 4 {
		t.Fatal("failed to retrieve indexes")
	}
}

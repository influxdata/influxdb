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
	indexes := [][]byte{
		[]byte("1"),
		[]byte("2"),
		[]byte("3"),
		[]byte("4"),
	}
	indexer.AddIndex([]byte("bucket"), indexes)
	indexer.Stop()

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
			if string(k) != string(indexes[count]) {
				t.Fatalf("failed to find correct index, found: %s, expected: %s", k, indexes[count])
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

package kv_test

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"testing"

	"github.com/influxdata/influxdb/inmem"
	"github.com/influxdata/influxdb/kv"
	"go.uber.org/zap/zaptest"
)

func TestMapper(t *testing.T) {

	type keyVal struct {
		key, val []byte
	}

	onehundredandfiftymaps := make([]keyVal, 150)
	for i := 0; i < 150; i++ {
		onehundredandfiftymaps[i] = keyVal{[]byte(fmt.Sprintf("%d", i)), []byte(fmt.Sprintf("%d", i))}
	}
	sort.Slice(onehundredandfiftymaps, func(i, j int) bool {
		return string(onehundredandfiftymaps[i].key) < string(onehundredandfiftymaps[j].key)
	})

	tests := []struct {
		initial []keyVal
		mapper  kv.MappingFunc
		result  []keyVal
	}{
		{
			initial: []keyVal{
				{[]byte("1"), []byte("2")},
				{[]byte("2"), []byte("4")},
				{[]byte("3"), []byte("6")},
				{[]byte("4"), []byte("8")},
			},
			mapper: func(k []byte, v []byte) ([]byte, []byte) {
				return v, k
			},
			result: []keyVal{
				{[]byte("2"), []byte("1")},
				{[]byte("4"), []byte("2")},
				{[]byte("6"), []byte("3")},
				{[]byte("8"), []byte("4")},
			},
		},
		{
			initial: onehundredandfiftymaps,
			mapper: func(k []byte, v []byte) ([]byte, []byte) {
				return k, v
			},
			result: onehundredandfiftymaps,
		},
	}

	for _, test := range tests {
		store := inmem.NewKVStore()
		mapper := kv.NewMapper(zaptest.NewLogger(t), store)
		// populate a bucket
		err := store.Update(context.Background(), func(tx kv.Tx) error {
			bucket, err := tx.Bucket([]byte("bucket1"))
			if err != nil {
				t.Fatal(err)
			}
			for _, initial := range test.initial {
				bucket.Put(initial.key, initial.val)
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		// map full bucket to second bucket
		err = mapper.Map(context.Background(), []byte("bucket1"), []byte("bucket2"), test.mapper)
		if err != nil {
			t.Fatal(err)
		}

		actualResult := []keyVal{}

		err = store.View(context.Background(), func(tx kv.Tx) error {
			bucket, err := tx.Bucket([]byte("bucket2"))
			if err != nil {
				t.Fatal(err)
			}
			cursor, err := bucket.ForwardCursor(nil)
			if err != nil {
				return err
			}
			for k, v := cursor.Next(); k != nil; k, v = cursor.Next() {
				actualResult = append(actualResult, keyVal{k, v})
			}

			return nil
		})
		if err != nil {
			t.Fatal(err)
		}

		if !reflect.DeepEqual(actualResult, test.result) {
			t.Errorf("map failed outputs %v, want %v", actualResult, test.result)
		}

	}

}

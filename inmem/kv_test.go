package inmem_test

import (
	"context"
	"reflect"
	"sort"
	"testing"

	"github.com/influxdata/influxdb/inmem"
	"github.com/influxdata/influxdb/kv"
	platformtesting "github.com/influxdata/influxdb/testing"
)

func initKVStore(f platformtesting.KVStoreFields, t *testing.T) (kv.Store, func()) {
	s := inmem.NewKVStore()

	err := s.Update(context.Background(), func(tx kv.Tx) error {
		b, err := tx.Bucket(f.Bucket)
		if err != nil {
			return err
		}

		for _, p := range f.Pairs {
			if err := b.Put(p.Key, p.Value); err != nil {
				return err
			}
		}

		return nil
	})
	if err != nil {
		t.Fatalf("failed to put keys: %v", err)
	}
	return s, func() {}
}

func TestKVStore(t *testing.T) {
	platformtesting.KVStore(initKVStore, t)
}

func TestKVStore_Buckets(t *testing.T) {
	tests := []struct {
		name    string
		buckets []string
		want    [][]byte
	}{
		{
			name:    "single bucket is returned if only one bucket is added",
			buckets: []string{"b1"},
			want:    [][]byte{[]byte("b1")},
		},
		{
			name:    "multiple buckets are returned if multiple buckets added",
			buckets: []string{"b1", "b2", "b3"},
			want:    [][]byte{[]byte("b1"), []byte("b2"), []byte("b3")},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &inmem.KVStore{}
			err := s.Update(context.Background(), func(tx kv.Tx) error {
				for _, b := range tt.buckets {
					if _, err := tx.Bucket([]byte(b)); err != nil {
						return err
					}
				}
				return nil
			})
			if err != nil {
				t.Fatalf("unable to setup store with buckets: %v", err)
			}
			got := s.Buckets(context.Background())
			sort.Slice(got, func(i, j int) bool {
				return string(got[i]) < string(got[j])
			})
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("KVStore.Buckets() = %v, want %v", got, tt.want)
			}
		})
	}
}

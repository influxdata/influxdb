package inmem_test

import (
	"bufio"
	"context"
	"math"
	"os"
	"reflect"
	"sort"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/influxdb/v2/inmem"
	"github.com/influxdata/influxdb/v2/kv"
	platformtesting "github.com/influxdata/influxdb/v2/testing"
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
			s := inmem.NewKVStore()
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

func TestKVStore_Bucket_CursorHintPredicate(t *testing.T) {
	s := inmem.NewKVStore()

	bucket := "urm"
	fillBucket(t, s, bucket, 10)

	t.Run("filter by key", func(t *testing.T) {
		_ = s.View(context.Background(), func(tx kv.Tx) error {
			b, err := tx.Bucket([]byte(bucket))
			if err != nil {
				return err
			}

			cur, _ := b.Cursor(kv.WithCursorHintPredicate(func(key, _ []byte) bool {
				return len(key) < 32 || string(key[16:]) == "8d5dc900004589c3"
			}))

			count := 0
			for k, _ := cur.First(); len(k) > 0; k, _ = cur.Next() {
				count++
			}

			if exp, got := 1, count; got != exp {
				t.Errorf("unexpected number of keys, -got/+exp\n%s", cmp.Diff(got, exp))
			}

			return nil
		})
	})

	t.Run("filter by value", func(t *testing.T) {
		_ = s.View(context.Background(), func(tx kv.Tx) error {
			b, err := tx.Bucket([]byte(bucket))
			if err != nil {
				return err
			}

			cur, _ := b.Cursor(kv.WithCursorHintPredicate(func(_, val []byte) bool {
				return len(val) < 32 || string(val[16:]) == "8d5dc900004589c3"
			}))

			count := 0
			for k, _ := cur.First(); len(k) > 0; k, _ = cur.Next() {
				count++
			}

			if exp, got := 1, count; got != exp {
				t.Errorf("unexpected number of keys, -got/+exp\n%s", cmp.Diff(got, exp))
			}

			return nil
		})
	})
}

func TestKVStoreTimeout(t *testing.T) {
	blockTillDone := func(countStarted *int64) func(tx kv.Tx) error {
		return func(tx kv.Tx) error {
			atomic.AddInt64(countStarted, 1)
			select {
			case <-tx.Context().Done():
			case <-time.After(30 * time.Second): // if this test takes longer than 30 seconds something is really wrong and it just needs to fail.
				t.Fatalf("%s timed out (this is separate from the global test timeout)\n", t.Name())
			}
			return tx.Context().Err()
		}
	}

	t.Run("context timeouts should block future update transactions on the same context", func(t *testing.T) {
		var countStarted int64
		kv := inmem.NewKVStore()

		ctx, cancel := context.WithCancel(context.Background())
		for i := 0; i < 20; i++ {
			go kv.Update(ctx, blockTillDone(&countStarted))
		}
		time.Sleep(time.Second / 2)
		cancel()
		time.Sleep(time.Second / 2)
		if countStarted != 1 {
			t.Fatalf("expected the number of started update transactions to be 1 but it was %d", atomic.LoadInt64(&countStarted))
		}
	})
	t.Run("view transactions shouldn't block each other", func(t *testing.T) {
		var countStarted int64
		kv := inmem.NewKVStore()

		ctx, cancel := context.WithCancel(context.Background())
		for i := 0; i < 20; i++ {
			go kv.View(ctx, blockTillDone(&countStarted))
		}
		time.Sleep(time.Second / 2)
		cancel()
		time.Sleep(time.Second / 2)
		if countStarted != 20 {
			t.Fatalf("expected the number of started view transactions to be 20 but it was %d", atomic.LoadInt64(&countStarted))
		}

	})

	t.Run("update transactions should block future view transactions", func(t *testing.T) {
		var countViewStarted int64
		var countUpdateStarted int64

		kv := inmem.NewKVStore()

		ctx, cancel := context.WithCancel(context.Background())
		go kv.Update(ctx, blockTillDone(&countUpdateStarted))
		time.Sleep(time.Second / 2)
		for i := 0; i < 20; i++ {
			go kv.View(ctx, blockTillDone(&countViewStarted))
		}
		time.Sleep(time.Second / 2)
		cancel()
		time.Sleep(time.Second / 2)
		if countUpdateStarted != 1 {
			t.Fatalf("expected the number of started update transactions to be 1 but it was %d", atomic.LoadInt64(&countUpdateStarted))
		}
		if countViewStarted != 0 {
			t.Fatalf("expected the number of started view transactions to be 0 but it was %d", atomic.LoadInt64(&countViewStarted))
		}

	})
	t.Run("view transactions should block future update transactions", func(t *testing.T) {
		var countViewStarted int64
		var countUpdateStarted int64

		kv := inmem.NewKVStore()

		ctx, cancel := context.WithCancel(context.Background())
		go kv.View(ctx, blockTillDone(&countViewStarted))
		time.Sleep(time.Second / 2)
		go kv.Update(ctx, blockTillDone(&countUpdateStarted))
		time.Sleep(time.Second / 2)

		for i := 0; i < 20; i++ {
			go kv.View(ctx, blockTillDone(&countViewStarted))
		}
		time.Sleep(time.Second / 2)
		cancel()
		time.Sleep(time.Second / 2)
		if countUpdateStarted != 0 {
			t.Fatalf("expected the number of started update transactions to be 0 but it was %d", atomic.LoadInt64(&countUpdateStarted))
		}
		if countViewStarted < 1 {
			t.Fatalf("expected the number of started view transactions to >= 1 but it was %d", atomic.LoadInt64(&countViewStarted))
		}

	})

}

func openCursor(t testing.TB, s *inmem.KVStore, bucket string, fn func(cur kv.Cursor), hints ...kv.CursorHint) {
	t.Helper()

	_ = s.View(context.Background(), func(tx kv.Tx) error {
		b, err := tx.Bucket([]byte(bucket))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		cur, err := b.Cursor(hints...)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if fn != nil {
			fn(cur)
		}

		return nil
	})
}

func BenchmarkKVStore_Bucket_Cursor(b *testing.B) {
	scanAll := func(cur kv.Cursor) {
		for k, v := cur.First(); k != nil; k, v = cur.Next() {
			_, _ = k, v
		}
	}

	searchKey := "629ffa00003dd2ce"
	predicate := kv.CursorPredicateFunc(func(key, _ []byte) bool {
		return len(key) < 32 || string(key[16:]) == searchKey
	})

	b.Run("16000 keys", func(b *testing.B) {
		s := inmem.NewKVStore()
		bucket := "urm"
		fillBucket(b, s, bucket, 0)

		b.Run("without hint", func(b *testing.B) {
			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				openCursor(b, s, bucket, scanAll)
			}
		})

		b.Run("with hint", func(b *testing.B) {
			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				openCursor(b, s, bucket, scanAll, kv.WithCursorHintPredicate(predicate))
			}
		})
	})
}

const sourceFile = "kvdata/keys.txt"

func fillBucket(t testing.TB, s *inmem.KVStore, bucket string, lines int64) {
	t.Helper()
	err := s.Update(context.Background(), func(tx kv.Tx) error {
		b, err := tx.Bucket([]byte(bucket))
		if err != nil {
			return err
		}

		f, err := os.Open(sourceFile)
		if err != nil {
			return err
		}
		defer f.Close()

		if lines == 0 {
			lines = int64(math.MaxInt64)
		}

		scan := bufio.NewScanner(bufio.NewReader(f))
		for scan.Scan() {
			var key []byte
			key = append(key, scan.Bytes()...)
			_ = b.Put(key, key)
			lines--
			if lines <= 0 {
				break
			}
		}

		return nil
	})

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

package testing

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/influxdb/v2/kv"
)

// KVStoreFields are background data that has to be set before
// the test runs.
type KVStoreFields struct {
	Bucket []byte
	Pairs  []kv.Pair
}

// KVStore tests the key value store contract
func KVStore(
	init func(KVStoreFields, *testing.T) (kv.Store, func()),
	t *testing.T,
) {
	tests := []struct {
		name string
		fn   func(
			init func(KVStoreFields, *testing.T) (kv.Store, func()),
			t *testing.T,
		)
	}{
		{
			name: "Get",
			fn:   KVGet,
		},
		{
			name: "GetBatch",
			fn:   KVGetBatch,
		},
		{
			name: "Put",
			fn:   KVPut,
		},
		{
			name: "Delete",
			fn:   KVDelete,
		},
		{
			name: "Cursor",
			fn:   KVCursor,
		},
		{
			name: "CursorWithHints",
			fn:   KVCursorWithHints,
		},
		{
			name: "ForwardCursor",
			fn:   KVForwardCursor,
		},
		{
			name: "View",
			fn:   KVView,
		},
		{
			name: "Update",
			fn:   KVUpdate,
		},
		{
			name: "ConcurrentUpdate",
			fn:   KVConcurrentUpdate,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt := tt
			t.Parallel()
			tt.fn(init, t)
		})
	}
}

// KVGet tests the get method contract for the key value store.
func KVGet(
	init func(KVStoreFields, *testing.T) (kv.Store, func()),
	t *testing.T,
) {
	type args struct {
		bucket []byte
		key    []byte
	}
	type wants struct {
		err error
		val []byte
	}

	tests := []struct {
		name   string
		fields KVStoreFields
		args   args
		wants  wants
	}{
		{
			name: "get key",
			fields: KVStoreFields{
				Bucket: []byte("bucket"),
				Pairs: []kv.Pair{
					{
						Key:   []byte("hello"),
						Value: []byte("world"),
					},
				},
			},
			args: args{
				bucket: []byte("bucket"),
				key:    []byte("hello"),
			},
			wants: wants{
				val: []byte("world"),
			},
		},
		{
			name: "get missing key",
			fields: KVStoreFields{
				Bucket: []byte("bucket"),
				Pairs:  []kv.Pair{},
			},
			args: args{
				bucket: []byte("bucket"),
				key:    []byte("hello"),
			},
			wants: wants{
				err: kv.ErrKeyNotFound,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, close := init(tt.fields, t)
			defer close()

			err := s.View(context.Background(), func(tx kv.Tx) error {
				b, err := tx.Bucket(tt.args.bucket)
				if err != nil {
					t.Errorf("unexpected error retrieving bucket: %v", err)
					return err
				}

				val, err := b.Get(tt.args.key)
				if (err != nil) != (tt.wants.err != nil) {
					t.Errorf("expected error '%v' got '%v'", tt.wants.err, err)
					return err
				}

				if err != nil && tt.wants.err != nil {
					if err.Error() != tt.wants.err.Error() {
						t.Errorf("expected error messages to match '%v' got '%v'", tt.wants.err, err.Error())
						return err
					}
				}

				if want, got := tt.wants.val, val; !bytes.Equal(want, got) {
					t.Errorf("exptected to get value %s got %s", string(want), string(got))
					return err
				}

				return nil
			})

			if err != nil {
				t.Fatalf("error during view transaction: %v", err)
			}
		})
	}
}

// KVGetBatch tests the get batch method contract for the key value store.
func KVGetBatch(
	init func(KVStoreFields, *testing.T) (kv.Store, func()),
	t *testing.T,
) {
	type args struct {
		bucket []byte
		keys   [][]byte
	}
	type wants struct {
		err  error
		vals [][]byte
	}

	tests := []struct {
		name   string
		fields KVStoreFields
		args   args
		wants  wants
	}{
		{
			name: "get keys",
			fields: KVStoreFields{
				Bucket: []byte("bucket"),
				Pairs: []kv.Pair{
					{
						Key:   []byte("hello"),
						Value: []byte("world"),
					},
					{
						Key:   []byte("color"),
						Value: []byte("orange"),
					},
					{
						Key:   []byte("organization"),
						Value: []byte("influx"),
					},
				},
			},
			args: args{
				bucket: []byte("bucket"),
				keys:   [][]byte{[]byte("hello"), []byte("organization")},
			},
			wants: wants{
				vals: [][]byte{[]byte("world"), []byte("influx")},
			},
		},
		{
			name: "get keys with missing",
			fields: KVStoreFields{
				Bucket: []byte("bucket"),
				Pairs: []kv.Pair{
					{
						Key:   []byte("hello"),
						Value: []byte("world"),
					},
					{
						Key:   []byte("organization"),
						Value: []byte("influx"),
					},
				},
			},
			args: args{
				bucket: []byte("bucket"),
				keys:   [][]byte{[]byte("hello"), []byte("color")},
			},
			wants: wants{
				vals: [][]byte{[]byte("world"), nil},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, close := init(tt.fields, t)
			defer close()

			err := s.View(context.Background(), func(tx kv.Tx) error {
				b, err := tx.Bucket(tt.args.bucket)
				if err != nil {
					t.Errorf("unexpected error retrieving bucket: %v", err)
					return err
				}

				vals, err := b.GetBatch(tt.args.keys...)
				if (err != nil) != (tt.wants.err != nil) {
					t.Errorf("expected error '%v' got '%v'", tt.wants.err, err)
					return err
				}

				if err != nil && tt.wants.err != nil {
					if err.Error() != tt.wants.err.Error() {
						t.Errorf("expected error messages to match '%v' got '%v'", tt.wants.err, err.Error())
						return err
					}
				}

				if want, got := tt.wants.vals, vals; !reflect.DeepEqual(want, got) {
					t.Errorf("exptected to get value %q got %q", want, got)
					return err
				}

				return nil
			})

			if err != nil {
				t.Fatalf("error during view transaction: %v", err)
			}
		})
	}
}

// KVPut tests the get method contract for the key value store.
func KVPut(
	init func(KVStoreFields, *testing.T) (kv.Store, func()),
	t *testing.T,
) {
	type args struct {
		bucket []byte
		key    []byte
		val    []byte
	}
	type wants struct {
		err error
	}

	tests := []struct {
		name   string
		fields KVStoreFields
		args   args
		wants  wants
	}{
		{
			name: "put pair",
			fields: KVStoreFields{
				Bucket: []byte("bucket"),
				Pairs:  []kv.Pair{},
			},
			args: args{
				bucket: []byte("bucket"),
				key:    []byte("hello"),
				val:    []byte("world"),
			},
			wants: wants{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, close := init(tt.fields, t)
			defer close()

			err := s.Update(context.Background(), func(tx kv.Tx) error {
				b, err := tx.Bucket(tt.args.bucket)
				if err != nil {
					t.Errorf("unexpected error retrieving bucket: %v", err)
					return err
				}

				{
					err := b.Put(tt.args.key, tt.args.val)
					if (err != nil) != (tt.wants.err != nil) {
						t.Errorf("expected error '%v' got '%v'", tt.wants.err, err)
						return err
					}

					if err != nil && tt.wants.err != nil {
						if err.Error() != tt.wants.err.Error() {
							t.Errorf("expected error messages to match '%v' got '%v'", tt.wants.err, err.Error())
							return err
						}
					}

					val, err := b.Get(tt.args.key)
					if err != nil {
						t.Errorf("unexpected error retrieving value: %v", err)
						return err
					}

					if want, got := tt.args.val, val; !bytes.Equal(want, got) {
						t.Errorf("exptected to get value %s got %s", string(want), string(got))
						return err
					}
				}

				return nil
			})

			if err != nil {
				t.Fatalf("error during view transaction: %v", err)
			}
		})
	}
}

// KVDelete tests the delete method contract for the key value store.
func KVDelete(
	init func(KVStoreFields, *testing.T) (kv.Store, func()),
	t *testing.T,
) {
	type args struct {
		bucket []byte
		key    []byte
	}
	type wants struct {
		err error
	}

	tests := []struct {
		name   string
		fields KVStoreFields
		args   args
		wants  wants
	}{
		{
			name: "delete key",
			fields: KVStoreFields{
				Bucket: []byte("bucket"),
				Pairs: []kv.Pair{
					{
						Key:   []byte("hello"),
						Value: []byte("world"),
					},
				},
			},
			args: args{
				bucket: []byte("bucket"),
				key:    []byte("hello"),
			},
			wants: wants{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, close := init(tt.fields, t)
			defer close()

			err := s.Update(context.Background(), func(tx kv.Tx) error {
				b, err := tx.Bucket(tt.args.bucket)
				if err != nil {
					t.Errorf("unexpected error retrieving bucket: %v", err)
					return err
				}

				{
					err := b.Delete(tt.args.key)
					if (err != nil) != (tt.wants.err != nil) {
						t.Errorf("expected error '%v' got '%v'", tt.wants.err, err)
						return err
					}

					if err != nil && tt.wants.err != nil {
						if err.Error() != tt.wants.err.Error() {
							t.Errorf("expected error messages to match '%v' got '%v'", tt.wants.err, err.Error())
							return err
						}
					}

					if _, err := b.Get(tt.args.key); err != kv.ErrKeyNotFound {
						t.Errorf("expected key not found error got %v", err)
						return err
					}
				}

				return nil
			})

			if err != nil {
				t.Fatalf("error during view transaction: %v", err)
			}
		})
	}
}

// KVCursor tests the cursor contract for the key value store.
func KVCursor(
	init func(KVStoreFields, *testing.T) (kv.Store, func()),
	t *testing.T,
) {
	type args struct {
		bucket []byte
		seek   []byte
	}
	type wants struct {
		err   error
		first kv.Pair
		last  kv.Pair
		seek  kv.Pair
		next  kv.Pair
		prev  kv.Pair
	}

	tests := []struct {
		name   string
		fields KVStoreFields
		args   args
		wants  wants
	}{
		{
			name: "basic cursor",
			fields: KVStoreFields{
				Bucket: []byte("bucket"),
				Pairs: []kv.Pair{
					{
						Key:   []byte("a"),
						Value: []byte("1"),
					},
					{
						Key:   []byte("ab"),
						Value: []byte("2"),
					},
					{
						Key:   []byte("abc"),
						Value: []byte("3"),
					},
					{
						Key:   []byte("abcd"),
						Value: []byte("4"),
					},
					{
						Key:   []byte("abcde"),
						Value: []byte("5"),
					},
					{
						Key:   []byte("bcd"),
						Value: []byte("6"),
					},
					{
						Key:   []byte("cd"),
						Value: []byte("7"),
					},
				},
			},
			args: args{
				bucket: []byte("bucket"),
				seek:   []byte("abc"),
			},
			wants: wants{
				first: kv.Pair{
					Key:   []byte("a"),
					Value: []byte("1"),
				},
				last: kv.Pair{
					Key:   []byte("cd"),
					Value: []byte("7"),
				},
				seek: kv.Pair{
					Key:   []byte("abc"),
					Value: []byte("3"),
				},
				next: kv.Pair{
					Key:   []byte("abcd"),
					Value: []byte("4"),
				},
				prev: kv.Pair{
					Key:   []byte("abc"),
					Value: []byte("3"),
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, close := init(tt.fields, t)
			defer close()

			err := s.View(context.Background(), func(tx kv.Tx) error {
				b, err := tx.Bucket(tt.args.bucket)
				if err != nil {
					t.Errorf("unexpected error retrieving bucket: %v", err)
					return err
				}

				cur, err := b.Cursor()
				if (err != nil) != (tt.wants.err != nil) {
					t.Errorf("expected error '%v' got '%v'", tt.wants.err, err)
					return err
				}

				if err != nil && tt.wants.err != nil {
					if err.Error() != tt.wants.err.Error() {
						t.Errorf("expected error messages to match '%v' got '%v'", tt.wants.err, err.Error())
						return err
					}
				}

				{
					key, val := cur.First()
					if want, got := tt.wants.first.Key, key; !bytes.Equal(want, got) {
						t.Errorf("exptected to get key %s got %s", string(want), string(got))
						return err
					}

					if want, got := tt.wants.first.Value, val; !bytes.Equal(want, got) {
						t.Errorf("exptected to get value %s got %s", string(want), string(got))
						return err
					}
				}

				{
					key, val := cur.Last()
					if want, got := tt.wants.last.Key, key; !bytes.Equal(want, got) {
						t.Errorf("exptected to get key %s got %s", string(want), string(got))
						return err
					}

					if want, got := tt.wants.last.Value, val; !bytes.Equal(want, got) {
						t.Errorf("exptected to get value %s got %s", string(want), string(got))
						return err
					}
				}

				{
					key, val := cur.Seek(tt.args.seek)
					if want, got := tt.wants.seek.Key, key; !bytes.Equal(want, got) {
						t.Errorf("exptected to get key %s got %s", string(want), string(got))
						return err
					}

					if want, got := tt.wants.seek.Value, val; !bytes.Equal(want, got) {
						t.Errorf("exptected to get value %s got %s", string(want), string(got))
						return err
					}
				}

				{
					key, val := cur.Next()
					if want, got := tt.wants.next.Key, key; !bytes.Equal(want, got) {
						t.Errorf("exptected to get key %s got %s", string(want), string(got))
						return err
					}

					if want, got := tt.wants.next.Value, val; !bytes.Equal(want, got) {
						t.Errorf("exptected to get value %s got %s", string(want), string(got))
						return err
					}
				}

				{
					key, val := cur.Prev()
					if want, got := tt.wants.prev.Key, key; !bytes.Equal(want, got) {
						t.Errorf("exptected to get key %s got %s", string(want), string(got))
						return err
					}

					if want, got := tt.wants.prev.Value, val; !bytes.Equal(want, got) {
						t.Errorf("exptected to get value %s got %s", string(want), string(got))
						return err
					}
				}

				return nil
			})

			if err != nil {
				t.Fatalf("error during view transaction: %v", err)
			}
		})
	}
}

// KVCursor tests the cursor contract for the key value store.
func KVCursorWithHints(
	init func(KVStoreFields, *testing.T) (kv.Store, func()),
	t *testing.T,
) {
	type args struct {
		seek  string
		until string
		hints []kv.CursorHint
	}

	pairs := func(keys ...string) []kv.Pair {
		p := make([]kv.Pair, len(keys))
		for i, k := range keys {
			p[i].Key = []byte(k)
			p[i].Value = []byte("val:" + k)
		}
		return p
	}

	tests := []struct {
		name   string
		fields KVStoreFields
		args   args
		exp    []string
	}{
		{
			name: "no hints",
			fields: KVStoreFields{
				Bucket: []byte("bucket"),
				Pairs: pairs(
					"aa/00", "aa/01",
					"aaa/00", "aaa/01", "aaa/02", "aaa/03",
					"bbb/00", "bbb/01", "bbb/02"),
			},
			args: args{
				seek:  "aaa",
				until: "bbb/00",
			},
			exp: []string{"aaa/00", "aaa/01", "aaa/02", "aaa/03", "bbb/00"},
		},
		{
			name: "prefix hint",
			fields: KVStoreFields{
				Bucket: []byte("bucket"),
				Pairs: pairs(
					"aa/00", "aa/01",
					"aaa/00", "aaa/01", "aaa/02", "aaa/03",
					"bbb/00", "bbb/01", "bbb/02"),
			},
			args: args{
				seek:  "aaa",
				until: "aaa/03",
				hints: []kv.CursorHint{kv.WithCursorHintPrefix("aaa/")},
			},
			exp: []string{"aaa/00", "aaa/01", "aaa/02", "aaa/03"},
		},
		{
			name: "start hint",
			fields: KVStoreFields{
				Bucket: []byte("bucket"),
				Pairs: pairs(
					"aa/00", "aa/01",
					"aaa/00", "aaa/01", "aaa/02", "aaa/03",
					"bbb/00", "bbb/01", "bbb/02"),
			},
			args: args{
				seek:  "aaa",
				until: "bbb/00",
				hints: []kv.CursorHint{kv.WithCursorHintKeyStart("aaa/")},
			},
			exp: []string{"aaa/00", "aaa/01", "aaa/02", "aaa/03", "bbb/00"},
		},
		{
			name: "predicate for key",
			fields: KVStoreFields{
				Bucket: []byte("bucket"),
				Pairs: pairs(
					"aa/00", "aa/01",
					"aaa/00", "aaa/01", "aaa/02", "aaa/03",
					"bbb/00", "bbb/01", "bbb/02"),
			},
			args: args{
				seek:  "aaa",
				until: "aaa/03",
				hints: []kv.CursorHint{
					kv.WithCursorHintPredicate(func(key, _ []byte) bool {
						return len(key) < 3 || string(key[:3]) == "aaa"
					})},
			},
			exp: []string{"aaa/00", "aaa/01", "aaa/02", "aaa/03"},
		},
		{
			name: "predicate for value",
			fields: KVStoreFields{
				Bucket: []byte("bucket"),
				Pairs: pairs(
					"aa/00", "aa/01",
					"aaa/00", "aaa/01", "aaa/02", "aaa/03",
					"bbb/00", "bbb/01", "bbb/02"),
			},
			args: args{
				seek:  "",
				until: "aa/01",
				hints: []kv.CursorHint{
					kv.WithCursorHintPredicate(func(_, val []byte) bool {
						return len(val) < 7 || string(val[:7]) == "val:aa/"
					})},
			},
			exp: []string{"aa/00", "aa/01"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, fin := init(tt.fields, t)
			defer fin()

			err := s.View(context.Background(), func(tx kv.Tx) error {
				b, err := tx.Bucket([]byte("bucket"))
				if err != nil {
					t.Errorf("unexpected error retrieving bucket: %v", err)
					return err
				}

				cur, err := b.Cursor(tt.args.hints...)
				if err != nil {
					t.Errorf("unexpected error: %v", err)
					return err
				}

				var got []string
				k, _ := cur.Seek([]byte(tt.args.seek))
				for len(k) > 0 {
					got = append(got, string(k))
					if string(k) == tt.args.until {
						break
					}
					k, _ = cur.Next()
				}

				if exp := tt.exp; !cmp.Equal(got, exp) {
					t.Errorf("unexpected cursor values: -got/+exp\n%v", cmp.Diff(got, exp))
				}

				return nil
			})

			if err != nil {
				t.Fatalf("error during view transaction: %v", err)
			}
		})
	}
}

// KVForwardCursor tests the forward cursor contract for the key value store.
func KVForwardCursor(
	init func(KVStoreFields, *testing.T) (kv.Store, func()),
	t *testing.T,
) {
	type args struct {
		seek  string
		until string
		opts  []kv.CursorOption
	}

	pairs := func(keys ...string) []kv.Pair {
		p := make([]kv.Pair, len(keys))
		for i, k := range keys {
			p[i].Key = []byte(k)
			p[i].Value = []byte("val:" + k)
		}
		return p
	}

	tests := []struct {
		name   string
		fields KVStoreFields
		args   args
		exp    []string
		expErr error
	}{
		{
			name: "no hints",
			fields: KVStoreFields{
				Bucket: []byte("bucket"),
				Pairs: pairs(
					"aa/00", "aa/01",
					"aaa/00", "aaa/01", "aaa/02", "aaa/03",
					"bbb/00", "bbb/01", "bbb/02"),
			},
			args: args{
				seek:  "aaa",
				until: "bbb/00",
			},
			exp: []string{"aaa/00", "aaa/01", "aaa/02", "aaa/03", "bbb/00"},
		},
		{
			name: "limit",
			fields: KVStoreFields{
				Bucket: []byte("bucket"),
				Pairs: pairs(
					"aa/00", "aa/01",
					"aaa/00", "aaa/01", "aaa/02", "aaa/03",
					"bbb/00", "bbb/01", "bbb/02"),
			},
			args: args{
				seek: "aaa",
				opts: []kv.CursorOption{
					kv.WithCursorLimit(4),
				},
			},
			exp: []string{"aaa/00", "aaa/01", "aaa/02", "aaa/03"},
		},
		{
			name: "prefix - no hints",
			fields: KVStoreFields{
				Bucket: []byte("bucket"),
				Pairs: pairs(
					"aa/00", "aa/01",
					"aaa/00", "aaa/01", "aaa/02", "aaa/03",
					"bbb/00", "bbb/01", "bbb/02"),
			},
			args: args{
				seek:  "aaa/00",
				until: "bbb/02",
				opts: []kv.CursorOption{
					kv.WithCursorPrefix([]byte("aaa")),
				},
			},
			exp: []string{"aaa/00", "aaa/01", "aaa/02", "aaa/03"},
		},

		{
			name: "prefix with limit",
			fields: KVStoreFields{
				Bucket: []byte("bucket"),
				Pairs: pairs(
					"aa/00", "aa/01",
					"aaa/00", "aaa/01", "aaa/02", "aaa/03",
					"bbb/00", "bbb/01", "bbb/02"),
			},
			args: args{
				seek:  "aaa/00",
				until: "bbb/02",
				opts: []kv.CursorOption{
					kv.WithCursorPrefix([]byte("aaa")),
					kv.WithCursorLimit(2),
				},
			},
			exp: []string{"aaa/00", "aaa/01"},
		},
		{
			name: "prefix - skip first",
			fields: KVStoreFields{
				Bucket: []byte("bucket"),
				Pairs: pairs(
					"aa/00", "aa/01",
					"aaa/00", "aaa/01", "aaa/02", "aaa/03",
					"bbb/00", "bbb/01", "bbb/02"),
			},
			args: args{
				seek:  "aaa/00",
				until: "bbb/02",
				opts: []kv.CursorOption{
					kv.WithCursorPrefix([]byte("aaa")),
					kv.WithCursorSkipFirstItem(),
				},
			},
			exp: []string{"aaa/01", "aaa/02", "aaa/03"},
		},
		{
			name: "prefix - skip first with limit",
			fields: KVStoreFields{
				Bucket: []byte("bucket"),
				Pairs: pairs(
					"aa/00", "aa/01",
					"aaa/00", "aaa/01", "aaa/02", "aaa/03",
					"bbb/00", "bbb/01", "bbb/02"),
			},
			args: args{
				seek:  "aaa/00",
				until: "bbb/02",
				opts: []kv.CursorOption{
					kv.WithCursorPrefix([]byte("aaa")),
					kv.WithCursorSkipFirstItem(),
					kv.WithCursorLimit(2),
				},
			},
			exp: []string{"aaa/01", "aaa/02"},
		},
		{
			name: "prefix - skip first (one item)",
			fields: KVStoreFields{
				Bucket: []byte("bucket"),
				Pairs: pairs(
					"aa/00", "aa/01",
					"aaa/00", "aaa/01", "aaa/02", "aaa/03",
					"bbb/00", "bbb/01", "bbb/02"),
			},
			args: args{
				seek: "bbb/02",
				opts: []kv.CursorOption{
					kv.WithCursorSkipFirstItem(),
				},
			},
			exp: nil,
		},
		{
			name: "prefix - does not prefix seek",
			fields: KVStoreFields{
				Bucket: []byte("bucket"),
				Pairs: pairs(
					"aa/00", "aa/01",
					"aaa/00", "aaa/01", "aaa/02", "aaa/03",
					"bbb/00", "bbb/01", "bbb/02"),
			},
			args: args{
				seek:  "aaa/00",
				until: "bbb/02",
				opts: []kv.CursorOption{
					kv.WithCursorPrefix([]byte("aab")),
				},
			},
			expErr: kv.ErrSeekMissingPrefix,
		},
		{
			name: "prefix hint",
			fields: KVStoreFields{
				Bucket: []byte("bucket"),
				Pairs: pairs(
					"aa/00", "aa/01",
					"aaa/00", "aaa/01", "aaa/02", "aaa/03",
					"bbb/00", "bbb/01", "bbb/02"),
			},
			args: args{
				seek:  "aaa",
				until: "aaa/03",
				opts: []kv.CursorOption{
					kv.WithCursorHints(kv.WithCursorHintPrefix("aaa/")),
				},
			},
			exp: []string{"aaa/00", "aaa/01", "aaa/02", "aaa/03"},
		},
		{
			name: "start hint",
			fields: KVStoreFields{
				Bucket: []byte("bucket"),
				Pairs: pairs(
					"aa/00", "aa/01",
					"aaa/00", "aaa/01", "aaa/02", "aaa/03",
					"bbb/00", "bbb/01", "bbb/02"),
			},
			args: args{
				seek:  "aaa",
				until: "bbb/00",
				opts: []kv.CursorOption{
					kv.WithCursorHints(kv.WithCursorHintKeyStart("aaa/")),
				},
			},
			exp: []string{"aaa/00", "aaa/01", "aaa/02", "aaa/03", "bbb/00"},
		},
		{
			name: "predicate for key",
			fields: KVStoreFields{
				Bucket: []byte("bucket"),
				Pairs: pairs(
					"aa/00", "aa/01",
					"aaa/00", "aaa/01", "aaa/02", "aaa/03",
					"bbb/00", "bbb/01", "bbb/02"),
			},
			args: args{
				seek:  "aaa",
				until: "aaa/03",
				opts: []kv.CursorOption{
					kv.WithCursorHints(kv.WithCursorHintPredicate(func(key, _ []byte) bool {
						return len(key) < 3 || string(key[:3]) == "aaa"
					})),
				},
			},
			exp: []string{"aaa/00", "aaa/01", "aaa/02", "aaa/03"},
		},
		{
			name: "predicate for value",
			fields: KVStoreFields{
				Bucket: []byte("bucket"),
				Pairs: pairs(
					"aa/00", "aa/01",
					"aaa/00", "aaa/01", "aaa/02", "aaa/03",
					"bbb/00", "bbb/01", "bbb/02"),
			},
			args: args{
				seek:  "",
				until: "aa/01",
				opts: []kv.CursorOption{
					kv.WithCursorHints(kv.WithCursorHintPredicate(func(_, val []byte) bool {
						return len(val) < 7 || string(val[:7]) == "val:aa/"
					})),
				},
			},
			exp: []string{"aa/00", "aa/01"},
		},
		{
			name: "no hints - descending",
			fields: KVStoreFields{
				Bucket: []byte("bucket"),
				Pairs: pairs(
					"aa/00", "aa/01",
					"aaa/00", "aaa/01", "aaa/02", "aaa/03",
					"bbb/00", "bbb/01", "bbb/02"),
			},
			args: args{
				seek:  "bbb/00",
				until: "aaa/00",
				opts:  []kv.CursorOption{kv.WithCursorDirection(kv.CursorDescending)},
			},
			exp: []string{"bbb/00", "aaa/03", "aaa/02", "aaa/01", "aaa/00"},
		},
		{
			name: "no hints - descending - with limit",
			fields: KVStoreFields{
				Bucket: []byte("bucket"),
				Pairs: pairs(
					"aa/00", "aa/01",
					"aaa/00", "aaa/01", "aaa/02", "aaa/03",
					"bbb/00", "bbb/01", "bbb/02"),
			},
			args: args{
				seek:  "bbb/00",
				until: "aaa/00",
				opts: []kv.CursorOption{
					kv.WithCursorDirection(kv.CursorDescending),
					kv.WithCursorLimit(3),
				},
			},
			exp: []string{"bbb/00", "aaa/03", "aaa/02"},
		},
		{
			name: "prefixed - no hints - descending",
			fields: KVStoreFields{
				Bucket: []byte("bucket"),
				Pairs: pairs(
					"aa/00", "aa/01",
					"aaa/00", "aaa/01", "aaa/02", "aaa/03",
					"bbb/00", "bbb/01", "bbb/02"),
			},
			args: args{
				seek:  "aaa/02",
				until: "aa/",
				opts: []kv.CursorOption{
					kv.WithCursorPrefix([]byte("aaa/")),
					kv.WithCursorDirection(kv.CursorDescending),
				},
			},
			exp: []string{"aaa/02", "aaa/01", "aaa/00"},
		},
		{
			name: "prefixed - no hints - descending - with limit",
			fields: KVStoreFields{
				Bucket: []byte("bucket"),
				Pairs: pairs(
					"aa/00", "aa/01",
					"aaa/00", "aaa/01", "aaa/02", "aaa/03",
					"bbb/00", "bbb/01", "bbb/02"),
			},
			args: args{
				seek:  "aaa/02",
				until: "aa/",
				opts: []kv.CursorOption{
					kv.WithCursorPrefix([]byte("aaa/")),
					kv.WithCursorDirection(kv.CursorDescending),
					kv.WithCursorLimit(2),
				},
			},
			exp: []string{"aaa/02", "aaa/01"},
		},
		{
			name: "start hint - descending",
			fields: KVStoreFields{
				Bucket: []byte("bucket"),
				Pairs: pairs(
					"aa/00", "aa/01",
					"aaa/00", "aaa/01", "aaa/02", "aaa/03",
					"bbb/00", "bbb/01", "bbb/02"),
			},
			args: args{
				seek:  "bbb/00",
				until: "aaa/00",
				opts: []kv.CursorOption{
					kv.WithCursorDirection(kv.CursorDescending),
					kv.WithCursorHints(kv.WithCursorHintKeyStart("aaa/")),
				},
			},
			exp: []string{"bbb/00", "aaa/03", "aaa/02", "aaa/01", "aaa/00"},
		},
		{
			name: "predicate for key - descending",
			fields: KVStoreFields{
				Bucket: []byte("bucket"),
				Pairs: pairs(
					"aa/00", "aa/01",
					"aaa/00", "aaa/01", "aaa/02", "aaa/03",
					"bbb/00", "bbb/01", "bbb/02"),
			},
			args: args{
				seek:  "aaa/03",
				until: "aaa/00",
				opts: []kv.CursorOption{
					kv.WithCursorDirection(kv.CursorDescending),
					kv.WithCursorHints(kv.WithCursorHintPredicate(func(key, _ []byte) bool {
						return len(key) < 3 || string(key[:3]) == "aaa"
					})),
				},
			},
			exp: []string{"aaa/03", "aaa/02", "aaa/01", "aaa/00"},
		},
		{
			name: "predicate for value - descending",
			fields: KVStoreFields{
				Bucket: []byte("bucket"),
				Pairs: pairs(
					"aa/00", "aa/01",
					"aaa/00", "aaa/01", "aaa/02", "aaa/03",
					"bbb/00", "bbb/01", "bbb/02"),
			},
			args: args{
				seek:  "aa/01",
				until: "aa/00",
				opts: []kv.CursorOption{
					kv.WithCursorDirection(kv.CursorDescending),
					kv.WithCursorHints(kv.WithCursorHintPredicate(func(_, val []byte) bool {
						return len(val) >= 7 && string(val[:7]) == "val:aa/"
					})),
				},
			},
			exp: []string{"aa/01", "aa/00"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, fin := init(tt.fields, t)
			defer fin()

			err := s.View(context.Background(), func(tx kv.Tx) error {
				b, err := tx.Bucket([]byte("bucket"))
				if err != nil {
					t.Errorf("unexpected error retrieving bucket: %v", err)
					return err
				}

				cur, err := b.ForwardCursor([]byte(tt.args.seek), tt.args.opts...)
				if err != nil {
					if tt.expErr != nil && errors.Is(err, tt.expErr) {
						// successfully returned expected error
						return nil
					}

					t.Errorf("unexpected error: %v", err)
					return err
				}

				var got []string

				k, _ := cur.Next()
				for len(k) > 0 {
					got = append(got, string(k))
					if string(k) == tt.args.until {
						break
					}

					k, _ = cur.Next()
				}

				if exp := tt.exp; !cmp.Equal(got, exp) {
					t.Errorf("unexpected cursor values: -got/+exp\n%v", cmp.Diff(got, exp))
				}

				if err := cur.Err(); !cmp.Equal(err, tt.expErr) {
					t.Errorf("expected error to be %v, got %v", tt.expErr, err)
				}

				if err := cur.Close(); err != nil {
					t.Errorf("expected cursor to close with nil error, found %v", err)
				}

				return nil
			})

			if err != nil {
				t.Fatalf("error during view transaction: %v", err)
			}
		})
	}
}

// KVView tests the view method contract for the key value store.
func KVView(
	init func(KVStoreFields, *testing.T) (kv.Store, func()),
	t *testing.T,
) {
	type args struct {
		bucket []byte
		key    []byte
		// If len(value) == 0 the test will not attempt a put
		value []byte
		// If true, the test will attempt to delete the provided key
		delete bool
	}
	type wants struct {
		value []byte
	}

	tests := []struct {
		name   string
		fields KVStoreFields
		args   args
		wants  wants
	}{
		{
			name: "basic view",
			fields: KVStoreFields{
				Bucket: []byte("bucket"),
				Pairs: []kv.Pair{
					{
						Key:   []byte("hello"),
						Value: []byte("cruel world"),
					},
				},
			},
			args: args{
				bucket: []byte("bucket"),
				key:    []byte("hello"),
			},
			wants: wants{
				value: []byte("cruel world"),
			},
		},
		{
			name: "basic view with delete",
			fields: KVStoreFields{
				Bucket: []byte("bucket"),
				Pairs: []kv.Pair{
					{
						Key:   []byte("hello"),
						Value: []byte("cruel world"),
					},
				},
			},
			args: args{
				bucket: []byte("bucket"),
				key:    []byte("hello"),
				delete: true,
			},
			wants: wants{
				value: []byte("cruel world"),
			},
		},
		{
			name: "basic view with put",
			fields: KVStoreFields{
				Bucket: []byte("bucket"),
				Pairs: []kv.Pair{
					{
						Key:   []byte("hello"),
						Value: []byte("cruel world"),
					},
				},
			},
			args: args{
				bucket: []byte("bucket"),
				key:    []byte("hello"),
				value:  []byte("world"),
				delete: true,
			},
			wants: wants{
				value: []byte("cruel world"),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, close := init(tt.fields, t)
			defer close()

			err := s.View(context.Background(), func(tx kv.Tx) error {
				b, err := tx.Bucket(tt.args.bucket)
				if err != nil {
					t.Errorf("unexpected error retrieving bucket: %v", err)
					return err
				}

				if len(tt.args.value) != 0 {
					err := b.Put(tt.args.key, tt.args.value)
					if err == nil {
						return fmt.Errorf("expected transaction to fail")
					}
					if err != kv.ErrTxNotWritable {
						return err
					}
					return nil
				}

				value, err := b.Get(tt.args.key)
				if err != nil {
					return err
				}

				if want, got := tt.wants.value, value; !bytes.Equal(want, got) {
					t.Errorf("exptected to get value %s got %s", string(want), string(got))
					return err
				}

				if tt.args.delete {
					err := b.Delete(tt.args.key)
					if err == nil {
						return fmt.Errorf("expected transaction to fail")
					}
					if err != kv.ErrTxNotWritable {
						return err
					}
					return nil
				}

				return nil
			})

			if err != nil {
				t.Fatalf("error during view transaction: %v", err)
			}
		})
	}
}

// KVUpdate tests the update method contract for the key value store.
func KVUpdate(
	init func(KVStoreFields, *testing.T) (kv.Store, func()),
	t *testing.T,
) {
	type args struct {
		bucket []byte
		key    []byte
		value  []byte
		delete bool
	}
	type wants struct {
		value []byte
	}

	tests := []struct {
		name   string
		fields KVStoreFields
		args   args
		wants  wants
	}{
		{
			name: "basic update",
			fields: KVStoreFields{
				Bucket: []byte("bucket"),
				Pairs: []kv.Pair{
					{
						Key:   []byte("hello"),
						Value: []byte("cruel world"),
					},
				},
			},
			args: args{
				bucket: []byte("bucket"),
				key:    []byte("hello"),
				value:  []byte("world"),
			},
			wants: wants{
				value: []byte("world"),
			},
		},
		{
			name: "basic update with delete",
			fields: KVStoreFields{
				Bucket: []byte("bucket"),
				Pairs: []kv.Pair{
					{
						Key:   []byte("hello"),
						Value: []byte("cruel world"),
					},
				},
			},
			args: args{
				bucket: []byte("bucket"),
				key:    []byte("hello"),
				value:  []byte("world"),
				delete: true,
			},
			wants: wants{},
		},
		// TODO: add case with failed update transaction that doesn't apply all of the changes.
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, close := init(tt.fields, t)
			defer close()

			{
				err := s.Update(context.Background(), func(tx kv.Tx) error {
					b, err := tx.Bucket(tt.args.bucket)
					if err != nil {
						t.Errorf("unexpected error retrieving bucket: %v", err)
						return err
					}

					if len(tt.args.value) != 0 {
						err := b.Put(tt.args.key, tt.args.value)
						if err != nil {
							return err
						}
					}

					if tt.args.delete {
						err := b.Delete(tt.args.key)
						if err != nil {
							return err
						}
					}

					value, err := b.Get(tt.args.key)
					if tt.args.delete {
						if err != kv.ErrKeyNotFound {
							return fmt.Errorf("expected key not found")
						}
						return nil
					} else if err != nil {
						return err
					}

					if want, got := tt.wants.value, value; !bytes.Equal(want, got) {
						t.Errorf("exptected to get value %s got %s", string(want), string(got))
						return err
					}

					return nil
				})

				if err != nil {
					t.Fatalf("error during update transaction: %v", err)
				}
			}

			{
				err := s.View(context.Background(), func(tx kv.Tx) error {
					b, err := tx.Bucket(tt.args.bucket)
					if err != nil {
						t.Errorf("unexpected error retrieving bucket: %v", err)
						return err
					}

					value, err := b.Get(tt.args.key)
					if tt.args.delete {
						if err != kv.ErrKeyNotFound {
							return fmt.Errorf("expected key not found")
						}
					} else if err != nil {
						return err
					}

					if want, got := tt.wants.value, value; !bytes.Equal(want, got) {
						t.Errorf("exptected to get value %s got %s", string(want), string(got))
						return err
					}

					return nil
				})

				if err != nil {
					t.Fatalf("error during view transaction: %v", err)
				}
			}
		})
	}
}

// KVConcurrentUpdate tests concurrent calls to update.
func KVConcurrentUpdate(
	init func(KVStoreFields, *testing.T) (kv.Store, func()),
	t *testing.T,
) {
	type args struct {
		bucket []byte
		key    []byte
		valueA []byte
		valueB []byte
	}
	type wants struct {
		value []byte
	}

	tests := []struct {
		name   string
		fields KVStoreFields
		args   args
		wants  wants
	}{
		{
			name: "basic concurrent update",
			fields: KVStoreFields{
				Bucket: []byte("bucket"),
				Pairs: []kv.Pair{
					{
						Key:   []byte("hello"),
						Value: []byte("cruel world"),
					},
				},
			},
			args: args{
				bucket: []byte("bucket"),
				key:    []byte("hello"),
				valueA: []byte("world"),
				valueB: []byte("darkness my new friend"),
			},
			wants: wants{
				value: []byte("darkness my new friend"),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Skip("https://github.com/influxdata/platform/issues/2371")
			s, closeFn := init(tt.fields, t)
			defer closeFn()

			errCh := make(chan error)
			var fn = func(v []byte) {
				err := s.Update(context.Background(), func(tx kv.Tx) error {
					b, err := tx.Bucket(tt.args.bucket)
					if err != nil {
						return err
					}

					if err := b.Put(tt.args.key, v); err != nil {
						return err
					}

					return nil
				})

				if err != nil {
					errCh <- fmt.Errorf("error during update transaction: %v", err)
				} else {
					errCh <- nil
				}
			}
			go fn(tt.args.valueA)
			// To ensure that a is scheduled before b
			time.Sleep(time.Millisecond)
			go fn(tt.args.valueB)

			count := 0
			for err := range errCh {
				count++
				if err != nil {
					t.Fatal(err)
				}
				if count == 2 {
					break
				}
			}

			close(errCh)

			{
				err := s.View(context.Background(), func(tx kv.Tx) error {
					b, err := tx.Bucket(tt.args.bucket)
					if err != nil {
						t.Errorf("unexpected error retrieving bucket: %v", err)
						return err
					}

					deadline := time.Now().Add(1 * time.Second)
					var returnErr error
					for {
						if time.Now().After(deadline) {
							break
						}

						value, err := b.Get(tt.args.key)
						if err != nil {
							return err
						}

						if want, got := tt.wants.value, value; !bytes.Equal(want, got) {
							returnErr = fmt.Errorf("exptected to get value %s got %s", string(want), string(got))
						} else {
							returnErr = nil
							break
						}
					}

					if returnErr != nil {
						return returnErr
					}

					return nil
				})

				if err != nil {
					t.Fatalf("error during view transaction: %v", err)
				}
			}
		})
	}
}

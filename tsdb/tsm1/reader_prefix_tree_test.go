package tsm1

import (
	"bytes"
	"fmt"
	"reflect"
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestPrefixTree(t *testing.T) {
	t.Run("Basic", func(t *testing.T) {
		ranges := func(ns ...int64) (out []TimeRange) {
			for _, n := range ns {
				out = append(out, TimeRange{n, n})
			}
			return out
		}

		check := func(t *testing.T, tree *prefixTree, key string, exp []TimeRange) {
			t.Helper()
			got := tree.Search([]byte(key), nil)
			if !reflect.DeepEqual(got, exp) {
				t.Fatalf("bad search: %q:\n%v", key, cmp.Diff(got, exp))
			}
		}

		tree := newPrefixTree()
		tree.Append([]byte("abcdefghABCDEFGH"), ranges(1)...)
		tree.Append([]byte("abcdefgh01234567"), ranges(2)...)
		tree.Append([]byte("abcd"), ranges(3)...)
		tree.Append([]byte("0123"), ranges(4)...)
		tree.Append([]byte("abcdefghABCDEFGH-m1"), ranges(5)...)
		tree.Append([]byte("abcdefghABCDEFGH-m1"), ranges(6)...)
		tree.Append([]byte("abcdefgh01234567-m1"), ranges(7)...)
		tree.Append([]byte("abcdefgh01234567-m1"), ranges(8)...)
		tree.Append([]byte("abcdefgh"), ranges(9, 10)...)

		check(t, tree, "abcd", ranges(3))
		check(t, tree, "abcdefgh", ranges(3, 9, 10))
		check(t, tree, "abcdefghABCDEFGH", ranges(3, 9, 10, 1))
		check(t, tree, "abcdefghABCDEFGH-m1", ranges(3, 9, 10, 1, 5, 6))
		check(t, tree, "abcdefgh01234567-m1", ranges(3, 9, 10, 2, 7, 8))
	})
}

func BenchmarkPrefixTree(b *testing.B) {
	b.Run("Append", func(b *testing.B) {
		run := func(b *testing.B, prefix []byte) {
			tree := newPrefixTree()

			for i := 0; i < b.N; i++ {
				tree.Append(prefix)
			}
		}

		for i := 0; i <= 32; i += 4 {
			b.Run(fmt.Sprint(i), func(b *testing.B) { run(b, bytes.Repeat([]byte("0"), i)) })
		}
	})

	b.Run("Search", func(b *testing.B) {
		run := func(b *testing.B, worst bool) {
			run := func(b *testing.B, key []byte) {
				tree := newPrefixTree()
				if worst {
					for i := range key {
						tree.Append(key[:i])
					}
				} else {
					tree.Append(key)
				}
				b.ReportAllocs()
				b.ResetTimer()

				for i := 0; i < b.N; i++ {
					tree.Search(key, nil)
				}
			}

			for i := 0; i <= 32; i += 4 {
				b.Run(fmt.Sprint(i), func(b *testing.B) { run(b, bytes.Repeat([]byte("0"), i)) })
			}
		}

		b.Run("Best", func(b *testing.B) { run(b, false) })
		b.Run("Worst", func(b *testing.B) { run(b, true) })
	})
}

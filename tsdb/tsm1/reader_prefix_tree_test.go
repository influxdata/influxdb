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

// Typical results on a 2018 MPB. Pay special attention to the
// 8 and 16 results as they are the most likely.
//
// BenchmarkPrefixTree/Append/0-8           300000000       5.93 ns/op
// BenchmarkPrefixTree/Append/4-8            20000000      93.7  ns/op
// BenchmarkPrefixTree/Append/8-8           100000000      12.9  ns/op
// BenchmarkPrefixTree/Append/12-8           20000000     100.0  ns/op
// BenchmarkPrefixTree/Append/16-8          100000000      20.4  ns/op
// BenchmarkPrefixTree/Append/20-8           20000000     111.0  ns/op
// BenchmarkPrefixTree/Append/24-8           50000000      28.5  ns/op
// BenchmarkPrefixTree/Append/28-8           20000000     118.0  ns/op
// BenchmarkPrefixTree/Append/32-8           50000000      35.8  ns/op
// BenchmarkPrefixTree/Search/Best/0-8      300000000       5.76 ns/op
// BenchmarkPrefixTree/Search/Best/4-8       20000000     102.0  ns/op
// BenchmarkPrefixTree/Search/Best/8-8      100000000      18.5  ns/op
// BenchmarkPrefixTree/Search/Best/12-8      20000000     116.0  ns/op
// BenchmarkPrefixTree/Search/Best/16-8      50000000      31.9  ns/op
// BenchmarkPrefixTree/Search/Best/20-8      10000000     131.0  ns/op
// BenchmarkPrefixTree/Search/Best/24-8      30000000      45.3  ns/op
// BenchmarkPrefixTree/Search/Best/28-8      10000000     142.0  ns/op
// BenchmarkPrefixTree/Search/Best/32-8      20000000      58.0  ns/op
// BenchmarkPrefixTree/Search/Worst/0-8     300000000       5.79 ns/op
// BenchmarkPrefixTree/Search/Worst/4-8      20000000      79.2  ns/op
// BenchmarkPrefixTree/Search/Worst/8-8      10000000     199.0  ns/op
// BenchmarkPrefixTree/Search/Worst/12-8      5000000     301.0  ns/op
// BenchmarkPrefixTree/Search/Worst/16-8      3000000     422.0  ns/op
// BenchmarkPrefixTree/Search/Worst/20-8      3000000     560.0  ns/op
// BenchmarkPrefixTree/Search/Worst/24-8      2000000     683.0  ns/op
// BenchmarkPrefixTree/Search/Worst/28-8      2000000     772.0  ns/op
// BenchmarkPrefixTree/Search/Worst/32-8      2000000     875.0  ns/op
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

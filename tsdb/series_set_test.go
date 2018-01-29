package tsdb

import (
	"fmt"
	"testing"
)

func TestSeriesIDSet_AndNot(t *testing.T) {
	examples := [][3][]uint64{
		[3][]uint64{
			{1, 10, 20, 30},
			{10, 12, 13, 14, 20},
			{1, 30},
		},
		[3][]uint64{
			{},
			{10},
			{},
		},
		[3][]uint64{
			{1, 10, 20, 30},
			{1, 10, 20, 30},
			{},
		},
		[3][]uint64{
			{1, 10},
			{1, 10, 100},
			{},
		},
		[3][]uint64{
			{1, 10},
			{},
			{1, 10},
		},
	}

	for i, example := range examples {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			// Build sets.
			a, b := NewSeriesIDSet(), NewSeriesIDSet()
			for _, v := range example[0] {
				a.Add(v)
			}
			for _, v := range example[1] {
				b.Add(v)
			}

			expected := NewSeriesIDSet()
			for _, v := range example[2] {
				expected.Add(v)
			}

			got := a.AndNot(b)
			if got.String() != expected.String() {
				t.Fatalf("got %s, expected %s", got.String(), expected.String())
			}
		})
	}

}

var resultBool bool

// Contains should be typically a constant time lookup. Example results on a laptop:
//
// BenchmarkSeriesIDSet_Contains/1-4 			20000000	        68.5 ns/op	       0 B/op	       0 allocs/op
// BenchmarkSeriesIDSet_Contains/2-4 			20000000	        70.8 ns/op	       0 B/op	       0 allocs/op
// BenchmarkSeriesIDSet_Contains/10-4         	20000000	        70.3 ns/op	       0 B/op	       0 allocs/op
// BenchmarkSeriesIDSet_Contains/100-4        	20000000	        71.3 ns/op	       0 B/op	       0 allocs/op
// BenchmarkSeriesIDSet_Contains/1000-4       	20000000	        80.5 ns/op	       0 B/op	       0 allocs/op
// BenchmarkSeriesIDSet_Contains/10000-4      	20000000	        67.3 ns/op	       0 B/op	       0 allocs/op
// BenchmarkSeriesIDSet_Contains/100000-4     	20000000	        73.1 ns/op	       0 B/op	       0 allocs/op
// BenchmarkSeriesIDSet_Contains/1000000-4    	20000000	        77.3 ns/op	       0 B/op	       0 allocs/op
// BenchmarkSeriesIDSet_Contains/10000000-4   	20000000	        75.3 ns/op	       0 B/op	       0 allocs/op
func BenchmarkSeriesIDSet_Contains(b *testing.B) {
	cardinalities := []uint64{1, 2, 10, 100, 1000, 10000, 100000, 1000000, 10000000}

	for _, cardinality := range cardinalities {
		// Setup...
		set := NewSeriesIDSet()
		for i := uint64(0); i < cardinality; i++ {
			set.Add(i)
		}

		lookup := cardinality / 2
		b.Run(fmt.Sprint(cardinality), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				resultBool = set.Contains(lookup)
			}
		})
	}
}

var set *SeriesIDSet

// Adding to a larger bitset shouldn't be significantly more expensive than adding
// to a smaller one. This benchmark adds a value to different cardinality sets.
//
// Example results from a laptop:
// BenchmarkSeriesIDSet_Add/1-4 	 		1000000	      1053 ns/op	      48 B/op	       2 allocs/op
// BenchmarkSeriesIDSet_Add/2-4 	 		5000000	       303 ns/op	       0 B/op	       0 allocs/op
// BenchmarkSeriesIDSet_Add/10-4         	5000000	       348 ns/op	       0 B/op	       0 allocs/op
// BenchmarkSeriesIDSet_Add/100-4        	5000000	       373 ns/op	       0 B/op	       0 allocs/op
// BenchmarkSeriesIDSet_Add/1000-4       	5000000	       342 ns/op	       0 B/op	       0 allocs/op
//
//
func BenchmarkSeriesIDSet_AddMore(b *testing.B) {
	cardinalities := []uint64{1, 2, 10, 100, 1000, 10000, 100000, 1000000, 10000000}

	for _, cardinality := range cardinalities {
		// Setup...
		set = NewSeriesIDSet()
		for i := uint64(0); i < cardinality-1; i++ {
			set.Add(i)
		}

		b.Run(fmt.Sprint(cardinality), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				// Add next value
				set.Add(cardinality)

				b.StopTimer()
				set.Remove(cardinality)
				b.StartTimer()
			}
		})
	}
}

// Add benchmarks the cost of adding the same element to a set versus the
// cost of checking if it exists before adding it.
//
// Typical benchmarks from a laptop:
//
// BenchmarkSeriesIDSet_Add/cardinality_1000000_add_same-4         			20000000	        89.5 ns/op	       0 B/op	       0 allocs/op
// BenchmarkSeriesIDSet_Add/cardinality_1000000_check_add_global_lock-4     30000000	        56.9 ns/op	       0 B/op	       0 allocs/op
// BenchmarkSeriesIDSet_Add/cardinality_1000000_check_add_multi_lock-4      20000000	        75.7 ns/op	       0 B/op	       0 allocs/op
//
func BenchmarkSeriesIDSet_Add(b *testing.B) {
	// Setup...
	set = NewSeriesIDSet()
	for i := uint64(0); i < 1000000; i++ {
		set.Add(i)
	}
	lookup := uint64(300032)

	// Add the same value over and over.
	b.Run(fmt.Sprint("cardinality_1000000_add_same"), func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			set.Add(lookup)
		}
	})

	// Check if the value exists before adding it. Subsequent repeats of the code
	// will result in contains checks.
	b.Run(fmt.Sprint("cardinality_1000000_check_add_global_lock"), func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			set.Lock()
			if !set.ContainsNoLock(lookup) {
				set.AddNoLock(lookup)
			}
			set.Unlock()
		}
	})

	// Check if the value exists before adding it under two locks.
	b.Run(fmt.Sprint("cardinality_1000000_check_add_multi_lock"), func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			if !set.Contains(lookup) {
				set.Add(lookup)
			}
		}
	})
}

// Remove benchmarks the cost of removing the same element in a set versus the
// cost of checking if it exists before removing it.
//
// Typical benchmarks from a laptop:
//
// BenchmarkSeriesIDSet_Remove/cardinality_1000000_remove_same-4         		20000000	        99.1 ns/op	       0 B/op	       0 allocs/op
// BenchmarkSeriesIDSet_Remove/cardinality_1000000_check_remove_global_lock-4   20000000	        57.7 ns/op	       0 B/op	       0 allocs/op
// BenchmarkSeriesIDSet_Remove/cardinality_1000000_check_remove_multi_lock-4    20000000	        80.1 ns/op	       0 B/op	       0 allocs/op
//
func BenchmarkSeriesIDSet_Remove(b *testing.B) {
	// Setup...
	set = NewSeriesIDSet()
	for i := uint64(0); i < 1000000; i++ {
		set.Add(i)
	}
	lookup := uint64(300032)

	// Remove the same value over and over.
	b.Run(fmt.Sprint("cardinality_1000000_remove_same"), func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			set.Remove(lookup)
		}
	})

	// Check if the value exists before adding it. Subsequent repeats of the code
	// will result in contains checks.
	b.Run(fmt.Sprint("cardinality_1000000_check_remove_global_lock"), func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			set.Lock()
			if set.ContainsNoLock(lookup) {
				set.RemoveNoLock(lookup)
			}
			set.Unlock()
		}
	})

	// Check if the value exists before adding it under two locks.
	b.Run(fmt.Sprint("cardinality_1000000_check_remove_multi_lock"), func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			if set.Contains(lookup) {
				set.Remove(lookup)
			}
		}
	})
}

// Typical benchmarks for a laptop:
//
// BenchmarkSeriesIDSet_Merge_Duplicates/cardinality_1/shards_1-4         	  200000	      8095 ns/op	   16656 B/op	      11 allocs/op
// BenchmarkSeriesIDSet_Merge_Duplicates/cardinality_1/shards_10-4        	  200000	     11755 ns/op	   18032 B/op	      47 allocs/op
// BenchmarkSeriesIDSet_Merge_Duplicates/cardinality_1/shards_100-4       	   50000	     41632 ns/op	   31794 B/op	     407 allocs/op
// BenchmarkSeriesIDSet_Merge_Duplicates/cardinality_10000/shards_1-4     	  200000	      6022 ns/op	    8384 B/op	       7 allocs/op
// BenchmarkSeriesIDSet_Merge_Duplicates/cardinality_10000/shards_10-4    	  100000	     19674 ns/op	    9760 B/op	      43 allocs/op
// BenchmarkSeriesIDSet_Merge_Duplicates/cardinality_10000/shards_100-4   	   10000	    152865 ns/op	   23522 B/op	     403 allocs/op
// BenchmarkSeriesIDSet_Merge_Duplicates/cardinality_1000000/shards_1-4   	  200000	      8252 ns/op	    9712 B/op	      44 allocs/op
// BenchmarkSeriesIDSet_Merge_Duplicates/cardinality_1000000/shards_10-4  	   50000	     29566 ns/op	   15984 B/op	     143 allocs/op
// BenchmarkSeriesIDSet_Merge_Duplicates/cardinality_1000000/shards_100-4 	   10000	    237672 ns/op	   78710 B/op	    1133 allocs/op
// BenchmarkSeriesIDSet_Merge_Duplicates/cardinality_10000000/shards_1-4  	  100000	     21559 ns/op	   25968 B/op	     330 allocs/op
// BenchmarkSeriesIDSet_Merge_Duplicates/cardinality_10000000/shards_10-4 	   20000	    102326 ns/op	  114325 B/op	     537 allocs/op
// BenchmarkSeriesIDSet_Merge_Duplicates/cardinality_10000000/shards_100-4      2000	   1042697 ns/op	  997909 B/op	    2608 allocs/op
func BenchmarkSeriesIDSet_Merge_Duplicates(b *testing.B) {
	cardinalities := []int{1, 10000, 1000000, 10000000}
	shards := []int{1, 10, 100}

	for _, cardinality := range cardinalities {
		set = NewSeriesIDSet()
		for i := 0; i < cardinality; i++ {
			set.Add(uint64(i))
		}

		for _, shard := range shards {
			others := make([]*SeriesIDSet, 0, shard)
			for s := 0; s < shard; s++ {
				others = append(others, &SeriesIDSet{bitmap: set.bitmap.Clone()})
			}

			b.Run(fmt.Sprintf("cardinality_%d/shards_%d", cardinality, shard), func(b *testing.B) {
				base := &SeriesIDSet{bitmap: set.bitmap.Clone()}
				for i := 0; i < b.N; i++ {
					base.Merge(others...)
					b.StopTimer()
					base.bitmap = set.bitmap.Clone()
					b.StartTimer()
				}
			})

		}
	}
}

// Typical benchmarks for a laptop:
//
// BenchmarkSeriesIDSet_Merge_Unique/cardinality_1/shards_1-4         	  200000	      7841 ns/op	   16656 B/op	      11 allocs/op
// BenchmarkSeriesIDSet_Merge_Unique/cardinality_1/shards_10-4        	  200000	     13093 ns/op	   18048 B/op	      47 allocs/op
// BenchmarkSeriesIDSet_Merge_Unique/cardinality_1/shards_100-4       	   30000	     57399 ns/op	   31985 B/op	     407 allocs/op
// BenchmarkSeriesIDSet_Merge_Unique/cardinality_10000/shards_1-4     	  200000	      7740 ns/op	    8384 B/op	       7 allocs/op
// BenchmarkSeriesIDSet_Merge_Unique/cardinality_10000/shards_10-4    	   50000	     37116 ns/op	   18208 B/op	      52 allocs/op
// BenchmarkSeriesIDSet_Merge_Unique/cardinality_10000/shards_100-4   	    5000	    409487 ns/op	  210563 B/op	     955 allocs/op
// BenchmarkSeriesIDSet_Merge_Unique/cardinality_1000000/shards_1-4   	  100000	     19289 ns/op	   19328 B/op	      79 allocs/op
// BenchmarkSeriesIDSet_Merge_Unique/cardinality_1000000/shards_10-4  	   10000	    129048 ns/op	  159716 B/op	     556 allocs/op
// BenchmarkSeriesIDSet_Merge_Unique/cardinality_1000000/shards_100-4 	     500	   3482907 ns/op	 5428116 B/op	    6174 allocs/op
// BenchmarkSeriesIDSet_Merge_Unique/cardinality_10000000/shards_1-4  	   30000	     43734 ns/op	   51872 B/op	     641 allocs/op
// BenchmarkSeriesIDSet_Merge_Unique/cardinality_10000000/shards_10-4 	    3000	    514412 ns/op	  748678 B/op	    3687 allocs/op
// BenchmarkSeriesIDSet_Merge_Unique/cardinality_10000000/shards_100-4         	      30	  61891687 ns/op	69626539 B/op	   36038 allocs/op
func BenchmarkSeriesIDSet_Merge_Unique(b *testing.B) {
	cardinalities := []int{1, 10000, 1000000, 10000000}
	shards := []int{1, 10, 100}

	for _, cardinality := range cardinalities {
		set = NewSeriesIDSet()
		for i := 0; i < cardinality; i++ {
			set.Add(uint64(i))
		}

		for _, shard := range shards {
			others := make([]*SeriesIDSet, 0, shard)
			for s := 1; s <= shard; s++ {
				other := NewSeriesIDSet()
				for i := 0; i < cardinality; i++ {
					other.Add(uint64(i + (s * cardinality)))
				}
				others = append(others, other)
			}

			b.Run(fmt.Sprintf("cardinality_%d/shards_%d", cardinality, shard), func(b *testing.B) {
				base := &SeriesIDSet{bitmap: set.bitmap.Clone()}
				for i := 0; i < b.N; i++ {
					base.Merge(others...)
					b.StopTimer()
					base.bitmap = set.bitmap.Clone()
					b.StartTimer()
				}
			})
		}
	}
}

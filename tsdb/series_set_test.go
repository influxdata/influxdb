package tsdb

import (
	"bytes"
	"fmt"
	"math"
	"math/rand"
	"runtime"
	"sync"
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

// Ensure that cloning is race-free.
func TestSeriesIDSet_Clone_Race(t *testing.T) {
	main := NewSeriesIDSet()
	total := NewSeriesIDSet()
	for i := uint64(0); i < 1024; i++ {
		main.AddNoLock(i)
		total.AddNoLock(i)
	}

	// One test with a closure around the main SeriesIDSet,
	// so that we can run a subtest with and without COW.
	test := func(t *testing.T) {
		n := 10 * (runtime.NumCPU() + 1)
		clones := make([]*SeriesIDSet, n)
		var wg sync.WaitGroup
		wg.Add(n)
		for i := 1; i <= n; i++ {
			go func(i int) {
				defer wg.Done()
				clones[i-1] = main.Clone()

				for j := 0; j < 1000; j++ {
					id := uint64(j + (100000 * i))
					total.Add(id)
					clones[i-1].AddNoLock(id)
				}
			}(i)
		}

		wg.Wait()
		for _, o := range clones {
			if got, exp := o.Cardinality(), uint64(2024); got != exp {
				t.Errorf("got cardinality %d, expected %d", got, exp)
			}
		}

		// The original set should be unaffected
		if got, exp := main.Cardinality(), uint64(1024); got != exp {
			t.Errorf("got cardinality %d, expected %d", got, exp)
		}

		// Merging the clones should result in only 1024 shared values.
		union := NewSeriesIDSet()
		for _, o := range clones {
			o.ForEachNoLock(func(id uint64) {
				union.AddNoLock(id)
			})
		}

		if !union.Equals(total) {
			t.Fatal("union not equal to total")
		}
	}
	t.Run("clone", test)
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
// BenchmarkSeriesIDSet_Add/cardinality_1000000_add/same-8    							20000000	        64.8 ns/op	       0 B/op	       0 allocs/op
// BenchmarkSeriesIDSet_Add/cardinality_1000000_add/random-8  	 						 2000000	       704 	 ns/op	       5 B/op	       0 allocs/op
// BenchmarkSeriesIDSet_Add/cardinality_1000000_add/same_no_lock-8         				50000000	        40.3 ns/op	       0 B/op	       0 allocs/op
// BenchmarkSeriesIDSet_Add/cardinality_1000000_add/random_no_lock-8       				 2000000	       644   ns/op	       5 B/op	       0 allocs/op
// BenchmarkSeriesIDSet_Add/cardinality_1000000_check_add/same_no_lock-8   				50000000	        34.0 ns/op	       0 B/op	       0 allocs/op
// BenchmarkSeriesIDSet_Add/cardinality_1000000_check_add/random_no_lock-8 	 		   	 2000000	       860   ns/op	      14 B/op	       0 allocs/op
// BenchmarkSeriesIDSet_Add/cardinality_1000000_check_add/same_global_lock-8         	30000000	        49.8 ns/op	       0 B/op	       0 allocs/op
// BenchmarkSeriesIDSet_Add/cardinality_1000000_check_add/random_global_lock-8       	 2000000	       914   ns/op	       0 B/op	       0 allocs/op
// BenchmarkSeriesIDSet_Add/cardinality_1000000_check_add/same_multi_lock-8          	30000000	        39.7 ns/op	       0 B/op	       0 allocs/op
// BenchmarkSeriesIDSet_Add/cardinality_1000000_check_add/random_multi_lock-8        	 1000000	      1002   ns/op	       0 B/op	       0 allocs/op
//
func BenchmarkSeriesIDSet_Add(b *testing.B) {
	// Setup...
	set = NewSeriesIDSet()
	for i := uint64(0); i < 1000000; i++ {
		set.Add(i)
	}
	lookup := uint64(300032)

	// Add the same value over and over.
	b.Run(fmt.Sprint("cardinality_1000000_add"), func(b *testing.B) {
		b.Run("same", func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				set.Add(lookup)
			}
		})

		b.Run("random", func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				x := rand.Intn(math.MaxInt32)
				b.StartTimer()
				set.Add(uint64(x))
			}
		})

		b.Run("same no lock", func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				set.AddNoLock(lookup)
			}
		})

		b.Run("random no lock", func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				x := rand.Intn(math.MaxInt32)
				b.StartTimer()
				set.AddNoLock(uint64(x))
			}
		})
	})

	// Add the same value over and over with no lock
	b.Run(fmt.Sprint("cardinality_1000000_check_add"), func(b *testing.B) {
		b.Run("same no lock", func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				if !set.ContainsNoLock(lookup) {
					set.AddNoLock(lookup)
				}
			}
		})

		b.Run("random no lock", func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				x := rand.Intn(math.MaxInt32)
				b.StartTimer()
				if !set.ContainsNoLock(uint64(x)) {
					set.AddNoLock(uint64(x))
				}
			}
		})

		b.Run("same global lock", func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				set.Lock()
				if !set.ContainsNoLock(lookup) {
					set.AddNoLock(lookup)
				}
				set.Unlock()
			}
		})

		b.Run("random global lock", func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				x := rand.Intn(math.MaxInt32)
				b.StartTimer()
				set.Lock()
				if !set.ContainsNoLock(uint64(x)) {
					set.AddNoLock(uint64(x))
				}
				set.Unlock()
			}
		})

		b.Run("same multi lock", func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				if !set.Contains(lookup) {
					set.Add(lookup)
				}
			}
		})

		b.Run("random multi lock", func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				x := rand.Intn(math.MaxInt32)
				b.StartTimer()
				if !set.Contains(uint64(x)) {
					set.Add(uint64(x))
				}
			}
		})
	})
}

var ssResult *SeriesIDSet

// Benchmark various ways of creating a copy of a bitmap. Note, Clone_COW will result
// in a bitmap where future modifications will involve copies.
//
// Typical results from an i7 laptop.
// BenchmarkSeriesIDSet_Clone/cardinality_1000/re-use/Clone-8         	   			   30000	     44171 ns/op	   47200 B/op	    1737 allocs/op
// BenchmarkSeriesIDSet_Clone/cardinality_1000/re-use/Merge-8         	  			  100000	     17877 ns/op	   39008 B/op	      30 allocs/op
// BenchmarkSeriesIDSet_Clone/cardinality_1000/re-use/MergeInPlace-8  	  			  200000	      7367 ns/op	       0 B/op	       0 allocs/op
// BenchmarkSeriesIDSet_Clone/cardinality_1000/re-use/Add-8           	   			   10000	    137460 ns/op	   62336 B/op	    2596 allocs/op
// BenchmarkSeriesIDSet_Clone/cardinality_1000/re-use/WriteTo-8       	   			   30000	     52896 ns/op	   35872 B/op	     866 allocs/op
// BenchmarkSeriesIDSet_Clone/cardinality_1000/don't_re-use/Clone-8   	   			   30000	     41940 ns/op	   47200 B/op	    1737 allocs/op
// BenchmarkSeriesIDSet_Clone/cardinality_1000/don't_re-use/Merge-8             	  100000	     17624 ns/op	   39008 B/op	      30 allocs/op
// BenchmarkSeriesIDSet_Clone/cardinality_1000/don't_re-use/MergeInPlace-8      	  100000	     17320 ns/op	   38880 B/op	      28 allocs/op
// BenchmarkSeriesIDSet_Clone/cardinality_1000/don't_re-use/Add-8               	   10000	    167544 ns/op	  101216 B/op	    2624 allocs/op
// BenchmarkSeriesIDSet_Clone/cardinality_1000/don't_re-use/WriteTo-8           	   20000	     66976 ns/op	   52897 B/op	     869 allocs/op
// BenchmarkSeriesIDSet_Clone/cardinality_10000/re-use/Clone-8                  	   10000	    179933 ns/op	  177072 B/op	    5895 allocs/op
// BenchmarkSeriesIDSet_Clone/cardinality_10000/re-use/Merge-8                  	   20000	     77574 ns/op	  210656 B/op	      42 allocs/op
// BenchmarkSeriesIDSet_Clone/cardinality_10000/re-use/MergeInPlace-8           	  100000	     23645 ns/op	       0 B/op	       0 allocs/op
// BenchmarkSeriesIDSet_Clone/cardinality_10000/re-use/Add-8                    	    2000	    689254 ns/op	  224161 B/op	    9572 allocs/op
// BenchmarkSeriesIDSet_Clone/cardinality_10000/re-use/WriteTo-8                	   10000	    199052 ns/op	  118791 B/op	    2945 allocs/op
// BenchmarkSeriesIDSet_Clone/cardinality_10000/don't_re-use/Clone-8            	   10000	    183137 ns/op	  177073 B/op	    5895 allocs/op
// BenchmarkSeriesIDSet_Clone/cardinality_10000/don't_re-use/Merge-8            	   20000	     77502 ns/op	  210656 B/op	      42 allocs/op
// BenchmarkSeriesIDSet_Clone/cardinality_10000/don't_re-use/MergeInPlace-8     	   20000	     72610 ns/op	  210528 B/op	      40 allocs/op
// BenchmarkSeriesIDSet_Clone/cardinality_10000/don't_re-use/Add-8              	    2000	    724789 ns/op	  434691 B/op	    9612 allocs/op
// BenchmarkSeriesIDSet_Clone/cardinality_10000/don't_re-use/WriteTo-8          	   10000	    215734 ns/op	  177159 B/op	    2948 allocs/op
// BenchmarkSeriesIDSet_Clone/cardinality_100000/re-use/Clone-8                 	    5000	    244971 ns/op	  377648 B/op	    6111 allocs/op
// BenchmarkSeriesIDSet_Clone/cardinality_100000/re-use/Merge-8                 	   20000	     90580 ns/op	  210656 B/op	      42 allocs/op
// BenchmarkSeriesIDSet_Clone/cardinality_100000/re-use/MergeInPlace-8          	   50000	     24697 ns/op	       0 B/op	       0 allocs/op
// BenchmarkSeriesIDSet_Clone/cardinality_100000/re-use/Add-8                   	     500	   3274456 ns/op	  758996 B/op	   19853 allocs/op
// BenchmarkSeriesIDSet_Clone/cardinality_100000/re-use/WriteTo-8               	    5000	    248791 ns/op	  122392 B/op	    3053 allocs/op
// BenchmarkSeriesIDSet_Clone/cardinality_100000/don't_re-use/Clone-8           	    5000	    269152 ns/op	  377648 B/op	    6111 allocs/op
// BenchmarkSeriesIDSet_Clone/cardinality_100000/don't_re-use/Merge-8           	   20000	     85948 ns/op	  210657 B/op	      42 allocs/op
// BenchmarkSeriesIDSet_Clone/cardinality_100000/don't_re-use/MergeInPlace-8    	   20000	     78142 ns/op	  210528 B/op	      40 allocs/op
// BenchmarkSeriesIDSet_Clone/cardinality_100000/don't_re-use/Add-8             	     500	   3123753 ns/op	  969529 B/op	   19893 allocs/op
// BenchmarkSeriesIDSet_Clone/cardinality_100000/don't_re-use/WriteTo-8         	   10000	    230657 ns/op	  180684 B/op	    3056 allocs/op
// BenchmarkSeriesIDSet_Clone/cardinality_1000000/re-use/Clone-8                	    3000	    551781 ns/op	 2245424 B/op	    6111 allocs/op
// BenchmarkSeriesIDSet_Clone/cardinality_1000000/re-use/Merge-8                	   20000	     92104 ns/op	  210656 B/op	      42 allocs/op
// BenchmarkSeriesIDSet_Clone/cardinality_1000000/re-use/MergeInPlace-8         	   50000	     27408 ns/op	       0 B/op	       0 allocs/op
// BenchmarkSeriesIDSet_Clone/cardinality_1000000/re-use/Add-8                  	     100	  22573498 ns/op	 6420446 B/op	   30520 allocs/op
// BenchmarkSeriesIDSet_Clone/cardinality_1000000/re-use/WriteTo-8              	    5000	    284901 ns/op	  123522 B/op	    3053 allocs/op
// BenchmarkSeriesIDSet_Clone/cardinality_1000000/don't_re-use/Clone-8          	    3000	    679284 ns/op	 2245424 B/op	    6111 allocs/op
// BenchmarkSeriesIDSet_Clone/cardinality_1000000/don't_re-use/Merge-8          	   20000	     68965 ns/op	  210656 B/op	      42 allocs/op
// BenchmarkSeriesIDSet_Clone/cardinality_1000000/don't_re-use/MergeInPlace-8   	   20000	     64236 ns/op	  210528 B/op	      40 allocs/op
// BenchmarkSeriesIDSet_Clone/cardinality_1000000/don't_re-use/Add-8            	     100	  21960668 ns/op	 6630979 B/op	   30560 allocs/op
// BenchmarkSeriesIDSet_Clone/cardinality_1000000/don't_re-use/WriteTo-8        	    5000	    298276 ns/op	  181890 B/op	    3056 allocs/op

func BenchmarkSeriesIDSet_Clone(b *testing.B) {
	toAddCardinalities := []int{1e3, 1e4, 1e5, 1e6}

	runBenchmarks := func(b *testing.B, other *SeriesIDSet, init func() *SeriesIDSet) {
		b.Run("Clone", func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				ssResult = other.Clone()
			}
		})

		b.Run("Merge", func(b *testing.B) {
			ssResult = init()
			for i := 0; i < b.N; i++ {
				ssResult.Merge(other)
				b.StopTimer()
				ssResult = init()
				b.StartTimer()
			}
		})

		b.Run("MergeInPlace", func(b *testing.B) {
			ssResult = init()
			for i := 0; i < b.N; i++ {
				ssResult.MergeInPlace(other)
				b.StopTimer()
				ssResult = init()
				b.StartTimer()
			}
		})

		b.Run("Add", func(b *testing.B) {
			ssResult = init()
			for i := 0; i < b.N; i++ {
				itr := other.Iterator()
				ssResult.Lock()
				for itr.HasNext() {
					ssResult.AddNoLock(uint64(itr.Next()))
				}
				ssResult.Unlock()
				b.StopTimer()
				ssResult = init()
				b.StartTimer()
			}
		})

		b.Run("WriteTo", func(b *testing.B) {
			var buf bytes.Buffer
			ssResult = init()
			for i := 0; i < b.N; i++ {
				other.WriteTo(&buf)
				ssResult.UnmarshalBinaryUnsafe(buf.Bytes())
				b.StopTimer()
				ssResult = init()
				buf.Reset()
				b.StartTimer()
			}
		})
	}

	for _, toAddCardinality := range toAddCardinalities {
		b.Run(fmt.Sprintf("cardinality %d", toAddCardinality), func(b *testing.B) {
			ids := make([]uint64, 0, toAddCardinality)
			for i := 0; i < toAddCardinality; i++ {
				ids = append(ids, uint64(rand.Intn(200000000)))
			}
			other := NewSeriesIDSet(ids...)

			b.Run("re-use", func(b *testing.B) {
				base := NewSeriesIDSet()
				runBenchmarks(b, other, func() *SeriesIDSet {
					base.Clear()
					return base
				})
			})

			b.Run("don't re-use", func(b *testing.B) {
				runBenchmarks(b, other, func() *SeriesIDSet {
					return NewSeriesIDSet()
				})
			})
		})
	}
}
func BenchmarkSeriesIDSet_AddMany(b *testing.B) {
	cardinalities := []int{1, 1e3, 1e4, 1e5, 1e6}
	toAddCardinalities := []int{1e3, 1e4, 1e5}

	for _, cardinality := range cardinalities {
		ids := make([]uint64, 0, cardinality)
		for i := 0; i < cardinality; i++ {
			ids = append(ids, uint64(rand.Intn(200000000)))
		}

		// Setup...
		set = NewSeriesIDSet(ids...)

		// Check if the value exists before adding it under two locks.
		b.Run(fmt.Sprintf("cardinality %d", cardinality), func(b *testing.B) {
			for _, toAddCardinality := range toAddCardinalities {
				ids := make([]uint64, 0, toAddCardinality)
				for i := 0; i < toAddCardinality; i++ {
					ids = append(ids, uint64(rand.Intn(200000000)))
				}

				b.Run(fmt.Sprintf("adding %d", toAddCardinality), func(b *testing.B) {
					b.Run("AddNoLock", func(b *testing.B) {
						clone := set.Clone()
						for i := 0; i < b.N; i++ {
							for _, id := range ids {
								clone.AddNoLock(id)
							}

							b.StopTimer()
							clone = set.Clone()
							b.StartTimer()
						}
					})

					b.Run("AddMany", func(b *testing.B) {
						clone := set.Clone()
						for i := 0; i < b.N; i++ {
							clone.AddMany(ids...)
							b.StopTimer()
							clone = set.Clone()
							b.StartTimer()
						}
					})

					// Merge will involve a new bitmap being allocated.
					b.Run("Merge", func(b *testing.B) {
						clone := set.Clone()
						for i := 0; i < b.N; i++ {
							other := NewSeriesIDSet(ids...)
							clone.Merge(other)

							b.StopTimer()
							clone = set.Clone()
							b.StartTimer()
						}
					})

					b.Run("MergeInPlace", func(b *testing.B) {
						clone := set.Clone()
						for i := 0; i < b.N; i++ {
							other := NewSeriesIDSet(ids...)
							clone.MergeInPlace(other)

							b.StopTimer()
							clone = set.Clone()
							b.StartTimer()
						}
					})
				})

			}
		})
	}
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

package stats_test

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/influxdata/influxdb/stats"
)

// Tests that there are no deadlocks or races under typical usage scenarios
func TestLifeCycleRaces(t *testing.T) {
	errors := make(chan error)
	go func() {
		defer func() {
			e := recover()
			if e != nil {
				errors <- fmt.Errorf("panic: %v", e)
			} else {
				errors <- nil
			}
		}()

		nwriters := 10
		nloops := 5
		start := make(chan struct{})
		stop := make(chan struct{})
		wg := sync.WaitGroup{}

		seen := map[string]struct{}{}

		monitor := func() {
			defer wg.Done()
			<-start
			view := stats.Root.Open()
			defer view.Close()

			iterator := func(s stats.Statistics) {
				seen[s.Key()] = struct{}{}
				s.Values()
			}

			for {
				select {
				case _ = <-stop:
					view.Do(iterator)
					return
				default:
					view.Do(iterator)
				}
			}
		}

		writer := func(i int) {
			defer wg.Done()
			<-start

			k := fmt.Sprintf("stat:%d", i)
			aStats := stats.Root.
				NewBuilder(k, "n", map[string]string{"tag": "T"}).
				DeclareInt("value", 0).
				DeclareInt("index", 0).
				MustBuild().
				Open()

			defer aStats.Close()

			for n := 0; n < nloops; n++ {
				aStats.
					AddInt("value", int64(1)).
					SetInt("index", int64(n))
				time.Sleep(time.Microsecond * 100)
			}
		}

		for i := 0; i < nwriters; i++ {
			wg.Add(1)
			go writer(i)
		}
		go monitor()

		close(start)
		wg.Wait() // wait until all the writers stop

		wg.Add(1)
		close(stop)
		wg.Wait() // wait for the monitor to stop

		// check that there are no statistics still registered
		count := 0
		stats.Root.
			Open().
			Do(func(s stats.Statistics) { count++ }).
			Close()

		if count != 0 {
			t.Fatalf("too many registered statistics. got: %d, expected: 0", count)
		}

		if len(seen) != nwriters {
			t.Fatalf("failed to observe some statistics. got: %d, expected: %d", len(seen), nwriters)
		}
	}()
	select {
	case err := <-errors:
		if err != nil {
			t.Fatalf("got: %v, expected: nil", err)
		}
	case <-time.NewTimer(time.Second * 15).C:
		// force a stack dump to allow analysis of issue
		panic("got: timeout, expected: normal return")
	}
}

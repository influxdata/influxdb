package tsm1_test

import (
	"fmt"
	"math/rand"
	"reflect"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/influxdata/influxdb/v2/tsdb/tsm1"
)

func TestCacheCheckConcurrentReadsAreSafe(t *testing.T) {
	values := make(tsm1.Values, 1000)
	timestamps := make([]int64, len(values))
	series := make([][]byte, 100)
	for i := range timestamps {
		timestamps[i] = int64(rand.Int63n(int64(len(values))))
	}

	for i := range values {
		values[i] = tsm1.NewValue(timestamps[i*len(timestamps)/len(values)], float64(i))
	}

	for i := range series {
		series[i] = []byte(fmt.Sprintf("series%d", i))
	}

	wg := sync.WaitGroup{}
	c := tsm1.NewCache(1000000)

	ch := make(chan struct{})
	for _, s := range series {
		for _, v := range values {
			c.Write(s, tsm1.Values{v})
		}
		wg.Add(3)
		go func(s []byte) {
			defer wg.Done()
			<-ch
			c.Values(s)
		}(s)
		go func(s []byte) {
			defer wg.Done()
			<-ch
			c.Values(s)
		}(s)
		go func(s []byte) {
			defer wg.Done()
			<-ch
			c.Values(s)
		}(s)
	}
	close(ch)
	wg.Wait()
}

func TestCacheRace(t *testing.T) {
	values := make(tsm1.Values, 1000)
	timestamps := make([]int64, len(values))
	series := make([][]byte, 100)
	for i := range timestamps {
		timestamps[i] = int64(rand.Int63n(int64(len(values))))
	}

	for i := range values {
		values[i] = tsm1.NewValue(timestamps[i*len(timestamps)/len(values)], float64(i))
	}

	for i := range series {
		series[i] = []byte(fmt.Sprintf("series%d", i))
	}

	wg := sync.WaitGroup{}
	c := tsm1.NewCache(1000000)

	ch := make(chan struct{})
	for _, s := range series {
		for _, v := range values {
			c.Write(s, tsm1.Values{v})
		}
		wg.Add(1)
		go func(s []byte) {
			defer wg.Done()
			<-ch
			c.Values(s)
		}(s)
	}

	errC := make(chan error)
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-ch
		s, err := c.Snapshot()
		if err == tsm1.ErrSnapshotInProgress {
			return
		}

		if err != nil {
			errC <- fmt.Errorf("failed to snapshot cache: %v", err)
			return
		}

		s.Deduplicate()
		c.ClearSnapshot(true)
	}()

	close(ch)

	go func() {
		wg.Wait()
		close(errC)
	}()

	for err := range errC {
		if err != nil {
			t.Error(err)
		}
	}
}

func TestCacheRace2Compacters(t *testing.T) {
	values := make(tsm1.Values, 1000)
	timestamps := make([]int64, len(values))
	series := make([][]byte, 100)
	for i := range timestamps {
		timestamps[i] = int64(rand.Int63n(int64(len(values))))
	}

	for i := range values {
		values[i] = tsm1.NewValue(timestamps[i*len(timestamps)/len(values)], float64(i))
	}

	for i := range series {
		series[i] = []byte(fmt.Sprintf("series%d", i))
	}

	wg := sync.WaitGroup{}
	c := tsm1.NewCache(1000000)

	ch := make(chan struct{})
	for _, s := range series {
		for _, v := range values {
			c.Write(s, tsm1.Values{v})
		}
		wg.Add(1)
		go func(s []byte) {
			defer wg.Done()
			<-ch
			c.Values(s)
		}(s)
	}
	fileCounter := 0
	mapFiles := map[int]bool{}
	mu := sync.Mutex{}
	errC := make(chan error)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-ch
			s, err := c.Snapshot()
			if err == tsm1.ErrSnapshotInProgress {
				return
			}

			if err != nil {
				errC <- fmt.Errorf("failed to snapshot cache: %v", err)
				return
			}

			mu.Lock()
			mapFiles[fileCounter] = true
			fileCounter++
			myFiles := map[int]bool{}
			for k, e := range mapFiles {
				myFiles[k] = e
			}
			mu.Unlock()
			s.Deduplicate()
			c.ClearSnapshot(true)
			mu.Lock()
			defer mu.Unlock()
			for k := range myFiles {
				if _, ok := mapFiles[k]; !ok {
					errC <- fmt.Errorf("something else deleted one of my files")
					return
				} else {
					delete(mapFiles, k)
				}
			}
		}()
	}
	close(ch)

	go func() {
		wg.Wait()
		close(errC)
	}()

	for err := range errC {
		if err != nil {
			t.Error(err)
		}
	}
}

func TestConcurrentReadAfterWrite(t *testing.T) {
	t.Parallel()

	var starttime int64 = 1594785691
	series := [][]byte{[]byte("key1"), []byte("key2")}

	concurrency := runtime.GOMAXPROCS(0) * 2
	batch := 1024

	errCh := make(chan error, concurrency)
	closing := make(chan struct{})
	var wg sync.WaitGroup

	c := tsm1.NewCache(1024 * 1024 * 16)
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		// read after read concurrently
		go func() {
			defer wg.Done()
			for {

				select {
				case <-closing:
					errCh <- nil
					return
				default:
				}

				ts := atomic.AddInt64(&starttime, int64(batch))
				writes := make(tsm1.Values, 0, batch)
				for j := 0; j < batch; j++ {
					writes = append(writes,
						tsm1.NewValue(ts+int64(j), ts+int64(j)))
				}
				for _, key := range series {
					if err := c.Write(key, writes); err != nil {
						errCh <- err
						return
					}
				}
				for _, key := range series {
					// check the read result
					reads := c.Values(key)

					if len(reads) < len(writes) {
						errCh <- fmt.Errorf("read count: %v less than write count: %v", len(reads), len(writes))
						return
					}

					sort.Slice(reads, func(i, j int) bool {
						return reads[i].UnixNano() < reads[j].UnixNano()
					})

					k := 0
					for j := range writes {
						write := writes[j].Value()

						found := false
						for k < len(reads) {
							read := reads[k].Value()
							if reflect.DeepEqual(read, write) {
								found = true
								break
							}
							k++
						}

						if !found {
							errCh <- fmt.Errorf("write value: %v not found in reads", write)
							return
						}
					}
				}
			}
		}()
	}

	// sleep for a little while and check
	time.Sleep(time.Second * 20)
	close(closing)
	wg.Wait()

	for i := 0; i < concurrency; i++ {
		err := <-errCh
		if err != nil {
			t.Fatal(err)
			return
		}
	}
}

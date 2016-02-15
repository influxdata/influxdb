// +build !race

package tsm1_test

import (
	"fmt"
	"github.com/influxdata/influxdb/tsdb/engine/tsm1"
	"math/rand"
	"sync"
	"testing"
	"time"
)

func TestCacheRace(t *testing.T) {
	values := make(tsm1.Values, 1000)
	timestamps := make([]time.Time, len(values))
	series := make([]string, 100)
	for i := range timestamps {
		timestamps[i] = time.Unix(int64(rand.Int63n(int64(len(values)))), 0).UTC()
	}

	for i := range values {
		values[i] = tsm1.NewValue(timestamps[i*len(timestamps)/len(values)], float64(i))
	}

	for i := range series {
		series[i] = fmt.Sprintf("series%d", i)
	}

	wg := sync.WaitGroup{}
	c := tsm1.NewCache(1000000, "")

	ch := make(chan struct{})
	for _, s := range series {
		for _, v := range values {
			c.Write(s, tsm1.Values{v})
		}
		wg.Add(1)
		go func(s string) {
			defer wg.Done()
			<-ch
			c.Values(s)
		}(s)
	}
	wg.Add(1)
	go func() {
		wg.Done()
		<-ch
		s := c.PrepareSnapshot()
		s.Deduplicate()
		c.CommitSnapshot()
	}()
	close(ch)
	wg.Wait()
}

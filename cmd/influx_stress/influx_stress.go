package main

import (
	"flag"
	"fmt"
	"math/rand"
	"net/url"
	"runtime"
	"sort"
	"sync"
	"time"

	"github.com/influxdb/influxdb/client"
)

var (
	batchSize     = flag.Int("batchsize", 5000, "number of points per batch")
	seriesCount   = flag.Int("series", 100000, "number of unique series to create")
	pointCount    = flag.Int("points", 100, "number of points per series to create")
	concurrency   = flag.Int("concurrency", 10, "number of simultaneous writes to run")
	batchInterval = flag.Duration("batchinterval", 0*time.Second, "duration between batches")
	database      = flag.String("database", "stress", "name of database")
	address       = flag.String("addr", "localhost:8086", "IP address and port of database (e.g., localhost:8086)")
)

func main() {
	flag.Parse()
	runtime.GOMAXPROCS(runtime.NumCPU())

	startTime := time.Now()
	counter := NewConcurrencyLimiter(*concurrency)

	u, _ := url.Parse(fmt.Sprintf("http://%s", *address))
	c, err := client.NewClient(client.Config{URL: *u})
	if err != nil {
		panic(err)
	}

	var mu sync.Mutex
	var wg sync.WaitGroup
	responseTimes := make([]int, 0)

	totalPoints := 0

	batch := &client.BatchPoints{
		Database:         *database,
		WriteConsistency: "any",
		Time:             time.Now(),
		Precision:        "n",
	}
	for i := 1; i <= *pointCount; i++ {
		for j := 1; j <= *seriesCount; j++ {
			p := client.Point{
				Measurement: "cpu",
				Tags:        map[string]string{"region": "uswest", "host": fmt.Sprintf("host-%d", j)},
				Fields:      map[string]interface{}{"value": rand.Float64()},
			}
			batch.Points = append(batch.Points, p)
			if len(batch.Points) >= *batchSize {
				wg.Add(1)
				counter.Increment()
				totalPoints += len(batch.Points)
				go func(b *client.BatchPoints, total int) {
					st := time.Now()
					if _, err := c.Write(*b); err != nil {
						fmt.Println("ERROR: ", err.Error())
					} else {
						mu.Lock()
						responseTimes = append(responseTimes, int(time.Since(st).Nanoseconds()))
						mu.Unlock()
					}
					wg.Done()
					counter.Decrement()
					if total%500000 == 0 {
						fmt.Printf("%d total points. %d in %s\n", total, *batchSize, time.Since(st))
					}
				}(batch, totalPoints)

				batch = &client.BatchPoints{
					Database:         *database,
					WriteConsistency: "any",
					Precision:        "n",
					Time:             time.Now(),
				}
			}
		}
	}

	wg.Wait()
	sort.Sort(sort.Reverse(sort.IntSlice(responseTimes)))

	total := int64(0)
	for _, t := range responseTimes {
		total += int64(t)
	}
	mean := total / int64(len(responseTimes))

	fmt.Printf("Wrote %d points at average rate of %.0f\n", totalPoints, float64(totalPoints)/time.Since(startTime).Seconds())
	fmt.Println("Average response time: ", time.Duration(mean))
	fmt.Println("Slowest response times:")
	for _, r := range responseTimes[:100] {
		fmt.Println(time.Duration(r))
	}
}

// ConcurrencyLimiter is a go routine safe struct that can be used to
// ensure that no more than a specifid max number of goroutines are
// executing.
type ConcurrencyLimiter struct {
	inc   chan chan struct{}
	dec   chan struct{}
	max   int
	count int
}

// NewConcurrencyLimiter returns a configured limiter that will
// ensure that calls to Increment will block if the max is hit.
func NewConcurrencyLimiter(max int) *ConcurrencyLimiter {
	c := &ConcurrencyLimiter{
		inc: make(chan chan struct{}),
		dec: make(chan struct{}, max),
		max: max,
	}
	go c.handleLimits()
	return c
}

// Increment will increase the count of running goroutines by 1.
// if the number is currently at the max, the call to Increment
// will block until another goroutine decrements.
func (c *ConcurrencyLimiter) Increment() {
	r := make(chan struct{})
	c.inc <- r
	<-r
}

// Decrement will reduce the count of running goroutines by 1
func (c *ConcurrencyLimiter) Decrement() {
	c.dec <- struct{}{}
}

// handleLimits runs in a goroutine to manage the count of
// running goroutines.
func (c *ConcurrencyLimiter) handleLimits() {
	for {
		r := <-c.inc
		if c.count >= c.max {
			<-c.dec
			c.count--
		}
		c.count++
		r <- struct{}{}
	}
}

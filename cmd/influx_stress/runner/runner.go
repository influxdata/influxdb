package runner

import (
	"fmt"
	"math/rand"
	"net/url"
	"sync"
	"time"

	"github.com/influxdb/influxdb/client"
)

type Timer struct {
	start time.Time
	end   time.Time
}

func (t *Timer) Start() {
	t.start = time.Now()
}

func (t *Timer) Stop() {
	t.end = time.Now()
}

func (t *Timer) Elapsed() time.Duration {
	return t.end.Sub(t.start)
}

func newTimer() *Timer {
	t := &Timer{}
	t.Start()
	return t
}

// Config

type Config struct {
	BatchSize     int
	SeriesCount   int
	PointCount    int
	Concurrency   int
	BatchInterval time.Duration
	Database      string
	Address       string
}

func (cfg *Config) newClient() *client.Client {
	u, _ := url.Parse(fmt.Sprintf("http://%s", cfg.Address))
	c, err := client.NewClient(client.Config{URL: *u})
	if err != nil {
		panic(err)
	}
	return c
}

// main runner
func Run(cfg *Config) (totalPoints int, responseTimes []int, timer *Timer) {
	timer = newTimer()
	defer timer.Stop()

	c := cfg.newClient()

	counter := NewConcurrencyLimiter(cfg.Concurrency)

	var mu sync.Mutex
	var wg sync.WaitGroup
	responseTimes = make([]int, 0)

	totalPoints = 0

	batch := &client.BatchPoints{
		Database:         cfg.Database,
		WriteConsistency: "any",
		Time:             time.Now(),
		Precision:        "n",
	}
	for i := 1; i <= cfg.PointCount; i++ {
		for j := 1; j <= cfg.SeriesCount; j++ {
			p := client.Point{
				Measurement: "cpu",
				Tags:        map[string]string{"region": "uswest", "host": fmt.Sprintf("host-%d", j)},
				Fields:      map[string]interface{}{"value": rand.Float64()},
			}
			batch.Points = append(batch.Points, p)
			if len(batch.Points) >= cfg.BatchSize {
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
						fmt.Printf("%d total points. %d in %s\n", total, cfg.BatchSize, time.Since(st))
					}
				}(batch, totalPoints)

				batch = &client.BatchPoints{
					Database:         cfg.Database,
					WriteConsistency: "any",
					Precision:        "n",
					Time:             time.Now(),
				}
			}
		}
	}

	wg.Wait()

	return
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

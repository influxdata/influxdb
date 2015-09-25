package runner

import (
	"fmt"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/influxdb/influxdb/client"
)

// Timer is struct that can be used to track elaspsed time
type Timer struct {
	start time.Time
	end   time.Time
}

// Start returns a Timers start field
func (t *Timer) Start() time.Time {
	return t.start
}

// End returns a Timers end field
func (t *Timer) End() time.Time {
	return t.end
}

// StartTimer sets a timers `start` field to the current time
func (t *Timer) StartTimer() {
	t.start = time.Now()
}

// StopTimer sets a timers `end` field to the current time
func (t *Timer) StopTimer() {
	t.end = time.Now()
}

// Elapsed returns the total elapsed time between the `start`
// and `end` fields on a timer.
func (t *Timer) Elapsed() time.Duration {
	return t.end.Sub(t.start)
}

// NewTimer returns a pointer to a `Timer` struct where the
// timers `start` field has been set to `time.Now()`
func NewTimer() *Timer {
	t := &Timer{}
	t.StartTimer()
	return t
}

// ResponseTime is a struct that contains `Value`
// `Time` pairing.
type ResponseTime struct {
	Value int
	Time  time.Time
}

// newResponseTime returns a new response time
// with value `v` and time `time.Now()`.
func NewResponseTime(v int) ResponseTime {
	r := ResponseTime{Value: v, Time: time.Now()}
	return r
}

type ResponseTimes []ResponseTime

// Implements the `Len` method for the
// sort.Interface type
func (rs ResponseTimes) Len() int {
	return len(rs)
}

// Implements the `Less` method for the
// sort.Interface type
func (rs ResponseTimes) Less(i, j int) bool {
	return rs[i].Value < rs[j].Value
}

// Implements the `Swap` method for the
// sort.Interface type
func (rs ResponseTimes) Swap(i, j int) {
	rs[i], rs[j] = rs[j], rs[i]
}

type Measurements []string

// String returns a string and implements the `String` method for
// the flag.Value interface.
func (ms *Measurements) String() string {
	return fmt.Sprint(*ms)
}

// Set implements the `Set` method for the flag.Value
// interface. Set splits a string of comma separated values
// into a `Measurement`.
func (ms *Measurements) Set(value string) error {
	values := strings.Split(value, ",")
	for _, m := range values {
		*ms = append(*ms, m)
	}
	return nil
}

// newClient returns a pointer to an InfluxDB client for
// a `Config`'s `Address` field. If an error is encountered
// when creating a new client, the function panics.
func (cfg *Config) NewClient() (*client.Client, error) {
	u, _ := url.Parse(fmt.Sprintf("http://%s", cfg.Write.Address))
	c, err := client.NewClient(client.Config{URL: *u})
	if err != nil {
		return nil, err
	}
	return c, nil
}

func resetDB(c *client.Client, database string) error {
	_, err := c.Query(client.Query{
		Command: fmt.Sprintf("DROP DATABASE %s", database),
	})

	if err != nil && !strings.Contains(err.Error(), "database not found") {
		return err
	}

	return nil
}

// Run runs the stress test that is specified by a `Config`.
// It returns the total number of points that were during the test,
// an slice of all of the stress tests response times,
// and the times that the test started at and ended as a `Timer`
func Run(cfg *Config, done chan struct{}, ts chan time.Time) (totalPoints int, failedRequests int, responseTimes ResponseTimes, timer *Timer) {

	c, err := cfg.NewClient()
	if err != nil {
		panic(err)
	}

	if cfg.Write.ResetDatabase {
		resetDB(c, cfg.Write.Database)
	}

	_, err = c.Query(client.Query{
		Command: fmt.Sprintf("CREATE DATABASE %s", cfg.Write.Database),
	})

	if err != nil && !strings.Contains(err.Error(), "database already exists") {
		fmt.Println(err)
	}

	counter := NewConcurrencyLimiter(cfg.Write.Concurrency)

	var mu sync.Mutex
	var wg sync.WaitGroup
	responseTimes = make(ResponseTimes, 0)

	failedRequests = 0

	totalPoints = 0

	lastSuccess := true

	ch := make(chan []client.Point, 100)

	go func() {
		points := []client.Point{}
		num := 0
		for _, s := range cfg.Series {
			num += s.PointCount * s.SeriesCount

		}

		if cfg.MeasurementQuery.Enabled {
			num = num / (len(cfg.Series) * len(cfg.MeasurementQuery.Aggregates) * len(cfg.MeasurementQuery.Fields))
		}

		ctr := 0
		for _, testSeries := range cfg.Series {
			for i := 0; i < testSeries.PointCount; i++ {
				iter := testSeries.Iter(cfg.Write.StartingPoint, i)
				p, ok := iter.Next()
				for ok {
					ctr++
					points = append(points, p)
					if len(points) >= cfg.Write.BatchSize {
						ch <- points
						points = []client.Point{}
					}

					if cfg.MeasurementQuery.Enabled && ctr%num == 0 {
						select {
						case ts <- p.Time:
							func() {}()
						default:
							func() {}()
						}
					}

					p, ok = iter.Next()
				}
			}
		}

		close(ch)

	}()

	timer = NewTimer()
	defer timer.StopTimer()

	for pnt := range ch {
		batch := &client.BatchPoints{
			Database:         cfg.Write.Database,
			WriteConsistency: "any",
			Time:             time.Now(),
			Precision:        cfg.Write.Precision,
			Points:           pnt,
		}

		wg.Add(1)
		counter.Increment()
		totalPoints += len(batch.Points)

		go func(b *client.BatchPoints, total int) {
			st := time.Now()
			if _, err := c.Write(*b); err != nil { // Should retry write if failed
				mu.Lock()
				if lastSuccess {
					fmt.Println("ERROR: ", err.Error())
				}
				failedRequests += 1
				totalPoints -= len(b.Points)
				lastSuccess = false
				mu.Unlock()
			} else {
				mu.Lock()
				if !lastSuccess {
					fmt.Println("success in ", time.Since(st))
				}
				lastSuccess = true
				responseTimes = append(responseTimes, NewResponseTime(int(time.Since(st).Nanoseconds())))
				mu.Unlock()
			}
			batchInterval, _ := time.ParseDuration(cfg.Write.BatchInterval)
			time.Sleep(batchInterval)
			wg.Done()
			counter.Decrement()
			if total%500000 == 0 {
				fmt.Printf("%d total points. %d in %s\n", total, cfg.Write.BatchSize, time.Since(st))
			}
		}(batch, totalPoints)

	}

	wg.Wait()

	if cfg.SeriesQuery.Enabled {
		done <- struct{}{}
	}

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

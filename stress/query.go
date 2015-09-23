package runner

import (
	"fmt"
	//	"net/url"
	//	"strings"
	"sync"
	"time"

	"github.com/influxdb/influxdb/client"
)

type QueryResults struct {
	TotalQueries  int
	ResponseTimes ResponseTimes
}

func SeriesQuery(cfg *Config, done chan struct{}, results chan QueryResults) {

	var totalQueries int
	var responseTimes ResponseTimes

	c, err := cfg.NewClient()
	if err != nil {
		panic(err)
	}

	var mu sync.Mutex
	var wg sync.WaitGroup
	responseTimes = make(ResponseTimes, 0)
	totalQueries = 0

	//counter := NewConcurrencyLimiter(cfg.Write.Concurrency)
	counter := NewConcurrencyLimiter(1)
	for _, s := range cfg.Series {
		for i := 0; i < s.SeriesCount; i++ {
			for _, a := range cfg.SeriesQuery.Aggregates {
				for _, f := range cfg.SeriesQuery.Fields {
					theQuery := fmt.Sprintf("SELECT %v(%v) FROM %v WHERE %v='%v-%v'", a, f, s.Measurement, s.Tags[0].Key, s.Tags[0].Value, i)

					wg.Add(1)
					counter.Increment()
					totalQueries += 1
					go func(q string) {

						select {
						case <-done:
							results <- QueryResults{
								TotalQueries:  totalQueries,
								ResponseTimes: responseTimes,
							}
						default:
							func() {}()
						}

						st := time.Now()
						_, err := c.Query(client.Query{
							Command:  q,
							Database: cfg.Write.Database,
						})
						if err != nil {
							fmt.Println("Error")
						}
						mu.Lock()
						totalQueries += 1
						responseTimes = append(responseTimes, NewResponseTime(int(time.Since(st).Nanoseconds())))
						mu.Unlock()
						wg.Done()
						t, err := time.ParseDuration(cfg.SeriesQuery.Interval)
						time.Sleep(t)
						counter.Decrement()

					}(theQuery)

				}
			}
		}
	}

	wg.Wait()
	fmt.Println(totalQueries)

	totalQueries = 0

	return

}

func MeasurementQuery(cfg *Config, done chan struct{}, timestamp chan time.Time) {

	var totalQueries int
	var responseTimes ResponseTimes

	c, err := cfg.NewClient()
	if err != nil {
		panic(err)
	}

	var mu sync.Mutex
	var wg sync.WaitGroup
	responseTimes = make(ResponseTimes, 0)
	totalQueries = 0

	//counter := NewConcurrencyLimiter(cfg.Write.Concurrency)
	counter := NewConcurrencyLimiter(1)
	for _, s := range cfg.Series {
		for _, a := range cfg.SeriesQuery.Aggregates {
			for _, f := range cfg.SeriesQuery.Fields {
				wg.Add(1)
				counter.Increment()
				totalQueries += 1
				go func(a string, f string, m string) {
					tsp := <-timestamp
					select {
					case <-done:
						done <- struct{}{}
					default:
						func() {}()
					}
					q := fmt.Sprintf("SELECT %v(%v) FROM %v WHERE time>=%vs", a, f, m, tsp.Add(-5*time.Hour).Unix())

					st := time.Now()
					rsp, err := c.Query(client.Query{
						Command:  q,
						Database: cfg.Write.Database,
					})
					fmt.Println("THE QUERY RAN!!!!!!")
					fmt.Println(rsp)
					fmt.Println(q)
					if err != nil {
						fmt.Println("Error")
					}
					mu.Lock()
					totalQueries += 1
					responseTimes = append(responseTimes, NewResponseTime(int(time.Since(st).Nanoseconds())))
					mu.Unlock()
					wg.Done()
					t, err := time.ParseDuration(cfg.SeriesQuery.Interval)
					time.Sleep(t)
					counter.Decrement()

				}(a, f, s.Measurement)

			}
		}
	}

	wg.Wait()
	fmt.Println("DONEEEE")

	totalQueries = 0

	return

}

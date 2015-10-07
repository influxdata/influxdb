package runner

import (
	"fmt"
	"sync"
	"time"

	"github.com/influxdb/influxdb/client"
)

type QueryResults struct {
	TotalQueries  int
	ResponseTimes ResponseTimes
}

func SeriesQuery(cfg *Config, done chan struct{}, results chan QueryResults) {

	var mu sync.Mutex
	var wg sync.WaitGroup
	responseTimes := make(ResponseTimes, 0)
	totalQueries := 0

	c, err := cfg.NewClient()
	if err != nil {
		panic(err)
	}

	counter := NewConcurrencyLimiter(cfg.SeriesQuery.Concurrency)
	for _, s := range cfg.Series {
		for i := 0; i < s.SeriesCount; i++ {
			for _, a := range cfg.SeriesQuery.Aggregates {
				for _, f := range cfg.SeriesQuery.Fields {
					q := fmt.Sprintf("SELECT %v(%v) FROM %v WHERE %v='%v-%v'", a, f, s.Measurement, s.Tags[0].Key, s.Tags[0].Value, i)

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
						}

						counter.Decrement()

					}(q)

				}
			}
		}
	}

	wg.Wait()

	return

}

func MeasurementQuery(cfg *Config, timestamp chan time.Time, results chan QueryResults) {

	var wg sync.WaitGroup
	responseTimes := make(ResponseTimes, 0)
	totalQueries := 0

	c, err := cfg.NewClient()
	if err != nil {
		panic(err)
	}

	for _, s := range cfg.Series {
		for _, a := range cfg.MeasurementQuery.Aggregates {
			for _, f := range cfg.MeasurementQuery.Fields {

				wg.Add(1)

				go func(a string, f string, m string) {

					tsp := <-timestamp

					offset, err := time.ParseDuration(cfg.MeasurementQuery.Offset)
					if err != nil {
						panic(err)
					}

					q := fmt.Sprintf("SELECT %v(%v) FROM %v WHERE time>=%vs", a, f, m, tsp.Add(-1*offset).Unix())
					st := time.Now()

					_, err = c.Query(client.Query{
						Command:  q,
						Database: cfg.Write.Database,
					})

					if err != nil {
						fmt.Println("Error")
					}

					totalQueries += 1
					responseTimes = append(responseTimes, NewResponseTime(int(time.Since(st).Nanoseconds())))
					wg.Done()

				}(a, f, s.Measurement)

			}
		}
	}

	wg.Wait()
	results <- QueryResults{
		TotalQueries:  totalQueries,
		ResponseTimes: responseTimes,
	}

	return

}

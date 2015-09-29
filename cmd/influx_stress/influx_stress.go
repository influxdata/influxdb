package main

import (
	"flag"
	"fmt"
	"runtime"
	"sort"
	"time"

	"github.com/influxdb/influxdb/stress"
)

var (
	batchSize     = flag.Int("batchsize", 5000, "number of points per batch")
	seriesCount   = flag.Int("series", 100000, "number of unique series to create")
	pointCount    = flag.Int("points", 100, "number of points per series to create")
	concurrency   = flag.Int("concurrency", 10, "number of simultaneous writes to run")
	batchInterval = flag.Duration("batchinterval", 0*time.Second, "duration between batches")
	database      = flag.String("database", "stress", "name of database")
	address       = flag.String("addr", "localhost:8086", "IP address and port of database (e.g., localhost:8086)")
	precision     = flag.String("precision", "n", "The precision that points in the database will be with")
	test          = flag.String("test", "", "The stress test file")
)

var ms runner.Measurements

func init() {
	flag.Var(&ms, "m", "comma-separated list of intervals to use between events")
}

func main() {
	var cfg *runner.Config
	var err error

	runtime.GOMAXPROCS(runtime.NumCPU())
	flag.Parse()

	cfg = runner.NewConfig()

	if len(ms) == 0 {
		ms = append(ms, "cpu")
	}

	for _, m := range ms {
		cfg.Series = append(cfg.Series, runner.NewSeries(m, 100, 100000))
	}

	if *test != "" {
		cfg, err = runner.DecodeFile(*test)

		if err != nil {
			fmt.Println(err)
			return
		}

	}

	d := make(chan struct{})
	seriesQueryResults := make(chan runner.QueryResults)

	if cfg.SeriesQuery.Enabled {
		go runner.SeriesQuery(cfg, d, seriesQueryResults)
	}

	measurementQueryResults := make(chan runner.QueryResults)

	ts := make(chan time.Time)
	if cfg.MeasurementQuery.Enabled {
		go runner.MeasurementQuery(cfg, ts, measurementQueryResults)
	}

	// Get the stress results
	totalPoints, failedRequests, responseTimes, timer := runner.Run(cfg, d, ts)

	sort.Sort(sort.Reverse(sort.Interface(responseTimes)))

	total := int64(0)
	for _, t := range responseTimes {
		total += int64(t.Value)
	}
	mean := total / int64(len(responseTimes))

	fmt.Printf("Wrote %d points at average rate of %.0f\n", totalPoints, float64(totalPoints)/timer.Elapsed().Seconds())
	fmt.Printf("%d requests failed for %d total points that didn't get posted.\n", failedRequests, failedRequests**batchSize)
	fmt.Println("Average response time: ", time.Duration(mean))
	fmt.Println("Slowest response times:")
	for _, r := range responseTimes[:100] {
		fmt.Println(time.Duration(r.Value))
	}

	// Get series query results
	if cfg.SeriesQuery.Enabled {
		qrs := <-seriesQueryResults

		queryTotal := int64(0)
		for _, qt := range qrs.ResponseTimes {
			queryTotal += int64(qt.Value)
		}
		seriesQueryMean := queryTotal / int64(len(qrs.ResponseTimes))

		fmt.Printf("Queried Series %d times with a average response time of %v milliseconds\n", qrs.TotalQueries, time.Duration(seriesQueryMean).Seconds()*1000)

	}

	// Get measurement query results
	if cfg.MeasurementQuery.Enabled {
		qrs := <-measurementQueryResults

		queryTotal := int64(0)
		for _, qt := range qrs.ResponseTimes {
			queryTotal += int64(qt.Value)
		}
		seriesQueryMean := queryTotal / int64(len(qrs.ResponseTimes))

		fmt.Printf("Queried Measurement %d times with a average response time of %v milliseconds\n", qrs.TotalQueries, time.Duration(seriesQueryMean).Seconds()*1000)

	}

	return

}

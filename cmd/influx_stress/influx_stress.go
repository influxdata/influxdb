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
)

var ms runner.Measurements

func init() {
	flag.Var(&ms, "m", "comma-separated list of intervals to use between events")
}

func main() {
	flag.Parse()
	runtime.GOMAXPROCS(runtime.NumCPU())

	if len(ms) == 0 {
		ms = append(ms, "cpu")
	}

	cfg := &runner.Config{
		BatchSize:     *batchSize,
		Measurements:  ms,
		SeriesCount:   *seriesCount,
		PointCount:    *pointCount,
		Concurrency:   *concurrency,
		BatchInterval: *batchInterval,
		Database:      *database,
		Address:       *address,
		Precision:     *precision,
	}

	totalPoints, failedRequests, responseTimes, timer := runner.Run(cfg)

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
}

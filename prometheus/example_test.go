package prometheus_test

import (
	"bytes"
	"fmt"

	"github.com/influxdata/influxdb/v2/prometheus"
	pr "github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
)

const metrics = `
# HELP go_goroutines Number of goroutines that currently exist.
# TYPE go_goroutines gauge
go_goroutines 85
# HELP go_info Information about the Go environment.
# TYPE go_info gauge
go_info{version="go1.11.4"} 1
# HELP storage_compactions_queued Number of queued compactions.
# TYPE storage_compactions_queued gauge
storage_compactions_queued{level="1"} 1
storage_compactions_queued{level="2"} 2
`

func ExampleFilter_Gather() {
	mfs, _ := prometheus.DecodeExpfmt(bytes.NewBufferString(metrics), expfmt.FmtText)
	fmt.Printf("Start with %d metric families\n", len(mfs))
	fmt.Printf("%s\n", metrics)

	filter := &prometheus.Filter{
		Gatherer: pr.GathererFunc(func() ([]*dto.MetricFamily, error) {
			return mfs, nil
		}),
		Matcher: prometheus.NewMatcher().
			Family("go_goroutines").
			Family(
				"storage_compactions_queued",
				prometheus.L("level", "2"),
			),
	}

	fmt.Printf("Filtering for the entire go_goroutines family and\njust the level=2 label of the storage_compactions_queued family.\n\n")
	filtered, _ := filter.Gather()
	b, _ := prometheus.EncodeExpfmt(filtered, expfmt.FmtText)

	fmt.Printf("After filtering:\n\n%s", string(b))

	// Output:
	// Start with 3 metric families
	//
	// # HELP go_goroutines Number of goroutines that currently exist.
	// # TYPE go_goroutines gauge
	// go_goroutines 85
	// # HELP go_info Information about the Go environment.
	// # TYPE go_info gauge
	// go_info{version="go1.11.4"} 1
	// # HELP storage_compactions_queued Number of queued compactions.
	// # TYPE storage_compactions_queued gauge
	// storage_compactions_queued{level="1"} 1
	// storage_compactions_queued{level="2"} 2
	//
	// Filtering for the entire go_goroutines family and
	// just the level=2 label of the storage_compactions_queued family.
	//
	// After filtering:
	//
	// # HELP go_goroutines Number of goroutines that currently exist.
	// # TYPE go_goroutines gauge
	// go_goroutines 85
	// # HELP storage_compactions_queued Number of queued compactions.
	// # TYPE storage_compactions_queued gauge
	// storage_compactions_queued{level="2"} 2
}

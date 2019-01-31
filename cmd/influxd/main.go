package main

import (
	"context"
	"fmt"
	_ "net/http/pprof"
	"os"
	"sync"
	"time"

	"github.com/influxdata/influxdb/cmd/influxd/launcher"
	"github.com/influxdata/influxdb/cmd/influxd/mem"
	"github.com/influxdata/influxdb/kit/signals"
	_ "github.com/influxdata/influxdb/query/builtin"
	"github.com/influxdata/influxdb/telemetry"
	_ "github.com/influxdata/influxdb/tsdb/tsi1"
	_ "github.com/influxdata/influxdb/tsdb/tsm1"
)

var (
	version = "dev"
	commit  = "none"
	date    = "unknown"
)

func hasMemTestHack() bool {
	for _, arg := range os.Args {
		if arg == "--inmem" {
			return true
		}
	}
	return false
}

func main() {
	// exit with SIGINT and SIGTERM
	ctx := context.Background()
	ctx = signals.WithStandardSignals(ctx)

	if hasMemTestHack() {
		m := mem.NewLauncher()
		if err := m.Run(ctx, os.Args[1:]...); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		} else if !m.Running() {
			os.Exit(1)
		}

		<-ctx.Done()

		// Attempt clean shutdown.
		ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
		defer cancel()
		m.Shutdown(ctx)
		return
	}

	m := launcher.NewLauncher()
	m.SetBuild(version, commit, date)
	if err := m.Run(ctx, os.Args[1:]...); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	} else if !m.Running() {
		os.Exit(1)
	}

	var wg sync.WaitGroup
	if !m.ReportingDisabled() {
		reporter := telemetry.NewReporter(m.Registry())
		reporter.Interval = 8 * time.Hour
		reporter.Logger = m.Logger()
		wg.Add(1)
		go func() {
			defer wg.Done()
			reporter.Report(ctx)
		}()
	}

	<-ctx.Done()

	// Attempt clean shutdown.
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	m.Shutdown(ctx)
	wg.Wait()
}

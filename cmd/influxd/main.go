package main

import (
	"context"
	"fmt"
	_ "net/http/pprof"
	"os"
	"time"

	"github.com/influxdata/platform/cmd/influxd/launcher"
	"github.com/influxdata/platform/kit/signals"
	_ "github.com/influxdata/platform/query/builtin"
	_ "github.com/influxdata/platform/tsdb/tsi1"
	_ "github.com/influxdata/platform/tsdb/tsm1"
)

func main() {
	// exit with SIGINT and SIGTERM
	ctx := context.Background()
	ctx = signals.WithStandardSignals(ctx)

	m := launcher.NewLauncher()
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
}

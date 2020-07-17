package main

import (
	"context"
	"fmt"
	_ "net/http/pprof"
	"os"
	"time"

	"github.com/influxdata/flux"
	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/cmd/influxd/generate"
	"github.com/influxdata/influxdb/v2/cmd/influxd/launcher"
	"github.com/influxdata/influxdb/v2/cmd/influxd/migrate"
	"github.com/influxdata/influxdb/v2/cmd/influxd/restore"
	_ "github.com/influxdata/influxdb/v2/query/builtin"
	_ "github.com/influxdata/influxdb/v2/tsdb/tsi1"
	_ "github.com/influxdata/influxdb/v2/tsdb/tsm1"
	"github.com/spf13/cobra"
)

var (
	version = "dev"
	commit  = "none"
	date    = ""
)

func main() {
	println("...")
	if len(date) == 0 {
		date = time.Now().UTC().Format(time.RFC3339)
	}

	influxdb.SetBuildInfo(version, commit, date)

	rootCmd := launcher.NewInfluxdCommand(context.Background(),
		generate.Command,
		restore.Command,
		migrate.Command,
		&cobra.Command{
			Use:   "version",
			Short: "Print the influxd server version",
			Run: func(cmd *cobra.Command, args []string) {
				fmt.Printf("InfluxDB %s (git: %s) build_date: %s\n", version, commit, date)
			},
		},
	)

	// TODO: this should be removed in the future: https://github.com/influxdata/influxdb/issues/16220
	if os.Getenv("QUERY_TRACING") == "1" {
		flux.EnableExperimentalTracing()
	}

	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}

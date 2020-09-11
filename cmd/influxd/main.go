package main

import (
	"context"
	"fmt"
	_ "net/http/pprof"
	"os"
	"time"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/cmd/influxd/launcher"
	"github.com/influxdata/influxdb/v2/cmd/influxd/upgrade"
	_ "github.com/influxdata/influxdb/v2/query/builtin"
	_ "github.com/influxdata/influxdb/v2/tsdb/engine/tsm1"
	_ "github.com/influxdata/influxdb/v2/tsdb/index/tsi1"
	"github.com/spf13/cobra"
)

var (
	version = "dev"
	commit  = "none"
	date    = ""
)

func main() {
	if len(date) == 0 {
		date = time.Now().UTC().Format(time.RFC3339)
	}

	influxdb.SetBuildInfo(version, commit, date)

	rootCmd := launcher.NewInfluxdCommand(context.Background(),
		// FIXME
		//generate.Command,
		//restore.Command,
		upgrade.Command,
		&cobra.Command{
			Use:   "version",
			Short: "Print the influxd server version",
			Run: func(cmd *cobra.Command, args []string) {
				fmt.Printf("InfluxDB %s (git: %s) build_date: %s\n", version, commit, date)
			},
		},
	)

	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}

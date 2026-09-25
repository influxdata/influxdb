package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/cmd/influxd/downgrade"
	"github.com/influxdata/influxdb/v2/cmd/influxd/inspect"
	"github.com/influxdata/influxdb/v2/cmd/influxd/launcher"
	"github.com/influxdata/influxdb/v2/cmd/influxd/recovery"
	"github.com/influxdata/influxdb/v2/cmd/influxd/upgrade"
	"github.com/influxdata/influxdb/v2/kit/exit"
	_ "github.com/influxdata/influxdb/v2/tsdb/engine/tsm1"
	_ "github.com/influxdata/influxdb/v2/tsdb/index/tsi1"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
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

	ctx := context.Background()
	v := viper.New()

	rootCmd, err := newRootCommand(ctx, v)
	if err != nil {
		handleErr(err)
	}

	rootCmd.SilenceUsage = true
	if err := rootCmd.Execute(); err != nil {
		// Not handleErr: cobra has already printed the error itself, because
		// SilenceErrors is left false. All that is wanted after it is the exit
		// status the error carries, and sometimes the pointer to -h.
		//
		// Sometimes, because the two channels have to agree. Help is the answer
		// to a command line the operator got wrong (64), and to a failure that
		// was never given a category (1), which is every subcommand and is the
		// behavior those have always had. It is not the answer to a busy port
		// (69), a full disk (73) or a bad config file (78): the status already
		// named the problem, and no amount of help text addresses any of them.
		if code := exit.Code(err); code == exit.CodeUsage || code == exit.CodeGeneric {
			_, _ = fmt.Fprintf(os.Stderr, "See '%s -h' for help\n", rootCmd.CommandPath())
		}
		os.Exit(exit.Code(err))
	}
}

// newRootCommand assembles influxd's command tree.
//
// Separate from main so that a test can inspect the assembled tree -- which
// commands report which exit status is a property of this wiring, and cobra
// resolves some of it by inheritance.
func newRootCommand(ctx context.Context, v *viper.Viper) (*cobra.Command, error) {
	rootCmd, err := launcher.NewInfluxdCommand(ctx, v)
	if err != nil {
		return nil, err
	}
	// upgrade binds options to env variables, so it must be added after rootCmd is initialized
	upgradeCmd, err := upgrade.NewCommand(ctx, v)
	if err != nil {
		return nil, err
	}
	rootCmd.AddCommand(upgradeCmd)
	inspectCmd, err := inspect.NewCommand(v)
	if err != nil {
		return nil, err
	}
	rootCmd.AddCommand(inspectCmd)
	versionCmd := versionCmd()
	rootCmd.AddCommand(versionCmd)
	recoveryCmd := recovery.NewCommand()
	rootCmd.AddCommand(recoveryCmd)
	downgradeCmd, err := downgrade.NewCommand(ctx, v)
	if err != nil {
		return nil, err
	}
	rootCmd.AddCommand(downgradeCmd)

	// A malformed command line for the server is the operator's to fix, not
	// something to retry, so it exits EX_USAGE.
	//
	// Only the server. Cobra resolves this func by walking up to the nearest
	// ancestor that has one, so a registration on the root alone would also
	// re-status every subcommand below it -- and those have not opted in:
	// `influxd inspect --bogus-flag` exits 1 today and must keep doing so,
	// which is what EXIT_CODES.md promises under "What did not change". A
	// pass-through of its own stops the walk at each of their subtrees.
	//
	// A subcommand added later belongs in the second list unless it has
	// deliberately opted into sysexits statuses, or it will inherit one its own
	// error vocabulary knows nothing about.
	rootCmd.SetFlagErrorFunc(func(_ *cobra.Command, err error) error {
		return exit.WithCode(exit.CodeUsage, err)
	})
	for _, c := range []*cobra.Command{upgradeCmd, inspectCmd, versionCmd, recoveryCmd, downgradeCmd} {
		c.SetFlagErrorFunc(func(_ *cobra.Command, err error) error { return err })
	}

	return rootCmd, nil
}

// handleErr prints err to stderr and exits with the status it carries.
//
// exit.Code returns 1 for an error nothing pinned a status to, so a command
// that has not opted in -- inspect, upgrade, downgrade, recovery -- exits
// exactly as it always has.
func handleErr(err error) {
	_, _ = fmt.Fprintln(os.Stderr, err)
	os.Exit(exit.Code(err))
}

func versionCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Print the influxd server version",
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Printf("InfluxDB %s (git: %s) build_date: %s\n", version, commit, date)
		},
	}
}

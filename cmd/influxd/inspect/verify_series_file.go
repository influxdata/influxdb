package inspect

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"runtime"

	"github.com/influxdata/influxdb/logger"
	"github.com/influxdata/influxdb/tsdb"
	"github.com/spf13/cobra"
	"go.uber.org/zap/zapcore"
)

// NewVerifySeriesFileCommand returns a new instance of verifySeriesCommand
// for execution of "influx_inspect verify-seriesfile".
func NewVerifySeriesFileCommand() *cobra.Command {
	verifySeriesCommand := &cobra.Command{
		Use:   "verify-seriesfile",
		Short: "Verifies the integrity of Series files",
		Long: `Verifies the integrity of Series files.
		Usage: influx_inspect verify-seriesfile [flags]
			-dir <path>
					Root data path.
					Defaults to "` + os.Getenv("HOME") + `/.influxdbv2/engine/_series".
			-series-file <path>
					Path to a series file. This overrides -dir.
			-v
					Enable verbose logging.
			-c
					How many concurrent workers to run.
					Defaults to "` + string(runtime.GOMAXPROCS(0)) + `" on this machine.`,
		RunE: verifySeriesRun,
	}

	verifySeriesCommand.Flags().StringVar(&VerifySeriesFlags.dir, "dir", filepath.Join(os.Getenv("HOME"), ".influxdbv2", "engine/_series"), "Data directory.")
	verifySeriesCommand.Flags().StringVar(&VerifySeriesFlags.seriesFile, "series-file", "",
		"Path to a series file. This overrides -dir.")
	verifySeriesCommand.Flags().BoolVar(&VerifySeriesFlags.verbose, "v", false,
		"Verbose output.")
	verifySeriesCommand.Flags().IntVar(&VerifySeriesFlags.concurrent, "c", runtime.GOMAXPROCS(0),
		"How many concurrent workers to run.")

	return verifySeriesCommand
}

var VerifySeriesFlags = struct {
	dir        string
	seriesFile string
	verbose    bool
	concurrent int
}{}

// verifySeriesRun executes the command.
func verifySeriesRun(cmd *cobra.Command, args []string) error {
	config := logger.NewConfig()
	config.Level = zapcore.WarnLevel
	if VerifySeriesFlags.verbose {
		config.Level = zapcore.InfoLevel
	}
	logger, err := config.New(os.Stderr)
	if err != nil {
		return err
	}

	v := tsdb.NewVerify()
	v.Logger = logger
	v.Concurrent = VerifySeriesFlags.concurrent

	if VerifySeriesFlags.seriesFile != "" {
		_, err := v.VerifySeriesFile(VerifySeriesFlags.seriesFile)
		return err
	}

	dbs, err := ioutil.ReadDir(VerifySeriesFlags.dir)
	if err != nil {
		return err
	}

	for _, db := range dbs {
		if !db.IsDir() {
			continue
		}
		_, err := v.VerifySeriesFile(filepath.Join(VerifySeriesFlags.dir, db.Name()))
		if err != nil {
			return err
		}
	}

	return nil
}

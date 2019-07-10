package inspect

import (
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"runtime"

	"github.com/influxdata/influxdb/logger"
	"github.com/influxdata/influxdb/tsdb"
	"github.com/spf13/cobra"
	"go.uber.org/zap/zapcore"
)

// Command represents the program execution for "influx_inspect verify-seriesfile".
type Command struct {
	Stdout io.Writer
	Stderr io.Writer

	dir        string
	db         string
	seriesFile string
	verbose    bool
	concurrent int
}

// NewVerifySeriesFileCommand returns a new instance of verifySeriesCommand.
func NewVerifySeriesFileCommand() *cobra.Command {
	verifySeriesCommand := &cobra.Command{
		Use:   "verify-seriesfile",
		Short: "Verifies the integrity of Series files",
		Long: `Verifies the integrity of Series files.
		Usage: influx_inspect verify-seriesfile [flags]
			-dir <path>
					Root data path.
					Defaults to "` + os.Getenv("HOME") + `/.influxdb/data".
			-db <name>
					Only verify this database inside of the data directory.
			-series-file <path>
					Path to a series file. This overrides -db and -dir.
			-v
					Enable verbose logging.
			-c
					How many concurrent workers to run.
					Defaults to "` + string(runtime.GOMAXPROCS(0)) + `" on this machine.`,
		RunE: verifySeriesRun,
	}

	verifySeriesCommand.Flags().StringVar(&VerifySeriesFlags.dir, "dir", filepath.Join(os.Getenv("HOME"), ".influxdb", "data"), "Data directory.")
	verifySeriesCommand.Flags().StringVar(&VerifySeriesFlags.db, "db", "",
		"Only use this database inside of the data directory.")
	verifySeriesCommand.Flags().StringVar(&VerifySeriesFlags.seriesFile, "series-file", "",
		"Path to a series file. This overrides -db and -dir.")
	verifySeriesCommand.Flags().BoolVar(&VerifySeriesFlags.verbose, "v", false,
		"Verbose output.")
	verifySeriesCommand.Flags().IntVar(&VerifySeriesFlags.concurrent, "c", runtime.GOMAXPROCS(0),
		"How many concurrent workers to run.")

	return verifySeriesCommand
}

var VerifySeriesFlags = struct {
	dir        string
	db         string
	seriesFile string
	verbose    bool
	concurrent int
}{}

// Run executes the command.
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

	if VerifySeriesFlags.db != "" {
		_, err := v.VerifySeriesFile(filepath.Join(VerifySeriesFlags.dir, VerifySeriesFlags.db, "_series"))
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
		_, err := v.VerifySeriesFile(filepath.Join(VerifySeriesFlags.dir, db.Name(), "_series"))
		if err != nil {
			return err
		}
	}

	return nil
}

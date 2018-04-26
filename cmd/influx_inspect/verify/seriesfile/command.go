// Package seriesfile verifies integrity of series files.
package seriesfile

import (
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"runtime"

	"github.com/influxdata/influxdb/logger"
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

// NewCommand returns a new instance of Command.
func NewCommand() *Command {
	return &Command{
		Stdout: os.Stdout,
		Stderr: os.Stderr,
	}
}

// Run executes the command.
func (cmd *Command) Run(args ...string) error {
	fs := flag.NewFlagSet("verify-seriesfile", flag.ExitOnError)
	fs.StringVar(&cmd.dir, "dir", filepath.Join(os.Getenv("HOME"), ".influxdb", "data"),
		"Data directory.")
	fs.StringVar(&cmd.db, "db", "",
		"Only use this database inside of the data directory.")
	fs.StringVar(&cmd.seriesFile, "series-file", "",
		"Path to a series file. This overrides -db and -dir.")
	fs.BoolVar(&cmd.verbose, "v", false,
		"Verbose output.")
	fs.IntVar(&cmd.concurrent, "c", runtime.GOMAXPROCS(0),
		"How many concurrent workers to run.")

	fs.SetOutput(cmd.Stdout)
	fs.Usage = cmd.printUsage

	if err := fs.Parse(args); err != nil {
		return err
	}

	config := logger.NewConfig()
	config.Level = zapcore.WarnLevel
	if cmd.verbose {
		config.Level = zapcore.InfoLevel
	}
	logger, err := config.New(cmd.Stderr)
	if err != nil {
		return err
	}

	v := NewVerify()
	v.Logger = logger
	v.Concurrent = cmd.concurrent

	if cmd.seriesFile != "" {
		_, err := v.VerifySeriesFile(cmd.seriesFile)
		return err
	}

	if cmd.db != "" {
		_, err := v.VerifySeriesFile(filepath.Join(cmd.dir, cmd.db, "_series"))
		return err
	}

	dbs, err := ioutil.ReadDir(cmd.dir)
	if err != nil {
		return err
	}

	for _, db := range dbs {
		if !db.IsDir() {
			continue
		}
		_, err := v.VerifySeriesFile(filepath.Join(cmd.dir, db.Name(), "_series"))
		if err != nil {
			return err
		}
	}

	return nil
}

func (cmd *Command) printUsage() {
	usage := `Verifies the integrity of Series files.

Usage: influx_inspect verify-seriesfile [flags]

    -dir <path>
            Root data path.
            Defaults to "%[1]s/.influxdb/data".
    -db <name>
            Only verify this database inside of the data directory.
    -series-file <path>
            Path to a series file. This overrides -db and -dir.
    -v
            Enable verbose logging.
    -c
            How many concurrent workers to run.
            Defaults to "%[2]d" on this machine.
`

	fmt.Printf(usage, os.Getenv("HOME"), runtime.GOMAXPROCS(0))
}

package parquet

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/influxdata/influxdb/cmd/influx_tools/internal/format/binary"
	"github.com/influxdata/influxdb/cmd/influx_tools/internal/format/line"
	export2 "github.com/influxdata/influxdb/cmd/influx_tools/parquet/exporter"
	"github.com/influxdata/influxdb/cmd/influx_tools/server"
	"go.uber.org/zap"
)

var (
	_ line.Writer
	_ binary.Writer
)

// Command represents the program execution for "store query".
type Command struct {
	// Standard input/output, overridden for testing.
	Stderr io.Writer
	Stdout io.Writer
	Logger *zap.Logger
	server server.Interface

	conflicts io.WriteCloser

	configPath    string
	database      string
	rp            string
	measurement   string
	shardDuration time.Duration
	format        string
	r             rangeValue
	conflictPath  string
	ignore        bool
	print         bool
}

// NewCommand returns a new instance of the export Command.
func NewCommand(server server.Interface) *Command {
	return &Command{
		Stderr: os.Stderr,
		Stdout: os.Stdout,
		server: server,
	}
}

// Run executes the export command using the specified args.
func (cmd *Command) Run(args []string) (err error) {
	err = cmd.parseFlags(args)
	if err != nil {
		return err
	}

	err = cmd.server.Open(cmd.configPath)
	if err != nil {
		return err
	}
	defer cmd.server.Close()

	e, err := cmd.openExporter()
	if err != nil {
		return err
	}
	defer e.Close()

	e.PrintPlan(cmd.Stderr)

	if cmd.print {
		return nil
	}

	e.exporter = export2.New(100000000)

	return e.WriteTo()
}

func (cmd *Command) openExporter() (*exporter, error) {
	cfg := &exporterConfig{Database: cmd.database, RP: cmd.rp, Measurement: cmd.measurement, ShardDuration: cmd.shardDuration, Min: cmd.r.Min(), Max: cmd.r.Max()}
	e, err := newExporter(cmd.server, cfg)
	if err != nil {
		return nil, err
	}

	return e, e.Open()
}

func (cmd *Command) parseFlags(args []string) error {
	fs := flag.NewFlagSet("export", flag.ContinueOnError)
	fs.StringVar(&cmd.configPath, "config", "", "Config file")
	fs.StringVar(&cmd.database, "database", "", "Database name")
	fs.StringVar(&cmd.rp, "rp", "", "Retention policy name")
	fs.StringVar(&cmd.measurement, "measurement", "", "Measurement name")
	fs.StringVar(&cmd.format, "format", "line", "Output format (line, binary)")
	fs.StringVar(&cmd.conflictPath, "conflict-path", "", "File name for writing field conflicts using line protocol and gzipped")
	fs.BoolVar(&cmd.ignore, "no-conflict-path", false, "Disable writing field conflicts to a file")
	fs.Var(&cmd.r, "range", "Range of target shards to export (default: all)")
	fs.BoolVar(&cmd.print, "print-only", false, "Print plan to stderr and exit")
	fs.DurationVar(&cmd.shardDuration, "duration", time.Hour*24*7, "Target shard duration")

	if err := fs.Parse(args); err != nil {
		return err
	}

	if cmd.database == "" {
		return errors.New("database is required")
	}

	switch cmd.format {
	case "line", "binary", "series", "values", "discard":
	default:
		return fmt.Errorf("invalid format '%s'", cmd.format)
	}

	if cmd.conflictPath == "" && !cmd.ignore {
		return errors.New("missing conflict-path")
	}

	return nil
}

type rangeValue struct {
	min, max uint64
	set      bool
}

func (rv *rangeValue) Min() uint64 { return rv.min }

func (rv *rangeValue) Max() uint64 {
	if !rv.set {
		return math.MaxUint64
	}
	return rv.max
}

func (rv *rangeValue) String() string {
	if rv.Min() == rv.Max() {
		return fmt.Sprint(rv.min)
	}
	return fmt.Sprintf("[%d,%d]", rv.Min(), rv.Max())
}

func (rv *rangeValue) Set(v string) (err error) {
	p := strings.Split(v, "-")
	switch {
	case len(p) == 1:
		rv.min, err = strconv.ParseUint(p[0], 10, 64)
		if err != nil {
			return fmt.Errorf("range error: invalid number %s", v)
		}
		rv.max = rv.min
	case len(p) == 2:
		rv.min, err = strconv.ParseUint(p[0], 10, 64)
		if err != nil {
			return fmt.Errorf("range error: min value %q is not a positive number", p[0])
		}
		rv.max = math.MaxUint64
		if len(p[1]) > 0 {
			rv.max, err = strconv.ParseUint(p[1], 10, 64)
			if err != nil {
				return fmt.Errorf("range error: max value %q is not empty or a positive number", p[1])
			}
		}
	default:
		return fmt.Errorf("range error: %q is not a valid range", v)
	}

	if rv.min > rv.max {
		return errors.New("range error: min > max")
	}

	rv.set = true

	return nil
}

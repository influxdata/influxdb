package export

import (
	"compress/gzip"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"os"
	"runtime"
	"runtime/pprof"
	"strconv"
	"strings"
	"time"

	"github.com/influxdata/influxdb/cmd/influx-tools/internal/format"
	"github.com/influxdata/influxdb/cmd/influx-tools/internal/format/binary"
	"github.com/influxdata/influxdb/cmd/influx-tools/internal/format/line"
	"github.com/influxdata/influxdb/cmd/influx-tools/internal/format/text"
	"github.com/influxdata/influxdb/cmd/influx-tools/server"
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

	cpu       *os.File
	mem       *os.File
	conflicts io.WriteCloser

	configPath    string
	cpuProfile    string
	memProfile    string
	database      string
	rp            string
	shardDuration time.Duration
	format        string
	r             rangeValue
	conflictPath  string
	ignore        bool
	print         bool
}

// NewCommand returns a new instance of Command.
func NewCommand(server server.Interface) *Command {
	return &Command{
		Stderr: os.Stderr,
		Stdout: os.Stdout,
		server: server,
	}
}

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

	if cmd.print {
		e.PrintPlan(os.Stdout)
		return nil
	}

	if !cmd.ignore {
		if f, err := os.Create(cmd.conflictPath); err != nil {
			return err
		} else {
			cmd.conflicts = gzip.NewWriter(f)
			defer func() {
				cmd.conflicts.Close()
				f.Close()
			}()
		}
	}

	cmd.startProfile()
	defer cmd.stopProfile()

	var wr format.Writer
	switch cmd.format {
	case "line":
		wr = line.NewWriter(os.Stdout)
	case "binary":
		wr = binary.NewWriter(os.Stdout, cmd.database, cmd.rp, cmd.shardDuration)
	case "series":
		wr = text.NewWriter(os.Stdout, text.Series)
	case "values":
		wr = text.NewWriter(os.Stdout, text.Values)
	case "discard":
		wr = format.Discard
	}
	defer func() {
		err = wr.Close()
	}()

	if cmd.conflicts != nil {
		wr = format.NewConflictWriter(wr, line.NewWriter(cmd.conflicts))
	} else {
		wr = format.NewConflictWriter(wr, format.DevNull)
	}

	return e.WriteTo(wr)
}

func (cmd *Command) openExporter() (*Exporter, error) {
	cfg := &ExporterConfig{Database: cmd.database, RP: cmd.rp, ShardDuration: cmd.shardDuration, Min: cmd.r.Min(), Max: cmd.r.Max()}
	e, err := NewExporter(cmd.server, cfg)
	if err != nil {
		return nil, err
	}

	return e, e.Open()
}

func (cmd *Command) parseFlags(args []string) error {
	fs := flag.NewFlagSet("export", flag.ContinueOnError)
	fs.StringVar(&cmd.configPath, "config", "", "Config file")
	fs.StringVar(&cmd.cpuProfile, "cpuprofile", "", "")
	fs.StringVar(&cmd.memProfile, "memprofile", "", "")
	fs.StringVar(&cmd.database, "database", "", "Database name")
	fs.StringVar(&cmd.rp, "rp", "", "Retention policy name")
	fs.StringVar(&cmd.format, "format", "line", "Output format (line, binary)")
	fs.StringVar(&cmd.conflictPath, "conflict-path", "", "File name for writing field conflicts using line protocol and gzipped")
	fs.BoolVar(&cmd.ignore, "no-conflict-path", false, "Disable writing field conflicts to a file")
	fs.Var(&cmd.r, "range", "Range of target shards to export (default: all)")
	fs.BoolVar(&cmd.print, "print", false, "Print plan to stdout")
	fs.DurationVar(&cmd.shardDuration, "duration", time.Hour*24*7, "Target shard duration")

	if err := fs.Parse(args); err != nil {
		return err
	}

	if cmd.database == "" {
		return errors.New("database is required")
	}

	if cmd.format != "line" && cmd.format != "binary" && cmd.format != "series" && cmd.format != "values" && cmd.format != "discard" {
		return fmt.Errorf("invalid format '%s'", cmd.format)
	}

	if cmd.conflictPath == "" && !cmd.ignore {
		return errors.New("missing conflict-path")
	}

	return nil
}

// StartProfile initializes the cpu and memory profile, if specified.
func (cmd *Command) startProfile() {
	if cmd.cpuProfile != "" {
		f, err := os.Create(cmd.cpuProfile)
		if err != nil {
			fmt.Fprintf(cmd.Stderr, "cpuprofile: %v\n", err)
			os.Exit(1)
		}
		cmd.cpu = f
		pprof.StartCPUProfile(cmd.cpu)
	}

	if cmd.memProfile != "" {
		f, err := os.Create(cmd.memProfile)
		if err != nil {
			fmt.Fprintf(cmd.Stderr, "memprofile: %v\n", err)
			os.Exit(1)
		}
		cmd.mem = f
		runtime.MemProfileRate = 4096
	}

}

// StopProfile closes the cpu and memory profiles if they are running.
func (cmd *Command) stopProfile() {
	if cmd.cpu != nil {
		pprof.StopCPUProfile()
		cmd.cpu.Close()
	}
	if cmd.mem != nil {
		pprof.Lookup("heap").WriteTo(cmd.mem, 0)
		cmd.mem.Close()
	}
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
		if len(p[1]) > 0 {
			rv.max, err = strconv.ParseUint(p[1], 10, 64)
			if err != nil {
				return fmt.Errorf("range error: max value %q is not empty or a positive number", p[1])
			}
		} else {
			rv.max = math.MaxUint64
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

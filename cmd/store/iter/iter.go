// Package iter provides a mechanism to query a datbase
package iter

import (
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"bufio"
	"errors"
	"log"
	"runtime"
	"runtime/pprof"
	"strconv"

	"github.com/influxdata/influxdb/cmd/influxd/run"
	"github.com/influxdata/influxdb/influxql"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/services/meta"
	"github.com/influxdata/influxdb/services/storage"
	"github.com/influxdata/influxdb/tsdb"
	"github.com/uber-go/zap"
)

// Command represents the program execution for "influx_inspect export".
type Command struct {
	// Standard input/output, overridden for testing.
	Stderr io.Writer
	Stdout io.Writer
	Logger zap.Logger

	cpuProfile      string
	memProfile      string
	configPath      string
	database        string
	retentionPolicy string
	startTime       int64
	endTime         int64
	limit           int
	offset          int
	desc            bool
	silent          bool
	iterator        bool
}

// NewCommand returns a new instance of Command.
func NewCommand() *Command {
	return &Command{
		Stderr: os.Stderr,
		Stdout: os.Stdout,
	}
}

func parseTime(v string) (int64, error) {
	if s, err := time.Parse(time.RFC3339, v); err == nil {
		return s.UnixNano(), nil
	}

	if i, err := strconv.ParseInt(v, 10, 64); err == nil {
		return i, nil
	}

	return 0, errors.New("invalid time")
}

// GetConfigPath returns the config path from the options.
// It will return a path by searching in this order:
//   1. The CLI option in ConfigPath
//   2. The environment variable INFLUXDB_CONFIG_PATH
//   3. The first influxdb.conf file on the path:
//        - ~/.influxdb
//        - /etc/influxdb
func (cmd *Command) GetConfigPath() string {
	if cmd.configPath != "" {
		if cmd.configPath == os.DevNull {
			return ""
		}
		return cmd.configPath
	} else if envVar := os.Getenv("INFLUXDB_CONFIG_PATH"); envVar != "" {
		return envVar
	}

	for _, path := range []string{
		os.ExpandEnv("${HOME}/.influxdb/influxdb.conf"),
		"/etc/influxdb/influxdb.conf",
	} {
		if _, err := os.Stat(path); err == nil {
			return path
		}
	}
	return ""
}

// Run executes the command.
func (cmd *Command) Run(args ...string) error {
	var start, end string
	fs := flag.NewFlagSet("iter", flag.ExitOnError)
	fs.StringVar(&cmd.cpuProfile, "cpuprofile", "", "CPU profile name")
	fs.StringVar(&cmd.memProfile, "memprofile", "", "memory profile name")
	fs.StringVar(&cmd.configPath, "config", "", "Path to config file")
	fs.StringVar(&cmd.database, "database", "", "Optional: the database to export")
	fs.StringVar(&cmd.retentionPolicy, "retention", "", "Optional: the retention policy to export (requires -database)")
	fs.StringVar(&start, "start", "", "Optional: the start time to export (RFC3339 format)")
	fs.StringVar(&end, "end", "", "Optional: the end time to export (RFC3339 format)")
	fs.IntVar(&cmd.limit, "limit", 10, "Optional: limit number of rows")
	fs.IntVar(&cmd.offset, "offset", 0, "Optional: start offset for rows")
	fs.BoolVar(&cmd.desc, "desc", false, "Optional: return results in descending order")
	fs.BoolVar(&cmd.silent, "silent", false, "silence output")
	fs.BoolVar(&cmd.iterator, "iter", false, "use iterator")

	fs.SetOutput(cmd.Stdout)
	fs.Usage = func() {
		fmt.Fprintln(cmd.Stdout, "Query local store .")
		fmt.Fprintf(cmd.Stdout, "Usage: %s iter [flags]\n\n", filepath.Base(os.Args[0]))
		fs.PrintDefaults()
	}

	if err := fs.Parse(args); err != nil {
		return err
	}

	// set defaults
	if start != "" {
		if t, err := parseTime(start); err != nil {
			return err
		} else {
			cmd.startTime = t
		}
	} else {
		cmd.startTime = models.MinNanoTime
	}
	if end != "" {
		if t, err := parseTime(end); err != nil {
			return err
		} else {
			cmd.endTime = t
		}
	} else {
		// set end time to max if it is not set.
		cmd.endTime = models.MaxNanoTime
	}

	if err := cmd.validate(); err != nil {
		return err
	}

	// Parse config
	config, err := cmd.ParseConfig(cmd.GetConfigPath())
	if err != nil {
		return fmt.Errorf("parse config: %s", err)
	}

	// Apply any environment variables on top of the parsed config
	if err := config.ApplyEnvOverrides(); err != nil {
		return fmt.Errorf("apply env config: %v", err)
	}

	// Validate the configuration.
	if err := config.Validate(); err != nil {
		return fmt.Errorf("%s. To generate a valid configuration file run `influxd config > influxdb.generated.conf`", err)
	}

	return cmd.query(config)
}

func (cmd *Command) validate() error {
	if cmd.retentionPolicy != "" && cmd.database == "" {
		return fmt.Errorf("must specify a db")
	}
	if cmd.startTime != 0 && cmd.endTime != 0 && cmd.endTime < cmd.startTime {
		return fmt.Errorf("end time before start time")
	}
	return nil
}

func (cmd *Command) query(c *run.Config) error {
	mc := meta.NewClient(c.Meta)
	if err := mc.Open(); err != nil {
		return err
	}

	s := tsdb.NewStore(c.Data.Dir)
	// s.Logger = cmd.Logger
	s.EngineOptions.Config = c.Data

	s.EngineOptions.EngineVersion = c.Data.Engine
	s.EngineOptions.IndexVersion = c.Data.Index

	if err := s.Open(); err != nil {
		return fmt.Errorf("open tsdb store: %s", err)
	}

	store := storage.NewStore()
	store.TSDBStore = s
	store.MetaClient = mc
	store.Logger = cmd.Logger

	startProfile(cmd.cpuProfile, cmd.memProfile)
	defer stopProfile()

	now := time.Now()
	defer func() {
		dur := time.Since(now)
		fmt.Printf("time: %v\n", dur)
	}()

	if cmd.iterator {
		return cmd.queryIterator(mc, s)
	} else {
		return cmd.queryCursor(store)
	}

	s.Close()
	mc.Close()

	return nil
}

func (cmd *Command) queryCursor(s *storage.Store) error {
	var req storage.ReadRequest
	req.Descending = cmd.desc
	req.TimestampRange.Start = cmd.startTime
	req.TimestampRange.End = cmd.endTime
	req.Database = cmd.database

	rs, err := s.Read(req)
	if err != nil {
		return err
	}

	i := 0
	sum := int64(0)
	if cmd.silent {
	LIMIT1:
		for rs.Next() {
			cur := rs.Cursor()
			icur, ok := cur.(tsdb.IntegerCursor)
			if !ok {
				continue
			}

			for {
				ts, v := icur.Next()
				if ts == tsdb.EOF {
					break
				} else {
					i++
					if i > cmd.limit {
						break LIMIT1
					}
					sum += v
				}
			}
		}
	} else {
		var buf [1024]byte
		var line []byte
		line = buf[:0]

		wr := bufio.NewWriter(os.Stdout)

	LIMIT2:
		for rs.Next() {
			cur := rs.Cursor()
			icur, ok := cur.(tsdb.IntegerCursor)
			if !ok {
				continue
			}

			series := icur.SeriesKey()
			wr.WriteString("series: ")
			wr.WriteString(series)
			wr.WriteString("\n")
			wr.Flush()

			for {
				ts, v := icur.Next()
				if ts == tsdb.EOF {
					break
				}

				i++
				if i > cmd.limit {
					break LIMIT2
				}

				line = buf[:0]
				wr.Write(strconv.AppendInt(line, ts, 10))
				wr.WriteByte(' ')

				line = buf[:0]
				wr.Write(strconv.AppendInt(line, v, 10))
				wr.WriteString("\n")
				wr.Flush()
				sum += v
			}
		}
	}

	fmt.Println("sum", sum)

	return nil
}

func (cmd *Command) queryIterator(mc *meta.Client, s *tsdb.Store) error {
	di := mc.Database(cmd.database)

	groups, err := mc.ShardGroupsByTimeRange(cmd.database, di.DefaultRetentionPolicy, time.Unix(0, cmd.startTime), time.Unix(0, cmd.endTime))
	if err != nil {
		return err
	}

	shardIDs := make([]uint64, 0, len(groups[0].Shards)*len(groups))
	for _, g := range groups {
		for _, si := range g.Shards {
			shardIDs = append(shardIDs, si.ID)
		}
	}

	var opt influxql.IteratorOptions
	opt.StartTime = cmd.startTime
	opt.EndTime = cmd.endTime
	opt.Ascending = !cmd.desc
	opt.Limit = cmd.limit
	opt.Offset = cmd.offset
	var ref influxql.Expr = &influxql.VarRef{Val: "v0", Type: influxql.Integer}
	//ref = &influxql.Call{Name: "sum", Args: []influxql.Expr{ref}}
	opt.Expr = ref

	sg := s.ShardGroup(shardIDs)
	it, err := sg.CreateIterator("m0", opt)
	if err != nil {
		return err
	}

	iit, ok := it.(influxql.IntegerIterator)
	if !ok {
		return fmt.Errorf("not an integer iterator: %T", it)
	}

	v, err := iit.Next()
	sum := int64(0)

	if cmd.silent {
		for ; v != nil && err == nil; v, err = iit.Next() {
			sum += v.Value
		}
	} else {
		var buf [1024]byte
		var line []byte

		wr := bufio.NewWriter(os.Stdout)

		for ; v != nil && err == nil; v, err = iit.Next() {
			//if s, ok := v.Aux[0].(string); ok {
			//	wr.WriteString(s)
			//	wr.WriteByte(' ')
			//}

			line = buf[:0]
			wr.Write(strconv.AppendInt(line, v.Time, 10))
			wr.WriteByte(' ')

			line = buf[:0]
			wr.Write(strconv.AppendInt(line, v.Value, 10))
			wr.WriteString("\n")
			wr.Flush()
			sum += v.Value
		}
	}

	fmt.Println("sum", sum)

	return nil
}

// ParseConfig parses the config at path.
// It returns a demo configuration if path is blank.
func (cmd *Command) ParseConfig(path string) (*run.Config, error) {
	// Use demo configuration if no config path is specified.
	if path == "" {
		return run.NewDemoConfig()
	}

	config := run.NewConfig()
	if err := config.FromTomlFile(path); err != nil {
		return nil, err
	}

	return config, nil
}

// prof stores the file locations of active profiles.
var prof struct {
	cpu *os.File
	mem *os.File
}

// StartProfile initializes the cpu and memory profile, if specified.
func startProfile(cpuprofile, memprofile string) {
	if cpuprofile != "" {
		f, err := os.Create(cpuprofile)
		if err != nil {
			log.Fatalf("cpuprofile: %v", err)
		}
		log.Printf("writing CPU profile to: %s\n", cpuprofile)
		prof.cpu = f
		pprof.StartCPUProfile(prof.cpu)
	}

	if memprofile != "" {
		f, err := os.Create(memprofile)
		if err != nil {
			log.Fatalf("memprofile: %v", err)
		}
		log.Printf("writing mem profile to: %s\n", memprofile)
		prof.mem = f
		runtime.MemProfileRate = 4096
	}

}

// StopProfile closes the cpu and memory profiles if they are running.
func stopProfile() {
	if prof.cpu != nil {
		pprof.StopCPUProfile()
		prof.cpu.Close()
		log.Println("CPU profile stopped")
	}
	if prof.mem != nil {
		pprof.Lookup("heap").WriteTo(prof.mem, 0)
		prof.mem.Close()
		log.Println("mem profile stopped")
	}
}

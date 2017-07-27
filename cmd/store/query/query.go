package query

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"errors"

	"github.com/gogo/protobuf/codec"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/services/storage"
	"github.com/influxdata/yarpc"
	"github.com/uber-go/zap"
)

// Command represents the program execution for "influx_inspect export".
type Command struct {
	// Standard input/output, overridden for testing.
	Stderr io.Writer
	Stdout io.Writer
	Logger zap.Logger

	addr            string
	cpuProfile      string
	memProfile      string
	database        string
	retentionPolicy string
	startTime       int64
	endTime         int64
	limit           int
	offset          int
	desc            bool
	silent          bool
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

// Run executes the command.
func (cmd *Command) Run(args ...string) error {
	var start, end string
	fs := flag.NewFlagSet("query", flag.ExitOnError)
	fs.StringVar(&cmd.cpuProfile, "cpuprofile", "", "CPU profile name")
	fs.StringVar(&cmd.memProfile, "memprofile", "", "memory profile name")
	fs.StringVar(&cmd.addr, "addr", ":8082", "the RPC address")
	fs.StringVar(&cmd.database, "database", "", "Optional: the database to export")
	fs.StringVar(&cmd.retentionPolicy, "retention", "", "Optional: the retention policy to export (requires -database)")
	fs.StringVar(&start, "start", "", "Optional: the start time to export (RFC3339 format)")
	fs.StringVar(&end, "end", "", "Optional: the end time to export (RFC3339 format)")
	fs.IntVar(&cmd.limit, "limit", 10, "Optional: limit number of rows")
	fs.IntVar(&cmd.offset, "offset", 0, "Optional: start offset for rows")
	fs.BoolVar(&cmd.desc, "desc", false, "Optional: return results in descending order")
	fs.BoolVar(&cmd.silent, "silent", false, "silence output")

	fs.SetOutput(cmd.Stdout)
	fs.Usage = func() {
		fmt.Fprintln(cmd.Stdout, "Query via RPC")
		fmt.Fprintf(cmd.Stdout, "Usage: %s query [flags]\n\n", filepath.Base(os.Args[0]))
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

	opts := []yarpc.DialOption{yarpc.WithCodec(codec.New(1000))}
	conn, err := yarpc.Dial(cmd.addr, opts...)
	if err != nil {
		return err
	}
	defer conn.Close()

	c := storage.NewStorageClient(conn)
	return cmd.query(c)
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

func (cmd *Command) query(c storage.StorageClient) error {
	var req storage.ReadRequest
	req.Database = cmd.database
	req.TimestampRange.Start = cmd.startTime
	req.TimestampRange.End = cmd.endTime
	req.Limit = int64(cmd.limit)

	stream, err := c.Read(context.Background(), &req)
	if err != nil {
		return err
	}

	integerSum := int64(0)
	floatSum := float64(0)
	var buf [1024]byte
	var line []byte
	wr := bufio.NewWriter(os.Stdout)

	now := time.Now()
	defer func() {
		dur := time.Since(now)
		fmt.Printf("time: %v\n", dur)
	}()

	for {
		var rep storage.ReadResponse

		if err := stream.RecvMsg(&rep); err != nil {
			if err == io.EOF {
				break
			}

			return err
		}

		for _, frame := range rep.Frames {
			if s := frame.GetSeries(); s != nil {
				if !cmd.silent {
					wr.WriteString("series:")
					wr.WriteString(s.Name)
					wr.WriteString("\n")
					wr.Flush()
				}
			} else if p := frame.GetIntegerPoints(); p != nil {
				if cmd.silent {
					for _, v := range p.Values {
						integerSum += v
					}
				} else {
					for i := 0; i < len(p.Timestamps); i++ {
						line = buf[:0]
						wr.Write(strconv.AppendInt(line, p.Timestamps[i], 10))
						wr.WriteByte(' ')

						line = buf[:0]
						wr.Write(strconv.AppendInt(line, p.Values[i], 10))
						wr.WriteString("\n")
						wr.Flush()

						integerSum += p.Values[i]
					}
				}
			} else if p := frame.GetFloatPoints(); p != nil {
				if cmd.silent {
					for _, v := range p.Values {
						floatSum += v
					}
				} else {
					for i := 0; i < len(p.Timestamps); i++ {
						line = buf[:0]
						wr.Write(strconv.AppendInt(line, p.Timestamps[i], 10))
						wr.WriteByte(' ')

						line = buf[:0]
						wr.Write(strconv.AppendFloat(line, p.Values[i], 'f', 10, 64))
						wr.WriteString("\n")
						wr.Flush()

						floatSum += p.Values[i]
					}
				}
			}
		}

	}

	fmt.Println("integerSum", integerSum, "floatSum", floatSum)

	return nil
}

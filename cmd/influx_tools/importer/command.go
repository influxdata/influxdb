package importer

import (
	"errors"
	"flag"
	"io"
	"os"
	"time"

	"github.com/influxdata/influxdb/cmd/influx_tools/internal/errlist"

	"github.com/influxdata/influxdb/cmd/influx_tools/internal/format/binary"
	"github.com/influxdata/influxdb/cmd/influx_tools/server"
	"github.com/influxdata/influxdb/services/meta"
	"github.com/influxdata/influxdb/tsdb/engine/tsm1"
	"go.uber.org/zap"
)

// Command represents the program execution for "store query".
type Command struct {
	// Standard input/output, overridden for testing.
	Stderr io.Writer
	Stdin  io.Reader
	Logger *zap.Logger
	server server.Interface

	configPath      string
	database        string
	retentionPolicy string
	replication     int
	duration        time.Duration
	shardDuration   time.Duration
	buildTSI        bool
	replace         bool
}

// NewCommand returns a new instance of Command.
func NewCommand(server server.Interface) *Command {
	return &Command{
		Stderr: os.Stderr,
		Stdin:  os.Stdin,
		server: server,
	}
}

// Run executes the import command using the specified args.
func (cmd *Command) Run(args []string) (err error) {
	err = cmd.parseFlags(args)
	if err != nil {
		return err
	}

	err = cmd.server.Open(cmd.configPath)
	if err != nil {
		return err
	}

	i := newImporter(cmd.server, cmd.database, cmd.retentionPolicy, cmd.replace, cmd.buildTSI, cmd.Logger)

	reader := binary.NewReader(cmd.Stdin)
	_, err = reader.ReadHeader()
	if err != nil {
		return err
	}

	rp := &meta.RetentionPolicySpec{Name: cmd.retentionPolicy, ShardGroupDuration: cmd.shardDuration}
	if cmd.duration >= time.Hour {
		rp.Duration = &cmd.duration
	}
	if cmd.replication > 0 {
		rp.ReplicaN = &cmd.replication
	}
	err = i.CreateDatabase(rp)
	if err != nil {
		return err
	}

	var bh *binary.BucketHeader
	for bh, err = reader.NextBucket(); (bh != nil) && (err == nil); bh, err = reader.NextBucket() {
		err = importShard(reader, i, bh.Start, bh.End)
		if err != nil {
			return err
		}
	}

	return err
}

func importShard(reader *binary.Reader, i *importer, start int64, end int64) error {
	err := i.StartShardGroup(start, end)
	if err != nil {
		return err
	}

	el := errlist.NewErrorList()
	var sh *binary.SeriesHeader
	var next bool
	for sh, err = reader.NextSeries(); (sh != nil) && (err == nil); sh, err = reader.NextSeries() {
		i.AddSeries(sh.SeriesKey)
		pr := reader.Points()
		seriesFieldKey := tsm1.SeriesFieldKeyBytes(string(sh.SeriesKey), string(sh.Field))

		for next, err = pr.Next(); next && (err == nil); next, err = pr.Next() {
			err = i.Write(seriesFieldKey, pr.Values())
			if err != nil {
				break
			}
		}
		if err != nil {
			break
		}
	}

	el.Add(err)
	el.Add(i.CloseShardGroup())

	return el.Err()
}

func (cmd *Command) parseFlags(args []string) error {
	fs := flag.NewFlagSet("import", flag.ContinueOnError)
	fs.StringVar(&cmd.configPath, "config", "", "Config file")
	fs.StringVar(&cmd.database, "database", "", "Database name")
	fs.StringVar(&cmd.retentionPolicy, "rp", "", "Retention policy")
	fs.IntVar(&cmd.replication, "replication", 0, "Retention policy replication")
	fs.DurationVar(&cmd.duration, "duration", time.Hour*0, "Retention policy duration")
	fs.DurationVar(&cmd.shardDuration, "shard-duration", time.Hour*24*7, "Retention policy shard duration")
	fs.BoolVar(&cmd.buildTSI, "build-tsi", false, "Build the on disk TSI")
	fs.BoolVar(&cmd.replace, "replace", false, "Enables replacing an existing retention policy")

	if err := fs.Parse(args); err != nil {
		return err
	}

	if cmd.database == "" {
		return errors.New("database is required")
	}

	if cmd.retentionPolicy == "" {
		return errors.New("retention policy is required")
	}

	return nil
}

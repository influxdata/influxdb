package compact

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/influxdata/influxdb/cmd/influx_tools/internal/format/binary"
	"github.com/influxdata/influxdb/cmd/influx_tools/internal/format/line"
	"github.com/influxdata/influxdb/cmd/influx_tools/internal/shard"
	"github.com/influxdata/influxdb/cmd/influx_tools/internal/storage"
	"github.com/influxdata/influxdb/cmd/influx_tools/server"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/tsdb"
	"github.com/influxdata/influxdb/tsdb/engine/tsm1"
	"go.uber.org/zap"
)

var (
	_ line.Writer
	_ binary.Writer
)

// Command represents the program execution for "influx-tools compact".
type Command struct {
	// Standard input/output, overridden for testing.
	Stderr io.Writer
	Stdout io.Writer
	Logger *zap.Logger

	server server.Interface
	store  *storage.Store

	configPath string
	database   string
	rp         string
	shardID    uint64
	force      bool
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

	err = cmd.openStore()
	if err != nil {
		return err
	}
	defer cmd.closeStore()

	if sh := cmd.getShard(); sh == nil {
		return fmt.Errorf("shard %d does not exist", cmd.shardID)
	} else if sh.IsIdle() {
		fmt.Printf("shard %d is fully compacted\n", cmd.shardID)
		return nil
	}

	dataPath := filepath.Join(cmd.server.TSDBConfig().Dir, cmd.database, cmd.rp)
	shardDataPath := filepath.Join(dataPath, strconv.Itoa(int(cmd.shardID)))
	walPath := filepath.Join(cmd.server.TSDBConfig().WALDir, cmd.database, cmd.rp)
	shardWalPath := filepath.Join(walPath, strconv.Itoa(int(cmd.shardID)))

	// snapshot existing of shard files (tsm, tombstone and wal)
	files, err := newShardFiles(shardDataPath, shardWalPath)
	if err != nil {
		return err
	}

	fmt.Println("The following files will be compacted:")
	fmt.Println()
	fmt.Println(files.String())

	if !cmd.force {
		fmt.Print("Proceed? [N] ")
		scan := bufio.NewScanner(os.Stdin)
		scan.Scan()
		if scan.Err() != nil {
			return fmt.Errorf("error reading STDIN: %v", scan.Err())
		}

		if strings.ToLower(scan.Text()) != "y" {
			return nil
		}
	}

	gen, seq := 1, 4
	if len(files.tsm) > 0 {
		sort.Strings(files.tsm)
		gen, _, err = tsm1.DefaultParseFileName(files.tsm[len(files.tsm)-1])
		if err != nil {
			return fmt.Errorf("failed to parse tsm file %q: %v", files.tsm[len(files.tsm)-1], err)
		}
		gen++
	}

	rs, err := cmd.read()
	if err != nil {
		return fmt.Errorf("read error: %v\n", err)
	}

	if rs == nil {
		fmt.Printf("no data to read")
		return nil
	}

	sw := shard.NewWriter(cmd.shardID, dataPath, shard.Temporary(), shard.Generation(gen), shard.Sequence(seq))

	var countSeries, countValues int

	values := make(tsm1.Values, 1000) // max block size
	var keyFieldSeparatorBytes = []byte("#!~#")

	makeKey := func(name []byte, tags models.Tags, field []byte) []byte {
		sz := 0 +
			len(name) +
			1 + // name delimiter
			tags.Size() + // total size of tags in bytes
			len(tags) - 1 + // tag delimiters
			len(keyFieldSeparatorBytes) +
			len(field)

		key := make([]byte, sz)
		models.AppendMakeKey(key, name, tags)
		key = append(key, keyFieldSeparatorBytes...)
		key = append(key, field...)
		return key
	}

	for rs.Next() {
		countSeries++
		seriesKey := makeKey(rs.Name(), rs.Tags(), rs.Field())
		ci := rs.CursorIterator()

		for ci.Next() {
			cur := ci.Cursor()
			switch c := cur.(type) {
			case tsdb.IntegerBatchCursor:
				for {
					keys, vals := c.Next()
					if len(keys) == 0 {
						break
					}
					countValues += len(keys)
					for i, k := range keys {
						values[i] = tsm1.NewIntegerValue(k, vals[i])
					}
					sw.Write(seriesKey, values[:len(keys)])
				}
			case tsdb.FloatBatchCursor:
				for {
					keys, vals := c.Next()
					if len(keys) == 0 {
						break
					}
					countValues += len(keys)
					for i, k := range keys {
						values[i] = tsm1.NewFloatValue(k, vals[i])
					}
					sw.Write(seriesKey, values[:len(keys)])
				}
			case tsdb.UnsignedBatchCursor:
				for {
					keys, vals := c.Next()
					if len(keys) == 0 {
						break
					}
					countValues += len(keys)
					for i, k := range keys {
						values[i] = tsm1.NewUnsignedValue(k, vals[i])
					}
					sw.Write(seriesKey, values[:len(keys)])
				}
			case tsdb.BooleanBatchCursor:
				for {
					keys, vals := c.Next()
					if len(keys) == 0 {
						break
					}
					countValues += len(keys)
					for i, k := range keys {
						values[i] = tsm1.NewBooleanValue(k, vals[i])
					}
					sw.Write(seriesKey, values[:len(keys)])
				}
			case tsdb.StringBatchCursor:
				for {
					keys, vals := c.Next()
					if len(keys) == 0 {
						break
					}
					countValues += len(keys)
					for i, k := range keys {
						values[i] = tsm1.NewStringValue(k, vals[i])
					}
					sw.Write(seriesKey, values[:len(keys)])
				}
			case nil:
				// no data for series key + field combination in this shard
				continue
			default:
				panic(fmt.Sprintf("unreachable: %T", c))
			}
			cur.Close()
		}
	}

	fmt.Printf("processed %d series, %d values\n", countSeries, countValues)

	rs.Close()
	sw.Close()
	if sw.Err() != nil {
		for _, f := range sw.Files() {
			os.Remove(f)
		}
		return sw.Err()
	}

	cmd.closeStore() // close TSDB store to release files

	newFiles, err := files.replace(sw.Files())
	if err != nil {
		fmt.Printf("Compaction failed: unable to replace files\n%v", err)
		return errors.New("unable to replace files")
	}

	fmt.Println("Compaction succeeded. New files:")
	for _, f := range newFiles {
		fmt.Printf("  %s\n", f)
	}

	return nil
}

func (cmd *Command) parseFlags(args []string) error {
	fs := flag.NewFlagSet("compact-shard", flag.ContinueOnError)
	fs.StringVar(&cmd.configPath, "config", "", "Config file")
	fs.StringVar(&cmd.database, "database", "", "Database name")
	fs.StringVar(&cmd.rp, "rp", "", "Retention policy name")
	fs.Uint64Var(&cmd.shardID, "shard-id", 0, "Shard ID to compact")
	fs.BoolVar(&cmd.force, "force", false, "Force compaction without prompting")

	if err := fs.Parse(args); err != nil {
		return err
	}

	if cmd.database == "" {
		return errors.New("database is required")
	}

	if cmd.rp == "" {
		return errors.New("rp is required")
	}

	if cmd.shardID == 0 {
		return errors.New("shard-id is required")
	}

	return nil
}

func (cmd *Command) openStore() error {
	store := tsdb.NewStore(cmd.server.TSDBConfig().Dir)
	store.EngineOptions.MonitorDisabled = true
	store.EngineOptions.CompactionDisabled = true
	store.EngineOptions.Config = cmd.server.TSDBConfig()
	store.EngineOptions.EngineVersion = cmd.server.TSDBConfig().Engine
	store.EngineOptions.IndexVersion = cmd.server.TSDBConfig().Index
	store.EngineOptions.DatabaseFilter = func(database string) bool {
		return database == cmd.database
	}
	store.EngineOptions.RetentionPolicyFilter = func(_, rp string) bool {
		return rp == cmd.rp
	}
	store.EngineOptions.ShardFilter = func(_, _ string, id uint64) bool {
		return id == cmd.shardID
	}

	if err := store.Open(); err != nil {
		return err
	}

	cmd.store = &storage.Store{TSDBStore: store}

	return nil
}

func (cmd *Command) closeStore() {
	if cmd.store != nil {
		cmd.store.TSDBStore.Close()
		cmd.store = nil
	}
}

func (cmd *Command) getShard() *tsdb.Shard {
	return cmd.store.TSDBStore.Shard(cmd.shardID)
}

func (cmd *Command) read() (*storage.ResultSet, error) {
	sh := cmd.getShard()
	if sh == nil {
		return nil, nil
	}

	req := storage.ReadRequest{
		Database: cmd.database,
		RP:       cmd.rp,
		Shards:   []*tsdb.Shard{sh},
		Start:    models.MinNanoTime,
		End:      models.MaxNanoTime,
	}

	return cmd.store.Read(context.Background(), &req)
}

package main

import (
	"encoding/binary"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"text/tabwriter"

	"github.com/influxdb/influxdb/tsdb"
	_ "github.com/influxdb/influxdb/tsdb/engine"
)

func usage() {
	println(`Usage: influx_inspect <command> [options]

Displays detailed information about InfluxDB data files.
`)

	println(`Commands:
  info - displays series meta-data for all shards.  Default location [$HOME/.influxdb]
  dumptsm - dumps low-level details about tsm1 files.`)
	println()
}

func main() {

	flag.Usage = usage
	flag.Parse()

	if len(flag.Args()) == 0 {
		flag.Usage()
		os.Exit(0)
	}

	switch flag.Args()[0] {
	case "info":
		var path string
		fs := flag.NewFlagSet("info", flag.ExitOnError)
		fs.StringVar(&path, "dir", os.Getenv("HOME")+"/.influxdb", "Root storage path. [$HOME/.influxdb]")

		fs.Usage = func() {
			println("Usage: influx_inspect info [options]\n\n   Displays series meta-data for all shards..")
			println()
			println("Options:")
			fs.PrintDefaults()
		}

		if err := fs.Parse(flag.Args()[1:]); err != nil {
			fmt.Printf("%v", err)
			os.Exit(1)
		}
		cmdInfo(path)
	case "dumptsm":

		var dumpAll bool
		opts := &tsdmDumpOpts{}
		fs := flag.NewFlagSet("file", flag.ExitOnError)
		fs.BoolVar(&opts.dumpIndex, "dump-index", false, "Dump raw index data")
		fs.BoolVar(&opts.dumpBlocks, "dump-blocks", false, "Dump raw block data")
		fs.BoolVar(&dumpAll, "dump-all", false, "Dump all data. Caution: This may print a lot of information")
		fs.StringVar(&opts.filterKey, "filter-key", "", "Only display index and block data match this key substring")

		fs.Usage = func() {
			println("Usage: influx_inspect dumptsm [options] <path>\n\n  Dumps low-level details about tsm1 files.")
			println()
			println("Options:")
			fs.PrintDefaults()
			os.Exit(0)
		}

		if err := fs.Parse(flag.Args()[1:]); err != nil {
			fmt.Printf("%v", err)
			os.Exit(1)
		}

		if len(fs.Args()) == 0 || fs.Args()[0] == "" {
			fmt.Printf("TSM file not specified\n\n")
			fs.Usage()
			fs.PrintDefaults()
			os.Exit(1)
		}
		opts.path = fs.Args()[0]
		opts.dumpBlocks = opts.dumpBlocks || dumpAll || opts.filterKey != ""
		opts.dumpIndex = opts.dumpIndex || dumpAll || opts.filterKey != ""
		dumpTsm1(opts)
		return

	default:
		flag.Usage()
		os.Exit(1)
	}
}

func cmdInfo(path string) {
	tstore := tsdb.NewStore(filepath.Join(path, "data"))
	tstore.Logger = log.New(ioutil.Discard, "", log.LstdFlags)
	tstore.EngineOptions.Config.Dir = filepath.Join(path, "data")
	tstore.EngineOptions.Config.WALLoggingEnabled = false
	tstore.EngineOptions.Config.WALDir = filepath.Join(path, "wal")
	if err := tstore.Open(); err != nil {
		fmt.Printf("Failed to open dir: %v\n", err)
		os.Exit(1)
	}

	size, err := tstore.DiskSize()
	if err != nil {
		fmt.Printf("Failed to determine disk usage: %v\n", err)
	}

	// Summary stats
	fmt.Printf("Shards: %d, Indexes: %d, Databases: %d, Disk Size: %d, Series: %d\n",
		tstore.ShardN(), tstore.DatabaseIndexN(), len(tstore.Databases()), size, countSeries(tstore))
	fmt.Println()

	tw := tabwriter.NewWriter(os.Stdout, 16, 8, 0, '\t', 0)

	fmt.Fprintln(tw, strings.Join([]string{"Shard", "DB", "Measurement", "Tags [#K/#V]", "Fields [Name:Type]", "Series"}, "\t"))

	shardIDs := tstore.ShardIDs()

	databases := tstore.Databases()
	sort.Strings(databases)

	for _, db := range databases {
		index := tstore.DatabaseIndex(db)
		measurements := index.Measurements()
		sort.Sort(measurements)
		for _, m := range measurements {
			tags := m.TagKeys()
			tagValues := 0
			for _, tag := range tags {
				tagValues += len(m.TagValues(tag))
			}
			fields := m.FieldNames()
			sort.Strings(fields)
			series := m.SeriesKeys()
			sort.Strings(series)
			sort.Sort(ShardIDs(shardIDs))

			// Sample a point from each measurement to determine the field types
			for _, shardID := range shardIDs {
				shard := tstore.Shard(shardID)
				if err != nil {
					fmt.Printf("Failed to get transaction: %v", err)
				}

				codec := shard.FieldCodec(m.Name)
				for _, field := range codec.Fields() {
					ft := fmt.Sprintf("%s:%s", field.Name, field.Type)
					fmt.Fprintf(tw, "%d\t%s\t%s\t%d/%d\t%d [%s]\t%d\n", shardID, db, m.Name, len(tags), tagValues,
						len(fields), ft, len(series))

				}

			}
		}
	}
	tw.Flush()
}

func countSeries(tstore *tsdb.Store) int {
	var count int
	for _, shardID := range tstore.ShardIDs() {
		shard := tstore.Shard(shardID)
		cnt, err := shard.SeriesCount()
		if err != nil {
			fmt.Printf("series count failed: %v\n", err)
			continue
		}
		count += cnt
	}
	return count
}

func btou64(b []byte) uint64 {
	return binary.BigEndian.Uint64(b)
}

// u64tob converts a uint64 into an 8-byte slice.
func u64tob(v uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, v)
	return b
}

type ShardIDs []uint64

func (a ShardIDs) Len() int           { return len(a) }
func (a ShardIDs) Less(i, j int) bool { return a[i] < a[j] }
func (a ShardIDs) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }

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

func main() {

	var path string
	flag.StringVar(&path, "p", os.Getenv("HOME")+"/.influxdb", "Root storage path. [$HOME/.influxdb]")
	flag.Parse()

	tstore := tsdb.NewStore(filepath.Join(path, "data"))
	tstore.Logger = log.New(ioutil.Discard, "", log.LstdFlags)
	tstore.EngineOptions.Config.WALEnableLogging = false
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

	fmt.Fprintln(tw, strings.Join([]string{"DB", "Measurement", "Tags [#K/#V]", "Fields [Name:Type]", "Series"}, "\t"))

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

			// Sample a point from each measurement to determine the field types
			fieldSummary := []string{}
			for _, shardID := range shardIDs {
				shard := tstore.Shard(shardID)
				tx, err := shard.ReadOnlyTx()
				if err != nil {
					fmt.Printf("Failed to get transaction: %v", err)
				}

				if len(series) > 0 {
					cursor := tx.Cursor(series[0])

					// Seek to the beginning
					_, value := cursor.Seek([]byte{})
					codec := shard.FieldCodec(m.Name)
					fields, err := codec.DecodeFieldsWithNames(value)
					if err != nil {
						fmt.Printf("Failed to decode values: %v", err)
					}

					for field, value := range fields {
						fieldSummary = append(fieldSummary, fmt.Sprintf("%s:%T", field, value))
					}
					sort.Strings(fieldSummary)
				}
				tx.Rollback()
				break
			}

			fmt.Fprintf(tw, "%s\t%s\t%d/%d\t%d [%s]\t%d\n", db, m.Name, len(tags), tagValues,
				len(fields), strings.Join(fieldSummary, ","), len(series))

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

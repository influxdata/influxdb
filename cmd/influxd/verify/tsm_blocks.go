package verify

import (
	"bytes"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"

	"github.com/influxdata/influxdb/kit/cli"
	"github.com/influxdata/influxdb/tsdb"
	"github.com/influxdata/influxdb/tsdb/cursors"
	"github.com/influxdata/influxdb/tsdb/tsm1"
	"github.com/spf13/cobra"
)

// tsmBlocksFlags defines the `tsm-blocks` Command.
var tsmBlocksFlags = struct {
	cli.OrgBucket
	path string
}{}

func newTSMBlocksCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "tsm-blocks <pathspec>...",
		Short: "Verifies consistency of TSM blocks",
		Long: `
This command will analyze a set of TSM files for inconsistencies between the
TSM index and the blocks.

The checks performed by this command are:

* CRC-32 checksums match for each block
* TSM index min and max timestamps match decoded data

OPTIONS

   <pathspec>...
      A list of files or directories to search for TSM files.

An optional organization or organization and bucket may be specified to limit
the analysis.
`,
		Run: verifyTSMBlocks,
	}

	tsmBlocksFlags.AddFlags(cmd)

	return cmd
}

func verifyTSMBlocks(cmd *cobra.Command, args []string) {
	for _, arg := range args {
		fi, err := os.Stat(arg)
		if err != nil {
			fmt.Printf("Error processing path %q: %v", arg, err)
			continue
		}

		var files []string
		if fi.IsDir() {
			files, _ = filepath.Glob(filepath.Join(arg, "*."+tsm1.TSMFileExtension))
		} else {
			files = append(files, arg)
		}
		for _, path := range files {
			if err := processFile(path); err != nil {
				fmt.Printf("Error processing file %q: %v", path, err)
			}
		}
	}
}

func processFile(path string) error {
	fmt.Println("processing file: " + path)

	file, err := os.OpenFile(path, os.O_RDONLY, 0600)
	if err != nil {
		return fmt.Errorf("OpenFile: %v", err)
	}

	reader, err := tsm1.NewTSMReader(file)
	if err != nil {
		return fmt.Errorf("failed to create TSM reader for %q: %v", path, err)
	}
	defer reader.Close()

	org, bucket := tsmBlocksFlags.OrgBucketID()
	var start []byte
	if org.Valid() {
		if bucket.Valid() {
			v := tsdb.EncodeName(org, bucket)
			start = v[:]
		} else {
			v := tsdb.EncodeOrgName(org)
			start = v[:]
		}
	}

	var ts cursors.TimestampArray
	count := 0
	totalErrors := 0
	iter := reader.Iterator(start)
	for iter.Next() {
		key := iter.Key()
		if len(start) > 0 && (len(key) < len(start) || !bytes.Equal(key[:len(start)], start)) {
			break
		}

		entries := iter.Entries()
		for i := range entries {
			entry := &entries[i]

			checksum, buf, err := reader.ReadBytes(entry, nil)
			if err != nil {
				fmt.Printf("could not read block %d due to error: %q\n", count, err)
				count++
				continue
			}

			if expected := crc32.ChecksumIEEE(buf); checksum != expected {
				totalErrors++
				fmt.Printf("unexpected checksum %d, expected %d for key %v, block %d\n", checksum, expected, key, count)
			}

			if err = tsm1.DecodeTimestampArrayBlock(buf, &ts); err != nil {
				totalErrors++
				fmt.Printf("unable to decode timestamps for block %d: %q\n", count, err)
			}

			if got, exp := entry.MinTime, ts.MinTime(); got != exp {
				totalErrors++
				fmt.Printf("unexpected min time %d, expected %d for block %d: %q\n", got, exp, count, err)
			}
			if got, exp := entry.MaxTime, ts.MaxTime(); got != exp {
				totalErrors++
				fmt.Printf("unexpected max time %d, expected %d for block %d: %q\n", got, exp, count, err)
			}

			count++
		}
	}

	fmt.Printf("Completed checking %d block(s)\n", count)

	return nil
}

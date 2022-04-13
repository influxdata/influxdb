package dump_tsm

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/influxdata/influxdb/v2/tsdb/engine/tsm1"
	"github.com/spf13/cobra"
	"golang.org/x/text/cases"
	"golang.org/x/text/language"
)

type args struct {
	dumpIndex  bool
	dumpBlocks bool
	dumpAll    bool
	filterKey  string
	path       string
}

func NewDumpTSMCommand() *cobra.Command {
	var arguments args
	cmd := &cobra.Command{
		Use:   "dump-tsm",
		Short: "Dumps low-level details about tsm1 files",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			if arguments.path == "" {
				cmd.PrintErrf("TSM File not specified\n")
				return nil
			}
			arguments.dumpBlocks = arguments.dumpBlocks || arguments.dumpAll
			arguments.dumpIndex = arguments.dumpIndex || arguments.dumpAll
			return dumpTSM(cmd, arguments)
		},
	}

	cmd.Flags().StringVar(&arguments.path, "file-path", "",
		"Path to TSM file")
	cmd.Flags().BoolVar(&arguments.dumpIndex, "index", false,
		"Dump raw index data")
	cmd.Flags().BoolVar(&arguments.dumpBlocks, "blocks", false,
		"Dump raw block data")
	cmd.Flags().BoolVar(&arguments.dumpAll, "all", false,
		"Dump all data. Caution: This may print a lot of information")
	cmd.Flags().StringVar(&arguments.filterKey, "filter-key", "",
		"Only display data matching this key substring")

	return cmd
}

func dumpTSM(cmd *cobra.Command, args args) error {
	f, err := os.Open(args.path)
	if err != nil {
		return err
	}
	defer f.Close()

	// Get the file size
	stat, err := f.Stat()
	if err != nil {
		return err
	}

	if stat.IsDir() {
		return fmt.Errorf("%s is a directory, must be a TSM file", args.path)
	}

	if filepath.Ext(args.path) != "."+tsm1.TSMFileExtension {
		return fmt.Errorf("%s is not a TSM file", args.path)
	}

	r, err := tsm1.NewTSMReader(f)
	if err != nil {
		return fmt.Errorf("error opening TSM file: %w", err)
	}
	defer r.Close()

	minTime, maxTime := r.TimeRange()
	keyCount := r.KeyCount()

	blockStats := &blockStats{}

	cmd.Println("Summary:")
	cmd.Printf("  File: %s\n", args.path)
	cmd.Printf("  Time Range: %s - %s\n",
		time.Unix(0, minTime).UTC().Format(time.RFC3339Nano),
		time.Unix(0, maxTime).UTC().Format(time.RFC3339Nano),
	)
	cmd.Printf("  Duration: %s ", time.Unix(0, maxTime).Sub(time.Unix(0, minTime)))
	cmd.Printf("  Series: %d ", keyCount)
	cmd.Printf("  File Size: %d\n\n", stat.Size())

	tw := tabwriter.NewWriter(cmd.OutOrStdout(), 8, 8, 1, '\t', 0)

	if args.dumpIndex {
		dumpIndex(cmd, args, dumpIndexParams{
			tw:       tw,
			minTime:  minTime,
			maxTime:  maxTime,
			keyCount: keyCount,
			r:        r,
		})
	}

	if args.dumpBlocks {
		tw = tabwriter.NewWriter(cmd.OutOrStdout(), 8, 8, 1, '\t', 0)
		fmt.Fprintln(tw, "  "+strings.Join([]string{"Blk", "Chk", "Ofs", "Len", "Type", "Min Time", "Points", "Enc [T/V]", "Len [T/V]"}, "\t"))
	}

	indexSize := r.IndexSize()
	blockCount, pointCount, blockSize, err := dumpBlocks(cmd, dumpBlocksParams{
		tw:         tw,
		keyCount:   keyCount,
		filterKey:  args.filterKey,
		dumpBlocks: args.dumpBlocks,
		blockStats: blockStats,
		f:          f,
		r:          r,
	})
	if err != nil {
		return fmt.Errorf("failed to decode block in tsm1 file: %w", err)
	}

	// Flush the printer to display all block details
	if args.dumpBlocks {
		cmd.Println("Blocks:")
		tw.Flush()
		cmd.Println()
	}

	// Always print summary statistics about the blocks
	var blockSizeAvg int64
	if blockCount > 0 {
		blockSizeAvg = blockSize / blockCount
	}
	cmd.Printf("Statistics\n")
	cmd.Printf("  Blocks:\n")
	cmd.Printf("    Total: %d Size: %d Min: %d Max: %d Avg: %d\n",
		blockCount, blockSize, blockStats.min, blockStats.max, blockSizeAvg)
	cmd.Printf("  Index:\n")
	cmd.Printf("    Total: %d Size: %d\n", blockCount, indexSize)
	cmd.Printf("  Points:\n")
	cmd.Printf("    Total: %d\n", pointCount)

	cmd.Println("  Encoding:")
	for i, counts := range blockStats.counts {
		if len(counts) == 0 {
			continue
		}
		cmd.Printf("    %s: ", cases.Title(language.Und).String(fieldType[i]))
		for j, v := range counts {
			cmd.Printf("\t%s: %d (%d%%) ", encDescs[i][j], v, int(float64(v)/float64(blockCount)*100))
		}
		cmd.Println()
	}
	cmd.Printf("  Compression:\n")
	cmd.Printf("    Per block: %0.2f bytes/point\n", float64(blockSize)/float64(pointCount))
	cmd.Printf("    Total: %0.2f bytes/point\n", float64(stat.Size())/float64(pointCount))

	return nil
}

type dumpIndexParams struct {
	tw       *tabwriter.Writer
	minTime  int64
	maxTime  int64
	keyCount int
	r        *tsm1.TSMReader
}

func dumpIndex(cmd *cobra.Command, args args, info dumpIndexParams) {
	cmd.Println("Index:")
	info.tw.Flush()
	cmd.Println()

	fmt.Fprintln(info.tw, "  "+strings.Join([]string{"Pos", "Min Time", "Max Time", "Ofs", "Size", "Key", "Field"}, "\t"))
	var pos int
	for i := 0; i < info.keyCount; i++ {
		key, _ := info.r.KeyAt(i)
		for _, e := range info.r.Entries(key) {
			pos++
			measurement, field, _ := strings.Cut(string(key), "#!~#")

			if args.filterKey != "" && !strings.Contains(string(key), args.filterKey) {
				continue
			}
			fmt.Fprintln(info.tw, "  "+strings.Join([]string{
				strconv.FormatInt(int64(pos), 10),
				time.Unix(0, e.MinTime).UTC().Format(time.RFC3339Nano),
				time.Unix(0, e.MaxTime).UTC().Format(time.RFC3339Nano),
				strconv.FormatInt(e.Offset, 10),
				strconv.FormatInt(int64(e.Size), 10),
				measurement,
				field,
			}, "\t"))
			info.tw.Flush()
		}
	}
}

type dumpBlocksParams struct {
	tw         *tabwriter.Writer
	keyCount   int
	filterKey  string
	dumpBlocks bool
	blockStats *blockStats
	f          *os.File
	r          *tsm1.TSMReader
}

func dumpBlocks(cmd *cobra.Command, params dumpBlocksParams) (int64, int64, int64, error) {
	// Starting at 5 because the magic number is 4 bytes + 1 byte version
	i := int64(5)
	b := make([]byte, 8)
	var blockCount, pointCount, blockSize int64

	// Start at the beginning and read every block
	for j := 0; j < params.keyCount; j++ {
		key, _ := params.r.KeyAt(j)
		for _, e := range params.r.Entries(key) {

			params.f.Seek(e.Offset, 0)
			params.f.Read(b[:4])

			chksum := binary.BigEndian.Uint32(b[:4])

			buf := make([]byte, e.Size-4)
			params.f.Read(buf)

			blockSize += int64(e.Size)

			if params.filterKey != "" && !strings.Contains(string(key), params.filterKey) {
				i += blockSize
				blockCount++
				continue
			}

			blockType := buf[0]

			encoded := buf[1:]

			var v []tsm1.Value
			v, err := tsm1.DecodeBlock(buf, v)
			if err != nil {
				return 0, 0, 0, err
			}
			startTime := time.Unix(0, v[0].UnixNano())

			pointCount += int64(len(v))

			// Length of the timestamp block
			tsLen, j := binary.Uvarint(encoded)

			// Unpack the timestamp bytes
			ts := encoded[j : j+int(tsLen)]

			// Unpack the value bytes
			values := encoded[j+int(tsLen):]

			tsEncoding := timeEnc[int(ts[0]>>4)]
			vEncoding := encDescs[int(blockType+1)][values[0]>>4]

			typeDesc := blockTypes[blockType]

			params.blockStats.inc(0, ts[0]>>4)
			params.blockStats.inc(int(blockType+1), values[0]>>4)
			params.blockStats.size(len(buf))

			// Add a row of block details to the printer. Doesn't actually print yet.
			if params.dumpBlocks {
				fmt.Fprintln(params.tw, "  "+strings.Join([]string{
					strconv.FormatInt(blockCount, 10),
					strconv.FormatUint(uint64(chksum), 10),
					strconv.FormatInt(i, 10),
					strconv.FormatInt(int64(len(buf)), 10),
					typeDesc,
					startTime.UTC().Format(time.RFC3339Nano),
					strconv.FormatInt(int64(len(v)), 10),
					fmt.Sprintf("%s/%s", tsEncoding, vEncoding),
					fmt.Sprintf("%d/%d", len(ts), len(values)),
				}, "\t"))
			}

			i += blockSize
			blockCount++
		}
	}
	return blockCount, pointCount, blockSize, nil
}

var (
	fieldType = []string{
		"timestamp", "float", "int", "bool", "string", "unsigned",
	}
	blockTypes = []string{
		"float64", "int64", "bool", "string", "unsigned",
	}
	timeEnc = []string{
		"none", "s8b", "rle",
	}
	floatEnc = []string{
		"none", "gor",
	}
	intEnc = []string{
		"none", "s8b", "rle",
	}
	boolEnc = []string{
		"none", "bp",
	}
	stringEnc = []string{
		"none", "snpy",
	}
	unsignedEnc = []string{
		"none", "s8b", "rle",
	}
	encDescs = [][]string{
		timeEnc, floatEnc, intEnc, boolEnc, stringEnc, unsignedEnc,
	}
)

type blockStats struct {
	min, max int
	counts   [][]int
}

func (b *blockStats) inc(typ int, enc byte) {
	for len(b.counts) <= typ {
		b.counts = append(b.counts, []int{})
	}
	for len(b.counts[typ]) <= int(enc) {
		b.counts[typ] = append(b.counts[typ], 0)
	}
	b.counts[typ][enc]++
}

func (b *blockStats) size(sz int) {
	if b.min == 0 || sz < b.min {
		b.min = sz
	}
	if b.min == 0 || sz > b.max {
		b.max = sz
	}
}

package main

import (
	"encoding/binary"
	"fmt"
	"os"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/influxdata/influxdb/tsdb/engine/tsm1"
)

type tsdmDumpOpts struct {
	dumpIndex  bool
	dumpBlocks bool
	filterKey  string
	path       string
}

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

var (
	fieldType = []string{
		"timestamp", "float", "int", "bool", "string",
	}
	blockTypes = []string{
		"float64", "int64", "bool", "string",
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
	encDescs = [][]string{
		timeEnc, floatEnc, intEnc, boolEnc, stringEnc,
	}
)

func cmdDumpTsm1(opts *tsdmDumpOpts) {
	var errors []error

	f, err := os.Open(opts.path)
	if err != nil {
		println(err.Error())
		os.Exit(1)
	}

	// Get the file size
	stat, err := f.Stat()
	if err != nil {
		println(err.Error())
		os.Exit(1)
	}
	b := make([]byte, 8)

	r, err := tsm1.NewTSMReader(f)
	if err != nil {
		println("Error opening TSM files: ", err.Error())
		os.Exit(1)
	}
	defer r.Close()

	minTime, maxTime := r.TimeRange()
	keyCount := r.KeyCount()

	blockStats := &blockStats{}

	println("Summary:")
	fmt.Printf("  File: %s\n", opts.path)
	fmt.Printf("  Time Range: %s - %s\n",
		time.Unix(0, minTime).UTC().Format(time.RFC3339Nano),
		time.Unix(0, maxTime).UTC().Format(time.RFC3339Nano),
	)
	fmt.Printf("  Duration: %s ", time.Unix(0, maxTime).Sub(time.Unix(0, minTime)))
	fmt.Printf("  Series: %d ", keyCount)
	fmt.Printf("  File Size: %d\n", stat.Size())
	println()

	tw := tabwriter.NewWriter(os.Stdout, 8, 8, 1, '\t', 0)
	fmt.Fprintln(tw, "  "+strings.Join([]string{"Pos", "Min Time", "Max Time", "Ofs", "Size", "Key", "Field"}, "\t"))
	var pos int
	for i := 0; i < keyCount; i++ {
		key, _ := r.KeyAt(i)
		for _, e := range r.Entries(key) {
			pos++
			split := strings.Split(key, "#!~#")

			// We dont' know know if we have fields so use an informative default
			var measurement, field string = "UNKNOWN", "UNKNOWN"

			// Possible corruption? Try to read as much as we can and point to the problem.
			measurement = split[0]
			field = split[1]

			if opts.filterKey != "" && !strings.Contains(key, opts.filterKey) {
				continue
			}
			fmt.Fprintln(tw, "  "+strings.Join([]string{
				strconv.FormatInt(int64(pos), 10),
				time.Unix(0, e.MinTime).UTC().Format(time.RFC3339Nano),
				time.Unix(0, e.MaxTime).UTC().Format(time.RFC3339Nano),
				strconv.FormatInt(int64(e.Offset), 10),
				strconv.FormatInt(int64(e.Size), 10),
				measurement,
				field,
			}, "\t"))
		}
	}

	if opts.dumpIndex {
		println("Index:")
		tw.Flush()
		println()
	}

	tw = tabwriter.NewWriter(os.Stdout, 8, 8, 1, '\t', 0)
	fmt.Fprintln(tw, "  "+strings.Join([]string{"Blk", "Chk", "Ofs", "Len", "Type", "Min Time", "Points", "Enc [T/V]", "Len [T/V]"}, "\t"))

	// Starting at 5 because the magic number is 4 bytes + 1 byte version
	i := int64(5)
	var blockCount, pointCount, blockSize int64
	indexSize := r.IndexSize()

	// Start at the beginning and read every block
	for j := 0; j < keyCount; j++ {
		key, _ := r.KeyAt(j)
		for _, e := range r.Entries(key) {

			f.Seek(int64(e.Offset), 0)
			f.Read(b[:4])

			chksum := binary.BigEndian.Uint32(b[:4])

			buf := make([]byte, e.Size-4)
			f.Read(buf)

			blockSize += int64(e.Size)

			blockType := buf[0]

			encoded := buf[1:]

			var v []tsm1.Value
			v, err := tsm1.DecodeBlock(buf, v)
			if err != nil {
				fmt.Printf("error: %v\n", err.Error())
				os.Exit(1)
			}
			startTime := time.Unix(0, v[0].UnixNano())

			pointCount += int64(len(v))

			// Length of the timestamp block
			tsLen, j := binary.Uvarint(encoded)

			// Unpack the timestamp bytes
			ts := encoded[int(j) : int(j)+int(tsLen)]

			// Unpack the value bytes
			values := encoded[int(j)+int(tsLen):]

			tsEncoding := timeEnc[int(ts[0]>>4)]
			vEncoding := encDescs[int(blockType+1)][values[0]>>4]

			typeDesc := blockTypes[blockType]

			blockStats.inc(0, ts[0]>>4)
			blockStats.inc(int(blockType+1), values[0]>>4)
			blockStats.size(len(buf))

			if opts.filterKey != "" && !strings.Contains(key, opts.filterKey) {
				i += blockSize
				blockCount++
				continue
			}

			fmt.Fprintln(tw, "  "+strings.Join([]string{
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

			i += blockSize
			blockCount++
		}
	}

	if opts.dumpBlocks {
		println("Blocks:")
		tw.Flush()
		println()
	}

	var blockSizeAvg int64
	if blockCount > 0 {
		blockSizeAvg = blockSize / blockCount
	}
	fmt.Printf("Statistics\n")
	fmt.Printf("  Blocks:\n")
	fmt.Printf("    Total: %d Size: %d Min: %d Max: %d Avg: %d\n",
		blockCount, blockSize, blockStats.min, blockStats.max, blockSizeAvg)
	fmt.Printf("  Index:\n")
	fmt.Printf("    Total: %d Size: %d\n", blockCount, indexSize)
	fmt.Printf("  Points:\n")
	fmt.Printf("    Total: %d", pointCount)
	println()

	println("  Encoding:")
	for i, counts := range blockStats.counts {
		if len(counts) == 0 {
			continue
		}
		fmt.Printf("    %s: ", strings.Title(fieldType[i]))
		for j, v := range counts {
			fmt.Printf("\t%s: %d (%d%%) ", encDescs[i][j], v, int(float64(v)/float64(blockCount)*100))
		}
		println()
	}
	fmt.Printf("  Compression:\n")
	fmt.Printf("    Per block: %0.2f bytes/point\n", float64(blockSize)/float64(pointCount))
	fmt.Printf("    Total: %0.2f bytes/point\n", float64(stat.Size())/float64(pointCount))

	if len(errors) > 0 {
		println()
		fmt.Printf("Errors (%d):\n", len(errors))
		for _, err := range errors {
			fmt.Printf("  * %v\n", err)
		}
		println()
	}
}

package main

import (
	"flag"
	"fmt"
	"os"

	_ "github.com/influxdata/influxdb/tsdb/engine"
)

func usage() {
	println(`Usage: influx_inspect <command> [options]

Displays detailed information about InfluxDB data files.
`)

	println(`Commands:
  dumptsm - dumps low-level details about tsm1 files.
  export - exports a tsm file to line protocol
  report - displays a shard level report`)
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
	case "report":
		opts := &reportOpts{}
		fs := flag.NewFlagSet("report", flag.ExitOnError)
		fs.StringVar(&opts.pattern, "pattern", "", "Include only files matching a pattern")
		fs.BoolVar(&opts.detailed, "detailed", false, "Report detailed cardinality estimates")

		fs.Usage = func() {
			println("Usage: influx_inspect report [options]\n\n   Displays shard level report")
			println()
			println("Options:")
			fs.PrintDefaults()
		}

		if err := fs.Parse(flag.Args()[1:]); err != nil {
			fmt.Printf("%v", err)
			os.Exit(1)
		}
		opts.dir = fs.Arg(0)
		cmdReport(opts)
	case "dumptsmdev":
		fmt.Fprintf(os.Stderr, "warning: dumptsmdev is deprecated, use dumptsm instead.\n")
		fallthrough
	case "dumptsm":
		var dumpAll bool
		opts := &tsdmDumpOpts{}
		fs := flag.NewFlagSet("file", flag.ExitOnError)
		fs.BoolVar(&opts.dumpIndex, "index", false, "Dump raw index data")
		fs.BoolVar(&opts.dumpBlocks, "blocks", false, "Dump raw block data")
		fs.BoolVar(&dumpAll, "all", false, "Dump all data. Caution: This may print a lot of information")
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
		cmdDumpTsm1(opts)
	case "verify":
		var path string
		fs := flag.NewFlagSet("verify", flag.ExitOnError)
		fs.StringVar(&path, "dir", os.Getenv("HOME")+"/.influxdb", "Root storage path. [$HOME/.influxdb]")

		fs.Usage = func() {
			println("Usage: influx_inspect verify [options]\n\n   verifies the the checksum of shards")
			println()
			println("Options:")
			fs.PrintDefaults()
		}

		if err := fs.Parse(flag.Args()[1:]); err != nil {
			fmt.Printf("%v", err)
			os.Exit(1)
		}
		cmdVerify(path)
	case "export":
		var path, out, db, rp string
		var compress bool
		fs := flag.NewFlagSet("export", flag.ExitOnError)
		fs.StringVar(&path, "dir", os.Getenv("HOME")+"/.influxdb", "Root storage path. [$HOME/.influxdb]")
		fs.StringVar(&out, "out", os.Getenv("HOME")+"/.influxdb/export", "Destination file to export to")
		fs.StringVar(&db, "db", "", "Optional: the database to export")
		fs.StringVar(&rp, "rp", "", "Optional: the retention policy to export (requires db parameter to be specified)")
		fs.BoolVar(&compress, "compress", false, "Compress the output")

		fs.Usage = func() {
			println("Usage: influx_inspect export [options]\n\n   exports TSM files into InfluxDB line protocol format")
			println()
			println("Options:")
			fs.PrintDefaults()
		}

		if err := fs.Parse(flag.Args()[1:]); err != nil {
			fmt.Printf("%v", err)
			os.Exit(1)
		}
		c := newCmdExport(path, out, db, rp, compress)
		if err := c.run(); err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
	default:
		flag.Usage()
		os.Exit(1)
	}
}

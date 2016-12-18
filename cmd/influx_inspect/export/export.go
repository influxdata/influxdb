package export

import (
	"compress/gzip"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/influxdata/influxdb/influxql"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/pkg/escape"
	"github.com/influxdata/influxdb/tsdb/engine/tsm1"
)

// Command represents the program execution for "influx_inspect export".
type Command struct {
	// Standard input/output, overridden for testing.
	Stderr io.Writer
	Stdout io.Writer

	dataDir         string
	walDir          string
	out             string
	database        string
	retentionPolicy string
	startTime       int64
	endTime         int64
	compress        bool

	manifest map[string]struct{}
	tsmFiles map[string][]string
	walFiles map[string][]string
}

// NewCommand returns a new instance of Command.
func NewCommand() *Command {
	return &Command{
		Stderr: os.Stderr,
		Stdout: os.Stdout,

		manifest: make(map[string]struct{}),
		tsmFiles: make(map[string][]string),
		walFiles: make(map[string][]string),
	}
}

// Run executes the command.
func (cmd *Command) Run(args ...string) error {
	var start, end string
	fs := flag.NewFlagSet("export", flag.ExitOnError)
	fs.StringVar(&cmd.dataDir, "datadir", os.Getenv("HOME")+"/.influxdb/data", "Data storage path")
	fs.StringVar(&cmd.walDir, "waldir", os.Getenv("HOME")+"/.influxdb/wal", "WAL storage path")
	fs.StringVar(&cmd.out, "out", os.Getenv("HOME")+"/.influxdb/export", "Destination file to export to")
	fs.StringVar(&cmd.database, "database", "", "Optional: the database to export")
	fs.StringVar(&cmd.retentionPolicy, "retention", "", "Optional: the retention policy to export (requires -database)")
	fs.StringVar(&start, "start", "", "Optional: the start time to export (RFC3339 format)")
	fs.StringVar(&end, "end", "", "Optional: the end time to export (RFC3339 format)")
	fs.BoolVar(&cmd.compress, "compress", false, "Compress the output")

	fs.SetOutput(cmd.Stdout)
	fs.Usage = func() {
		fmt.Fprintf(cmd.Stdout, "Exports TSM files into InfluxDB line protocol format.\n\n")
		fmt.Fprintf(cmd.Stdout, "Usage: %s export [flags]\n\n", filepath.Base(os.Args[0]))
		fs.PrintDefaults()
	}

	if err := fs.Parse(args); err != nil {
		return err
	}

	// set defaults
	if start != "" {
		s, err := time.Parse(time.RFC3339, start)
		if err != nil {
			return err
		}
		cmd.startTime = s.UnixNano()
	} else {
		cmd.startTime = math.MinInt64
	}
	if end != "" {
		e, err := time.Parse(time.RFC3339, end)
		if err != nil {
			return err
		}
		cmd.endTime = e.UnixNano()
	} else {
		// set end time to max if it is not set.
		cmd.endTime = math.MaxInt64
	}

	if err := cmd.validate(); err != nil {
		return err
	}

	return cmd.export()
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

func (cmd *Command) export() error {
	if err := cmd.walkTSMFiles(); err != nil {
		return err
	}
	if err := cmd.walkWALFiles(); err != nil {
		return err
	}
	return cmd.write()
}

func (cmd *Command) walkTSMFiles() error {
	return filepath.Walk(cmd.dataDir, func(path string, f os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		// check to see if this is a tsm file
		if filepath.Ext(path) != "."+tsm1.TSMFileExtension {
			return nil
		}

		relPath, err := filepath.Rel(cmd.dataDir, path)
		if err != nil {
			return err
		}
		dirs := strings.Split(relPath, string(byte(os.PathSeparator)))
		if len(dirs) < 2 {
			return fmt.Errorf("invalid directory structure for %s", path)
		}
		if dirs[0] == cmd.database || cmd.database == "" {
			if dirs[1] == cmd.retentionPolicy || cmd.retentionPolicy == "" {
				key := filepath.Join(dirs[0], dirs[1])
				cmd.manifest[key] = struct{}{}
				cmd.tsmFiles[key] = append(cmd.tsmFiles[key], path)
			}
		}
		return nil
	})
}

func (cmd *Command) walkWALFiles() error {
	return filepath.Walk(cmd.walDir, func(path string, f os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		// check to see if this is a wal file
		fileName := filepath.Base(path)
		if filepath.Ext(path) != "."+tsm1.WALFileExtension || !strings.HasPrefix(fileName, tsm1.WALFilePrefix) {
			return nil
		}

		relPath, err := filepath.Rel(cmd.walDir, path)
		if err != nil {
			return err
		}
		dirs := strings.Split(relPath, string(byte(os.PathSeparator)))
		if len(dirs) < 2 {
			return fmt.Errorf("invalid directory structure for %s", path)
		}
		if dirs[0] == cmd.database || cmd.database == "" {
			if dirs[1] == cmd.retentionPolicy || cmd.retentionPolicy == "" {
				key := filepath.Join(dirs[0], dirs[1])
				cmd.manifest[key] = struct{}{}
				cmd.walFiles[key] = append(cmd.walFiles[key], path)
			}
		}
		return nil
	})
}

func (cmd *Command) write() error {
	// open our output file and create an output buffer
	var w io.WriteCloser
	w, err := os.Create(cmd.out)
	if err != nil {
		return err
	}
	defer w.Close()
	if cmd.compress {
		w = gzip.NewWriter(w)
		defer w.Close()
	}

	s, e := time.Unix(0, cmd.startTime).Format(time.RFC3339), time.Unix(0, cmd.endTime).Format(time.RFC3339)
	fmt.Fprintf(w, "# INFLUXDB EXPORT: %s - %s\n", s, e)

	// Write out all the DDL
	fmt.Fprintln(w, "# DDL")
	for key := range cmd.manifest {
		keys := strings.Split(key, string(os.PathSeparator))
		db, rp := influxql.QuoteIdent(keys[0]), influxql.QuoteIdent(keys[1])
		fmt.Fprintf(w, "CREATE DATABASE %s WITH NAME %s\n", db, rp)
	}

	fmt.Fprintln(w, "# DML")
	for key := range cmd.manifest {
		keys := strings.Split(key, string(os.PathSeparator))
		fmt.Fprintf(w, "# CONTEXT-DATABASE:%s\n", keys[0])
		fmt.Fprintf(w, "# CONTEXT-RETENTION-POLICY:%s\n", keys[1])
		if files, ok := cmd.tsmFiles[key]; ok {
			fmt.Fprintf(cmd.Stdout, "writing out tsm file data for %s...", key)
			if err := cmd.writeTsmFiles(w, files); err != nil {
				return err
			}
			fmt.Fprintln(cmd.Stdout, "complete.")
		}
		if _, ok := cmd.walFiles[key]; ok {
			fmt.Fprintf(cmd.Stdout, "writing out wal file data for %s...", key)
			if err := cmd.writeWALFiles(w, cmd.walFiles[key], key); err != nil {
				return err
			}
			fmt.Fprintln(cmd.Stdout, "complete.")
		}
	}
	return nil
}

func (cmd *Command) writeTsmFiles(w io.Writer, files []string) error {
	fmt.Fprintln(w, "# writing tsm data")

	// we need to make sure we write the same order that the files were written
	sort.Strings(files)

	for _, f := range files {
		if err := cmd.exportTSMFile(f, w); err != nil {
			return err
		}
	}

	return nil
}

func (cmd *Command) exportTSMFile(tsmFilePath string, w io.Writer) error {
	f, err := os.Open(tsmFilePath)
	if err != nil {
		return err
	}
	defer f.Close()

	r, err := tsm1.NewTSMReader(f)
	if err != nil {
		log.Printf("unable to read %s, skipping: %s\n", f, err.Error())
		return nil
	}
	defer r.Close()

	if sgStart, sgEnd := r.TimeRange(); sgStart > cmd.endTime || sgEnd < cmd.startTime {
		return nil
	}

	for i := 0; i < r.KeyCount(); i++ {
		var pairs string
		key, typ := r.KeyAt(i)
		values, _ := r.ReadAll(string(key))
		measurement, field := tsm1.SeriesAndFieldFromCompositeKey(key)
		// measurements are stored escaped, field names are not
		field = escape.String(field)

		for _, value := range values {
			if (value.UnixNano() < cmd.startTime) || (value.UnixNano() > cmd.endTime) {
				continue
			}

			switch typ {
			case tsm1.BlockFloat64:
				pairs = field + "=" + fmt.Sprintf("%v", value.Value())
			case tsm1.BlockInteger:
				pairs = field + "=" + fmt.Sprintf("%vi", value.Value())
			case tsm1.BlockBoolean:
				pairs = field + "=" + fmt.Sprintf("%v", value.Value())
			case tsm1.BlockString:
				pairs = field + "=" + fmt.Sprintf("%q", models.EscapeStringField(fmt.Sprintf("%s", value.Value())))
			default:
				pairs = field + "=" + fmt.Sprintf("%v", value.Value())
			}

			fmt.Fprintln(w, string(measurement), pairs, value.UnixNano())
		}
	}
	return nil
}

func (cmd *Command) writeWALFiles(w io.Writer, files []string, key string) error {
	fmt.Fprintln(w, "# writing wal data")

	// we need to make sure we write the same order that the wal received the data
	sort.Strings(files)

	var once sync.Once
	warnDelete := func() {
		once.Do(func() {
			msg := fmt.Sprintf(`WARNING: detected deletes in wal file.
Some series for %q may be brought back by replaying this data.
To resolve, you can either let the shard snapshot prior to exporting the data
or manually editing the exported file.
			`, key)
			fmt.Fprintln(cmd.Stderr, msg)
		})
	}

	for _, f := range files {
		if err := cmd.exportWALFile(f, w, warnDelete); err != nil {
			return err
		}
	}

	return nil
}

// exportWAL reads every WAL entry from r and exports it to w.
func (cmd *Command) exportWALFile(walFilePath string, w io.Writer, warnDelete func()) error {
	f, err := os.Open(walFilePath)
	if err != nil {
		return err
	}
	defer f.Close()

	r := tsm1.NewWALSegmentReader(f)
	defer r.Close()

	for r.Next() {
		entry, err := r.Read()
		if err != nil {
			n := r.Count()
			fmt.Fprintf(cmd.Stderr, "file %s corrupt at position %d", walFilePath, n)
			break
		}

		switch t := entry.(type) {
		case *tsm1.DeleteWALEntry, *tsm1.DeleteRangeWALEntry:
			warnDelete()
			continue
		case *tsm1.WriteWALEntry:
			var pairs string

			for key, values := range t.Values {
				measurement, field := tsm1.SeriesAndFieldFromCompositeKey([]byte(key))
				// measurements are stored escaped, field names are not
				field = escape.String(field)

				for _, value := range values {
					if (value.UnixNano() < cmd.startTime) || (value.UnixNano() > cmd.endTime) {
						continue
					}

					switch value.Value().(type) {
					case float64:
						pairs = field + "=" + fmt.Sprintf("%v", value.Value())
					case int64:
						pairs = field + "=" + fmt.Sprintf("%vi", value.Value())
					case bool:
						pairs = field + "=" + fmt.Sprintf("%v", value.Value())
					case string:
						pairs = field + "=" + fmt.Sprintf("%q", models.EscapeStringField(fmt.Sprintf("%s", value.Value())))
					default:
						pairs = field + "=" + fmt.Sprintf("%v", value.Value())
					}
					fmt.Fprintln(w, string(measurement), pairs, value.UnixNano())
				}
			}
		}
	}
	return nil
}

// Package export exports TSM files into InfluxDB line protocol format.
package export

import (
	"bufio"
	"compress/gzip"
	"flag"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/pkg/escape"
	"github.com/influxdata/influxdb/tsdb/engine/tsm1"
	"github.com/influxdata/influxql"
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
	measurement     string
	startTime       int64
	endTime         int64
	compress        bool
	lponly          bool
	parquet         bool
	pqChunkSize     int

	manifest map[string]struct{}
	tsmFiles map[string][]string
	walFiles map[string][]string

	writeValues func(io.Writer, []byte, string, []tsm1.Value) error
	exportDone  func(string) error
}

const stdoutMark = "-"

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

// Are we writing to standard out?
func (cmd *Command) usingStdOut() bool {
	return cmd.out == stdoutMark
}

// Run executes the command.
func (cmd *Command) Run(args ...string) error {
	var start, end string
	fs := flag.NewFlagSet("export", flag.ExitOnError)
	fs.StringVar(&cmd.dataDir, "datadir", os.Getenv("HOME")+"/.influxdb/data", "Data storage path")
	fs.StringVar(&cmd.walDir, "waldir", os.Getenv("HOME")+"/.influxdb/wal", "WAL storage path")
	fs.StringVar(&cmd.out, "out", os.Getenv("HOME")+"/.influxdb/export", "'-' for standard out or the destination file to export to")
	fs.StringVar(&cmd.database, "database", "", "Optional: the database to export")
	fs.StringVar(&cmd.retentionPolicy, "retention", "", "Optional: the retention policy to export (requires -database)")
	fs.StringVar(&cmd.measurement, "measurement", "", "Name of measurement to export")
	fs.StringVar(&start, "start", "", "Optional: the start time to export (RFC3339 format)")
	fs.StringVar(&end, "end", "", "Optional: the end time to export (RFC3339 format)")
	fs.BoolVar(&cmd.lponly, "lponly", false, "Only export line protocol")
	fs.BoolVar(&cmd.compress, "compress", false, "Compress the output")
	fs.BoolVar(&cmd.parquet, "parquet", false, "Export to Parquet format (requires -database -retention -measurement)")
	fs.IntVar(&cmd.pqChunkSize, "chunk-size", 100_000_000, "Size to partition Parquet files (in bytes)")

	fs.SetOutput(cmd.Stdout)
	fs.Usage = func() {
		fmt.Fprintf(cmd.Stdout, "Exports TSM files into InfluxDB line protocol or Parquet format.\n\n")
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

	if cmd.parquet {
		cmd.writeValues = cmd.writeValuesParquet
		cmd.exportDone = cmd.exportDoneParquet
	} else {
		cmd.writeValues = cmd.writeValuesLp
		cmd.exportDone = func(_ string) error { return nil }
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
	if cmd.parquet && (cmd.database == "" || cmd.retentionPolicy == "" || cmd.measurement == "") {
		return fmt.Errorf("must specify database, renetion and measurement when exporting to Parquet")
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

	if cmd.parquet {
		return cmd.writeDML(io.Discard, io.Discard)
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

func (cmd *Command) writeDDL(mw io.Writer, w io.Writer) error {
	// Write out all the DDL
	fmt.Fprintln(mw, "# DDL")
	manifest := make(map[string][]string)
	for key := range cmd.manifest {
		keys := strings.Split(key, string(os.PathSeparator))
		db, rp := influxql.QuoteIdent(keys[0]), influxql.QuoteIdent(keys[1])
		manifest[db] = append(manifest[db], rp)
	}
	for db, rps := range manifest {
		if len(rps) > 1 {
			fmt.Fprintf(w, "CREATE DATABASE %s WITH NAME autogen\n", db)
			for _, rp := range rps {
				if rp != "autogen" {
					fmt.Fprintf(w, "CREATE RETENTION POLICY %s ON %s DURATION 0s REPLICATION 1\n", rp, db)
				}
			}
		} else {
			fmt.Fprintf(w, "CREATE DATABASE %s WITH NAME %s\n", db, rps[0])
		}
	}

	return nil
}

func (cmd *Command) writeDML(mw io.Writer, w io.Writer) error {
	fmt.Fprintln(mw, "# DML")
	var msgOut io.Writer
	if cmd.usingStdOut() {
		msgOut = cmd.Stderr
	} else {
		msgOut = cmd.Stdout
	}
	for key := range cmd.manifest {
		keys := strings.Split(key, string(os.PathSeparator))
		fmt.Fprintf(mw, "# CONTEXT-DATABASE:%s\n", keys[0])
		fmt.Fprintf(mw, "# CONTEXT-RETENTION-POLICY:%s\n", keys[1])
		if files, ok := cmd.tsmFiles[key]; ok {
			fmt.Fprintf(msgOut, "writing out tsm file data for %s...", key)
			if err := cmd.writeTsmFiles(mw, w, files); err != nil {
				return err
			}
			fmt.Fprintln(msgOut, "complete.")
		}
		if _, ok := cmd.walFiles[key]; ok {
			fmt.Fprintf(msgOut, "writing out wal file data for %s...", key)
			if err := cmd.writeWALFiles(mw, w, cmd.walFiles[key], key); err != nil {
				return err
			}
			fmt.Fprintln(msgOut, "complete.")
		}
	}

	return nil
}

// writeFull writes the full DML and DDL to the supplied io.Writers.  mw is the
// "meta" writer where comments and other informational writes go and w is for
// the actual payload of the writes -- DML and DDL.
//
// Typically mw and w are the same but if we'd like to, for example, filter out
// comments and other meta data, we can pass io.Discard to mw to only
// include the raw data that writeFull() generates.
func (cmd *Command) writeFull(mw io.Writer, w io.Writer) error {
	s, e := time.Unix(0, cmd.startTime).Format(time.RFC3339), time.Unix(0, cmd.endTime).Format(time.RFC3339)

	fmt.Fprintf(mw, "# INFLUXDB EXPORT: %s - %s\n", s, e)

	if shouldWriteDDL := !cmd.lponly; shouldWriteDDL {
		if err := cmd.writeDDL(mw, w); err != nil {
			return err
		}
	}

	if err := cmd.writeDML(mw, w); err != nil {
		return err
	}

	return nil
}

func (cmd *Command) write() error {
	var w io.Writer
	if cmd.usingStdOut() {
		w = cmd.Stdout
	} else {
		// open our output file and create an output buffer
		f, err := os.Create(cmd.out)
		if err != nil {
			return err
		}
		defer f.Close()
		w = f
	}
	// Because calling (*os.File).Write is relatively expensive,
	// and we don't *need* to sync to disk on every written line of export,
	// use a sized buffered writer so that we only sync the file every megabyte.
	bw := bufio.NewWriterSize(w, 1024*1024)
	defer bw.Flush()
	w = bw

	if cmd.compress {
		gzw := gzip.NewWriter(w)
		defer gzw.Close()
		w = gzw
	}

	// mw is our "meta writer" -- the io.Writer to which meta/out-of-band data
	// like comments will be sent.  If the lponly flag is set, mw will be
	// io.Discard which effectively filters out comments and any other
	// non-line protocol data.
	//
	// Otherwise, mw is set to the same writer as the actual DDL and line
	// protocol DML which will cause the comments to be intermixed with the
	// data..
	//
	mw := w
	if cmd.lponly {
		mw = io.Discard
	}

	return cmd.writeFull(mw, w)
}

func (cmd *Command) writeTsmFiles(mw io.Writer, w io.Writer, files []string) error {
	fmt.Fprintln(mw, "# writing tsm data")

	// we need to make sure we write the same order that the files were written
	sort.Strings(files)

	for _, f := range files {
		if err := cmd.exportTSMFile(f, w); err != nil {
			return err
		}
		if err := cmd.exportDone(f); err != nil {
			return err
		}
	}

	return nil
}

func (cmd *Command) exportTSMFile(tsmFilePath string, w io.Writer) error {
	f, err := os.Open(tsmFilePath)
	if err != nil {
		if os.IsNotExist(err) {
			fmt.Fprintf(cmd.Stderr, "skipped missing file: %s", tsmFilePath)
			return nil
		}
		return err
	}
	defer f.Close()

	r, err := tsm1.NewTSMReader(f)
	if err != nil {
		fmt.Fprintf(cmd.Stderr, "unable to read %s, skipping: %s\n", tsmFilePath, err.Error())
		return nil
	}
	defer r.Close()

	if sgStart, sgEnd := r.TimeRange(); sgStart > cmd.endTime || sgEnd < cmd.startTime {
		return nil
	}

	for i := 0; i < r.KeyCount(); i++ {
		key, _ := r.KeyAt(i)
		values, err := r.ReadAll(key)
		if err != nil {
			fmt.Fprintf(cmd.Stderr, "unable to read key %q in %s, skipping: %s\n", string(key), tsmFilePath, err.Error())
			continue
		}
		measurement, field := tsm1.SeriesAndFieldFromCompositeKey(key)
		field = escape.Bytes(field)

		if cmd.measurement != "" && cmd.measurement != strings.Split(string(measurement), ",")[0] {
			continue
		}

		if err := cmd.writeValues(w, measurement, string(field), values); err != nil {
			// An error from writeValues indicates an IO error, which should be returned.
			return err
		}
	}

	return nil
}

func (cmd *Command) writeWALFiles(mw io.Writer, w io.Writer, files []string, key string) error {
	fmt.Fprintln(mw, "# writing wal data")

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
		if os.IsNotExist(err) {
			fmt.Fprintf(cmd.Stderr, "skipped missing file: %s", walFilePath)
			return nil
		}
		return err
	}
	defer f.Close()

	r := tsm1.NewWALSegmentReader(f)
	defer r.Close()

	for r.Next() {
		entry, err := r.Read()
		if err != nil {
			n := r.Count()
			fmt.Fprintf(cmd.Stderr, "file %s corrupt at position %d: %v", walFilePath, n, err)
			break
		}

		switch t := entry.(type) {
		case *tsm1.DeleteWALEntry, *tsm1.DeleteRangeWALEntry:
			warnDelete()
			continue
		case *tsm1.WriteWALEntry:
			for key, values := range t.Values {
				measurement, field := tsm1.SeriesAndFieldFromCompositeKey([]byte(key))
				// measurements are stored escaped, field names are not
				field = escape.Bytes(field)

				if cmd.measurement != "" && cmd.measurement != strings.Split(string(measurement), ",")[0] {
					continue
				}

				if err := cmd.writeValues(w, measurement, string(field), values); err != nil {
					// An error from writeValues indicates an IO error, which should be returned.
					return err
				}
			}
		}
	}

	return nil
}

// writeValues writes every value in values to w, using the given series key and field name.
// If any call to w.Write fails, that error is returned.
func (cmd *Command) writeValuesLp(w io.Writer, seriesKey []byte, field string, values []tsm1.Value) error {
	buf := []byte(string(seriesKey) + " " + field + "=")
	prefixLen := len(buf)

	for _, value := range values {
		ts := value.UnixNano()
		if (ts < cmd.startTime) || (ts > cmd.endTime) {
			continue
		}

		// Re-slice buf to be "<series_key> <field>=".
		buf = buf[:prefixLen]

		// Append the correct representation of the value.
		switch v := value.Value().(type) {
		case float64:
			buf = strconv.AppendFloat(buf, v, 'g', -1, 64)
		case int64:
			buf = strconv.AppendInt(buf, v, 10)
			buf = append(buf, 'i')
		case uint64:
			buf = strconv.AppendUint(buf, v, 10)
			buf = append(buf, 'u')
		case bool:
			buf = strconv.AppendBool(buf, v)
		case string:
			buf = append(buf, '"')
			buf = append(buf, models.EscapeStringField(v)...)
			buf = append(buf, '"')
		default:
			// This shouldn't be possible, but we'll format it anyway.
			buf = append(buf, fmt.Sprintf("%v", v)...)
		}

		// Now buf has "<series_key> <field>=<value>".
		// Append the timestamp and a newline, then write it.
		buf = append(buf, ' ')
		buf = strconv.AppendInt(buf, ts, 10)
		buf = append(buf, '\n')
		if _, err := w.Write(buf); err != nil {
			// Underlying IO error needs to be returned.
			return err
		}
	}

	return nil
}

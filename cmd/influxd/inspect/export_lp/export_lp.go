package export_lp

import (
	"bufio"
	"compress/gzip"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/influxdata/influxdb/v2/kit/cli"
	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/models"
	"github.com/influxdata/influxdb/v2/pkg/escape"
	"github.com/influxdata/influxdb/v2/tsdb/engine/tsm1"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// exportFlags contains CLI-compatible forms of export options.
type exportFlags struct {
	enginePath   string
	bucketID     platform.ID
	measurements []string
	startTime    string
	endTime      string

	outputPath string
	compress   bool

	logLevel zapcore.Level
}

// exportFilters contains storage-optimized forms of parameters used to restrict exports.
type exportFilters struct {
	measurements map[string]struct{}
	start        int64
	end          int64
}

func newFilters() *exportFilters {
	return &exportFilters{
		measurements: make(map[string]struct{}),
		start:        math.MinInt64,
		end:          math.MaxInt64,
	}
}

// filters converts CLI-specified filters into storage-optimized forms.
func (f *exportFlags) filters() (*exportFilters, error) {
	filters := newFilters()

	if f.startTime != "" {
		s, err := time.Parse(time.RFC3339, f.startTime)
		if err != nil {
			return nil, err
		}
		filters.start = s.UnixNano()
	}

	if f.endTime != "" {
		e, err := time.Parse(time.RFC3339, f.endTime)
		if err != nil {
			return nil, err
		}
		filters.end = e.UnixNano()
	}

	for _, m := range f.measurements {
		filters.measurements[m] = struct{}{}
	}

	return filters, nil
}

func newFlags() *exportFlags {
	return &exportFlags{
		logLevel: zapcore.InfoLevel,
		compress: false,
	}
}

// NewExportLineProtocolCommand builds and registers the `export` subcommand of `influxd inspect`.
func NewExportLineProtocolCommand(v *viper.Viper) (*cobra.Command, error) {
	flags := newFlags()

	cmd := &cobra.Command{
		Use:   `export-lp`,
		Short: "Export TSM data as line protocol",
		Long: `
This command will export all TSM data stored in a bucket
to line protocol for inspection and re-ingestion.`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return exportRunE(cmd, flags)
		},
	}

	opts := []cli.Opt{
		{
			DestP:    &flags.enginePath,
			Flag:     "engine-path",
			Desc:     "path to persistent engine files",
			Required: true,
		},
		{
			DestP:    &flags.bucketID,
			Flag:     "bucket-id",
			Desc:     "ID of bucket containing data to export",
			Required: true,
		},
		{
			DestP: &flags.measurements,
			Flag:  "measurement",
			Desc:  "optional: name(s) of specific measurement to export",
		},
		{
			DestP: &flags.startTime,
			Flag:  "start",
			Desc:  "optional: the start time to export (RFC3339 format)",
		},
		{
			DestP: &flags.endTime,
			Flag:  "end",
			Desc:  "optional: the end time to export (RFC3339 format)",
		},
		{
			DestP:    &flags.outputPath,
			Flag:     "output-path",
			Desc:     "path where exported line-protocol should be written. Use '-' to write to standard out",
			Required: true,
		},
		{
			DestP: &flags.compress,
			Flag:  "compress",
			Desc:  "if true, compress output with GZIP",
		},
		{
			DestP:   &flags.logLevel,
			Flag:    "log-level",
			Default: flags.logLevel,
		},
	}

	if err := cli.BindOptions(v, cmd, opts); err != nil {
		return nil, err
	}
	return cmd, nil
}

func exportRunE(cmd *cobra.Command, flags *exportFlags) error {
	logconf := zap.NewProductionConfig()
	logconf.Level = zap.NewAtomicLevelAt(flags.logLevel)
	logger, err := logconf.Build()
	if err != nil {
		return err
	}

	filters, err := flags.filters()
	if err != nil {
		return err
	}

	var w io.Writer
	if flags.outputPath == "-" {
		w = cmd.OutOrStdout()
	} else {
		f, err := os.Create(flags.outputPath)
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

	if flags.compress {
		gzw := gzip.NewWriter(w)
		defer gzw.Close()
		w = gzw
	}

	if err := exportTSMs(flags.enginePath, flags.bucketID, filters, w, logger); err != nil {
		return err
	}

	if err := exportWALs(flags.enginePath, flags.bucketID, filters, w, logger); err != nil {
		return err
	}

	logger.Info("export complete")
	return nil
}

// exportTSMs finds, reads, and exports all data stored in TSM files for a bucket that matches a set of filters.
func exportTSMs(engineDir string, bucketID platform.ID, filters *exportFilters, out io.Writer, log *zap.Logger) error {
	// TSM is stored under `<engine>/data/<bucket-id>/<rp>/<shard-id>/*.tsm`
	tsmDir := filepath.Join(engineDir, "data", bucketID.String())
	tsmPattern := filepath.Join(tsmDir, "*", "*", fmt.Sprintf("*.%s", tsm1.TSMFileExtension))
	log.Debug("searching for TSM files", zap.String("file_pattern", tsmPattern))
	tsmFiles, err := filepath.Glob(tsmPattern)
	if err != nil {
		return err
	}

	log.Info("exporting TSM files", zap.String("tsm_dir", tsmDir), zap.Int("file_count", len(tsmFiles)))

	// Ensure we export in the same order that the TSM file store would process the files.
	// See FileStore.Open() in tsm1/file_store.go
	sort.Strings(tsmFiles)

	for _, f := range tsmFiles {
		if err := exportTSM(f, filters, out, log); err != nil {
			return err
		}
	}

	return nil
}

func exportTSM(tsmFile string, filters *exportFilters, out io.Writer, log *zap.Logger) error {
	log.Debug("exporting TSM file", zap.String("file_path", tsmFile))
	f, err := os.Open(tsmFile)
	if err != nil {
		// TSM files can disappear if we're exporting from the engine dir of a live DB,
		// and compactions run between our path-lookup and export steps.
		if os.IsNotExist(err) {
			log.Warn("skipping missing TSM file", zap.String("file_path", tsmFile))
			return nil
		}
		return err
	}
	defer f.Close()

	reader, err := tsm1.NewTSMReader(f)
	if err != nil {
		return err
	}
	defer reader.Close()

	if !reader.OverlapsTimeRange(filters.start, filters.end) {
		return nil
	}
	filterMeasurement := len(filters.measurements) > 0

	for i := 0; i < reader.KeyCount(); i++ {
		key, _ := reader.KeyAt(i)
		values, err := reader.ReadAll(key)
		if err != nil {
			log.Error(
				"unable to read key, skipping point",
				zap.ByteString("key", key),
				zap.String("tsm_file", tsmFile),
				zap.Error(err),
			)
			continue
		}
		key, field := tsm1.SeriesAndFieldFromCompositeKey(key)
		if filterMeasurement {
			measurement, _ := models.ParseKey(key)
			if _, ok := filters.measurements[measurement]; !ok {
				continue
			}
		}
		field = escape.Bytes(field)

		if err := writeValues(key, field, values, filters, out, log); err != nil {
			return err
		}
	}

	return nil
}

// exportTSMs finds, reads, and exports all data stored in WAL files for a bucket that matches a set of filters.
//
// N.B. exported lines can include some duplicates from a matching call to exportTSMs on the same engine/bucket.
// This is OK since writes are idempotent.
func exportWALs(engineDir string, bucketID platform.ID, filters *exportFilters, out io.Writer, log *zap.Logger) error {
	// WAL is stored under `<engine>/wal/<bucket-id>/<rp>/<shard-id>/*.wal`
	walDir := filepath.Join(engineDir, "wal", bucketID.String())
	walPattern := filepath.Join(walDir, "*", "*", fmt.Sprintf("*.%s", tsm1.WALFileExtension))
	log.Debug("searching for WAL files", zap.String("file_pattern", walPattern))
	walFiles, err := filepath.Glob(walPattern)
	if err != nil {
		return err
	}

	// N.B. WAL files might contain tombstone markers that haven't been sync'd down into TSM yet.
	// We can't really deal with them when working at this low level, so we warn the user if we encounter one.
	var tombstoneWarnOnce sync.Once
	warnTombstone := func() {
		tombstoneWarnOnce.Do(func() {
			log.Warn("detected deletes in WAL file, some deleted data may be brought back by replaying this export")
		})
	}

	// Ensure we export in the same order that the TSM WAL would process the files.
	// See segmentFileNames in tsm1/wal.go
	sort.Strings(walFiles)

	log.Info("exporting WAL files", zap.String("wal_dir", walDir), zap.Int("file_count", len(walFiles)))
	for _, f := range walFiles {
		if err := exportWAL(f, filters, out, log, warnTombstone); err != nil {
			return err
		}
	}

	return nil
}

func exportWAL(walFile string, filters *exportFilters, out io.Writer, log *zap.Logger, onDelete func()) error {
	log.Debug("exporting WAL file", zap.String("file_path", walFile))
	f, err := os.Open(walFile)
	if err != nil {
		// WAL files can disappear if we're exporting from the engine dir of a live DB,
		// and a snapshot is written between our path-lookup and export steps.
		if os.IsNotExist(err) {
			log.Warn("skipping missing WAL file", zap.String("file_path", walFile))
			return nil
		}
	}
	defer f.Close()

	reader := tsm1.NewWALSegmentReader(f)
	defer reader.Close()

	filterMeasurement := len(filters.measurements) > 0

	for reader.Next() {
		entry, err := reader.Read()
		if err != nil {
			n := reader.Count()
			log.Error(
				"stopping at corrupt position in WAL file",
				zap.String("file_path", walFile),
				zap.Int64("position", n),
			)
			break
		}

		switch t := entry.(type) {
		case *tsm1.DeleteWALEntry, *tsm1.DeleteRangeWALEntry:
			onDelete()
			continue
		case *tsm1.WriteWALEntry:
			for key, values := range t.Values {
				key, field := tsm1.SeriesAndFieldFromCompositeKey([]byte(key))
				if filterMeasurement {
					measurement, _ := models.ParseKey(key)
					if _, ok := filters.measurements[measurement]; !ok {
						continue
					}
				}
				field = escape.Bytes(field)
				if err := writeValues(key, field, values, filters, out, log); err != nil {
					return err
				}
			}
		}
	}

	return nil
}

func writeValues(key []byte, field []byte, values []tsm1.Value, filters *exportFilters, out io.Writer, log *zap.Logger) error {
	buf := []byte(fmt.Sprintf("%s %s=", key, field))
	prefixLen := len(buf)

	for _, value := range values {
		ts := value.UnixNano()
		if ts < filters.start || ts > filters.end {
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
			// This shouldn't be possible.
			log.Error(
				"ignoring value with unsupported type",
				zap.ByteString("key", key),
				zap.ByteString("field", field),
				zap.String("value", value.String()),
			)
			continue
		}

		// Now buf has "<series_key> <field>=<value>".
		// Append the timestamp and a newline, then write it.
		buf = append(buf, ' ')
		buf = strconv.AppendInt(buf, ts, 10)
		buf = append(buf, '\n')
		if _, err := out.Write(buf); err != nil {
			// Underlying IO error needs to be returned.
			return err
		}
	}

	return nil
}

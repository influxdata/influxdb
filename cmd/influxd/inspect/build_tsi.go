package inspect

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/user"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/influxdata/influxdb/v2/cmd/influx_inspect/buildtsi"
	"github.com/influxdata/influxdb/v2/logger"
	"github.com/influxdata/influxdb/v2/storage"
	"github.com/influxdata/influxdb/v2/v1/tsdb"
	"github.com/influxdata/influxdb/v2/v1/tsdb/index/tsi1"
	"github.com/influxdata/influxdb/v2/v1/tsdb/engine/tsm1"
	"github.com/spf13/cobra"
)

const defaultBatchSize = 10000

var buildTSIFlags = struct {
	// Standard input/output, overridden for testing.
	Stderr io.Writer
	Stdout io.Writer

	// Data path options
	DataPath       string // optional. Defaults to <engine_path>/engine/data
	WALPath        string // optional. Defaults to <engine_path>/engine/wal
	SeriesFilePath string // optional. Defaults to <engine_path>/engine/_series
	IndexPath      string // optional. Defaults to <engine_path>/engine/index

	BatchSize      int    // optional. Defaults to 10000
	MaxLogFileSize int64  // optional. Defaults to tsi1.DefaultMaxIndexLogFileSize
	MaxCacheSize   uint64 // optional. Defaults to tsm1.DefaultCacheMaxMemorySize

	Concurrency int  // optional. Defaults to GOMAXPROCS(0)
	Verbose     bool // optional. Defaults to false.
}{
	Stderr: os.Stderr,
	Stdout: os.Stdout,
}

// NewBuildTSICommand returns a new instance of Command with default setting applied.
func NewBuildTSICommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "build-tsi",
		Short: "Rebuilds the TSI index and (where necessary) the Series File.",
		Long: `This command will rebuild the TSI index and if needed the Series
		File. 

		The index is built by reading all of the TSM indexes in the TSM data 
		directory, and all of the WAL entries in the WAL data directory. If the 
		Series File directory is missing, then the series file will be rebuilt.

		If the TSI index directory already exists, then this tool will fail.

		Performance of the tool can be tweaked by adjusting the max log file size,
		max cache file size and the batch size.
		
		max-log-file-size determines how big in-memory parts of the index have to
			get before they're compacted into memory-mappable index files. 
			Consider decreasing this from the default if you find the heap 
			requirements of your TSI index are too much.

		max-cache-size refers to the maximum cache size allowed. If there are WAL
			files to index, then they need to be replayed into a tsm1.Cache first
			by this tool. If the maximum cache size isn't large enough then there
			will be an error and this tool will fail. Increase max-cache-size to
			address this.

		batch-size refers to the size of the batches written into the index. 
			Increasing this can improve performance but can result in much more
			memory usage.
		`,
		RunE: RunBuildTSI,
	}

	defaultPath := filepath.Join(os.Getenv("HOME"), "/.influxdbv2/engine/")
	defaultDataPath := filepath.Join(defaultPath, storage.DefaultEngineDirectoryName)
	defaultWALPath := filepath.Join(defaultPath, storage.DefaultWALDirectoryName)
	defaultSFilePath := filepath.Join(defaultPath, storage.DefaultSeriesFileDirectoryName)
	defaultIndexPath := filepath.Join(defaultPath, storage.DefaultIndexDirectoryName)

	cmd.Flags().StringVar(&buildTSIFlags.DataPath, "tsm-path", defaultDataPath, "Path to the TSM data directory. Defaults to "+defaultDataPath)
	cmd.Flags().StringVar(&buildTSIFlags.WALPath, "wal-path", defaultWALPath, "Path to the WAL data directory. Defaults to "+defaultWALPath)
	cmd.Flags().StringVar(&buildTSIFlags.SeriesFilePath, "sfile-path", defaultSFilePath, "Path to the Series File directory. Defaults to "+defaultSFilePath)
	cmd.Flags().StringVar(&buildTSIFlags.IndexPath, "tsi-path", defaultIndexPath, "Path to the TSI index directory. Defaults to "+defaultIndexPath)

	cmd.Flags().IntVar(&buildTSIFlags.Concurrency, "concurrency", runtime.GOMAXPROCS(0), "Number of workers to dedicate to shard index building. Defaults to GOMAXPROCS")
	cmd.Flags().Int64Var(&buildTSIFlags.MaxLogFileSize, "max-log-file-size", tsi1.DefaultMaxIndexLogFileSize, "optional: maximum log file size")
	cmd.Flags().Uint64Var(&buildTSIFlags.MaxCacheSize, "max-cache-size", uint64(tsm1.DefaultCacheMaxMemorySize), "optional: maximum cache size")
	cmd.Flags().IntVar(&buildTSIFlags.BatchSize, "batch-size", defaultBatchSize, "optional: set the size of the batches we write to the index. Setting this can have adverse affects on performance and heap requirements")
	cmd.Flags().BoolVar(&buildTSIFlags.Verbose, "v", false, "verbose")

	cmd.SetOutput(buildTSIFlags.Stdout)

	return cmd
}

// RunBuildTSI executes the run command for BuildTSI.
func RunBuildTSI(cmd *cobra.Command, args []string) error {
	// Verify the user actually wants to run as root.
	if isRoot() {
		fmt.Fprintln(buildTSIFlags.Stdout, "You are currently running as root. This will build your")
		fmt.Fprintln(buildTSIFlags.Stdout, "index files with root ownership and will be inaccessible")
		fmt.Fprintln(buildTSIFlags.Stdout, "if you run influxd as a non-root user. You should run")
		fmt.Fprintln(buildTSIFlags.Stdout, "influxd inspect buildtsi as the same user you are running influxd.")
		fmt.Fprint(buildTSIFlags.Stdout, "Are you sure you want to continue? (y/N): ")
		var answer string
		if fmt.Scanln(&answer); !strings.HasPrefix(strings.TrimSpace(strings.ToLower(answer)), "y") {
			return fmt.Errorf("operation aborted")
		}
	}

	log := logger.New(buildTSIFlags.Stdout)

	sfile := seriesfile.NewSeriesFile(buildTSIFlags.SeriesFilePath)
	sfile.Logger = log
	if err := sfile.Open(context.Background()); err != nil {
		return err
	}
	defer sfile.Close()

	return buildtsi.IndexShard(sfile, buildTSIFlags.IndexPath, buildTSIFlags.DataPath, buildTSIFlags.WALPath,
		buildTSIFlags.MaxLogFileSize, buildTSIFlags.MaxCacheSize, buildTSIFlags.BatchSize,
		log, buildTSIFlags.Verbose)
}

func isRoot() bool {
	user, _ := user.Current()
	return user != nil && user.Username == "root"
}

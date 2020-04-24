package inspect

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"

	"github.com/influxdata/influxdb/v2/internal/fs"
	"github.com/influxdata/influxdb/v2/storage"
	"github.com/spf13/cobra"
	"golang.org/x/sync/errgroup"
)

var compactSeriesFileFlags = struct {
	// Standard input/output, overridden for testing.
	Stderr io.Writer
	Stdout io.Writer

	// Data path options
	SeriesFilePath string // optional. Defaults to <engine_path>/engine/_series
	IndexPath      string // optional. Defaults to <engine_path>/engine/index

	Concurrency int // optional. Defaults to GOMAXPROCS(0)
}{
	Stderr: os.Stderr,
	Stdout: os.Stdout,
}

// NewCompactSeriesFileCommand returns a new instance of Command with default setting applied.
func NewCompactSeriesFileCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "compact-series-file",
		Short: "Compacts the series file to removed deleted series.",
		Long:  `This command will compact the series file by removing deleted series.`,
		RunE:  RunCompactSeriesFile,
	}

	home, _ := fs.InfluxDir()
	defaultPath := filepath.Join(home, "engine")
	defaultSFilePath := filepath.Join(defaultPath, storage.DefaultSeriesFileDirectoryName)
	defaultIndexPath := filepath.Join(defaultPath, storage.DefaultIndexDirectoryName)

	cmd.Flags().StringVar(&compactSeriesFileFlags.SeriesFilePath, "sfile-path", defaultSFilePath, "Path to the Series File directory. Defaults to "+defaultSFilePath)
	cmd.Flags().StringVar(&compactSeriesFileFlags.IndexPath, "tsi-path", defaultIndexPath, "Path to the TSI index directory. Defaults to "+defaultIndexPath)

	cmd.Flags().IntVar(&compactSeriesFileFlags.Concurrency, "concurrency", runtime.GOMAXPROCS(0), "Number of workers to dedicate to compaction. Defaults to GOMAXPROCS. Max 8.")

	cmd.SetOutput(compactSeriesFileFlags.Stdout)

	return cmd
}

// RunCompactSeriesFile executes the run command for CompactSeriesFile.
func RunCompactSeriesFile(cmd *cobra.Command, args []string) error {
	// Verify the user actually wants to run as root.
	if isRoot() {
		fmt.Fprintln(compactSeriesFileFlags.Stdout, "You are currently running as root. This will compact your")
		fmt.Fprintln(compactSeriesFileFlags.Stdout, "series file with root ownership and will be inaccessible")
		fmt.Fprintln(compactSeriesFileFlags.Stdout, "if you run influxd as a non-root user. You should run")
		fmt.Fprintln(compactSeriesFileFlags.Stdout, "influxd inspect compact-series-file as the same user you are running influxd.")
		fmt.Fprint(compactSeriesFileFlags.Stdout, "Are you sure you want to continue? (y/N): ")
		var answer string
		if fmt.Scanln(&answer); !strings.HasPrefix(strings.TrimSpace(strings.ToLower(answer)), "y") {
			return fmt.Errorf("operation aborted")
		}
	}

	paths, err := seriesFilePartitionPaths(compactSeriesFileFlags.SeriesFilePath)
	if err != nil {
		return err
	}

	// Build input channel.
	pathCh := make(chan string, len(paths))
	for _, path := range paths {
		pathCh <- path
	}
	close(pathCh)

	// Limit maximum concurrency to the total number of series file partitions.
	concurrency := compactSeriesFileFlags.Concurrency
	if concurrency > seriesfile.SeriesFilePartitionN {
		concurrency = seriesfile.SeriesFilePartitionN
	}

	// Concurrently process each partition in the series file
	var g errgroup.Group
	for i := 0; i < concurrency; i++ {
		g.Go(func() error {
			for path := range pathCh {
				if err := compactSeriesFilePartition(path); err != nil {
					return err
				}
			}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return err
	}

	// Build new series file indexes
	sfile := seriesfile.NewSeriesFile(compactSeriesFileFlags.SeriesFilePath)
	if err = sfile.Open(context.Background()); err != nil {
		return err
	}

	compactor := seriesfile.NewSeriesPartitionCompactor()
	for _, partition := range sfile.Partitions() {
		duration, err := compactor.Compact(partition)
		if err != nil {
			return err
		}
		fmt.Fprintf(compactSeriesFileFlags.Stdout, "compacted %s in %s\n", partition.Path(), duration)
	}
	return nil
}

func compactSeriesFilePartition(path string) error {
	const tmpExt = ".tmp"

	fmt.Fprintf(compactSeriesFileFlags.Stdout, "processing partition for %q\n", path)

	// Open partition so index can recover from entries not in the snapshot.
	partitionID, err := strconv.Atoi(filepath.Base(path))
	if err != nil {
		return fmt.Errorf("cannot parse partition id from path: %s", path)
	}
	p := seriesfile.NewSeriesPartition(partitionID, path)
	if err := p.Open(); err != nil {
		return fmt.Errorf("cannot open partition: path=%s err=%s", path, err)
	}
	defer p.Close()

	// Loop over segments and compact.
	indexPath := p.IndexPath()
	var segmentPaths []string
	for _, segment := range p.Segments() {
		fmt.Fprintf(compactSeriesFileFlags.Stdout, "processing segment %q %d\n", segment.Path(), segment.ID())

		if err := segment.CompactToPath(segment.Path()+tmpExt, p.Index()); err != nil {
			return err
		}
		segmentPaths = append(segmentPaths, segment.Path())
	}

	// Close partition.
	if err := p.Close(); err != nil {
		return err
	}

	// Remove the old segment files and replace with new ones.
	for _, dst := range segmentPaths {
		src := dst + tmpExt

		fmt.Fprintf(compactSeriesFileFlags.Stdout, "renaming new segment %q to %q\n", src, dst)
		if err = os.Rename(src, dst); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("serious failure. Please rebuild index and series file: %v", err)
		}
	}

	// Remove index file so it will be rebuilt when reopened.
	fmt.Fprintln(compactSeriesFileFlags.Stdout, "removing index file", indexPath)
	if err = os.Remove(indexPath); err != nil && !os.IsNotExist(err) { // index won't exist for low cardinality
		return err
	}

	return nil
}

// seriesFilePartitionPaths returns the paths to each partition in the series file.
func seriesFilePartitionPaths(path string) ([]string, error) {
	sfile := seriesfile.NewSeriesFile(path)
	if err := sfile.Open(context.Background()); err != nil {
		return nil, err
	}

	var paths []string
	for _, partition := range sfile.Partitions() {
		paths = append(paths, partition.Path())
	}
	if err := sfile.Close(); err != nil {
		return nil, err
	}
	return paths, nil
}

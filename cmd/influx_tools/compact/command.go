package compact

import (
	"bufio"
	"bytes"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"time"

	"github.com/influxdata/influxdb/cmd/influx_tools/internal/errlist"
	"github.com/influxdata/influxdb/cmd/influx_tools/internal/format/binary"
	"github.com/influxdata/influxdb/cmd/influx_tools/internal/format/line"
	"github.com/influxdata/influxdb/logger"
	"github.com/influxdata/influxdb/pkg/limiter"
	"github.com/influxdata/influxdb/tsdb/engine/tsm1"
	"go.uber.org/zap"
)

var (
	_ line.Writer
	_ binary.Writer
)

// Command represents the program execution for "influx-tools compact".
type Command struct {
	// Standard input/output, overridden for testing.
	Stderr io.Writer
	Stdout io.Writer
	Logger *zap.Logger

	path    string
	force   bool
	verbose bool
}

// NewCommand returns a new instance of the export Command.
func NewCommand() *Command {
	return &Command{
		Stderr: os.Stderr,
		Stdout: os.Stdout,
	}
}

// Run executes the export command using the specified args.
func (cmd *Command) Run(args []string) (err error) {
	err = cmd.parseFlags(args)
	if err != nil {
		return err
	}

	var log = zap.NewNop()
	if cmd.verbose {
		cfg := logger.Config{Format: "logfmt"}
		log, err = cfg.New(os.Stdout)
		if err != nil {
			return err
		}
	}

	fmt.Fprintf(cmd.Stdout, "opening shard at path %q\n\n", cmd.path)

	sc, err := newShardCompactor(cmd.path, log)
	if err != nil {
		return err
	}

	fmt.Fprintln(cmd.Stdout)
	fmt.Fprintln(cmd.Stdout, "The following files will be compacted:")
	fmt.Fprintln(cmd.Stdout)
	fmt.Fprintln(cmd.Stdout, sc.String())

	if !cmd.force {
		fmt.Fprint(cmd.Stdout, "Proceed? [N] ")
		scan := bufio.NewScanner(os.Stdin)
		scan.Scan()
		if scan.Err() != nil {
			return fmt.Errorf("error reading STDIN: %v", scan.Err())
		}

		if strings.ToLower(scan.Text()) != "y" {
			return nil
		}
	}

	fmt.Fprintln(cmd.Stdout, "Compacting shard.")

	err = sc.CompactShard()
	if err != nil {
		return fmt.Errorf("compaction failed: %v", err)
	}

	fmt.Fprintln(cmd.Stdout, "Compaction succeeded. New files:")
	for _, f := range sc.newTSM {
		fmt.Fprintf(cmd.Stdout, "  %s\n", f)
	}

	return nil
}

func (cmd *Command) parseFlags(args []string) error {
	fs := flag.NewFlagSet("compact-shard", flag.ContinueOnError)
	fs.StringVar(&cmd.path, "path", "", "path of shard to be compacted")
	fs.BoolVar(&cmd.force, "force", false, "Force compaction without prompting")
	fs.BoolVar(&cmd.verbose, "verbose", false, "Enable verbose logging")

	if err := fs.Parse(args); err != nil {
		return err
	}

	if cmd.path == "" {
		return errors.New("shard-path is required")
	}

	return nil
}

type shardCompactor struct {
	logger    *zap.Logger
	path      string
	tsm       []string
	tombstone []string
	readers   []*tsm1.TSMReader
	files     map[string]*tsm1.TSMReader
	newTSM    []string
}

func newShardCompactor(path string, logger *zap.Logger) (sc *shardCompactor, err error) {
	sc = &shardCompactor{
		logger: logger,
		path:   path,
		files:  make(map[string]*tsm1.TSMReader),
	}

	sc.tsm, err = filepath.Glob(filepath.Join(path, fmt.Sprintf("*.%s", tsm1.TSMFileExtension)))
	if err != nil {
		return nil, fmt.Errorf("newFileStore: error reading tsm files at path %q: %v", path, err)
	}
	if len(sc.tsm) == 0 {
		return nil, fmt.Errorf("newFileStore: no tsm files at path %q", path)
	}
	sort.Strings(sc.tsm)

	sc.tombstone, err = filepath.Glob(filepath.Join(path, fmt.Sprintf("*.%s", "tombstone")))
	if err != nil {
		return nil, fmt.Errorf("error reading tombstone files: %v", err)
	}

	if err := sc.openFiles(); err != nil {
		return nil, err
	}

	return sc, nil
}

func (sc *shardCompactor) openFiles() error {
	sc.readers = make([]*tsm1.TSMReader, 0, len(sc.tsm))

	// struct to hold the result of opening each reader in a goroutine
	type res struct {
		r   *tsm1.TSMReader
		err error
	}

	lim := limiter.NewFixed(runtime.GOMAXPROCS(0))

	readerC := make(chan *res)
	for i, fn := range sc.tsm {
		file, err := os.OpenFile(fn, os.O_RDONLY, 0666)
		if err != nil {
			return fmt.Errorf("newFileStore: failed to open file %q: %v", fn, err)
		}

		go func(idx int, file *os.File) {
			// Ensure a limited number of TSM files are loaded at once.
			// Systems which have very large datasets (1TB+) can have thousands
			// of TSM files which can cause extremely long load times.
			lim.Take()
			defer lim.Release()

			start := time.Now()
			df, err := tsm1.NewTSMReader(file)
			sc.logger.Info("Opened file",
				zap.String("path", file.Name()),
				zap.Int("id", idx),
				zap.Duration("duration", time.Since(start)))

			// If we are unable to read a TSM file then log the error, rename
			// the file, and continue loading the shard without it.
			if err != nil {
				sc.logger.Error("Cannot read corrupt tsm file, renaming", zap.String("path", file.Name()), zap.Int("id", idx), zap.Error(err))
				if e := os.Rename(file.Name(), file.Name()+"."+tsm1.BadTSMFileExtension); e != nil {
					sc.logger.Error("Cannot rename corrupt tsm file", zap.String("path", file.Name()), zap.Int("id", idx), zap.Error(e))
					readerC <- &res{r: df, err: fmt.Errorf("cannot rename corrupt file %s: %v", file.Name(), e)}
					return
				}
			}

			readerC <- &res{r: df}
		}(i, file)
	}

	for range sc.tsm {
		res := <-readerC
		if res.err != nil {
			return res.err
		} else if res.r == nil {
			continue
		}
		sc.readers = append(sc.readers, res.r)
		sc.files[res.r.Path()] = res.r
	}
	close(readerC)
	sort.Sort(tsmReaders(sc.readers))

	return nil
}

func (sc *shardCompactor) CompactShard() (err error) {
	c := tsm1.NewCompactor()
	c.Dir = sc.path
	c.Size = tsm1.DefaultSegmentSize
	c.FileStore = sc
	c.Open()

	tsmFiles, err := c.CompactFull(sc.tsm)
	if err == nil {
		sc.newTSM, err = sc.replace(tsmFiles)
	}
	return err
}

// replace replaces the existing shard files with temporary tsmFiles
func (sc *shardCompactor) replace(tsmFiles []string) ([]string, error) {
	// rename .tsm.tmp → .tsm
	var newNames []string
	for _, file := range tsmFiles {
		var newName = file[:len(file)-4] // remove extension
		if err := os.Rename(file, newName); err != nil {
			return nil, err
		}
		newNames = append(newNames, newName)
	}

	var errs errlist.ErrorList

	// close all readers
	for _, r := range sc.readers {
		r.Close()
	}

	sc.readers = nil
	sc.files = nil

	// remove existing .tsm and .tombstone
	for _, file := range sc.tsm {
		errs.Add(os.Remove(file))
	}

	for _, file := range sc.tombstone {
		errs.Add(os.Remove(file))
	}

	return newNames, errs.Err()
}

func (sc *shardCompactor) NextGeneration() int {
	panic("not implemented")
}

func (sc *shardCompactor) TSMReader(path string) *tsm1.TSMReader {
	r := sc.files[path]
	if r != nil {
		r.Ref()
	}
	return r
}

func (sc *shardCompactor) String() string {
	var sb bytes.Buffer
	sb.WriteString("TSM:\n")
	for _, f := range sc.tsm {
		sb.WriteString("  ")
		sb.WriteString(f)
		sb.WriteByte('\n')
	}

	if len(sc.tombstone) > 0 {
		sb.WriteString("\nTombstone:\n")
		for _, f := range sc.tombstone {
			sb.WriteString("  ")
			sb.WriteString(f)
			sb.WriteByte('\n')
		}
	}

	return sb.String()
}

type tsmReaders []*tsm1.TSMReader

func (a tsmReaders) Len() int           { return len(a) }
func (a tsmReaders) Less(i, j int) bool { return a[i].Path() < a[j].Path() }
func (a tsmReaders) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }

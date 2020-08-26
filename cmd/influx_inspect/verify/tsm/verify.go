// Package tsm verifies integrity of TSM files.
package tsm

import (
	"flag"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"text/tabwriter"
	"time"
	"unicode/utf8"

	"github.com/influxdata/influxdb/v2/tsdb/engine/tsm1"
	"github.com/pkg/errors"
)

// Command represents the program execution for "influx_inspect verify".
type Command struct {
	Stderr io.Writer
	Stdout io.Writer
}

// NewCommand returns a new instance of Command.
func NewCommand() *Command {
	return &Command{
		Stderr: os.Stderr,
		Stdout: os.Stdout,
	}
}

// Run executes the command.
func (cmd *Command) Run(args ...string) error {
	var path string
	fs := flag.NewFlagSet("verify", flag.ExitOnError)
	fs.StringVar(&path, "dir", os.Getenv("HOME")+"/.influxdb", "Root storage path. [$HOME/.influxdb]")

	var checkUTF8 bool
	fs.BoolVar(&checkUTF8, "check-utf8", false, "Verify series keys are valid UTF-8")

	fs.SetOutput(cmd.Stdout)
	fs.Usage = cmd.printUsage

	if err := fs.Parse(args); err != nil {
		return err
	}

	dataPath := filepath.Join(path, "data")
	tw := tabwriter.NewWriter(cmd.Stdout, 16, 8, 0, '\t', 0)

	var runner verifier
	if checkUTF8 {
		runner = &verifyUTF8{}
	} else {
		runner = &verifyChecksums{}
	}
	err := runner.Run(tw, dataPath)
	tw.Flush()
	return err
}

// printUsage prints the usage message to STDERR.
func (cmd *Command) printUsage() {
	usage := fmt.Sprintf(`Verifies the integrity of TSM files.

Usage: influx_inspect verify [flags]

    -dir <path>
            The root storage path.
            Must be changed if you are using a non-default storage directory.
            Defaults to "%[1]s/.influxdb".
    -check-utf8 
            Verify series keys are valid UTF-8.
            This check skips verification of block checksums.
 `, os.Getenv("HOME"))

	fmt.Fprintf(cmd.Stdout, usage)
}

type verifyTSM struct {
	files []string
	f     string
	start time.Time
	err   error
}

func (v *verifyTSM) loadFiles(dataPath string) error {
	err := filepath.Walk(dataPath, func(path string, f os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if filepath.Ext(path) == "."+tsm1.TSMFileExtension {
			v.files = append(v.files, path)
		}
		return nil
	})

	if err != nil {
		return errors.Wrap(err, "could not load storage files (use -dir for custom storage root)")
	}

	return nil
}

func (v *verifyTSM) Next() bool {
	if len(v.files) == 0 {
		return false
	}

	v.f, v.files = v.files[0], v.files[1:]
	return true
}

func (v *verifyTSM) TSMReader() (string, *tsm1.TSMReader) {
	file, err := os.OpenFile(v.f, os.O_RDONLY, 0600)
	if err != nil {
		v.err = err
		return "", nil
	}

	reader, err := tsm1.NewTSMReader(file)
	if err != nil {
		file.Close()
		v.err = err
		return "", nil
	}

	return v.f, reader
}

func (v *verifyTSM) Start() {
	v.start = time.Now()
}

func (v *verifyTSM) Elapsed() time.Duration {
	return time.Since(v.start)
}

type verifyChecksums struct {
	verifyTSM
	totalErrors int
	total       int
}

func (v *verifyChecksums) Run(w io.Writer, dataPath string) error {
	if err := v.loadFiles(dataPath); err != nil {
		return err
	}

	v.Start()

	for v.Next() {
		f, reader := v.TSMReader()
		if reader == nil {
			break
		}

		blockItr := reader.BlockIterator()
		fileErrors := 0
		count := 0
		for blockItr.Next() {
			v.total++
			key, _, _, _, checksum, buf, err := blockItr.Read()
			if err != nil {
				v.totalErrors++
				fileErrors++
				fmt.Fprintf(w, "%s: could not get checksum for key %v block %d due to error: %q\n", f, key, count, err)
			} else if expected := crc32.ChecksumIEEE(buf); checksum != expected {
				v.totalErrors++
				fileErrors++
				fmt.Fprintf(w, "%s: got %d but expected %d for key %v, block %d\n", f, checksum, expected, key, count)
			}
			count++
		}
		if fileErrors == 0 {
			fmt.Fprintf(w, "%s: healthy\n", f)
		}
		reader.Close()
	}

	fmt.Fprintf(w, "Broken Blocks: %d / %d, in %vs\n", v.totalErrors, v.total, v.Elapsed().Seconds())

	return v.err
}

type verifyUTF8 struct {
	verifyTSM
	totalErrors int
	total       int
}

func (v *verifyUTF8) Run(w io.Writer, dataPath string) error {
	if err := v.loadFiles(dataPath); err != nil {
		return err
	}

	v.Start()

	for v.Next() {
		f, reader := v.TSMReader()
		if reader == nil {
			break
		}

		n := reader.KeyCount()
		fileErrors := 0
		v.total += n
		for i := 0; i < n; i++ {
			key, _ := reader.KeyAt(i)
			if !utf8.Valid(key) {
				v.totalErrors++
				fileErrors++
				fmt.Fprintf(w, "%s: key #%d is not valid UTF-8\n", f, i)
			}
		}
		if fileErrors == 0 {
			fmt.Fprintf(w, "%s: healthy\n", f)
		}
	}

	fmt.Fprintf(w, "Invalid Keys: %d / %d, in %vs\n", v.totalErrors, v.total, v.Elapsed().Seconds())
	if v.totalErrors > 0 && v.err == nil {
		v.err = errors.New("check-utf8: failed")
	}

	return v.err
}

type verifier interface {
	Run(w io.Writer, dataPath string) error
}

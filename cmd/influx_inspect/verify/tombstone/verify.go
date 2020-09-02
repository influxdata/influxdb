// Package tombstone verifies integrity of tombstones.
package tombstone

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/influxdata/influxdb/v2/tsdb/engine/tsm1"
)

// Command represents the program execution for "influx_inspect verify-tombstone".
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
	runner := verifier{w: cmd.Stdout}
	fs := flag.NewFlagSet("verify-tombstone", flag.ExitOnError)
	fs.StringVar(&runner.path, "path", os.Getenv("HOME")+"/.influxdb", "path to find tombstone files")
	v := fs.Bool("v", false, "verbose: emit periodic progress")
	vv := fs.Bool("vv", false, "very verbose: emit every tombstone entry key and time range")
	vvv := fs.Bool("vvv", false, "very very verbose: emit every tombstone entry key and RFC3339Nano time range")

	fs.SetOutput(cmd.Stdout)

	if err := fs.Parse(args); err != nil {
		return err
	}

	if *v {
		runner.verbosity = verbose
	}
	if *vv {
		runner.verbosity = veryVerbose
	}
	if *vvv {
		runner.verbosity = veryVeryVerbose
	}

	return runner.Run()
}

const (
	quiet = iota
	verbose
	veryVerbose
	veryVeryVerbose
)

type verifier struct {
	path      string
	verbosity int

	w     io.Writer
	files []string
	f     string
}

func (v *verifier) loadFiles() error {
	return filepath.Walk(v.path, func(path string, f os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if filepath.Ext(path) == "."+tsm1.TombstoneFileExtension {
			v.files = append(v.files, path)
		}
		return nil
	})
}

func (v *verifier) Next() bool {
	if len(v.files) == 0 {
		return false
	}

	v.f, v.files = v.files[0], v.files[1:]
	return true
}

func (v *verifier) Run() error {
	if err := v.loadFiles(); err != nil {
		return err
	}

	var failed bool
	start := time.Now()
	for v.Next() {
		if v.verbosity > quiet {
			fmt.Fprintf(v.w, "Verifying: %q\n", v.f)
		}

		tombstoner := tsm1.NewTombstoner(v.f, nil)
		if !tombstoner.HasTombstones() {
			fmt.Fprintf(v.w, "%s has no tombstone entries", v.f)
			continue
		}

		var totalEntries int64
		err := tombstoner.Walk(func(t tsm1.Tombstone) error {
			totalEntries++
			if v.verbosity > quiet && totalEntries%(10*1e6) == 0 {
				fmt.Fprintf(v.w, "Verified %d tombstone entries\n", totalEntries)
			} else if v.verbosity > verbose {
				var min interface{} = t.Min
				var max interface{} = t.Max
				if v.verbosity > veryVerbose {
					min = time.Unix(0, t.Min)
					max = time.Unix(0, t.Max)
				}
				fmt.Printf("key: %q, min: %v, max: %v\n", t.Key, min, max)
			}
			return nil
		})
		if err != nil {
			fmt.Fprintf(v.w, "%q failed to walk tombstone entries: %v. Last okay entry: %d\n", v.f, err, totalEntries)
			failed = true
			continue
		}

		fmt.Fprintf(v.w, "Completed verification for %q in %v.\nVerified %d entries\n\n", v.f, time.Since(start), totalEntries)
	}

	if failed {
		return errors.New("failed tombstone verification")
	}
	return nil
}

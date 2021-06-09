package inspect

import (
	"errors"
	"os"
	"path/filepath"
	"time"

	"github.com/influxdata/influxdb/v2/tsdb/engine/tsm1"
	"github.com/spf13/cobra"
)

type args struct {
	dir string
	v   bool
	vv  bool
	vvv bool
}

type verifier struct {
	path      string
	verbosity int
	files     []string
	f         string
}

const (
	quiet = iota
	verbose
	veryVerbose
	veryVeryVerbose
)

func NewVerifyTombstoneCommand() *cobra.Command {
	var arguments args
	cmd := &cobra.Command{
		Use: "verify-tombstone",
		Short: "Verify the integrity of tombstone files",
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			runner := verifier{path: arguments.dir}
			if arguments.vvv {
				runner.verbosity = veryVeryVerbose
			} else if arguments.vv {
				runner.verbosity = veryVerbose
			} else if arguments.v {
				runner.verbosity = verbose
			}
			return runner.Run(cmd)
		},
	}

	// TODO Make sure this is where tombstones default to when made
	cmd.Flags().StringVar(&arguments.dir, "dir", filepath.Join(os.Getenv("HOME"), ".influxdbv2"),
		"Path to find tombstone files.")
	cmd.Flags().BoolVar(&arguments.v, "v", false,
		"Verbose: Emit periodic progress.")
	cmd.Flags().BoolVar(&arguments.vv, "vv", false,
		"Very verbose: Emit every tombstone entry key and time range.")
	cmd.Flags().BoolVar(&arguments.vvv, "vvv", false,
		"Very very verbose: Emit every tombstone entry key and RFC3339Nano time range.")
	return cmd
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

func (v *verifier) Run(cmd *cobra.Command) error {
	if err := v.loadFiles(); err != nil {
		return err
	}

	var failed bool
	var foundTombstoneFile bool
	start := time.Now()
	for v.Next() {
		foundTombstoneFile = true
		if v.verbosity > quiet {
			cmd.Printf("Verifying: %q\n", v.f)
		}

		tombstoner := tsm1.NewTombstoner(v.f, nil)
		if !tombstoner.HasTombstones() {
			cmd.Printf("%s has no tombstone entries", v.f)
			continue
		}

		var totalEntries int64
		err := tombstoner.Walk(func(t tsm1.Tombstone) error {
			totalEntries++
			if v.verbosity > quiet && totalEntries%(10*1e6) == 0 {
				cmd.Printf("Verified %d tombstone entries\n", totalEntries)
			} else if v.verbosity > verbose {
				var min interface{} = t.Min
				var max interface{} = t.Max
				if v.verbosity > veryVerbose {
					min = time.Unix(0, t.Min)
					max = time.Unix(0, t.Max)
				}
				cmd.Printf("key: %q, min: %v, max: %v\n", t.Key, min, max)
			}
			return nil
		})
		if err != nil {
			cmd.Printf("%q failed to walk tombstone entries: %v. Last okay entry: %d\n", v.f, err, totalEntries)
			failed = true
			continue
		}

		cmd.Printf("Completed verification for %q in %v.\nVerified %d entries\n\n", v.f, time.Since(start), totalEntries)
	}

	if failed {
		return errors.New("failed tombstone verification")
	}
	if !foundTombstoneFile {
		cmd.Printf("No tombstone files found\n")
	}
	return nil
}

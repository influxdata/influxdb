package inspect

import (
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"time"
	"unicode/utf8"

	"github.com/influxdata/influxdb/v2/tsdb/engine/tsm1"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
)

type verifier interface {
	Run(cmd *cobra.Command, dataPath string, verbose bool) error
}

type verifyTSM struct {
	files []string
	f     string
	start time.Time
}

type verifyUTF8 struct {
	verifyTSM
	totalErrors int
	total       int
}

type verifyChecksums struct {
	verifyTSM
	totalErrors int
	total       int
}

func NewTSMVerifyCommand() *cobra.Command {
	var checkUTF8 bool
	var dir string
	var verbose bool

	cmd := &cobra.Command{
		Use:   `verify-tsm`,
		Short: `Verifies the integrity of TSM files`,
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			var runner verifier
			if checkUTF8 {
				runner = &verifyUTF8{}
			} else {
				runner = &verifyChecksums{}
			}
			err := runner.Run(cmd, dir, verbose)
			return err
		},
	}
	cmd.Flags().StringVar(&dir, "dir", os.Getenv("HOME")+"/.influxdbv2", "Root storage path.")
	cmd.Flags().BoolVar(&checkUTF8, "check-utf8", false, "Verify series keys are valid UTF-8. This check skips verification of block checksums.")
	cmd.Flags().BoolVar(&verbose, "verbose", false, "Enable verbose logging")
	return cmd
}

func (v *verifyUTF8) Run(cmd *cobra.Command, dataPath string, verbose bool) error {
	if err := v.loadFiles(dataPath); err != nil {
		return err
	}

	v.Start()

	for v.Next() {
		reader, closer, err := v.TSMReader()
		if err != nil {
			if closer != nil {
				go closer()
			}
			return err
		}

		n := reader.KeyCount()
		fileErrors := 0
		v.total += n
		for i := 0; i < n; i++ {
			key, _ := reader.KeyAt(i)
			if !utf8.Valid(key) {
				v.totalErrors++
				fileErrors++
				if verbose {
					cmd.PrintErrf("%s: key #%d is not valid UTF-8\n", v.f, i)
				}
			}
		}
		if fileErrors == 0 && verbose {
			cmd.PrintErrf("%s: healthy\n", v.f)
		}
		go closer()
	}

	cmd.PrintErrf("Invalid Keys: %d / %d, in %vs\n", v.totalErrors, v.total, v.Elapsed().Seconds())
	if v.totalErrors > 0 {
		return errors.New("check-utf8: failed")
	}

	return nil
}

func (v *verifyChecksums) Run(cmd *cobra.Command, dataPath string, verbose bool) error {
	if err := v.loadFiles(dataPath); err != nil {
		return err
	}

	v.Start()

	for v.Next() {
		reader, closer, err := v.TSMReader()
		if err != nil {
			if closer != nil {
				go closer()
			}
			return err
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
				if verbose {
					cmd.PrintErrf("%s: could not get checksum for key %v block %d due to error: %q\n", v.f, key, count, err)
				}
			} else if expected := crc32.ChecksumIEEE(buf); checksum != expected {
				v.totalErrors++
				fileErrors++
				if verbose {
					cmd.PrintErrf("%s: got %d but expected %d for key %v, block %d\n", v.f, checksum, expected, key, count)
				}
			}
			count++
		}
		if fileErrors == 0 && verbose {
			cmd.PrintErrf("%s: healthy\n", v.f)
		}
		go closer()
	}

	cmd.PrintErrf("Broken Blocks: %d / %d, in %vs\n", v.totalErrors, v.total, v.Elapsed().Seconds())

	return nil
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
		return fmt.Errorf("could not load storage files (use -dir for custom storage root): %w", err)
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

func (v *verifyTSM) TSMReader() (*tsm1.TSMReader, func(), error) {
	file, err := os.OpenFile(v.f, os.O_RDONLY, 0600)
	if err != nil {
		return nil, nil, err
	}

	reader, err := tsm1.NewTSMReader(file)
	if err != nil {
		closer := func() {
			file.Close()
		}
		return nil, closer, err
	}

	closer := func() {
		file.Close()
		reader.Close()
	}
	return reader, closer, nil
}

func (v *verifyTSM) Start() {
	v.start = time.Now()
}

func (v *verifyTSM) Elapsed() time.Duration {
	return time.Since(v.start)
}

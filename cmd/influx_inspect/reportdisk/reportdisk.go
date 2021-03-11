// Package report reports statistics about TSM files.
package reportdisk

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/pkg/reporthelper"
	"github.com/influxdata/influxdb/tsdb/engine/tsm1"
)

// Command represents the program execution for "influxd report".
type Command struct {
	Stderr io.Writer
	Stdout io.Writer

	dir      string
	pattern  string
	detailed bool
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
	fs := flag.NewFlagSet("report", flag.ExitOnError)
	fs.StringVar(&cmd.pattern, "pattern", "", "Include only files matching a pattern")
	fs.BoolVar(&cmd.detailed, "detailed", false, "Report disk size by measurement")

	fs.SetOutput(cmd.Stdout)
	fs.Usage = cmd.printUsage

	if err := fs.Parse(args); err != nil {
		return err
	}

	cmd.dir = fs.Arg(0)

	start := time.Now()

	shardSizes := ShardSizes{}
	if err := reporthelper.WalkShardDirs(cmd.dir, func(db, rp, id, path string) error {
		if cmd.pattern != "" && !strings.Contains(path, cmd.pattern) {
			return nil
		}

		stat, err := os.Stat(path)
		if err != nil {
			fmt.Fprintf(cmd.Stderr, "error: %s: %v. Skipping.\n", path, err)
			return nil
		}

		shardSizes.AddTsmFileWithSize(db, rp, id, stat.Size())
		return nil
	}); err != nil {
		return err
	}

	measurementSizes := MeasurementSizes{}
	if cmd.detailed {
		processedFiles := 0
		progress := NewProgressReporter(cmd.Stderr)

		if err := reporthelper.WalkShardDirs(cmd.dir, func(db, rp, id, path string) error {
			if cmd.pattern != "" && !strings.Contains(path, cmd.pattern) {
				return nil
			}
			file, err := os.OpenFile(path, os.O_RDONLY, 0600)
			if err != nil {
				fmt.Fprintf(cmd.Stderr, "error: %s: %v. Skipping.\n", path, err)
				return nil
			}

			progress.Report(fmt.Sprintf("TSM files inspected: %d\t/%d", processedFiles, shardSizes.files))
			processedFiles++

			reader, err := tsm1.NewTSMReader(file)
			if err != nil {
				fmt.Fprintf(cmd.Stderr, "error: %s: %v. Skipping.\n", file.Name(), err)
				return nil
			}

			keyNum := reader.KeyCount()
			for i := 0; i < keyNum; i++ {
				key, _ := reader.KeyAt(i)
				series, _ := tsm1.SeriesAndFieldFromCompositeKey(key)
				measurement := models.ParseName(series)
				var size int64
				for _, entry := range reader.Entries(key) {
					size += int64(entry.Size)
				}
				measurementSizes.AddSize(db, rp, string(measurement), size)
			}

			return nil
		}); err != nil {
			return err
		}
		progress.Report(fmt.Sprintf("TSM files inspected: %d\t/%d", processedFiles, shardSizes.files))
	}
	fmt.Fprintf(cmd.Stderr, "\nCompleted in %s\n", time.Since(start))

	sanitize := func(s string) []byte {
		b, _ := json.Marshal(s) // json shouldn't be throwing errors when marshalling a string
		return b
	}

	fmt.Fprintf(cmd.Stdout, `{
  "Summary": {"shards": %d, "tsm_files": %d, "total_tsm_size": %d },
  "Shard": [`, shardSizes.shards, shardSizes.files, shardSizes.totalSize)

	first := true
	shardSizes.ForEach(func(db, rp, id string, detail ShardDetails) {
		var shardString []byte
		if s, err := strconv.ParseInt(id, 10, 64); err != nil && strconv.FormatInt(s, 10) == id {
			shardString = []byte(id)
		} else {
			shardString = sanitize(id)
		}
		if !first {
			fmt.Fprint(cmd.Stdout, ",")
		}
		first = false
		fmt.Fprintf(cmd.Stdout, `
    {"db": %s, "rp": %s, "shard": %s, "tsm_files": %d, "size": %d}`,
			sanitize(db), sanitize(rp), shardString, detail.files, detail.size)
	})

	if cmd.detailed {
		fmt.Fprintf(cmd.Stdout, `
  ],
  "Measurement": [`)

		first = true
		measurementSizes.ForEach(func(db, rp, measurement string, detail MeasurementDetails) {
			if !first {
				fmt.Fprint(cmd.Stdout, ",")
			}
			first = false
			fmt.Fprintf(cmd.Stdout, `
    {"db": %s, "rp": %s, "measurement": %s, "size": %d}`,
				sanitize(db), sanitize(rp), sanitize(measurement), detail.size)
		})

	}
	fmt.Fprintf(cmd.Stdout, `
  ]
}
`,
	)

	return nil
}

// printUsage prints the usage message to STDERR.
func (cmd *Command) printUsage() {
	usage := `Displays report of disk usage.

Usage: influx_inspect report [flags] <directory>

    -pattern <pattern>
            Include only files matching a pattern.
    -detailed
            Report disk usage by measurement.
            Defaults to "false".
`

	fmt.Fprintf(cmd.Stdout, usage)
}

type ShardDetails struct {
	size  int64
	files int64
}

type ShardSizes struct {
	m         map[string]map[string]map[string]*ShardDetails
	files     int64
	shards    int64
	totalSize int64
}

func (s *ShardSizes) AddTsmFileWithSize(db, rp, id string, size int64) {
	if s.m == nil {
		s.m = make(map[string]map[string]map[string]*ShardDetails)
	}
	if _, ok := s.m[db]; !ok {
		s.m[db] = make(map[string]map[string]*ShardDetails)
	}
	if _, ok := s.m[db][rp]; !ok {
		s.m[db][rp] = make(map[string]*ShardDetails)
	}
	if _, ok := s.m[db][rp][id]; !ok {
		s.m[db][rp][id] = &ShardDetails{}
		s.shards += 1
	}
	s.m[db][rp][id].size += size
	s.m[db][rp][id].files += 1
	s.files += 1
	s.totalSize += size
}

func (s *ShardSizes) ForEach(f func(db, rp, id string, detail ShardDetails)) {
	dbKeys := make([]string, 0, len(s.m))
	for db, _ := range s.m {
		dbKeys = append(dbKeys, db)
	}
	sort.Strings(dbKeys)
	for _, db := range dbKeys {
		rpKeys := make([]string, 0, len(s.m[db]))
		for rp, _ := range s.m[db] {
			rpKeys = append(rpKeys, rp)
		}
		sort.Strings(rpKeys)
		for _, rp := range rpKeys {
			idKeys := make([]string, 0, len(s.m[db][rp]))
			for id, _ := range s.m[db][rp] {
				idKeys = append(idKeys, id)
			}
			sort.Strings(idKeys)
			for _, id := range idKeys {
				f(db, rp, id, *s.m[db][rp][id])
			}
		}
	}
}

type MeasurementDetails struct {
	size int64
}

type MeasurementSizes struct {
	m map[string]map[string]map[string]*MeasurementDetails
}

func (s *MeasurementSizes) AddSize(db, rp, measurement string, size int64) {
	if s.m == nil {
		s.m = make(map[string]map[string]map[string]*MeasurementDetails)
	}
	if _, ok := s.m[db]; !ok {
		s.m[db] = make(map[string]map[string]*MeasurementDetails)
	}
	if _, ok := s.m[db][rp]; !ok {
		s.m[db][rp] = make(map[string]*MeasurementDetails)
	}
	if _, ok := s.m[db][rp][measurement]; !ok {
		s.m[db][rp][measurement] = &MeasurementDetails{}
	}
	s.m[db][rp][measurement].size += size
}

func (s *MeasurementSizes) ForEach(f func(db, rp, measurement string, detail MeasurementDetails)) {
	dbKeys := make([]string, 0, len(s.m))
	for db, _ := range s.m {
		dbKeys = append(dbKeys, db)
	}
	sort.Strings(dbKeys)
	for _, db := range dbKeys {
		rpKeys := make([]string, 0, len(s.m[db]))
		for rp, _ := range s.m[db] {
			rpKeys = append(rpKeys, rp)
		}
		sort.Strings(rpKeys)
		for _, rp := range rpKeys {
			mKeys := make([]string, 0, len(s.m[db][rp]))
			for m, _ := range s.m[db][rp] {
				mKeys = append(mKeys, m)
			}
			sort.Strings(mKeys)
			for _, m := range mKeys {
				f(db, rp, m, *s.m[db][rp][m])
			}
		}
	}
}

type ProgressReporter struct {
	maxLength int
	w         io.Writer
}

func NewProgressReporter(w io.Writer) *ProgressReporter {
	return &ProgressReporter{w: w}
}

func (p *ProgressReporter) Report(line string) {
	if p.maxLength == 0 {
		fmt.Fprintf(p.w, "\n")
		p.maxLength = 1
	}
	for len(line) < p.maxLength {
		line += " "
	}
	p.maxLength = len(line)
	p.maxLength++
	fmt.Fprint(p.w, line+"\r")
}

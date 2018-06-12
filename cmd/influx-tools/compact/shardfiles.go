package compact

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"

	"github.com/influxdata/influxdb/cmd/influx-tools/internal/errlist"
	"github.com/influxdata/influxdb/tsdb/engine/tsm1"
)

type shardFiles struct {
	tsm       []string
	tombstone []string
	wal       []string
}

func newShardFiles(dataPath, walPath string) (s *shardFiles, err error) {
	s = &shardFiles{}

	s.tsm, err = filepath.Glob(filepath.Join(dataPath, fmt.Sprintf("*.%s", tsm1.TSMFileExtension)))
	if err != nil {
		return nil, fmt.Errorf("error reading tsm files: %v", err)
	}

	s.tombstone, err = filepath.Glob(filepath.Join(dataPath, fmt.Sprintf("*.%s", "tombstone")))
	if err != nil {
		return nil, fmt.Errorf("error reading tombstone files: %v", err)
	}

	s.wal, err = filepath.Glob(filepath.Join(walPath, fmt.Sprintf("*.%s", tsm1.WALFileExtension)))
	if err != nil {
		return nil, fmt.Errorf("error reading tombstone files: %v", err)
	}

	return s, nil
}

// replace replaces the existing shard files with temporary tsmFiles
func (s *shardFiles) replace(tsmFiles []string) ([]string, error) {
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

	// remove existing .tsm, .tombstone and .wal files
	for _, file := range s.tsm {
		errs.Add(os.Remove(file))
	}

	for _, file := range s.tombstone {
		errs.Add(os.Remove(file))
	}

	for _, file := range s.wal {
		errs.Add(os.Remove(file))
	}

	return newNames, errs.Err()
}

func (s *shardFiles) String() string {
	var sb bytes.Buffer
	sb.WriteString("TSM:\n")
	for _, f := range s.tsm {
		sb.WriteString("  ")
		sb.WriteString(f)
		sb.WriteByte('\n')
	}

	if len(s.tombstone) > 0 {
		sb.WriteString("\nTombstone:\n")
		for _, f := range s.tombstone {
			sb.WriteString("  ")
			sb.WriteString(f)
			sb.WriteByte('\n')
		}
	}

	if len(s.wal) > 0 {
		sb.WriteString("\nWAL:\n")
		for _, f := range s.wal {
			sb.WriteString("  ")
			sb.WriteString(f)
			sb.WriteByte('\n')
		}
	}

	return sb.String()
}

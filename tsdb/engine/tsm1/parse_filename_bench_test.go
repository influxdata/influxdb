package tsm1

import (
	"fmt"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
)

// Current implementation
func parseFileNameCurrent(name string) (int, int, error) {
	base := filepath.Base(name)
	idx := strings.Index(base, ".")
	if idx == -1 {
		return 0, 0, fmt.Errorf("file %s is named incorrectly", name)
	}

	id := base[:idx]

	idx = strings.Index(id, "-")
	if idx == -1 {
		return 0, 0, fmt.Errorf("file %s is named incorrectly", name)
	}

	generation, err := strconv.ParseUint(id[:idx], 10, 32)
	if err != nil {
		return 0, 0, fmt.Errorf("file %s is named incorrectly", name)
	}

	sequence, err := strconv.ParseUint(id[idx+1:], 10, 32)
	if err != nil {
		return 0, 0, fmt.Errorf("file %s is named incorrectly", name)
	}

	return int(generation), int(sequence), nil
}

var tsmFileRegex = regexp.MustCompile(`(\d+)-(\d+)\.tsm$`)

func parseFileNameRegex(name string) (int, int, error) {
	matches := tsmFileRegex.FindStringSubmatch(name)
	if len(matches) != 3 {
		return 0, 0, fmt.Errorf("file %s is named incorrectly", name)
	}

	generation, err := strconv.ParseUint(matches[1], 10, 32)
	if err != nil {
		return 0, 0, fmt.Errorf("file %s is named incorrectly", name)
	}

	sequence, err := strconv.ParseUint(matches[2], 10, 32)
	if err != nil {
		return 0, 0, fmt.Errorf("file %s is named incorrectly", name)
	}

	return int(generation), int(sequence), nil
}

func parseFileNameScanf(name string) (int, int, error) {
	base := filepath.Base(name)
	var generation, sequence int
	n, err := fmt.Sscanf(base, "%d-%d.tsm", &generation, &sequence)
	if n != 2 || err != nil {
		return 0, 0, fmt.Errorf("file %s is named incorrectly: %v", name, err)
	}
	return generation, sequence, nil
}

var testFilenames = []string{
	"000001-000001.tsm",
	"/var/lib/influxdb/data/mydb/autogen/1/000042-000123.tsm",
	"000999-001234.tsm",
	"/long/path/to/influxdb/data/database/retention/shard/001337-005678.tsm",
	"123456-789012.tsm",
}

func BenchmarkParseFileNameCurrent(b *testing.B) {
	for i := 0; i < b.N; i++ {
		for _, filename := range testFilenames {
			_, _, _ = parseFileNameCurrent(filename)
		}
	}
}

func BenchmarkParseFileNameRegex(b *testing.B) {
	for i := 0; i < b.N; i++ {
		for _, filename := range testFilenames {
			_, _, _ = parseFileNameRegex(filename)
		}
	}
}

func BenchmarkParseFileNameScanf(b *testing.B) {
	for i := 0; i < b.N; i++ {
		for _, filename := range testFilenames {
			_, _, _ = parseFileNameScanf(filename)
		}
	}
}

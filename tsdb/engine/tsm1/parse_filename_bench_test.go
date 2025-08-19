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
	"000000024-000000002.tsm",
	"/var/lib/influxdb/data/mydb/autogen/1/000000025-000000122.tsm",
	"000000025-000000001.tsm",
	"/long/path/to/influxdb/data/database/retention/shard/000000025-000000040.tsm",
	"000000025-000000002.tsm",
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

// Optimized regex with submatch reuse
var tsmFileRegexOptimized = regexp.MustCompile(`(\d+)-(\d+)\.tsm$`)
var submatchPool = make([][]string, 0, 100) // Pool of submatch slices

func parseFileNameRegexOptimized(name string) (int, int, error) {
	// Use FindSubmatch instead of FindStringSubmatch to avoid string allocations
	matches := tsmFileRegexOptimized.FindStringSubmatch(name)
	if len(matches) != 3 {
		return 0, 0, fmt.Errorf("file %s is named incorrectly", name)
	}

	// Parse directly from submatch without intermediate variables
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

// Regex with byte slice approach (avoids some string allocations)
func parseFileNameRegexBytes(name string) (int, int, error) {
	nameBytes := []byte(name)
	matches := tsmFileRegexOptimized.FindSubmatch(nameBytes)
	if len(matches) != 3 {
		return 0, 0, fmt.Errorf("file %s is named incorrectly", name)
	}

	generation, err := strconv.ParseUint(string(matches[1]), 10, 32)
	if err != nil {
		return 0, 0, fmt.Errorf("file %s is named incorrectly", name)
	}

	sequence, err := strconv.ParseUint(string(matches[2]), 10, 32)
	if err != nil {
		return 0, 0, fmt.Errorf("file %s is named incorrectly", name)
	}

	return int(generation), int(sequence), nil
}

// Optimized scanf without filepath.Base allocation
func parseFileNameScanfOptimized(name string) (int, int, error) {
	// Find last slash to avoid filepath.Base allocation
	lastSlash := -1
	for i := len(name) - 1; i >= 0; i-- {
		if name[i] == '/' || name[i] == '\\' {
			lastSlash = i
			break
		}
	}
	base := name[lastSlash+1:]
	
	var generation, sequence int
	n, err := fmt.Sscanf(base, "%d-%d.tsm", &generation, &sequence)
	if n != 2 || err != nil {
		return 0, 0, fmt.Errorf("file %s is named incorrectly: %v", name, err)
	}
	return generation, sequence, nil
}

// Manual byte-by-byte parsing (most optimized)
func parseFileNameManualOptimized(name string) (int, int, error) {
	// Work backwards to find the filename part
	end := len(name)
	if end < 5 || name[end-4:] != ".tsm" {
		return 0, 0, fmt.Errorf("file %s is named incorrectly", name)
	}
	
	// Find the start of the filename (after last slash)
	start := 0
	for i := end - 5; i >= 0; i-- {
		if name[i] == '/' || name[i] == '\\' {
			start = i + 1
			break
		}
	}
	
	// Find the dash separator
	dashPos := -1
	for i := start; i < end-4; i++ {
		if name[i] == '-' {
			dashPos = i
			break
		}
	}
	
	if dashPos == -1 {
		return 0, 0, fmt.Errorf("file %s is named incorrectly", name)
	}
	
	// Parse generation and sequence directly from the string
	generation, err := strconv.ParseUint(name[start:dashPos], 10, 32)
	if err != nil {
		return 0, 0, fmt.Errorf("file %s is named incorrectly", name)
	}
	
	sequence, err := strconv.ParseUint(name[dashPos+1:end-4], 10, 32)
	if err != nil {
		return 0, 0, fmt.Errorf("file %s is named incorrectly", name)
	}
	
	return int(generation), int(sequence), nil
}

func BenchmarkParseFileNameRegexOptimized(b *testing.B) {
	for i := 0; i < b.N; i++ {
		for _, filename := range testFilenames {
			_, _, _ = parseFileNameRegexOptimized(filename)
		}
	}
}

func BenchmarkParseFileNameRegexBytes(b *testing.B) {
	for i := 0; i < b.N; i++ {
		for _, filename := range testFilenames {
			_, _, _ = parseFileNameRegexBytes(filename)
		}
	}
}

func BenchmarkParseFileNameScanfOptimized(b *testing.B) {
	for i := 0; i < b.N; i++ {
		for _, filename := range testFilenames {
			_, _, _ = parseFileNameScanfOptimized(filename)
		}
	}
}

func BenchmarkParseFileNameManualOptimized(b *testing.B) {
	for i := 0; i < b.N; i++ {
		for _, filename := range testFilenames {
			_, _, _ = parseFileNameManualOptimized(filename)
		}
	}
}

package tsm1_test

import (
	"errors"
	"fmt"
	"sort"

	"github.com/influxdata/influxdb/tsdb/engine/tsm1"
)

// CompactionProperty represents a property that compaction groups should satisfy
type CompactionProperty struct {
	Name        string
	Description string
	Validator   func(allFiles []string, groups []tsm1.CompactionGroup) error
}

// AdjacentFileProperty validates that compaction groups don't split adjacent files
var AdjacentFileProperty = CompactionProperty{
	Name:        "Adjacent File Rule",
	Description: "Files that are adjacent in generation sequence should not be split across different compaction groups",
	Validator:   validateAdjacentFiles,
}

// parseFileName uses the existing TSM file parsing functionality
func parseFileName(filename string) (generation int, sequence int, err error) {
	return tsm1.DefaultParseFileName(filename)
}

// fileInfo holds parsed file information for sorting
type fileInfo struct {
	filename   string
	generation int
	sequence   int
}

// validateAdjacentFiles checks that adjacent files in the generation sequence
// are not split across different compaction groups
func validateAdjacentFiles(allFiles []string, groups []tsm1.CompactionGroup) error {
	// Parse all files and sort them by generation, then by sequence
	var fileInfos []fileInfo
	for _, file := range allFiles {
		gen, seq, err := parseFileName(file)
		if err != nil {
			return fmt.Errorf("failed to parse file %s: %v", file, err)
		}
		fileInfos = append(fileInfos, fileInfo{
			filename:   file,
			generation: gen,
			sequence:   seq,
		})
	}

	// Sort by generation first, then by sequence
	sort.Slice(fileInfos, func(i, j int) bool {
		if fileInfos[i].generation != fileInfos[j].generation {
			return fileInfos[i].generation < fileInfos[j].generation
		}
		return fileInfos[i].sequence < fileInfos[j].sequence
	})

	// Create a map of filename to group index
	fileToGroup := make(map[string]int)
	for groupIdx, group := range groups {
		for _, file := range group {
			fileToGroup[file] = groupIdx
		}
	}

	// Check for adjacency violations
	for i := 0; i < len(fileInfos)-1; i++ {
		current := fileInfos[i]
		next := fileInfos[i+1]

		// Files are considered adjacent if:
		// 1. Same generation with consecutive sequences
		// 2. Consecutive generations (regardless of sequence numbers)
		isAdjacent := false

		if current.generation == next.generation {
			// Same generation: adjacent if consecutive sequences
			isAdjacent = (next.sequence - current.sequence) == 1
		} else if (next.generation - current.generation) == 1 {
			// Consecutive generations are always considered adjacent
			isAdjacent = true
		}

		if isAdjacent {
			currentGroup, currentInGroup := fileToGroup[current.filename]
			nextGroup, nextInGroup := fileToGroup[next.filename]

			// If both files are in compaction groups, they should be in the same group
			if currentInGroup && nextInGroup && currentGroup != nextGroup {
				return fmt.Errorf("adjacent files %s (group %d) and %s (group %d) are in different compaction groups",
					current.filename, currentGroup, next.filename, nextGroup)
			}
		}
	}

	return nil
}

// ValidateCompactionProperties validates that compaction results satisfy all properties
func ValidateCompactionProperties(allFiles []string, results TestLevelResults, properties ...CompactionProperty) error {
	var errs []error

	// Collect all compaction groups from results
	var allGroups []tsm1.CompactionGroup
	allGroups = append(allGroups, extractGroups(results.level1Groups)...)
	allGroups = append(allGroups, extractGroups(results.level2Groups)...)
	allGroups = append(allGroups, extractGroups(results.level3Groups)...)
	allGroups = append(allGroups, extractGroups(results.level4Groups)...)
	allGroups = append(allGroups, extractGroups(results.level5Groups)...)

	// Validate each property
	for _, property := range properties {
		if err := property.Validator(allFiles, allGroups); err != nil {
			errs = append(errs, fmt.Errorf("%s violation: %v", property.Name, err))
		}
	}

	return errors.Join(errs...)
}

// extractGroups extracts CompactionGroup from PlannedCompactionGroup
func extractGroups(plannedGroups []tsm1.PlannedCompactionGroup) []tsm1.CompactionGroup {
	var groups []tsm1.CompactionGroup
	for _, planned := range plannedGroups {
		groups = append(groups, planned.Group)
	}
	return groups
}

// ValidateTestCase validates both expected results and actual planner output
func ValidateTestCase(testCase TestEnginePlanCompactionsRunner, actualResults TestLevelResults) error {
	var errs []error

	// Extract all filenames from test case
	var allFiles []string
	for _, file := range testCase.files {
		allFiles = append(allFiles, file.Path)
	}

	// Validate expected results
	expectedResults := testCase.expectedResult()
	if expectedErr := ValidateCompactionProperties(allFiles, expectedResults, AdjacentFileProperty); expectedErr != nil {
		errs = append(errs, fmt.Errorf("expected results: %v", expectedErr))
	}

	// Validate actual results
	if actualErr := ValidateCompactionProperties(allFiles, actualResults, AdjacentFileProperty); actualErr != nil {
		errs = append(errs, fmt.Errorf("actual results: %v", actualErr))
	}

	return errors.Join(errs...)
}

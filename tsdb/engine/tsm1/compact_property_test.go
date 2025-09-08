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

// AdjacentFileProperty validates that compaction groups don't create gaps
var AdjacentFileProperty = CompactionProperty{
	Name:        "No Gaps Rule",
	Description: "Files should not have gaps within the same compaction level - if files A and C are in the same level group, any file B between them should also be in a group at the same level or higher",
	Validator:   validateNoGaps,
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

// validateNoGaps checks that there are no gaps within the same compaction level
// A gap occurs when files A and C are in the same level group, but file B (between A and C)
// is in a different level group, creating a "hole" in the sequence
func validateNoGaps(allFiles []string, groups []tsm1.CompactionGroup) error {
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

	// Group files by their compaction group
	groupToFiles := make(map[int][]fileInfo)
	for _, info := range fileInfos {
		if groupIdx, inGroup := fileToGroup[info.filename]; inGroup {
			groupToFiles[groupIdx] = append(groupToFiles[groupIdx], info)
		}
	}

	// For each group, check if there are gaps in the file sequence
	for groupIdx, filesInGroup := range groupToFiles {
		if len(filesInGroup) < 2 {
			continue // No gaps possible with less than 2 files
		}

		// Check for gaps between files in this group
		for i := 0; i < len(filesInGroup)-1; i++ {
			current := filesInGroup[i]
			next := filesInGroup[i+1]

			// Find all files between current and next in the sorted sequence
			currentPos := findFilePosition(current, fileInfos)
			nextPos := findFilePosition(next, fileInfos)

			// Check if there are files between current and next that are in different groups
			for pos := currentPos + 1; pos < nextPos; pos++ {
				betweenFile := fileInfos[pos]
				betweenGroup, inGroup := fileToGroup[betweenFile.filename]

				if inGroup && betweenGroup != groupIdx {
					return fmt.Errorf("gap detected: files %s and %s are in group %d, but file %s (between them) is in group %d",
						current.filename, next.filename, groupIdx, betweenFile.filename, betweenGroup)
				}
			}
		}
	}

	return nil
}

// findFilePosition returns the position of a file in the sorted fileInfos slice
func findFilePosition(target fileInfo, fileInfos []fileInfo) int {
	for i, info := range fileInfos {
		if info.filename == target.filename {
			return i
		}
	}
	return -1
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

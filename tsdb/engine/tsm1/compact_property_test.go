package tsm1_test

import (
	"errors"
	"fmt"

	"github.com/influxdata/influxdb/tsdb/engine/tsm1"
	"golang.org/x/exp/slices"
)

// CompactionProperty represents a property that compaction groups should satisfy
type CompactionProperty struct {
	Name        string
	Description string
	Validator   func(allFiles []string, groups []tsm1.CompactionGroup) error
}

// AdjacentFileProperty validates that compaction groups don't create gaps
var AdjacentFileProperty = CompactionProperty{
	Name:        "Adjacency Rule",
	Description: "Files should not have non-adjacent files within the same compaction level - if files A and C are in the same level group, any file B between them should also be in a group at the same level or higher",
	Validator:   validateNoGaps,
}

type fileInfo struct {
	gen      int
	seq      int
	index    int
	fileName string
}

// validateNoGaps checks that there are no gaps between compaction groups
// An adjacency violation occurs when files A and C are in different groups, but file B (between A and C)
// is also in a different group, creating overlapping or non-contiguous ranges
func validateNoGaps(allFiles []string, groups []tsm1.CompactionGroup) error {
	var inputFiles []fileInfo
	for i, file := range allFiles {
		gen, seq, err := tsm1.DefaultParseFileName(file)
		if err != nil {
			return err
		}
		inputFiles = append(inputFiles, fileInfo{
			gen:      gen,
			seq:      seq,
			index:    i,
			fileName: file,
		})
	}

	slices.SortFunc(inputFiles, func(a, b fileInfo) int {
		if a.gen != b.gen {
			return a.gen - b.gen
		}

		return a.seq - b.seq
	})

	var fileMap = make(map[string]fileInfo, len(inputFiles))
	for _, file := range inputFiles {
		fileMap[file.fileName] = file
	}

	for groupIndex, group := range groups {
		lastIndex := -1
		for _, file := range group {
			f, ok := fileMap[file]
			if !ok {
				return fmt.Errorf("file %s not found in group %d", file, groupIndex)
			}

			if lastIndex == -1 {
				lastIndex = f.index
			} else {
				// Check lastIndex wrt f.index
				if lastIndex+1 != f.index {
					return fmt.Errorf("file %s in compaction group %d violates adjacency policy", file, groupIndex+1)
				}
				lastIndex = f.index
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

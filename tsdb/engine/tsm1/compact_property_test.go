package tsm1_test

import (
	"errors"
	"fmt"
	"golang.org/x/exp/slices"

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
	Name:        "Adjacency Rule",
	Description: "Files should not have non-adjacent files within the same compaction level - if files A and C are in the same level group, any file B between them should also be in a group at the same level or higher",
	Validator:   validateFileAdjacency,
}

type fileInfo struct {
	filename   string
	generation int
	sequence   int
	index      int
}

// validateFileAdjacency checks that there are no adjacency violations between TSM files
// The following example will highlight an adjacency violation:
// Given the following list of files [01-01.tsm, 02-02.tsm, 03-03.tsm] let's say we have the following compaction plans created
// Group 1: [01-01.tsm, 03-03.tsm] & Group 2: [02-02.tsm]
// This violates file adjacency as the first compaction group sees [01-01.tsm, X, 03-03.tsm] and the second group: [X, 02-02.tsm, X]
// these are non-contiguous blocks, when the first group performs compaction we will have two files that are out of order compacted together.
// This rule is important to maintain the ordering of files, with improper ordering we cannot determine which point is the newest point when overwrites occur.
// We always want the newest write to win.
func validateFileAdjacency(allFiles []string, groups []tsm1.CompactionGroup) error {
	fileInfos := make([]*fileInfo, 0, len(allFiles))
	for _, file := range allFiles {
		gen, seq, err := tsm1.DefaultParseFileName(file)
		if err != nil {
			return fmt.Errorf("failed to parse file %s: %v", file, err)
		}
		fileInfos = append(fileInfos, &fileInfo{
			filename:   file,
			generation: gen,
			sequence:   seq,
		})
	}

	slices.SortFunc(fileInfos, func(a, b *fileInfo) int {
		if a.generation != b.generation {
			return a.generation - b.generation
		}

		return a.sequence - b.sequence
	})

	var fileMap = make(map[string]*fileInfo, len(fileInfos))
	for i, fi := range fileInfos {
		fi.index = i
		fileMap[fi.filename] = fi
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
	var allFiles = make([]string, len(testCase.files))
	for i, file := range testCase.files {
		allFiles[i] = file.Path
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

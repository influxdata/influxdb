package tsm1_test

import (
	"fmt"
	"time"

	"github.com/influxdata/influxdb/tsdb"
	"github.com/influxdata/influxdb/tsdb/engine/tsm1"
)

// CompactionTestScenario defines a test scenario with level sequence
type CompactionTestScenario struct {
	Name        string
	Description string
	Levels      []int // sequence of file levels (e.g., [4,5,2,2,4])
	// Variations
	SizeVariation   bool // vary file sizes
	PointsVariation bool // vary points per block
}

type TestLevelResults struct {
	level1Groups []tsm1.PlannedCompactionGroup
	level2Groups []tsm1.PlannedCompactionGroup
	level3Groups []tsm1.PlannedCompactionGroup
	level4Groups []tsm1.PlannedCompactionGroup
	level5Groups []tsm1.PlannedCompactionGroup
}

type TestEnginePlanCompactionsRunner struct {
	name              string
	files             []tsm1.ExtFileStat
	defaultBlockCount int // Default block count if member of files has FirstBlockCount of 0.
	// This is specifically used to adjust the modification time
	// so we can simulate the passage of time in tests
	testShardTime time.Duration
	// Each result is for the different plantypes
	expectedResult func() TestLevelResults
}

// generateCompactionTestScenarios creates comprehensive test scenarios
func generateCompactionTestScenarios() []CompactionTestScenario {
	scenarios := []CompactionTestScenario{}

	// Basic scenarios from the requirements
	scenarios = append(scenarios, CompactionTestScenario{
		Name: "basic_high_level", Description: "Basic high-level sequence",
		Levels: []int{4, 5, 4, 5, 4},
	})

	scenarios = append(scenarios, CompactionTestScenario{
		Name: "leading_low_with_high", Description: "Leading low-level with high-level",
		Levels: []int{2, 4, 5, 4, 5, 4},
	})

	scenarios = append(scenarios, CompactionTestScenario{
		Name: "leading_mixed_with_high", Description: "Leading mixed low-level with high-level",
		Levels: []int{3, 2, 4, 5, 4, 5, 4},
	})

	scenarios = append(scenarios, CompactionTestScenario{
		Name: "leading_high_trailing_low", Description: "Leading low + high + trailing low",
		Levels: []int{3, 2, 4, 5, 4, 5, 4, 2},
	})

	scenarios = append(scenarios, CompactionTestScenario{
		Name: "multiple_leading_trailing", Description: "Multiple leading low + high + trailing",
		Levels: []int{2, 2, 3, 2, 4, 5, 4, 5, 4, 2},
	})

	scenarios = append(scenarios, CompactionTestScenario{
		Name: "complex_nested_pattern", Description: "Complex nested pattern",
		Levels: []int{2, 2, 3, 2, 4, 5, 2, 4, 5, 4, 2},
	})

	scenarios = append(scenarios, CompactionTestScenario{
		Name: "very_complex_nested", Description: "Very complex nested",
		Levels: []int{2, 2, 3, 2, 4, 5, 2, 2, 4, 5, 3, 4, 2},
	})

	scenarios = append(scenarios, CompactionTestScenario{
		Name: "high_nested_trailing", Description: "High + nested + trailing",
		Levels: []int{4, 5, 2, 2, 4, 5, 3, 5, 4, 2, 2, 2},
	})

	scenarios = append(scenarios, CompactionTestScenario{
		Name: "maximum_complexity", Description: "Maximum complexity scenario",
		Levels: []int{4, 5, 2, 2, 4, 5, 3, 5, 2, 3, 4, 2, 2, 2, 5, 4, 5},
	})

	// Leading low-level cases (various run lengths)
	for runLength := 1; runLength <= 9; runLength++ {
		levels := make([]int, runLength)
		for i := 0; i < runLength; i++ {
			levels[i] = 2 // level 2 files
		}
		levels = append(levels, []int{4, 5, 4, 5, 4}...) // followed by high-level files

		scenarios = append(scenarios, CompactionTestScenario{
			Name:        fmt.Sprintf("leading_low_run_%d", runLength),
			Description: fmt.Sprintf("Leading low-level files (run of %d) followed by high-level", runLength),
			Levels:      levels,
		})
	}

	// Trailing low-level cases (various run lengths)
	for runLength := 1; runLength <= 9; runLength++ {
		levels := []int{4, 5, 4, 5, 4} // high-level files
		for i := 0; i < runLength; i++ {
			levels = append(levels, 2) // followed by level 2 files
		}

		scenarios = append(scenarios, CompactionTestScenario{
			Name:        fmt.Sprintf("trailing_low_run_%d", runLength),
			Description: fmt.Sprintf("High-level files followed by trailing low-level (run of %d)", runLength),
			Levels:      levels,
		})
	}

	// Single nested low-level cases
	nestedPositions := []int{1, 2, 3} // position to insert low-level files
	for _, pos := range nestedPositions {
		for runLength := 1; runLength <= 9; runLength++ {
			levels := []int{4, 5, 4, 5, 4}
			// Insert low-level files at specified position
			lowFiles := make([]int, runLength)
			for i := 0; i < runLength; i++ {
				lowFiles[i] = 2
			}
			// Split and insert
			if pos <= len(levels) {
				result := append(levels[:pos], lowFiles...)
				result = append(result, levels[pos:]...)

				scenarios = append(scenarios, CompactionTestScenario{
					Name:        fmt.Sprintf("nested_single_pos_%d_run_%d", pos, runLength),
					Description: fmt.Sprintf("Single nested low-level run (%d files) at position %d", runLength, pos),
					Levels:      result,
				})
			}
		}
	}

	// Multiple nested low-level cases
	multiNestedPatterns := [][]int{
		{4, 5, 2, 4, 5, 2, 4},                            // two single nested
		{4, 5, 2, 2, 4, 5, 2, 2, 4},                      // two double nested
		{4, 5, 2, 4, 5, 3, 2, 4},                         // mixed level nested
		{4, 5, 2, 2, 4, 5, 3, 3, 2, 4},                   // mixed run length nested
		{2, 4, 5, 2, 4, 5, 3, 4, 2},                      // leading + nested
		{2, 2, 4, 5, 2, 2, 4, 5, 3, 4, 2, 2},             // leading + multiple nested + trailing
		{3, 2, 4, 5, 2, 4, 5, 3, 2, 4, 2, 3},             // very complex pattern
		{2, 3, 4, 5, 2, 2, 3, 4, 5, 2, 3, 4, 5, 2, 2, 2}, // long complex pattern
	}

	for i, pattern := range multiNestedPatterns {
		scenarios = append(scenarios, CompactionTestScenario{
			Name:        fmt.Sprintf("multiple_nested_%d", i+1),
			Description: fmt.Sprintf("Multiple nested pattern %d", i+1),
			Levels:      pattern,
		})
	}

	// Add size and points variations to some scenarios
	for i := range scenarios {
		if i%3 == 0 {
			scenarios[i].SizeVariation = true
		}
		if i%5 == 0 {
			scenarios[i].PointsVariation = true
		}
	}

	return scenarios
}

// generateTestEnginePlanCompactionsRunners converts scenarios to test runners
func generateTestEnginePlanCompactionsRunners() []TestEnginePlanCompactionsRunner {
	scenarios := generateCompactionTestScenarios()
	runners := make([]TestEnginePlanCompactionsRunner, 0, len(scenarios)*3) // space for variations

	for _, scenario := range scenarios {
		// Base case
		runners = append(runners, createTestRunner(scenario, false, false))

		// Size variation if specified
		if scenario.SizeVariation {
			runners = append(runners, createTestRunner(scenario, true, false))
		}

		// Points variation if specified
		if scenario.PointsVariation {
			runners = append(runners, createTestRunner(scenario, false, true))
		}
	}

	return runners
}

// createTestRunner creates a testEnginePlanCompactionsRunner from a scenario
func createTestRunner(scenario CompactionTestScenario, sizeVar, pointsVar bool) TestEnginePlanCompactionsRunner {
	name := scenario.Name
	if sizeVar {
		name += "_size_var"
	}
	if pointsVar {
		name += "_points_var"
	}

	files := createTestExtFileStats(scenario.Levels, sizeVar, pointsVar)

	return TestEnginePlanCompactionsRunner{
		name:              name,
		files:             files,
		defaultBlockCount: tsdb.DefaultMaxPointsPerBlock,
		testShardTime:     -1, // Use default time
		expectedResult: func() TestLevelResults {
			// Generate expected results based on the scenario
			return generateExpectedResults(scenario.Levels, sizeVar, pointsVar)
		},
	}
}


// createTestExtFileStats generates ExtFileStat slice from level sequence
func createTestExtFileStats(levels []int, sizeVar, pointsVar bool) []tsm1.ExtFileStat {
	files := make([]tsm1.ExtFileStat, len(levels))

	for i, level := range levels {
		// Generate filename: generation-level.tsm
		// Use generation based on position to create realistic scenarios
		generation := (i / 3) + 1 // group every 3 files into same generation roughly
		filename := fmt.Sprintf("%06d-%02d.tsm", generation, level)

		// Base file size
		var size int64 = 256 * 1024 * 1024 // 256MB default

		if sizeVar {
			// Vary sizes realistically based on level
			switch level {
			case 1:
				size = int64(1 * 1024 * 1024) // 1MB for level 1
			case 2:
				size = int64(16 * 1024 * 1024) // 16MB for level 2
			case 3:
				size = int64(64 * 1024 * 1024) // 64MB for level 3
			case 4:
				size = int64(256 * 1024 * 1024) // 256MB for level 4
			case 5:
				size = int64(512 * 1024 * 1024) // 512MB for level 5
			default:
				size = int64(128 * 1024 * 1024) // 128MB default
			}
			// Add some variation based on position
			variation := int64(i%3-1) * (size / 10) // ±10% variation
			size += variation
			if size < 1024*1024 {
				size = 1024 * 1024 // minimum 1MB
			}
		}

		// Points per block
		pointsPerBlock := tsdb.DefaultMaxPointsPerBlock
		if pointsVar {
			switch i % 4 {
			case 0:
				pointsPerBlock = tsdb.DefaultMaxPointsPerBlock
			case 1:
				pointsPerBlock = tsdb.DefaultAggressiveMaxPointsPerBlock
			case 2:
				pointsPerBlock = tsdb.DefaultMaxPointsPerBlock / 2
			case 3:
				pointsPerBlock = tsdb.DefaultMaxPointsPerBlock * 3 / 4
			}
		}

		files[i] = tsm1.ExtFileStat{
			FileStat: tsm1.FileStat{
				Path: filename,
				Size: uint32(size),
			},
			FirstBlockCount: pointsPerBlock,
		}
	}

	return files
}

// generateExpectedResults creates expected test results based on level sequence
func generateExpectedResults(levels []int, sizeVar, pointsVar bool) TestLevelResults {
	// This is a simplified expectation generator
	// In practice, you'd want to implement the actual planning logic expectations
	// based on your understanding of when files should be compacted

	results := TestLevelResults{}

	// Count high-level files (level 4+) that should be compacted
	highLevelFiles := []string{}
	for i, level := range levels {
		if level >= 4 {
			generation := (i / 3) + 1
			filename := fmt.Sprintf("%06d-%02d.tsm", generation, level)
			highLevelFiles = append(highLevelFiles, filename)
		}
	}

	// Determine expected points per block for the compaction
	expectedPointsPerBlock := tsdb.DefaultMaxPointsPerBlock
	if len(levels) > 10 {
		// For complex scenarios with many files, use aggressive compaction
		expectedPointsPerBlock = tsdb.DefaultAggressiveMaxPointsPerBlock
	}

	// Create expected compaction groups based on file levels
	if len(highLevelFiles) >= 4 {
		// Should have level 5 compaction (full compaction of high-level files)
		results.level5Groups = []tsm1.PlannedCompactionGroup{
			{
				highLevelFiles,
				expectedPointsPerBlock,
			},
		}
	} else if len(highLevelFiles) > 0 {
		// Should have level 4 compaction
		results.level4Groups = []tsm1.PlannedCompactionGroup{
			{
				highLevelFiles,
				expectedPointsPerBlock,
			},
		}
	}

	// Add level-specific compactions for lower level files
	level2Files := []string{}
	level3Files := []string{}
	for i, level := range levels {
		generation := (i / 3) + 1
		filename := fmt.Sprintf("%06d-%02d.tsm", generation, level)
		switch level {
		case 2:
			level2Files = append(level2Files, filename)
		case 3:
			level3Files = append(level3Files, filename)
		}
	}

	if len(level2Files) >= 4 {
		results.level2Groups = []tsm1.PlannedCompactionGroup{
			{
				level2Files[:4], // Take first 4 files
				expectedPointsPerBlock,
			},
		}
	}

	if len(level3Files) >= 4 {
		results.level3Groups = []tsm1.PlannedCompactionGroup{
			{
				level3Files[:4], // Take first 4 files
				expectedPointsPerBlock,
			},
		}
	}

	return results
}

// AddGeneratedCompactionTestCases appends generated test cases to the existing tests slice
// This function should be called in TestEnginePlanCompactions after the static tests are defined
func AddGeneratedCompactionTestCases(existingTests []TestEnginePlanCompactionsRunner) []TestEnginePlanCompactionsRunner {
	generatedTests := generateTestEnginePlanCompactionsRunners()
	return append(existingTests, generatedTests...)
}

// Example of how to integrate into TestEnginePlanCompactions:
/*
In compact_test.go, after the existing tests slice definition (around line 5270), add:

	// Add generated test cases for comprehensive level sequence testing
	tests = addGeneratedCompactionTestCases(tests)

Then the existing for loop continues as normal.
*/

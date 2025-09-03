package tsm1_test

import (
	"github.com/influxdata/influxdb/tsdb"
	"github.com/influxdata/influxdb/tsdb/engine/tsm1"
)

// AddStaticCompactionTestCases appends static test cases to the existing tests slice
// These test the compaction planning logic for various file level patterns
// including leading, trailing, and nested low-level files
func AddStaticCompactionTestCases(existingTests []TestEnginePlanCompactionsRunner) []TestEnginePlanCompactionsRunner {
	staticTests := []TestEnginePlanCompactionsRunner{
		// Basic high-level sequences
		{
			name: "basic_high_level_4_5_4_5_4",
			files: []tsm1.ExtFileStat{
				{FileStat: tsm1.FileStat{Path: "000001-04.tsm", Size: 256 * 1024 * 1024}, FirstBlockCount: tsdb.DefaultMaxPointsPerBlock},
				{FileStat: tsm1.FileStat{Path: "000002-05.tsm", Size: 256 * 1024 * 1024}, FirstBlockCount: tsdb.DefaultMaxPointsPerBlock},
				{FileStat: tsm1.FileStat{Path: "000003-04.tsm", Size: 256 * 1024 * 1024}, FirstBlockCount: tsdb.DefaultMaxPointsPerBlock},
				{FileStat: tsm1.FileStat{Path: "000004-05.tsm", Size: 256 * 1024 * 1024}, FirstBlockCount: tsdb.DefaultMaxPointsPerBlock},
				{FileStat: tsm1.FileStat{Path: "000005-04.tsm", Size: 256 * 1024 * 1024}, FirstBlockCount: tsdb.DefaultMaxPointsPerBlock},
			},
			defaultBlockCount: tsdb.DefaultMaxPointsPerBlock,
			testShardTime:     -1,
			expectedResult: func() TestLevelResults {
				return TestLevelResults{
					level4Groups: []tsm1.PlannedCompactionGroup{
						{
							tsm1.CompactionGroup{"000001-04.tsm", "000002-05.tsm", "000003-04.tsm", "000004-05.tsm"},
							tsdb.DefaultMaxPointsPerBlock,
						},
					},
				}
			},
		},

		// Leading low-level files (should be excluded from high-level compaction)
		{
			name: "leading_low_2_4_5_4_5_4",
			files: []tsm1.ExtFileStat{
				{FileStat: tsm1.FileStat{Path: "000001-02.tsm", Size: 16 * 1024 * 1024}, FirstBlockCount: tsdb.DefaultMaxPointsPerBlock},
				{FileStat: tsm1.FileStat{Path: "000002-04.tsm", Size: 256 * 1024 * 1024}, FirstBlockCount: tsdb.DefaultMaxPointsPerBlock},
				{FileStat: tsm1.FileStat{Path: "000003-05.tsm", Size: 256 * 1024 * 1024}, FirstBlockCount: tsdb.DefaultMaxPointsPerBlock},
				{FileStat: tsm1.FileStat{Path: "000004-04.tsm", Size: 256 * 1024 * 1024}, FirstBlockCount: tsdb.DefaultMaxPointsPerBlock},
				{FileStat: tsm1.FileStat{Path: "000005-05.tsm", Size: 256 * 1024 * 1024}, FirstBlockCount: tsdb.DefaultMaxPointsPerBlock},
				{FileStat: tsm1.FileStat{Path: "000006-04.tsm", Size: 256 * 1024 * 1024}, FirstBlockCount: tsdb.DefaultMaxPointsPerBlock},
			},
			defaultBlockCount: tsdb.DefaultMaxPointsPerBlock,
			testShardTime:     -1,
			expectedResult: func() TestLevelResults {
				return TestLevelResults{
					level4Groups: []tsm1.PlannedCompactionGroup{
						{
							tsm1.CompactionGroup{"000001-02.tsm", "000002-04.tsm", "000003-05.tsm", "000004-04.tsm"},
							tsdb.DefaultMaxPointsPerBlock,
						},
					},
					level5Groups: []tsm1.PlannedCompactionGroup{
						{
							tsm1.CompactionGroup{"000005-05.tsm", "000006-04.tsm"},
							tsdb.DefaultMaxPointsPerBlock,
						},
					},
				}
			},
		},

		// Multiple leading low-level files
		{
			name: "leading_low_run_2_2_4_5_4_5_4",
			files: []tsm1.ExtFileStat{
				{FileStat: tsm1.FileStat{Path: "000001-02.tsm", Size: 16 * 1024 * 1024}, FirstBlockCount: tsdb.DefaultMaxPointsPerBlock},
				{FileStat: tsm1.FileStat{Path: "000002-02.tsm", Size: 16 * 1024 * 1024}, FirstBlockCount: tsdb.DefaultMaxPointsPerBlock},
				{FileStat: tsm1.FileStat{Path: "000003-04.tsm", Size: 256 * 1024 * 1024}, FirstBlockCount: tsdb.DefaultMaxPointsPerBlock},
				{FileStat: tsm1.FileStat{Path: "000004-05.tsm", Size: 256 * 1024 * 1024}, FirstBlockCount: tsdb.DefaultMaxPointsPerBlock},
				{FileStat: tsm1.FileStat{Path: "000005-04.tsm", Size: 256 * 1024 * 1024}, FirstBlockCount: tsdb.DefaultMaxPointsPerBlock},
				{FileStat: tsm1.FileStat{Path: "000006-05.tsm", Size: 256 * 1024 * 1024}, FirstBlockCount: tsdb.DefaultMaxPointsPerBlock},
				{FileStat: tsm1.FileStat{Path: "000007-04.tsm", Size: 256 * 1024 * 1024}, FirstBlockCount: tsdb.DefaultMaxPointsPerBlock},
			},
			defaultBlockCount: tsdb.DefaultMaxPointsPerBlock,
			testShardTime:     -1,
			expectedResult: func() TestLevelResults {
				return TestLevelResults{
					level4Groups: []tsm1.PlannedCompactionGroup{
						{
							tsm1.CompactionGroup{"000001-02.tsm", "000002-02.tsm", "000003-04.tsm", "000004-05.tsm"},
							tsdb.DefaultMaxPointsPerBlock,
						},
					},
					level5Groups: []tsm1.PlannedCompactionGroup{
						{
							tsm1.CompactionGroup{"000005-04.tsm", "000006-05.tsm", "000007-04.tsm"},
							tsdb.DefaultMaxPointsPerBlock,
						},
					},
				}
			},
		},

		// Trailing low-level files (should be excluded from high-level compaction)
		{
			name: "trailing_low_4_5_4_5_4_2",
			files: []tsm1.ExtFileStat{
				{FileStat: tsm1.FileStat{Path: "000001-04.tsm", Size: 256 * 1024 * 1024}, FirstBlockCount: tsdb.DefaultMaxPointsPerBlock},
				{FileStat: tsm1.FileStat{Path: "000002-05.tsm", Size: 256 * 1024 * 1024}, FirstBlockCount: tsdb.DefaultMaxPointsPerBlock},
				{FileStat: tsm1.FileStat{Path: "000003-04.tsm", Size: 256 * 1024 * 1024}, FirstBlockCount: tsdb.DefaultMaxPointsPerBlock},
				{FileStat: tsm1.FileStat{Path: "000004-05.tsm", Size: 256 * 1024 * 1024}, FirstBlockCount: tsdb.DefaultMaxPointsPerBlock},
				{FileStat: tsm1.FileStat{Path: "000005-04.tsm", Size: 256 * 1024 * 1024}, FirstBlockCount: tsdb.DefaultMaxPointsPerBlock},
				{FileStat: tsm1.FileStat{Path: "000006-02.tsm", Size: 16 * 1024 * 1024}, FirstBlockCount: tsdb.DefaultMaxPointsPerBlock},
			},
			defaultBlockCount: tsdb.DefaultMaxPointsPerBlock,
			testShardTime:     -1,
			expectedResult: func() TestLevelResults {
				return TestLevelResults{
					level4Groups: []tsm1.PlannedCompactionGroup{
						{
							tsm1.CompactionGroup{"000001-04.tsm", "000002-05.tsm", "000003-04.tsm", "000004-05.tsm"},
							tsdb.DefaultMaxPointsPerBlock,
						},
					},
					level5Groups: []tsm1.PlannedCompactionGroup{
						{
							tsm1.CompactionGroup{"000005-04.tsm", "000006-02.tsm"},
							tsdb.DefaultMaxPointsPerBlock,
						},
					},
				}
			},
		},

		// Multiple trailing low-level files
		{
			name: "trailing_low_run_4_5_4_4_2_2",
			files: []tsm1.ExtFileStat{
				{FileStat: tsm1.FileStat{Path: "000001-04.tsm", Size: 256 * 1024 * 1024}, FirstBlockCount: tsdb.DefaultMaxPointsPerBlock},
				{FileStat: tsm1.FileStat{Path: "000002-05.tsm", Size: 256 * 1024 * 1024}, FirstBlockCount: tsdb.DefaultMaxPointsPerBlock},
				{FileStat: tsm1.FileStat{Path: "000003-04.tsm", Size: 256 * 1024 * 1024}, FirstBlockCount: tsdb.DefaultMaxPointsPerBlock},
				{FileStat: tsm1.FileStat{Path: "000003-05.tsm", Size: 256 * 1024 * 1024}, FirstBlockCount: tsdb.DefaultMaxPointsPerBlock},
				{FileStat: tsm1.FileStat{Path: "000004-05.tsm", Size: 256 * 1024 * 1024}, FirstBlockCount: tsdb.DefaultMaxPointsPerBlock},
				{FileStat: tsm1.FileStat{Path: "000005-02.tsm", Size: 16 * 1024 * 1024}, FirstBlockCount: 200},
				{FileStat: tsm1.FileStat{Path: "000006-02.tsm", Size: 16 * 1024 * 1024}, FirstBlockCount: 300},
			},
			defaultBlockCount: tsdb.DefaultMaxPointsPerBlock,
			testShardTime:     -1,
			expectedResult: func() TestLevelResults {
				return TestLevelResults{
					level4Groups: []tsm1.PlannedCompactionGroup{
						{
							tsm1.CompactionGroup{"000001-04.tsm", "000002-05.tsm", "000003-04.tsm", "000003-05.tsm", "000004-05.tsm"},
							tsdb.DefaultMaxPointsPerBlock,
						},
					},
				}
			},
		},

		// Nested low-level files (should be included in high-level compaction)
		{
			name: "nested_4_5_2_4_5",
			files: []tsm1.ExtFileStat{
				{FileStat: tsm1.FileStat{Path: "000001-04.tsm", Size: 256 * 1024 * 1024}, FirstBlockCount: tsdb.DefaultMaxPointsPerBlock},
				{FileStat: tsm1.FileStat{Path: "000002-05.tsm", Size: 256 * 1024 * 1024}, FirstBlockCount: tsdb.DefaultMaxPointsPerBlock},
				{FileStat: tsm1.FileStat{Path: "000003-02.tsm", Size: 16 * 1024 * 1024}, FirstBlockCount: tsdb.DefaultMaxPointsPerBlock},
				{FileStat: tsm1.FileStat{Path: "000004-04.tsm", Size: 256 * 1024 * 1024}, FirstBlockCount: tsdb.DefaultMaxPointsPerBlock},
				{FileStat: tsm1.FileStat{Path: "000005-05.tsm", Size: 256 * 1024 * 1024}, FirstBlockCount: tsdb.DefaultMaxPointsPerBlock},
			},
			defaultBlockCount: tsdb.DefaultMaxPointsPerBlock,
			testShardTime:     -1,
			expectedResult: func() TestLevelResults {
				return TestLevelResults{
					level5Groups: []tsm1.PlannedCompactionGroup{
						{
							tsm1.CompactionGroup{"000001-04.tsm", "000002-05.tsm", "000003-02.tsm", "000004-04.tsm", "000005-05.tsm"},
							tsdb.DefaultMaxPointsPerBlock,
						},
					},
				}
			},
		},

		// Multiple nested low-level files
		{
			name: "nested_4_5_2_2_4_5",
			files: []tsm1.ExtFileStat{
				{FileStat: tsm1.FileStat{Path: "000001-04.tsm", Size: 256 * 1024 * 1024}, FirstBlockCount: tsdb.DefaultMaxPointsPerBlock},
				{FileStat: tsm1.FileStat{Path: "000002-05.tsm", Size: 256 * 1024 * 1024}, FirstBlockCount: tsdb.DefaultMaxPointsPerBlock},
				{FileStat: tsm1.FileStat{Path: "000003-02.tsm", Size: 16 * 1024 * 1024}, FirstBlockCount: tsdb.DefaultMaxPointsPerBlock},
				{FileStat: tsm1.FileStat{Path: "000004-02.tsm", Size: 16 * 1024 * 1024}, FirstBlockCount: tsdb.DefaultMaxPointsPerBlock},
				{FileStat: tsm1.FileStat{Path: "000005-04.tsm", Size: 256 * 1024 * 1024}, FirstBlockCount: tsdb.DefaultMaxPointsPerBlock},
				{FileStat: tsm1.FileStat{Path: "000006-05.tsm", Size: 256 * 1024 * 1024}, FirstBlockCount: tsdb.DefaultMaxPointsPerBlock},
			},
			defaultBlockCount: tsdb.DefaultMaxPointsPerBlock,
			testShardTime:     -1,
			expectedResult: func() TestLevelResults {
				return TestLevelResults{
					level4Groups: []tsm1.PlannedCompactionGroup{
						{
							tsm1.CompactionGroup{"000003-02.tsm", "000004-02.tsm", "000005-04.tsm", "000006-05.tsm"},
							tsdb.DefaultMaxPointsPerBlock,
						},
					},
					level5Groups: []tsm1.PlannedCompactionGroup{
						{
							tsm1.CompactionGroup{"000001-04.tsm", "000002-05.tsm"},
							tsdb.DefaultMaxPointsPerBlock,
						},
					},
				}
			},
		},

		// Complex nested pattern - multiple nested sections
		{
			name: "complex_nested_4_5_2_4_5_2_4",
			files: []tsm1.ExtFileStat{
				{FileStat: tsm1.FileStat{Path: "000001-04.tsm", Size: 256 * 1024 * 1024}, FirstBlockCount: tsdb.DefaultMaxPointsPerBlock},
				{FileStat: tsm1.FileStat{Path: "000002-05.tsm", Size: 256 * 1024 * 1024}, FirstBlockCount: tsdb.DefaultMaxPointsPerBlock},
				{FileStat: tsm1.FileStat{Path: "000003-02.tsm", Size: 16 * 1024 * 1024}, FirstBlockCount: tsdb.DefaultMaxPointsPerBlock},
				{FileStat: tsm1.FileStat{Path: "000004-04.tsm", Size: 256 * 1024 * 1024}, FirstBlockCount: tsdb.DefaultMaxPointsPerBlock},
				{FileStat: tsm1.FileStat{Path: "000005-05.tsm", Size: 256 * 1024 * 1024}, FirstBlockCount: tsdb.DefaultMaxPointsPerBlock},
				{FileStat: tsm1.FileStat{Path: "000006-02.tsm", Size: 16 * 1024 * 1024}, FirstBlockCount: tsdb.DefaultMaxPointsPerBlock},
				{FileStat: tsm1.FileStat{Path: "000007-04.tsm", Size: 256 * 1024 * 1024}, FirstBlockCount: tsdb.DefaultMaxPointsPerBlock},
			},
			defaultBlockCount: tsdb.DefaultMaxPointsPerBlock,
			testShardTime:     -1,
			expectedResult: func() TestLevelResults {
				return TestLevelResults{
					level4Groups: []tsm1.PlannedCompactionGroup{
						{
							tsm1.CompactionGroup{"000003-02.tsm", "000004-04.tsm", "000005-05.tsm", "000006-02.tsm"},
							tsdb.DefaultMaxPointsPerBlock,
						},
					},
					level5Groups: []tsm1.PlannedCompactionGroup{
						{
							tsm1.CompactionGroup{"000001-04.tsm", "000002-05.tsm", "000007-04.tsm"},
							tsdb.DefaultMaxPointsPerBlock,
						},
					},
				}
			},
		},

		// Mixed: leading + nested + trailing
		{
			name: "mixed_leading_nested_trailing_2_4_5_2_4_5_2",
			files: []tsm1.ExtFileStat{
				{FileStat: tsm1.FileStat{Path: "000001-02.tsm", Size: 16 * 1024 * 1024}, FirstBlockCount: tsdb.DefaultMaxPointsPerBlock},
				{FileStat: tsm1.FileStat{Path: "000002-04.tsm", Size: 256 * 1024 * 1024}, FirstBlockCount: tsdb.DefaultMaxPointsPerBlock},
				{FileStat: tsm1.FileStat{Path: "000003-05.tsm", Size: 256 * 1024 * 1024}, FirstBlockCount: tsdb.DefaultMaxPointsPerBlock},
				{FileStat: tsm1.FileStat{Path: "000004-02.tsm", Size: 16 * 1024 * 1024}, FirstBlockCount: tsdb.DefaultMaxPointsPerBlock},
				{FileStat: tsm1.FileStat{Path: "000005-04.tsm", Size: 256 * 1024 * 1024}, FirstBlockCount: tsdb.DefaultMaxPointsPerBlock},
				{FileStat: tsm1.FileStat{Path: "000006-05.tsm", Size: 256 * 1024 * 1024}, FirstBlockCount: tsdb.DefaultMaxPointsPerBlock},
				{FileStat: tsm1.FileStat{Path: "000007-02.tsm", Size: 16 * 1024 * 1024}, FirstBlockCount: tsdb.DefaultMaxPointsPerBlock},
			},
			defaultBlockCount: tsdb.DefaultMaxPointsPerBlock,
			testShardTime:     -1,
			expectedResult: func() TestLevelResults {
				return TestLevelResults{
					level5Groups: []tsm1.PlannedCompactionGroup{
						{
							tsm1.CompactionGroup{"000002-04.tsm", "000003-05.tsm", "000004-02.tsm", "000005-04.tsm", "000006-05.tsm", "000007-02.tsm"},
							tsdb.DefaultMaxPointsPerBlock,
						},
					},
				}
			},
		},

		// The original problem case: high-level files with rogue trailing low-level
		{
			name: "original_problem_case_01_04_through_04_04_with_05_02",
			files: []tsm1.ExtFileStat{
				{FileStat: tsm1.FileStat{Path: "000001-04.tsm", Size: 256 * 1024 * 1024}, FirstBlockCount: tsdb.DefaultMaxPointsPerBlock},
				{FileStat: tsm1.FileStat{Path: "000002-04.tsm", Size: 256 * 1024 * 1024}, FirstBlockCount: tsdb.DefaultMaxPointsPerBlock},
				{FileStat: tsm1.FileStat{Path: "000003-04.tsm", Size: 256 * 1024 * 1024}, FirstBlockCount: tsdb.DefaultMaxPointsPerBlock},
				{FileStat: tsm1.FileStat{Path: "000004-04.tsm", Size: 256 * 1024 * 1024}, FirstBlockCount: tsdb.DefaultMaxPointsPerBlock},
				{FileStat: tsm1.FileStat{Path: "000005-02.tsm", Size: 16 * 1024 * 1024}, FirstBlockCount: tsdb.DefaultMaxPointsPerBlock},
			},
			defaultBlockCount: tsdb.DefaultMaxPointsPerBlock,
			testShardTime:     -1,
			expectedResult: func() TestLevelResults {
				return TestLevelResults{
					level4Groups: []tsm1.PlannedCompactionGroup{
						{
							tsm1.CompactionGroup{"000001-04.tsm", "000002-04.tsm", "000003-04.tsm", "000004-04.tsm"},
							tsdb.DefaultMaxPointsPerBlock,
						},
					},
				}
			},
		},

		// Lower block count tests
		{
			name: "basic_high_level_4_5_4_5_4_lower_block_count",
			files: []tsm1.ExtFileStat{
				{FileStat: tsm1.FileStat{Path: "000001-04.tsm", Size: 256 * 1024 * 1024}, FirstBlockCount: 200},
				{FileStat: tsm1.FileStat{Path: "000002-05.tsm", Size: 256 * 1024 * 1024}, FirstBlockCount: 200},
				{FileStat: tsm1.FileStat{Path: "000003-04.tsm", Size: 256 * 1024 * 1024}, FirstBlockCount: 200},
				{FileStat: tsm1.FileStat{Path: "000004-05.tsm", Size: 256 * 1024 * 1024}, FirstBlockCount: 200},
				{FileStat: tsm1.FileStat{Path: "000005-04.tsm", Size: 256 * 1024 * 1024}, FirstBlockCount: 200},
			},
			defaultBlockCount: tsdb.DefaultMaxPointsPerBlock,
			testShardTime:     -1,
			expectedResult: func() TestLevelResults {
				return TestLevelResults{
					level4Groups: []tsm1.PlannedCompactionGroup{
						{
							tsm1.CompactionGroup{"000001-04.tsm", "000002-05.tsm", "000003-04.tsm", "000004-05.tsm"},
							tsdb.DefaultMaxPointsPerBlock,
						},
					},
				}
			},
		},

		// Leading low-level files (should be excluded from high-level compaction)
		{
			name: "leading_low_2_4_5_4_5_4_lower_block_count",
			files: []tsm1.ExtFileStat{
				{FileStat: tsm1.FileStat{Path: "000001-02.tsm", Size: 16 * 1024 * 1024}, FirstBlockCount: 200},
				{FileStat: tsm1.FileStat{Path: "000002-04.tsm", Size: 256 * 1024 * 1024}, FirstBlockCount: 200},
				{FileStat: tsm1.FileStat{Path: "000003-05.tsm", Size: 256 * 1024 * 1024}, FirstBlockCount: 200},
				{FileStat: tsm1.FileStat{Path: "000004-04.tsm", Size: 256 * 1024 * 1024}, FirstBlockCount: 200},
				{FileStat: tsm1.FileStat{Path: "000005-05.tsm", Size: 256 * 1024 * 1024}, FirstBlockCount: 200},
				{FileStat: tsm1.FileStat{Path: "000006-04.tsm", Size: 256 * 1024 * 1024}, FirstBlockCount: 200},
			},
			defaultBlockCount: tsdb.DefaultMaxPointsPerBlock,
			testShardTime:     -1,
			expectedResult: func() TestLevelResults {
				return TestLevelResults{
					level4Groups: []tsm1.PlannedCompactionGroup{
						{
							tsm1.CompactionGroup{"000001-02.tsm", "000002-04.tsm", "000003-05.tsm", "000004-04.tsm"},
							tsdb.DefaultMaxPointsPerBlock,
						},
					},
					level5Groups: []tsm1.PlannedCompactionGroup{
						{
							tsm1.CompactionGroup{"000005-05.tsm", "000006-04.tsm"},
							tsdb.DefaultMaxPointsPerBlock,
						},
					},
				}
			},
		},

		// Multiple leading low-level files
		{
			name: "leading_low_run_2_2_4_5_4_5_4_lower_block_count",
			files: []tsm1.ExtFileStat{
				{FileStat: tsm1.FileStat{Path: "000001-02.tsm", Size: 16 * 1024 * 1024}, FirstBlockCount: 200},
				{FileStat: tsm1.FileStat{Path: "000002-02.tsm", Size: 16 * 1024 * 1024}, FirstBlockCount: 200},
				{FileStat: tsm1.FileStat{Path: "000003-04.tsm", Size: 256 * 1024 * 1024}, FirstBlockCount: 200},
				{FileStat: tsm1.FileStat{Path: "000004-05.tsm", Size: 256 * 1024 * 1024}, FirstBlockCount: 200},
				{FileStat: tsm1.FileStat{Path: "000005-04.tsm", Size: 256 * 1024 * 1024}, FirstBlockCount: 200},
				{FileStat: tsm1.FileStat{Path: "000006-05.tsm", Size: 256 * 1024 * 1024}, FirstBlockCount: 200},
				{FileStat: tsm1.FileStat{Path: "000007-04.tsm", Size: 256 * 1024 * 1024}, FirstBlockCount: 200},
			},
			defaultBlockCount: tsdb.DefaultMaxPointsPerBlock,
			testShardTime:     -1,
			expectedResult: func() TestLevelResults {
				return TestLevelResults{
					level4Groups: []tsm1.PlannedCompactionGroup{
						{
							tsm1.CompactionGroup{"000001-02.tsm", "000002-02.tsm", "000003-04.tsm", "000004-05.tsm"},
							tsdb.DefaultMaxPointsPerBlock,
						},
					},
					level5Groups: []tsm1.PlannedCompactionGroup{
						{
							tsm1.CompactionGroup{"000005-04.tsm", "000006-05.tsm", "000007-04.tsm"},
							tsdb.DefaultMaxPointsPerBlock,
						},
					},
				}
			},
		},

		// Trailing low-level files (should be excluded from high-level compaction)
		{
			name: "trailing_low_4_5_4_5_4_2_lower_block_count",
			files: []tsm1.ExtFileStat{
				{FileStat: tsm1.FileStat{Path: "000001-04.tsm", Size: 256 * 1024 * 1024}, FirstBlockCount: 200},
				{FileStat: tsm1.FileStat{Path: "000002-05.tsm", Size: 256 * 1024 * 1024}, FirstBlockCount: 200},
				{FileStat: tsm1.FileStat{Path: "000003-04.tsm", Size: 256 * 1024 * 1024}, FirstBlockCount: 200},
				{FileStat: tsm1.FileStat{Path: "000004-05.tsm", Size: 256 * 1024 * 1024}, FirstBlockCount: 200},
				{FileStat: tsm1.FileStat{Path: "000005-04.tsm", Size: 256 * 1024 * 1024}, FirstBlockCount: 200},
				{FileStat: tsm1.FileStat{Path: "000006-02.tsm", Size: 16 * 1024 * 1024}, FirstBlockCount: 200},
			},
			defaultBlockCount: tsdb.DefaultMaxPointsPerBlock,
			testShardTime:     -1,
			expectedResult: func() TestLevelResults {
				return TestLevelResults{
					level4Groups: []tsm1.PlannedCompactionGroup{
						{
							tsm1.CompactionGroup{"000001-04.tsm", "000002-05.tsm", "000003-04.tsm", "000004-05.tsm"},
							tsdb.DefaultMaxPointsPerBlock,
						},
					},
					level5Groups: []tsm1.PlannedCompactionGroup{
						{
							tsm1.CompactionGroup{"000005-04.tsm", "000006-02.tsm"},
							tsdb.DefaultMaxPointsPerBlock,
						},
					},
				}
			},
		},

		// Multiple trailing low-level files
		{
			name: "trailing_low_run_4_5_4_4_2_2_lower_block_count",
			files: []tsm1.ExtFileStat{
				{FileStat: tsm1.FileStat{Path: "000001-04.tsm", Size: 256 * 1024 * 1024}, FirstBlockCount: 200},
				{FileStat: tsm1.FileStat{Path: "000002-05.tsm", Size: 256 * 1024 * 1024}, FirstBlockCount: 200},
				{FileStat: tsm1.FileStat{Path: "000003-04.tsm", Size: 256 * 1024 * 1024}, FirstBlockCount: 200},
				{FileStat: tsm1.FileStat{Path: "000003-05.tsm", Size: 256 * 1024 * 1024}, FirstBlockCount: 200},
				{FileStat: tsm1.FileStat{Path: "000004-05.tsm", Size: 256 * 1024 * 1024}, FirstBlockCount: 200},
				{FileStat: tsm1.FileStat{Path: "000005-02.tsm", Size: 16 * 1024 * 1024}, FirstBlockCount: 200},
				{FileStat: tsm1.FileStat{Path: "000006-02.tsm", Size: 16 * 1024 * 1024}, FirstBlockCount: 300},
			},
			defaultBlockCount: tsdb.DefaultMaxPointsPerBlock,
			testShardTime:     -1,
			expectedResult: func() TestLevelResults {
				return TestLevelResults{
					level4Groups: []tsm1.PlannedCompactionGroup{
						{
							tsm1.CompactionGroup{"000001-04.tsm", "000002-05.tsm", "000003-04.tsm", "000003-05.tsm", "000004-05.tsm"},
							tsdb.DefaultMaxPointsPerBlock,
						},
					},
				}
			},
		},

		// Nested low-level files (should be included in high-level compaction)
		{
			name: "nested_4_5_2_4_5_lower_block_count",
			files: []tsm1.ExtFileStat{
				{FileStat: tsm1.FileStat{Path: "000001-04.tsm", Size: 256 * 1024 * 1024}, FirstBlockCount: 200},
				{FileStat: tsm1.FileStat{Path: "000002-05.tsm", Size: 256 * 1024 * 1024}, FirstBlockCount: 200},
				{FileStat: tsm1.FileStat{Path: "000003-02.tsm", Size: 16 * 1024 * 1024}, FirstBlockCount: 200},
				{FileStat: tsm1.FileStat{Path: "000004-04.tsm", Size: 256 * 1024 * 1024}, FirstBlockCount: 200},
				{FileStat: tsm1.FileStat{Path: "000005-05.tsm", Size: 256 * 1024 * 1024}, FirstBlockCount: 200},
			},
			defaultBlockCount: tsdb.DefaultMaxPointsPerBlock,
			testShardTime:     -1,
			expectedResult: func() TestLevelResults {
				return TestLevelResults{
					level5Groups: []tsm1.PlannedCompactionGroup{
						{
							tsm1.CompactionGroup{"000001-04.tsm", "000002-05.tsm", "000003-02.tsm", "000004-04.tsm", "000005-05.tsm"},
							tsdb.DefaultMaxPointsPerBlock,
						},
					},
				}
			},
		},

		// Multiple nested low-level files
		{
			name: "nested_4_5_2_2_4_5_lower_block_count",
			files: []tsm1.ExtFileStat{
				{FileStat: tsm1.FileStat{Path: "000001-04.tsm", Size: 256 * 1024 * 1024}, FirstBlockCount: 200},
				{FileStat: tsm1.FileStat{Path: "000002-05.tsm", Size: 256 * 1024 * 1024}, FirstBlockCount: 200},
				{FileStat: tsm1.FileStat{Path: "000003-02.tsm", Size: 16 * 1024 * 1024}, FirstBlockCount: 200},
				{FileStat: tsm1.FileStat{Path: "000004-02.tsm", Size: 16 * 1024 * 1024}, FirstBlockCount: 200},
				{FileStat: tsm1.FileStat{Path: "000005-04.tsm", Size: 256 * 1024 * 1024}, FirstBlockCount: 200},
				{FileStat: tsm1.FileStat{Path: "000006-05.tsm", Size: 256 * 1024 * 1024}, FirstBlockCount: 200},
			},
			defaultBlockCount: tsdb.DefaultMaxPointsPerBlock,
			testShardTime:     -1,
			expectedResult: func() TestLevelResults {
				return TestLevelResults{
					level4Groups: []tsm1.PlannedCompactionGroup{
						{
							tsm1.CompactionGroup{"000003-02.tsm", "000004-02.tsm", "000005-04.tsm", "000006-05.tsm"},
							tsdb.DefaultMaxPointsPerBlock,
						},
					},
					level5Groups: []tsm1.PlannedCompactionGroup{
						{
							tsm1.CompactionGroup{"000001-04.tsm", "000002-05.tsm"},
							tsdb.DefaultMaxPointsPerBlock,
						},
					},
				}
			},
		},

		// Complex nested pattern - multiple nested sections
		{
			name: "complex_nested_4_5_2_4_5_2_4_lower_block_count",
			files: []tsm1.ExtFileStat{
				{FileStat: tsm1.FileStat{Path: "000001-04.tsm", Size: 256 * 1024 * 1024}, FirstBlockCount: 200},
				{FileStat: tsm1.FileStat{Path: "000002-05.tsm", Size: 256 * 1024 * 1024}, FirstBlockCount: 200},
				{FileStat: tsm1.FileStat{Path: "000003-02.tsm", Size: 16 * 1024 * 1024}, FirstBlockCount: 200},
				{FileStat: tsm1.FileStat{Path: "000004-04.tsm", Size: 256 * 1024 * 1024}, FirstBlockCount: 200},
				{FileStat: tsm1.FileStat{Path: "000005-05.tsm", Size: 256 * 1024 * 1024}, FirstBlockCount: 200},
				{FileStat: tsm1.FileStat{Path: "000006-02.tsm", Size: 16 * 1024 * 1024}, FirstBlockCount: 200},
				{FileStat: tsm1.FileStat{Path: "000007-04.tsm", Size: 256 * 1024 * 1024}, FirstBlockCount: 200},
			},
			defaultBlockCount: tsdb.DefaultMaxPointsPerBlock,
			testShardTime:     -1,
			expectedResult: func() TestLevelResults {
				return TestLevelResults{
					level4Groups: []tsm1.PlannedCompactionGroup{
						{
							tsm1.CompactionGroup{"000003-02.tsm", "000004-04.tsm", "000005-05.tsm", "000006-02.tsm"},
							tsdb.DefaultMaxPointsPerBlock,
						},
					},
					level5Groups: []tsm1.PlannedCompactionGroup{
						{
							tsm1.CompactionGroup{"000001-04.tsm", "000002-05.tsm", "000007-04.tsm"},
							tsdb.DefaultMaxPointsPerBlock,
						},
					},
				}
			},
		},

		// Mixed: leading + nested + trailing
		{
			name: "mixed_leading_nested_trailing_2_4_5_2_4_5_2_lower_block_count",
			files: []tsm1.ExtFileStat{
				{FileStat: tsm1.FileStat{Path: "000001-02.tsm", Size: 16 * 1024 * 1024}, FirstBlockCount: 200},
				{FileStat: tsm1.FileStat{Path: "000002-04.tsm", Size: 256 * 1024 * 1024}, FirstBlockCount: 200},
				{FileStat: tsm1.FileStat{Path: "000003-05.tsm", Size: 256 * 1024 * 1024}, FirstBlockCount: 200},
				{FileStat: tsm1.FileStat{Path: "000004-02.tsm", Size: 16 * 1024 * 1024}, FirstBlockCount: 200},
				{FileStat: tsm1.FileStat{Path: "000005-04.tsm", Size: 256 * 1024 * 1024}, FirstBlockCount: 200},
				{FileStat: tsm1.FileStat{Path: "000006-05.tsm", Size: 256 * 1024 * 1024}, FirstBlockCount: 200},
				{FileStat: tsm1.FileStat{Path: "000007-02.tsm", Size: 16 * 1024 * 1024}, FirstBlockCount: 200},
			},
			defaultBlockCount: tsdb.DefaultMaxPointsPerBlock,
			testShardTime:     -1,
			expectedResult: func() TestLevelResults {
				return TestLevelResults{
					level5Groups: []tsm1.PlannedCompactionGroup{
						{
							tsm1.CompactionGroup{"000002-04.tsm", "000003-05.tsm", "000004-02.tsm", "000005-04.tsm", "000006-05.tsm", "000007-02.tsm"},
							tsdb.DefaultMaxPointsPerBlock,
						},
					},
				}
			},
		},

		// The original problem case: high-level files with rogue trailing low-level
		{
			name: "original_problem_case_01_04_through_04_04_with_05_02_lower_block_count",
			files: []tsm1.ExtFileStat{
				{FileStat: tsm1.FileStat{Path: "000001-04.tsm", Size: 256 * 1024 * 1024}, FirstBlockCount: 200},
				{FileStat: tsm1.FileStat{Path: "000002-04.tsm", Size: 256 * 1024 * 1024}, FirstBlockCount: 200},
				{FileStat: tsm1.FileStat{Path: "000003-04.tsm", Size: 256 * 1024 * 1024}, FirstBlockCount: 200},
				{FileStat: tsm1.FileStat{Path: "000004-04.tsm", Size: 256 * 1024 * 1024}, FirstBlockCount: 200},
				{FileStat: tsm1.FileStat{Path: "000005-02.tsm", Size: 16 * 1024 * 1024}, FirstBlockCount: 200},
			},
			defaultBlockCount: tsdb.DefaultMaxPointsPerBlock,
			testShardTime:     -1,
			expectedResult: func() TestLevelResults {
				return TestLevelResults{
					level4Groups: []tsm1.PlannedCompactionGroup{
						{
							tsm1.CompactionGroup{"000001-04.tsm", "000002-04.tsm", "000003-04.tsm", "000004-04.tsm"},
							tsdb.DefaultMaxPointsPerBlock,
						},
					},
				}
			},
		},
	}

	return append(existingTests, staticTests...)
}

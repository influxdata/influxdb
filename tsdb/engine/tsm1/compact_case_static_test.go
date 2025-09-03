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
		{
			name: "Mixed generations with varying file sizes",
			files: []tsm1.ExtFileStat{
				{
					FileStat: tsm1.FileStat{
						Path: "000016684-000000007.tsm",
						Size: 2147483648, // 2.1GB
					},
					FirstBlockCount: 456,
				},
				{
					FileStat: tsm1.FileStat{
						Path: "000016684-000000008.tsm",
						Size: 2147483648, // 2.1GB
					},
					FirstBlockCount: 623,
				},
				{
					FileStat: tsm1.FileStat{
						Path: "000016684-000000009.tsm",
						Size: 2147483648, // 2.1GB
					},
					FirstBlockCount: 389,
				},
				{
					FileStat: tsm1.FileStat{
						Path: "000016684-000000010.tsm",
						Size: 394264576, // 376MB
					},
					FirstBlockCount: 287,
				},
				{
					FileStat: tsm1.FileStat{
						Path: "000016812-000000004.tsm",
						Size: 2147483648, // 2.1GB
					},
					FirstBlockCount: 734,
				},
				{
					FileStat: tsm1.FileStat{
						Path: "000016812-000000005.tsm",
						Size: 1503238553, // 1.4GB
					},
					FirstBlockCount: 412,
				},
				{
					FileStat: tsm1.FileStat{
						Path: "000016844-000000002.tsm",
						Size: 1395864371, // 1.3GB
					},
					FirstBlockCount: 178,
				},
				{
					FileStat: tsm1.FileStat{
						Path: "000016948-000000004.tsm",
						Size: 2147483648, // 2.1GB
					},
					FirstBlockCount: 245,
				},
				{
					FileStat: tsm1.FileStat{
						Path: "000016948-000000005.tsm",
						Size: 1503238553, // 1.4GB
					},
					FirstBlockCount: 334,
				},
				{
					FileStat: tsm1.FileStat{
						Path: "000017076-000000004.tsm",
						Size: 2147483648, // 2.1GB
					},
					FirstBlockCount: 567,
				},
				{
					FileStat: tsm1.FileStat{
						Path: "000017094-000000004.tsm",
						Size: 2147483648, // 2.1GB
					},
					FirstBlockCount: 245,
				},
				{
					FileStat: tsm1.FileStat{
						Path: "000017095-000000005.tsm",
						Size: 1503238553, // 1.4GB
					},
					FirstBlockCount: 334,
				},
			},
			testShardTime: -1,
			expectedResult: func() TestLevelResults {
				return TestLevelResults{
					// Our rogue level 2 file should be picked up in the full compaction
					level4Groups: []tsm1.PlannedCompactionGroup{
						{
							tsm1.CompactionGroup{
								"000016844-000000002.tsm",
								"000016948-000000004.tsm",
								"000016948-000000005.tsm",
								"000017076-000000004.tsm",
								"000017094-000000004.tsm",
							},
							tsdb.DefaultMaxPointsPerBlock,
						},
					},
					// Other files should get picked up by optimize compaction
					level5Groups: []tsm1.PlannedCompactionGroup{
						{
							tsm1.CompactionGroup{
								"000016684-000000007.tsm",
								"000016684-000000008.tsm",
								"000016684-000000009.tsm",
								"000016684-000000010.tsm",
								"000016812-000000004.tsm",
								"000016812-000000005.tsm",
								"000017095-000000005.tsm",
							},
							tsdb.DefaultMaxPointsPerBlock,
						},
					},
				}
			},
		},
		{
			name: "Mixed generations with 2 level 2 files",
			files: []tsm1.ExtFileStat{
				{
					FileStat: tsm1.FileStat{
						Path: "000016684-000000007.tsm",
						Size: 2147483648, // 2.1GB
					},
					FirstBlockCount: 347,
				},
				{
					FileStat: tsm1.FileStat{
						Path: "000016684-000000008.tsm",
						Size: 2147483648, // 2.1GB
					},
					FirstBlockCount: 523,
				},
				{
					FileStat: tsm1.FileStat{
						Path: "000016684-000000009.tsm",
						Size: 2147483648, // 2.1GB
					},
					FirstBlockCount: 681,
				},
				{
					FileStat: tsm1.FileStat{
						Path: "000016684-000000010.tsm",
						Size: 394264576, // 376MB
					},
					FirstBlockCount: 156,
				},
				{
					FileStat: tsm1.FileStat{
						Path: "000016812-000000004.tsm",
						Size: 2147483648, // 2.1GB
					},
					FirstBlockCount: 254,
				},
				{
					FileStat: tsm1.FileStat{
						Path: "000016812-000000005.tsm",
						Size: 1503238553, // 1.4GB
					},
					FirstBlockCount: 243,
				},
				{
					FileStat: tsm1.FileStat{
						Path: "000016844-000000002.tsm",
						Size: 1395864371, // 1.3GB
					},
					FirstBlockCount: 389,
				},
				{
					FileStat: tsm1.FileStat{
						Path: "000016845-000000002.tsm",
						Size: 1395864371, // 1.3GB
					},
					FirstBlockCount: 127,
				},
				{
					FileStat: tsm1.FileStat{
						Path: "000016948-000000004.tsm",
						Size: 2147483648, // 2.1GB
					},
					FirstBlockCount: 412,
				},
				{
					FileStat: tsm1.FileStat{
						Path: "000016948-000000005.tsm",
						Size: 1503238553, // 1.4GB
					},
					FirstBlockCount: 468,
				},
				{
					FileStat: tsm1.FileStat{
						Path: "000017076-000000004.tsm",
						Size: 2147483648, // 2.1GB
					},
					FirstBlockCount: 756,
				},
			},
			testShardTime: -1,
			expectedResult: func() TestLevelResults {
				return TestLevelResults{
					level4Groups: []tsm1.PlannedCompactionGroup{
						{
							tsm1.CompactionGroup{
								"000016844-000000002.tsm",
								"000016845-000000002.tsm",
								"000016948-000000004.tsm",
								"000016948-000000005.tsm",
								"000017076-000000004.tsm",
							},
							tsdb.DefaultMaxPointsPerBlock,
						},
					},
					// Other files should get picked up by optimize compaction
					level5Groups: []tsm1.PlannedCompactionGroup{
						{
							tsm1.CompactionGroup{
								"000016684-000000007.tsm",
								"000016684-000000008.tsm",
								"000016684-000000009.tsm",
								"000016684-000000010.tsm",
								"000016812-000000004.tsm",
								"000016812-000000005.tsm",
							},
							tsdb.DefaultMaxPointsPerBlock,
						},
					},
				}
			},
		},
		{
			name: "Mixed generations with 3 level 2 files",
			files: []tsm1.ExtFileStat{
				{
					FileStat: tsm1.FileStat{
						Path: "000016684-000000007.tsm",
						Size: 2147483648, // 2.1GB
					},
					FirstBlockCount: 189,
				},
				{
					FileStat: tsm1.FileStat{
						Path: "000016684-000000008.tsm",
						Size: 2147483648, // 2.1GB
					},
					FirstBlockCount: 635,
				},
				{
					FileStat: tsm1.FileStat{
						Path: "000016684-000000009.tsm",
						Size: 2147483648, // 2.1GB
					},
					FirstBlockCount: 298,
				},
				{
					FileStat: tsm1.FileStat{
						Path: "000016684-000000010.tsm",
						Size: 394264576, // 376MB
					},
					FirstBlockCount: 298,
				},
				{
					FileStat: tsm1.FileStat{
						Path: "000016812-000000004.tsm",
						Size: 2147483648, // 2.1GB
					},
					FirstBlockCount: 573,
				},
				{
					FileStat: tsm1.FileStat{
						Path: "000016812-000000005.tsm",
						Size: 1503238553, // 1.4GB
					},
					FirstBlockCount: 149,
				},
				{
					FileStat: tsm1.FileStat{
						Path: "000016844-000000002.tsm",
						Size: 1395864371, // 1.3GB
					},
					FirstBlockCount: 342,
				},
				{
					FileStat: tsm1.FileStat{
						Path: "000016845-000000002.tsm",
						Size: 1395864371, // 1.3GB
					},
					FirstBlockCount: 418,
				},
				{
					FileStat: tsm1.FileStat{
						Path: "000016846-000000002.tsm",
						Size: 1395864371, // 1.3GB
					},
					FirstBlockCount: 267,
				},
				{
					FileStat: tsm1.FileStat{
						Path: "000016948-000000004.tsm",
						Size: 2147483648, // 2.1GB
					},
					FirstBlockCount: 721,
				},
				{
					FileStat: tsm1.FileStat{
						Path: "000016948-000000005.tsm",
						Size: 1503238553, // 1.4GB
					},
					FirstBlockCount: 195,
				},
				{
					FileStat: tsm1.FileStat{
						Path: "000017076-000000004.tsm",
						Size: 2147483648, // 2.1GB
					},
					FirstBlockCount: 463,
				},
			},
			testShardTime: -1,
			expectedResult: func() TestLevelResults {
				return TestLevelResults{
					level4Groups: []tsm1.PlannedCompactionGroup{
						{
							tsm1.CompactionGroup{
								"000016844-000000002.tsm",
								"000016845-000000002.tsm",
								"000016846-000000002.tsm",
								"000016948-000000004.tsm",
								"000016948-000000005.tsm",
							},
							tsdb.DefaultMaxPointsPerBlock,
						},
					},
					// Other files should get picked up by optimize compaction
					level5Groups: []tsm1.PlannedCompactionGroup{
						{
							tsm1.CompactionGroup{
								"000016684-000000007.tsm",
								"000016684-000000008.tsm",
								"000016684-000000009.tsm",
								"000016684-000000010.tsm",
								"000016812-000000004.tsm",
								"000016812-000000005.tsm",
								"000017076-000000004.tsm",
							},
							tsdb.DefaultMaxPointsPerBlock,
						},
					},
				}
			},
		},
		{
			name: "Mixed generations with 4 level 2 files",
			files: []tsm1.ExtFileStat{
				{
					FileStat: tsm1.FileStat{
						Path: "000016684-000000007.tsm",
						Size: 2147483648, // 2.1GB
					},
					FirstBlockCount: 700,
				},
				{
					FileStat: tsm1.FileStat{
						Path: "000016684-000000008.tsm",
						Size: 2147483648, // 2.1GB
					},
					FirstBlockCount: 800,
				},
				{
					FileStat: tsm1.FileStat{
						Path: "000016684-000000009.tsm",
						Size: 2147483648, // 2.1GB
					},
					FirstBlockCount: 378,
				},
				{
					FileStat: tsm1.FileStat{
						Path: "000016684-000000010.tsm",
						Size: 394264576, // 376MB
					},
					FirstBlockCount: 254,
				},
				{
					FileStat: tsm1.FileStat{
						Path: "000016812-000000004.tsm",
						Size: 2147483648, // 2.1GB
					},
					FirstBlockCount: 723,
				},
				{
					FileStat: tsm1.FileStat{
						Path: "000016812-000000005.tsm",
						Size: 1503238553, // 1.4GB
					},
					FirstBlockCount: 386,
				},
				{
					FileStat: tsm1.FileStat{
						Path: "000016844-000000002.tsm",
						Size: 1395864371, // 1.3GB
					},
					FirstBlockCount: 142,
				},
				{
					FileStat: tsm1.FileStat{
						Path: "000016845-000000002.tsm",
						Size: 1395864371, // 1.3GB
					},
					FirstBlockCount: 301,
				},
				{
					FileStat: tsm1.FileStat{
						Path: "000016846-000000002.tsm",
						Size: 1395864371, // 1.3GB
					},
					FirstBlockCount: 489,
				},
				{
					FileStat: tsm1.FileStat{
						Path: "000016847-000000002.tsm",
						Size: 1395864371, // 1.3GB
					},
					FirstBlockCount: 217,
				},
				{
					FileStat: tsm1.FileStat{
						Path: "000016948-000000004.tsm",
						Size: 2147483648, // 2.1GB
					},
					FirstBlockCount: 800,
				},
				{
					FileStat: tsm1.FileStat{
						Path: "000016948-000000005.tsm",
						Size: 1503238553, // 1.4GB
					},
					FirstBlockCount: 364,
				},
				{
					FileStat: tsm1.FileStat{
						Path: "000017076-000000004.tsm",
						Size: 2147483648, // 2.1GB
					},
					FirstBlockCount: 800,
				},
			},
			testShardTime: -1,
			expectedResult: func() TestLevelResults {
				return TestLevelResults{
					level2Groups: []tsm1.PlannedCompactionGroup{
						{
							tsm1.CompactionGroup{
								"000016844-000000002.tsm",
								"000016845-000000002.tsm",
								"000016846-000000002.tsm",
								"000016847-000000002.tsm",
							},
							tsdb.DefaultMaxPointsPerBlock,
						},
					},
					level4Groups: []tsm1.PlannedCompactionGroup{
						{
							tsm1.CompactionGroup{
								"000016684-000000007.tsm",
								"000016684-000000008.tsm",
								"000016684-000000009.tsm",
								"000016684-000000010.tsm",
								"000016812-000000004.tsm",
								"000016812-000000005.tsm",
								"000016948-000000004.tsm",
								"000016948-000000005.tsm",
								"000017076-000000004.tsm",
							},
							tsdb.DefaultMaxPointsPerBlock,
						},
					},
				}
			},
		},
		{
			name: "Mixed generations with 5 level 2 files",
			files: []tsm1.ExtFileStat{
				{
					FileStat: tsm1.FileStat{
						Path: "000016684-000000007.tsm",
						Size: 2147483648, // 2.1GB
					},
					FirstBlockCount: 156,
				},
				{
					FileStat: tsm1.FileStat{
						Path: "000016684-000000008.tsm",
						Size: 2147483648, // 2.1GB
					},
					FirstBlockCount: 693,
				},
				{
					FileStat: tsm1.FileStat{
						Path: "000016684-000000009.tsm",
						Size: 2147483648, // 2.1GB
					},
					FirstBlockCount: 425,
				},
				{
					FileStat: tsm1.FileStat{
						Path: "000016684-000000010.tsm",
						Size: 394264576, // 376MB
					},
					FirstBlockCount: 312,
				},
				{
					FileStat: tsm1.FileStat{
						Path: "000016812-000000004.tsm",
						Size: 2147483648, // 2.1GB
					},
					FirstBlockCount: 784,
				},
				{
					FileStat: tsm1.FileStat{
						Path: "000016812-000000005.tsm",
						Size: 1503238553, // 1.4GB
					},
					FirstBlockCount: 457,
				},
				{
					FileStat: tsm1.FileStat{
						Path: "000016844-000000002.tsm",
						Size: 1395864371, // 1.3GB
					},
					FirstBlockCount: 183,
				},
				{
					FileStat: tsm1.FileStat{
						Path: "000016845-000000002.tsm",
						Size: 1395864371, // 1.3GB
					},
					FirstBlockCount: 276,
				},
				{
					FileStat: tsm1.FileStat{
						Path: "000016846-000000002.tsm",
						Size: 1395864371, // 1.3GB
					},
					FirstBlockCount: 439,
				},
				{
					FileStat: tsm1.FileStat{
						Path: "000016847-000000002.tsm",
						Size: 1395864371, // 1.3GB
					},
					FirstBlockCount: 128,
				},
				{
					FileStat: tsm1.FileStat{
						Path: "000016848-000000002.tsm",
						Size: 1395864371, // 1.3GB
					},
					FirstBlockCount: 375,
				},
				{
					FileStat: tsm1.FileStat{
						Path: "000016948-000000004.tsm",
						Size: 2147483648, // 2.1GB
					},
					FirstBlockCount: 218,
				},
				{
					FileStat: tsm1.FileStat{
						Path: "000016948-000000005.tsm",
						Size: 1503238553, // 1.4GB
					},
					FirstBlockCount: 253,
				},
				{
					FileStat: tsm1.FileStat{
						Path: "000017076-000000004.tsm",
						Size: 2147483648, // 2.1GB
					},
					FirstBlockCount: 542,
				},
				{
					FileStat: tsm1.FileStat{
						Path: "000017094-000000005.tsm",
						Size: 1503238553, // 1.4GB
					},
					FirstBlockCount: 253,
				},
				{
					FileStat: tsm1.FileStat{
						Path: "000017095-000000004.tsm",
						Size: 2147483648, // 2.1GB
					},
					FirstBlockCount: 542,
				},
			},
			testShardTime: -1,
			expectedResult: func() TestLevelResults {
				return TestLevelResults{
					// First group of 4 level 2 files gets picked up for compaction
					level2Groups: []tsm1.PlannedCompactionGroup{
						{
							tsm1.CompactionGroup{
								"000016844-000000002.tsm",
								"000016845-000000002.tsm",
								"000016846-000000002.tsm",
								"000016847-000000002.tsm",
							},
							tsdb.DefaultMaxPointsPerBlock,
						},
					},
					level4Groups: []tsm1.PlannedCompactionGroup{
						{
							tsm1.CompactionGroup{
								// Lone 5th level 2 file gets picked up by full planner
								"000016848-000000002.tsm",
								"000016948-000000004.tsm",
								"000016948-000000005.tsm",
								"000017076-000000004.tsm",
								"000017094-000000005.tsm",
							},
							tsdb.DefaultMaxPointsPerBlock,
						},
					},
					level5Groups: []tsm1.PlannedCompactionGroup{
						{
							tsm1.CompactionGroup{
								"000016684-000000007.tsm",
								"000016684-000000008.tsm",
								"000016684-000000009.tsm",
								"000016684-000000010.tsm",
								"000016812-000000004.tsm",
								"000016812-000000005.tsm",
								"000017095-000000004.tsm",
							},
							tsdb.DefaultMaxPointsPerBlock,
						},
					},
				}
			},
		},
		{
			name: "First file is lower level than next files",
			files: []tsm1.ExtFileStat{
				{
					FileStat: tsm1.FileStat{
						Path: "000016090-000000002.tsm",
						Size: 1395864371, // 1.3GB
					},
					FirstBlockCount: 178,
				},
				{
					FileStat: tsm1.FileStat{
						Path: "000016684-000000007.tsm",
						Size: 2147483648, // 2.1GB
					},
					FirstBlockCount: 456,
				},
				{
					FileStat: tsm1.FileStat{
						Path: "000016684-000000008.tsm",
						Size: 2147483648, // 2.1GB
					},
					FirstBlockCount: 623,
				},
				{
					FileStat: tsm1.FileStat{
						Path: "000016684-000000009.tsm",
						Size: 2147483648, // 2.1GB
					},
					FirstBlockCount: 389,
				},
				{
					FileStat: tsm1.FileStat{
						Path: "000016684-000000010.tsm",
						Size: 394264576, // 376MB
					},
					FirstBlockCount: 287,
				},
				{
					FileStat: tsm1.FileStat{
						Path: "000016812-000000004.tsm",
						Size: 2147483648, // 2.1GB
					},
					FirstBlockCount: 734,
				},
				{
					FileStat: tsm1.FileStat{
						Path: "000016812-000000005.tsm",
						Size: 1503238553, // 1.4GB
					},
					FirstBlockCount: 412,
				},
				{
					FileStat: tsm1.FileStat{
						Path: "000016948-000000004.tsm",
						Size: 2147483648, // 2.1GB
					},
					FirstBlockCount: 245,
				},
				{
					FileStat: tsm1.FileStat{
						Path: "000016948-000000005.tsm",
						Size: 1503238553, // 1.4GB
					},
					FirstBlockCount: 334,
				},
				{
					FileStat: tsm1.FileStat{
						Path: "000017076-000000004.tsm",
						Size: 2147483648, // 2.1GB
					},
					FirstBlockCount: 567,
				},
				{
					FileStat: tsm1.FileStat{
						Path: "000017094-000000004.tsm",
						Size: 2147483648, // 2.1GB
					},
					FirstBlockCount: 245,
				},
				{
					FileStat: tsm1.FileStat{
						Path: "000017095-000000005.tsm",
						Size: 1503238553, // 1.4GB
					},
					FirstBlockCount: 334,
				},
			},
			testShardTime: -1,
			expectedResult: func() TestLevelResults {
				return TestLevelResults{
					// Our rogue level 2 file should be picked up in the full compaction
					level4Groups: []tsm1.PlannedCompactionGroup{
						{
							tsm1.CompactionGroup{
								"000016090-000000002.tsm",
								"000016684-000000007.tsm",
								"000016684-000000008.tsm",
								"000016684-000000009.tsm",
								"000016684-000000010.tsm",
								"000016812-000000004.tsm",
								"000016812-000000005.tsm",
								"000016948-000000004.tsm",
								"000016948-000000005.tsm",
							},
							tsdb.DefaultMaxPointsPerBlock,
						},
					},
					// Other files should get picked up by optimize compaction
					level5Groups: []tsm1.PlannedCompactionGroup{
						{
							tsm1.CompactionGroup{
								"000017076-000000004.tsm",
								"000017094-000000004.tsm",
								"000017095-000000005.tsm",
							},
							tsdb.DefaultMaxPointsPerBlock,
						},
					},
				}
			},
		},
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

		{
			name: "complex_nested_4_5_2_2_1_2_4_5",
			files: []tsm1.ExtFileStat{
				{FileStat: tsm1.FileStat{Path: "000001-04.tsm", Size: 256 * 1024 * 1024}, FirstBlockCount: tsdb.DefaultMaxPointsPerBlock},
				{FileStat: tsm1.FileStat{Path: "000002-05.tsm", Size: 256 * 1024 * 1024}, FirstBlockCount: tsdb.DefaultMaxPointsPerBlock},
				{FileStat: tsm1.FileStat{Path: "000003-02.tsm", Size: 16 * 1024 * 1024}, FirstBlockCount: tsdb.DefaultMaxPointsPerBlock},
				{FileStat: tsm1.FileStat{Path: "000004-02.tsm", Size: 256 * 1024 * 1024}, FirstBlockCount: tsdb.DefaultMaxPointsPerBlock},
				{FileStat: tsm1.FileStat{Path: "000005-01.tsm", Size: 256 * 1024 * 1024}, FirstBlockCount: tsdb.DefaultMaxPointsPerBlock},
				{FileStat: tsm1.FileStat{Path: "000005-02.tsm", Size: 16 * 1024 * 1024}, FirstBlockCount: tsdb.DefaultMaxPointsPerBlock},
				{FileStat: tsm1.FileStat{Path: "000007-04.tsm", Size: 256 * 1024 * 1024}, FirstBlockCount: tsdb.DefaultMaxPointsPerBlock},
				{FileStat: tsm1.FileStat{Path: "000007-05.tsm", Size: 256 * 1024 * 1024}, FirstBlockCount: tsdb.DefaultMaxPointsPerBlock},
			},
			defaultBlockCount: tsdb.DefaultMaxPointsPerBlock,
			testShardTime:     -1,
			expectedResult: func() TestLevelResults {
				return TestLevelResults{
					level4Groups: []tsm1.PlannedCompactionGroup{
						{
							tsm1.CompactionGroup{"000003-02.tsm", "000004-02.tsm", "000005-01.tsm", "000005-02.tsm", "000007-04.tsm", "000007-05.tsm"},
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

		{
			name: "complex_nested_4_5_2_2_1_2_4_5_different_gens",
			files: []tsm1.ExtFileStat{
				{FileStat: tsm1.FileStat{Path: "000001-04.tsm", Size: 256 * 1024 * 1024}, FirstBlockCount: tsdb.DefaultMaxPointsPerBlock},
				{FileStat: tsm1.FileStat{Path: "000002-05.tsm", Size: 256 * 1024 * 1024}, FirstBlockCount: tsdb.DefaultMaxPointsPerBlock},
				{FileStat: tsm1.FileStat{Path: "000003-02.tsm", Size: 16 * 1024 * 1024}, FirstBlockCount: tsdb.DefaultMaxPointsPerBlock},
				{FileStat: tsm1.FileStat{Path: "000004-02.tsm", Size: 256 * 1024 * 1024}, FirstBlockCount: tsdb.DefaultMaxPointsPerBlock},
				{FileStat: tsm1.FileStat{Path: "000005-01.tsm", Size: 256 * 1024 * 1024}, FirstBlockCount: tsdb.DefaultMaxPointsPerBlock},
				{FileStat: tsm1.FileStat{Path: "000007-02.tsm", Size: 16 * 1024 * 1024}, FirstBlockCount: tsdb.DefaultMaxPointsPerBlock},
				{FileStat: tsm1.FileStat{Path: "000008-04.tsm", Size: 256 * 1024 * 1024}, FirstBlockCount: tsdb.DefaultMaxPointsPerBlock},
				{FileStat: tsm1.FileStat{Path: "000009-05.tsm", Size: 256 * 1024 * 1024}, FirstBlockCount: tsdb.DefaultMaxPointsPerBlock},
			},
			defaultBlockCount: tsdb.DefaultMaxPointsPerBlock,
			testShardTime:     -1,
			expectedResult: func() TestLevelResults {
				return TestLevelResults{
					level2Groups: []tsm1.PlannedCompactionGroup{
						{
							tsm1.CompactionGroup{"000003-02.tsm", "000004-02.tsm", "000005-01.tsm", "000007-02.tsm"},
							tsdb.DefaultMaxPointsPerBlock,
						},
					},
					level4Groups: []tsm1.PlannedCompactionGroup{
						{
							tsm1.CompactionGroup{"000001-04.tsm", "000002-05.tsm", "000008-04.tsm", "000009-05.tsm"},
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

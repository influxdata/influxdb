package tsm1_test

import (
	"time"

	"github.com/influxdata/influxdb/v2/tsdb/engine/tsm1"
)

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

package tsm1_test

import (
	"github.com/influxdata/influxdb/tsdb/engine/tsm1"
	"time"
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

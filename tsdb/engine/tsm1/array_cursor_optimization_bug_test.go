package tsm1

import (
	"context"
	"os"
	"testing"

	"github.com/influxdata/influxdb/tsdb"
)

// TestAscendingCursorDuplicateDataBug demonstrates the real bug that existed
// in the ascending cursor optimization. The bug is encountered when cache data is merged with tsm data, specifically
// in an earlier call to Next(), the cache must fill part but not all of the response buffer, which is 1000 points. TSM
// data then fills the rest. The cache must not be exhausted. The following Next() sees that only TSM data is needed,
// and incorrectly copies TSM data including the points from the previous Next() call. Additionally, cache data
// needs to be older than most of the TSM data. There needs to be enough tsm data younger than the cache data to completely
// fill the remaining space in the response buffer.
func TestAscendingCursorDuplicateDataBug(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)
	fs := NewFileStore(dir)

	tsmMinTime := 10
	tsmMaxTime := 70000
	// Create TSM data
	tsmData := []keyValues{
		{"measurement,field=value#!~#value", []Value{
			NewFloatValue(int64(tsmMinTime), 1.1), // tsm points can be older than the cache points
			// these next 5 points are the critical tsm points. They will be used after the cache is exhausted
			// as they are younger than all the cache points.
			NewFloatValue(10000, 10.1),
			NewFloatValue(20000, 20.2),
			NewFloatValue(30000, 30.3),
			NewFloatValue(40000, 40.4),
			NewFloatValue(50000, 50.5),
			// there can be more tsm values - additional values here don't impact the result
			NewFloatValue(60000, 60.5),
			NewFloatValue(int64(tsmMaxTime), 70.5),
		}},
	}

	files, err := newFiles(dir, tsmData...)
	if err != nil {
		t.Fatalf("unexpected error creating files: %v", err)
	}

	err = fs.Replace(nil, files)
	if err != nil {
		t.Fatalf("unexpected error updating filestore: %v", err)
	}

	// Create cache data that triggers the bug scenario:
	// Cache fills most of the buffer, TSM provides remaining data
	var cacheValues Values
	bufferSize := tsdb.DefaultMaxPointsPerBlock

	// Cache: 100 to 100+bufferSize-5 (leaving room for some TSM data)
	// The cache data timestamps are older than the TSM data timestamps
	for i := 0; i < bufferSize-5; i++ {
		cacheValues = append(cacheValues, NewFloatValue(int64(100+i), float64(100+i)))
	}

	// Verify precondition: TSM and cache timestamps must not overlap
	tsmTimestamps := make(map[int64]bool)
	for _, v := range tsmData[0].values {
		tsmTimestamps[v.UnixNano()] = true
	}

	for _, cv := range cacheValues {
		if tsmTimestamps[cv.UnixNano()] {
			t.Fatalf("Test precondition failed: TSM and cache data share timestamp %d. This will interfere with test results.", cv.UnixNano())
		}
	}
	// Verify the cache fills most but not all of the buffer
	if len(cacheValues) >= bufferSize {
		t.Fatalf("Test precondition failed: Cache must not fill entire buffer (size %d), has %d values", bufferSize, len(cacheValues))
	}

	// Verify cache has sufficient data to trigger the bug
	minCacheSize := bufferSize - 10 // Leave room for at least some TSM data
	if len(cacheValues) < minCacheSize {
		t.Fatalf("Test precondition failed: Cache must have at least %d values to trigger bug, has %d", minCacheSize, len(cacheValues))
	}

	// Verify TSM has data both before and after cache range
	var tsmBeforeCache, tsmAfterCache int
	cacheMin, cacheMax := cacheValues[0].UnixNano(), cacheValues[len(cacheValues)-1].UnixNano()
	for _, v := range tsmData[0].values {
		if v.UnixNano() < cacheMin {
			tsmBeforeCache++
		} else if v.UnixNano() > cacheMax {
			tsmAfterCache++
		}
	}
	if tsmAfterCache < 2 {
		t.Fatalf("Test precondition failed: Need at least 2 TSM points after cache range to trigger bug, has %d", tsmAfterCache)
	}

	kc := fs.KeyCursor(context.Background(), []byte("measurement,field=value#!~#value"), 0, true)
	defer kc.Close()

	cursor := newFloatArrayAscendingCursor()
	defer cursor.Close()

	// Find min and max timestamps across both TSM and cache data
	minTime := int64(tsmMinTime) // Start with first TSM timestamp
	maxTime := int64(tsmMaxTime) // Start with last TSM timestamp

	// Check if cache has earlier timestamps
	if len(cacheValues) > 0 && cacheValues[0].UnixNano() < minTime {
		minTime = cacheValues[0].UnixNano()
	}

	// Check if cache has later timestamps
	if len(cacheValues) > 0 && cacheValues[len(cacheValues)-1].UnixNano() > maxTime {
		maxTime = cacheValues[len(cacheValues)-1].UnixNano()
	}

	// Add buffer to ensure we capture all data
	maxTime += 1000

	// search over the whole time range, ascending
	err = cursor.reset(minTime, maxTime, cacheValues, kc)
	if err != nil {
		t.Fatalf("unexpected error resetting cursor: %v", err)
	}

	// Collect all timestamps and values from all Next() calls
	var allTimestamps []int64
	var allValues []float64
	seenTimestamps := make(map[int64]bool)
	callNum := 0

	for {
		result := cursor.Next() // run to exhaustion of the cursor
		if len(result.Timestamps) == 0 {
			break
		}
		callNum++

		// Verify timestamp/value counts match
		if len(result.Timestamps) != len(result.Values) {
			t.Errorf("Call %d: Timestamp/value count mismatch: %d timestamps, %d values",
				callNum, len(result.Timestamps), len(result.Values))
		}

		for i, ts := range result.Timestamps {
			if seenTimestamps[ts] {
				t.Errorf("DUPLICATE DATA BUG at call %d, position %d: Timestamp %d returned multiple times",
					callNum, i, ts)
				// Print first few timestamps from this batch for debugging
				if len(result.Timestamps) > 0 {
					endIdx := 10
					if endIdx > len(result.Timestamps) {
						endIdx = len(result.Timestamps)
					}
					t.Logf("First %d timestamps from call %d: %v", endIdx, callNum, result.Timestamps[:endIdx])
				}
			}
			seenTimestamps[ts] = true
			allTimestamps = append(allTimestamps, ts)
		}

		// Collect values for verification
		allValues = append(allValues, result.Values...)
	}

	// Verify no duplicates were found
	if len(allTimestamps) != len(seenTimestamps) {
		t.Errorf("Found duplicate timestamps! Total: %d, Unique: %d", len(allTimestamps), len(seenTimestamps))
	}

	// Verify we got all expected data (cache + TSM)
	expectedCount := len(cacheValues) + len(tsmData[0].values)
	if len(allTimestamps) != expectedCount {
		t.Errorf("Expected %d total timestamps, got %d", expectedCount, len(allTimestamps))
	}

	// Verify data is in ascending order
	for i := 1; i < len(allTimestamps); i++ {
		if allTimestamps[i] <= allTimestamps[i-1] {
			t.Errorf("Timestamps not in ascending order: %d followed by %d", allTimestamps[i-1], allTimestamps[i])
		}
	}
}

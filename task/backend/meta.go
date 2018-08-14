package backend

import (
	"bytes"

	"github.com/influxdata/platform"
)

// This file contains helper methods for the StoreTaskMeta type defined in protobuf.

// FinishRun removes the run matching runID from m's CurrentlyRunning slice,
// and if that run's Now value is greater than m's LastCompleted value,
// updates the value of LastCompleted to the run's Now value.
//
// If runID matched a run, FinishRun returns true. Otherwise it returns false.
func (stm *StoreTaskMeta) FinishRun(runID platform.ID) bool {
	for i, runner := range stm.CurrentlyRunning {
		if bytes.Equal(runner.RunID, runID) {
			stm.CurrentlyRunning = append(stm.CurrentlyRunning[:i], stm.CurrentlyRunning[i+1:]...)
			if runner.Now > stm.LastCompleted {
				stm.LastCompleted = runner.Now
				return true
			}
		}
	}
	return false
}

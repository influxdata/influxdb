package monitor

import (
	"os"
	"time"

	"github.com/influxdata/influxdb/monitor/diagnostics"
)

var startTime time.Time

func init() {
	startTime = time.Now().UTC()
}

// system captures system-level diagnostics.
type system struct{}

func (s *system) Diagnostics() (*diagnostics.Diagnostics, error) {
	d := map[string]interface{}{
		"PID":         os.Getpid(),
		"currentTime": time.Now().UTC(),
		"started":     startTime,
		"uptime":      time.Since(startTime).String(),
	}

	return diagnostics.RowFromMap(d), nil
}

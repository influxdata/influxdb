package monitor

import (
	"os"
	"time"
)

var startTime time.Time

func init() {
	startTime = time.Now().UTC()
}

// system captures network statistics and implements the monitor client interface
type system struct{}

// Statistics returns the statistics for the system type
func (s *system) Statistics() (map[string]interface{}, error) {
	return nil, nil
}

func (s *system) Diagnostics() ([]string, [][]interface{}, error) {
	diagnostics := map[string]interface{}{
		"PID":         os.Getpid(),
		"currentTime": time.Now().UTC(),
		"started":     startTime,
		"uptime":      time.Since(startTime).String(),
	}

	a, b := diagnosticsFromMap(diagnostics)
	return a, [][]interface{}{b}, nil
}

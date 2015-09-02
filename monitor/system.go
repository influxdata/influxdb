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
		"currentTime": time.Now(),
		"started":     startTime.String(),
		"uptime":      time.Since(startTime).String(),
	}

	a := make([]string, 0, len(diagnostics))
	b := []interface{}{}
	for k, v := range diagnostics {
		a = append(a, k)
		b = append(b, v)
	}
	return a, [][]interface{}{b}, nil
}

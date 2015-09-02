package monitor

import (
	"runtime"
)

// goRuntime captures Go runtime statistics and implements the monitor client interface
type goRuntime struct{}

// Statistics returns the statistics for the goRuntime type
func (g *goRuntime) Statistics() (map[string]interface{}, error) {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	return map[string]interface{}{
		"Alloc":        int64(m.Alloc),
		"TotalAlloc":   int64(m.TotalAlloc),
		"Sys":          int64(m.Sys),
		"Lookups":      int64(m.Lookups),
		"Mallocs":      int64(m.Mallocs),
		"Frees":        int64(m.Frees),
		"HeapAlloc":    int64(m.HeapAlloc),
		"HeapSys":      int64(m.HeapSys),
		"HeapIdle":     int64(m.HeapIdle),
		"HeapInUse":    int64(m.HeapInuse),
		"HeapReleased": int64(m.HeapReleased),
		"HeapObjects":  int64(m.HeapObjects),
		"PauseTotalNs": int64(m.PauseTotalNs),
		"NumGC":        int64(m.NumGC),
		"NumGoroutine": int64(runtime.NumGoroutine()),
	}, nil
}

// Diagnostics returns the statistics for the goRuntime type
func (g *goRuntime) Diagnostics() (map[string]interface{}, error) {
	return nil, nil
}

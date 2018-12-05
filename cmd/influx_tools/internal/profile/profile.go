package profile

import (
	"log"
	"os"
	"runtime"
	"runtime/pprof"
)

type Config struct {
	// CPU, if set, specifies the file name of the CPU profile to capture
	CPU string

	// Memory, if set, specifies the file name of the CPU profile to capture
	Memory string
}

func (c *Config) noProfiles() bool {
	return c.CPU == "" && c.Memory == ""
}

// Start starts a CPU and / or Memory profile if configured and returns a
// function that should be called to terminate the profiles.
func (c *Config) Start() func() {
	if c.noProfiles() {
		return func() { return }
	}

	var prof struct {
		cpu *os.File
		mem *os.File
	}

	if c.CPU != "" {
		f, err := os.Create(c.CPU)
		if err != nil {
			log.Fatalf("cpuprofile: %v", err)
		}
		prof.cpu = f
		_ = pprof.StartCPUProfile(prof.cpu)
	}

	if c.Memory != "" {
		f, err := os.Create(c.Memory)
		if err != nil {
			log.Fatalf("memprofile: %v", err)
		}
		prof.mem = f
		runtime.MemProfileRate = 4096
	}

	return func() {
		if prof.cpu != nil {
			pprof.StopCPUProfile()
			_ = prof.cpu.Close()
			prof.cpu = nil
		}
		if prof.mem != nil {
			_ = pprof.Lookup("heap").WriteTo(prof.mem, 0)
			_ = prof.mem.Close()
			prof.mem = nil
		}
	}
}

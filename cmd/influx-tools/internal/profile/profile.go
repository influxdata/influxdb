package profile

import (
	"fmt"
	"io"
	"os"
	"runtime"
	"runtime/pprof"
)

type Profiler struct {
	cpuProfile string
	memProfile string
	stderr     io.Writer
	cpu        *os.File
	mem        *os.File
}

func NewProfiler(cpuProfile string, memProfile string, Stderr io.Writer) *Profiler {
	return &Profiler{cpuProfile: cpuProfile, memProfile: memProfile, stderr: Stderr}
}

// StartProfile initializes the cpu and memory profile, if specified.
func (p *Profiler) StartProfile() {
	if p.cpuProfile != "" {
		f, err := os.Create(p.cpuProfile)
		if err != nil {
			fmt.Fprintf(p.stderr, "cpuprofile: %v\n", err)
			os.Exit(1)
		}
		p.cpu = f
		pprof.StartCPUProfile(p.cpu)
	}

	if p.memProfile != "" {
		f, err := os.Create(p.memProfile)
		if err != nil {
			fmt.Fprintf(p.stderr, "memprofile: %v\n", err)
			os.Exit(1)
		}
		p.mem = f
		runtime.MemProfileRate = 4096
	}

}

// StopProfile closes the cpu and memory profiles if they are running.
func (p *Profiler) StopProfile() {
	if p.cpu != nil {
		pprof.StopCPUProfile()
		p.cpu.Close()
	}
	if p.mem != nil {
		pprof.Lookup("heap").WriteTo(p.mem, 0)
		p.mem.Close()
	}
}

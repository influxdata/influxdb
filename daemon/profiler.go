// build this file if the profile tag is specified and we're running
// on linux

// +build linux,profile

package main

// #include "google/heap-profiler.h"
// #include "google/profiler.h"
import "C"

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"runtime"
	"runtime/pprof"
	"syscall"
	"time"

	log "code.google.com/p/log4go"
)

var profileFilename *string

func init() {
	profileFilename = flag.String("profile", "", "filename prefix where cpu and memory profile data will be written")
}

func waitForSignals(stoppable Stoppable, filename string, stopped <-chan bool) {
	ch := make(chan os.Signal)
	signal.Notify(ch, syscall.SIGTERM, syscall.SIGINT)
outer:
	for {
		sig := <-ch
		log.Info("Received signal: %s", sig.String())
		switch sig {
		case syscall.SIGINT, syscall.SIGTERM:
			runtime.SetCPUProfileRate(0)
			f, err := os.OpenFile(fmt.Sprintf("%s.mem", filename), os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0600)
			if err != nil {
				log.Error("Cannot open memory profile: %s", err)
				break outer
			}
			if err := pprof.WriteHeapProfile(f); err != nil {
				log.Error("Cannot write memory profile: %s", err)
			}
			f.Close()
			stopCHeapProfiler()
			// stopCCpuProfiler()
			stoppable.Stop()
			break outer
			// make sure everything stopped before exiting
		}
	}
	// wait for all logging messages to be printed
	<-stopped
	time.Sleep(5 * time.Second)
	os.Exit(0)
}

func startProfiler(stoppable Stoppable) error {
	if profileFilename == nil || *profileFilename == "" {
		log.Info("Not starting profiling since the profile prefix is not set")
		return nil
	}

	log.Info("Starting profiling with prefix %s", *profileFilename)

	startCHeapProfiler(*profileFilename)
	// startCCpuProfiler(*profileFilename)
	runtime.MemProfileRate = 1024

	cpuProfileFile, err := os.Create(fmt.Sprintf("%s.cpu", *profileFilename))
	if err != nil {
		return err
	}
	runtime.SetCPUProfileRate(500)
	stopped := make(chan bool)

	go waitForSignals(stoppable, *profileFilename, stopped)

	go func() {
		for {
			select {
			default:
				data := runtime.CPUProfile()
				if data == nil {
					cpuProfileFile.Close()
					stopped <- true
					break
				}
				cpuProfileFile.Write(data)
			}
		}
	}()
	return nil
}

func startCCpuProfiler(prefix string) {
	log.Info("Starting native cpu profiling")
	_prefix := C.CString(fmt.Sprintf("%s.native.cpu", prefix))
	C.ProfilerStart(_prefix)
}

func stopCCpuProfiler() {
	log.Info("Stopping native cpu profiling")
	C.ProfilerStop()
}

func startCHeapProfiler(prefix string) {
	log.Info("Starting tcmalloc profiling")
	_prefix := C.CString(prefix)
	C.HeapProfilerStart(_prefix)
}

func stopCHeapProfiler() {
	log.Info("Stopping tcmalloc profiling")
	C.HeapProfilerDump(nil)
	C.HeapProfilerStop()
}

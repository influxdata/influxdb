// build this file if the profile tag is specified and we're running
// on linux

// +build linux
// +build profile

package main

// #include "google/heap-profiler.h"
import "C"

import (
	log "code.google.com/p/log4go"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"runtime"
	"runtime/pprof"
	"syscall"
	"time"
)

var profileFilename *string

func init() {
	profileFilename = flag.String("profile", "", "filename prefix where cpu and memory profile data will be written")
}

func waitForSignals(filename string, stopped <-chan bool) {
	ch := make(chan os.Signal)
	signal.Notify(ch, syscall.SIGTERM, syscall.SIGINT)
outer:
	for {
		sig := <-ch
		log.Info("Received signal: %s\n", sig.String())
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
			break outer
			// make sure everything stopped before exiting
		}
	}
	// wait for all logging messages to be printed
	time.Sleep(time.Second)
	<-stopped
	os.Exit(0)
}

func startProfiler() error {
	if profileFilename == nil || *profileFilename == "" {
		return nil
	}

	startCHeapProfiler(*profileFilename)
	runtime.MemProfileRate = 1024

	cpuProfileFile, err := os.Create(fmt.Sprintf("%s.cpu", *profileFilename))
	if err != nil {
		return err
	}
	runtime.SetCPUProfileRate(500)
	stopped := make(chan bool)

	go waitForSignals(*profileFilename, stopped)

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

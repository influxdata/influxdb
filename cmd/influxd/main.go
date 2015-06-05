package main

import (
	"fmt"
	"io"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"runtime"
	"runtime/pprof"
	"strings"
	"time"

	"github.com/influxdb/influxdb/cmd/influxd/help"
	"github.com/influxdb/influxdb/cmd/influxd/run"
	"github.com/influxdb/influxdb/cmd/influxd/version"
)

func main() {
	rand.Seed(time.Now().UnixNano())

	m := NewMain()
	if err := m.Run(os.Args[1:]...); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

// Main represents the program execution.
type Main struct {
	Stdin  io.Reader
	Stdout io.Writer
	Stderr io.Writer
}

// NewMain return a new instance of Main.
func NewMain() *Main {
	return &Main{
		Stdin:  os.Stdin,
		Stdout: os.Stdout,
		Stderr: os.Stderr,
	}
}

// Run determines and runs the command specified by the CLI args.
func (m *Main) Run(args ...string) error {
	name, args := ParseCommandName(args)

	// FIXME(benbjohnson): Parse profiling args & start profiling.

	// Extract name from args.
	switch name {
	case "", "run":
		if err := run.NewCommand().Run(args...); err != nil {
			return fmt.Errorf("run: %s", err)
		}

		// Wait indefinitely.
		<-(chan struct{})(nil)

	// case "backup":
	// 	name := NewBackupCommand()
	// 	if err := name.Run(args...); err != nil {
	// 		return fmt.Errorf("backup: %s", err)
	// 	}
	// case "restore":
	// 	name := NewRestoreCommand()
	// 	if err := name.Run(args...); err != nil {
	// 		return fmt.Errorf("restore: %s", err)
	// 	}
	case "config":
		if err := run.NewPrintConfigCommand().Run(args...); err != nil {
			return fmt.Errorf("config: %s", err)
		}
	case "version":
		if err := version.NewCommand().Run(args...); err != nil {
			return fmt.Errorf("version: %s", err)
		}
	case "help":
		if err := help.NewCommand().Run(args...); err != nil {
			return fmt.Errorf("help: %s", err)
		}
	default:
		return fmt.Errorf(`unknown command "%s"`+"\n"+`Run 'influxd help' for usage`+"\n\n", name)
	}

	return nil
}

// ParseCommandName extracts the command name and args from the args list.
func ParseCommandName(args []string) (string, []string) {
	// Retrieve command name as first argument.
	var name string
	if len(args) > 0 && !strings.HasPrefix(args[0], "-") {
		name = args[0]
	}

	// Special case -h immediately following binary name
	if len(args) > 0 && args[0] == "-h" {
		name = "help"
	}

	// If command is "help" and has an argument then rewrite args to use "-h".
	if name == "help" && len(args) > 1 {
		args[0], args[1] = args[1], "-h"
		name = args[0]
	}

	// If a named command is specified then return it with its arguments.
	if name != "" {
		return name, args[1:]
	}
	return "", args
}

// prof stores the file locations of active profiles.
var prof struct {
	cpu *os.File
	mem *os.File
}

// StartProfile initializes the cpu and memory profile, if specified.
func StartProfile(cpuprofile, memprofile string) {
	if cpuprofile != "" {
		f, err := os.Create(cpuprofile)
		if err != nil {
			log.Fatalf("cpuprofile: %v", err)
		}
		prof.cpu = f
		pprof.StartCPUProfile(prof.cpu)
	}

	if memprofile != "" {
		f, err := os.Create(memprofile)
		if err != nil {
			log.Fatalf("memprofile: %v", err)
		}
		prof.mem = f
		runtime.MemProfileRate = 4096
	}

	go func() {
		c := make(chan os.Signal, 1)
		signal.Notify(c, os.Interrupt)
		<-c
		StopProfile()
		os.Exit(0)
	}()
}

// StopProfile closes the cpu and memory profiles if they are running.
func StopProfile() {
	if prof.cpu != nil {
		pprof.StopCPUProfile()
		prof.cpu.Close()
	}
	if prof.mem != nil {
		pprof.Lookup("heap").WriteTo(prof.mem, 0)
		prof.mem.Close()
	}
}

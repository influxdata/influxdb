package main

import (
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"runtime"
	"runtime/pprof"
	"strings"
	"time"

	"github.com/influxdb/influxdb/cmd/influxd/run"
)

// These variables are populated via the Go linker.
var (
	version string = "0.9"
	commit  string
)

// Various constants used by the main package.
const (
	messagingClientFile       string = "messaging"
	monitoringDatabase        string = "_influxdb"
	monitoringRetentionPolicy string = "default"
)

func main() {
	log.SetFlags(0)
	log.SetPrefix(`[srvr] `)
	log.SetFlags(log.LstdFlags)
	rand.Seed(time.Now().UnixNano())

	// If commit not set, make that clear.
	if commit == "" {
		commit = "unknown"
	}

	// Shift binary name off argument list.
	args := os.Args[1:]

	// Retrieve command name as first argument.
	var cmd string
	if len(args) > 0 && !strings.HasPrefix(args[0], "-") {
		cmd = args[0]
	}

	// Special case -h immediately following binary name
	if len(args) > 0 && args[0] == "-h" {
		cmd = "help"
	}

	// If command is "help" and has an argument then rewrite args to use "-h".
	if cmd == "help" && len(args) > 1 {
		args[0], args[1] = args[1], "-h"
		cmd = args[0]
	}

	// FIXME(benbjohnson): Parse profiling args & start profiling.

	// Extract name from args.
	switch cmd {
	case "":
		if err := run.NewCommand().Run(args...); err != nil {
			log.Fatalf("run: %s", err)
		}
	case "run":
		if err := run.NewCommand().Run(args[1:]...); err != nil {
			log.Fatalf("run: %s", err)
		}
	// case "backup":
	// 	cmd := NewBackupCommand()
	// 	if err := cmd.Run(args[1:]...); err != nil {
	// 		log.Fatalf("backup: %s", err)
	// 	}
	// case "restore":
	// 	cmd := NewRestoreCommand()
	// 	if err := cmd.Run(args[1:]...); err != nil {
	// 		log.Fatalf("restore: %s", err)
	// 	}
	case "version":
		execVersion(args[1:])
	case "config":
		if err := run.NewPrintConfigCommand().Run(args[1:]...); err != nil {
			log.Fatalf("config: %s", err)
		}
	// case "help":
	// 	if err := help.NewCommand().Run(args[1:]...); err != nil {
	// 		log.Fatalf("help: %s", err)
	// 	}
	default:
		log.Fatalf(`influxd: unknown command "%s"`+"\n"+`Run 'influxd help' for usage`+"\n\n", cmd)
	}
}

// execVersion runs the "version" command.
// Prints the commit SHA1 if set by the build process.
func execVersion(args []string) {
	fs := flag.NewFlagSet("", flag.ExitOnError)
	fs.Usage = func() {
		fmt.Println(`usage: version

	version displays the InfluxDB version and build git commit hash
	`)
	}
	fs.Parse(args)

	s := fmt.Sprintf("InfluxDB v%s", version)
	if commit != "" {
		s += fmt.Sprintf(" (git: %s)", commit)
	}
	log.Print(s)
}

type Stopper interface {
	Stop()
}

type State struct {
	Mode string `json:"mode"`
}

var prof struct {
	cpu *os.File
	mem *os.File
}

func startProfiling(cpuprofile, memprofile string) {
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
		stopProfiling()
		os.Exit(0)
	}()
}

func stopProfiling() {
	if prof.cpu != nil {
		pprof.StopCPUProfile()
		prof.cpu.Close()
	}
	if prof.mem != nil {
		pprof.Lookup("heap").WriteTo(prof.mem, 0)
		prof.mem.Close()
	}
}

func warn(v ...interface{})              { fmt.Fprintln(os.Stderr, v...) }
func warnf(msg string, v ...interface{}) { fmt.Fprintf(os.Stderr, msg+"\n", v...) }

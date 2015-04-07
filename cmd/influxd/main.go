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
)

const logo = `
 8888888           .d888 888                   8888888b.  888888b.
   888            d88P"  888                   888  "Y88b 888  "88b
   888            888    888                   888    888 888  .88P
   888   88888b.  888888 888 888  888 888  888 888    888 8888888K.
   888   888 "88b 888    888 888  888  Y8bd8P' 888    888 888  "Y88b
   888   888  888 888    888 888  888   X88K   888    888 888    888
   888   888  888 888    888 Y88b 888 .d8""8b. 888  .d88P 888   d88P
 8888888 888  888 888    888  "Y88888 888  888 8888888P"  8888888P"

`

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

	// Extract name from args.
	switch cmd {
	case "run":
		cmd := NewRunCommand()
		if err := cmd.Run(args[1:]...); err != nil {
			log.Fatalf("run: %s", err)
		}
	case "":
		cmd := NewRunCommand()
		if err := cmd.Run(args...); err != nil {
			log.Fatalf("run: %s", err)
		}
	case "backup":
		cmd := NewBackupCommand()
		if err := cmd.Run(args[1:]...); err != nil {
			log.Fatalf("backup: %s", err)
		}
	case "restore":
		cmd := NewRestoreCommand()
		if err := cmd.Run(args[1:]...); err != nil {
			log.Fatalf("restore: %s", err)
		}
	case "version":
		execVersion(args[1:])
	case "config":
		execConfig(args[1:])
	case "help":
		cmd := NewHelpCommand()
		if err := cmd.Run(args[1:]...); err != nil {
			log.Fatalf("help: %s", err)
		}
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

// execConfig parses and prints the current config loaded.
func execConfig(args []string) {
	// Parse command flags.
	fs := flag.NewFlagSet("", flag.ExitOnError)
	fs.Usage = func() {
		fmt.Println(`usage: config

	config displays the default configuration
						    `)
	}

	var (
		configPath = fs.String("config", "", "")
		hostname   = fs.String("hostname", "", "")
	)
	fs.Parse(args)

	config, err := parseConfig(*configPath, *hostname)
	if err != nil {
		log.Fatalf("parse config: %s", err)
	}

	config.Write(os.Stdout)
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

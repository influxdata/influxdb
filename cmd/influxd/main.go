package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"runtime"
	"runtime/pprof"
	"strings"
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
	messagingClientFile string = "messaging"
)

func main() {
	log.SetFlags(0)

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
		execRun(args[1:])
	case "":
		execRun(args)
	case "version":
		execVersion(args[1:])
	case "config":
		execConfig(args[1:])
	case "help":
		execHelp(args[1:])
	default:
		log.Fatalf(`influxd: unknown command "%s"`+"\n"+`Run 'influxd help' for usage`+"\n\n", cmd)
	}
}

// execRun runs the "run" command.
func execRun(args []string) {
	// Parse command flags.
	fs := flag.NewFlagSet("", flag.ExitOnError)
	var (
		configPath = fs.String("config", "", "")
		pidPath    = fs.String("pidfile", "", "")
		hostname   = fs.String("hostname", "", "")
		join       = fs.String("join", "", "")
		cpuprofile = fs.String("cpuprofile", "", "")
		memprofile = fs.String("memprofile", "", "")
	)
	fs.Usage = printRunUsage
	fs.Parse(args)

	// Start profiling, if set.
	startProfiling(*cpuprofile, *memprofile)
	defer stopProfiling()

	// Print sweet InfluxDB logo and write the process id to file.
	log.Print(logo)
	log.SetPrefix(`[srvr] `)
	log.SetFlags(log.LstdFlags)
	writePIDFile(*pidPath)

	if *configPath == "" {
		log.Println("No config provided, using default settings")
	}
	config := parseConfig(*configPath, *hostname)

	// Create a logging writer.
	logWriter := os.Stderr
	if config.Logging.File != "" {
		var err error
		logWriter, err = os.OpenFile(config.Logging.File, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0660)
		if err != nil {
			log.Fatalf("unable to open log file %s: %s", config.Logging.File, err.Error())
		}
	}
	log.SetOutput(logWriter)

	Run(config, *join, version, logWriter)

	// Wait indefinitely.
	<-(chan struct{})(nil)
}

// execVersion runs the "version" command.
// Prints the commit SHA1 if set by the build process.
func execVersion(args []string) {
	fs := flag.NewFlagSet("", flag.ExitOnError)
	fs.Usage = func() {
		log.Println(`usage: version

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
	var (
		configPath = fs.String("config", "", "")
		hostname   = fs.String("hostname", "", "")
	)
	fs.Parse(args)

	config := parseConfig(*configPath, *hostname)

	config.Write(os.Stdout)
}

// execHelp runs the "help" command.
func execHelp(args []string) {
	fmt.Println(`
Configure and start an InfluxDB server.

Usage:

	influxd [[command] [arguments]]

The commands are:

    join-cluster         create a new node that will join an existing cluster
    run                  run node with existing configuration
    version              displays the InfluxDB version

"run" is the default command.

Use "influxd help [command]" for more information about a command.
`)
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

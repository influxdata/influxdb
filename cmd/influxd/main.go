package main

import (
	"flag"
	"fmt"
	"log"
	"os"
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
	case "help":
		execHelp(args[1:])
	default:
		log.Fatalf(`influxd: unknown command "%s"`+"\n"+`Run 'influxd help' for usage`+"\n\n", cmd)
	}
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
